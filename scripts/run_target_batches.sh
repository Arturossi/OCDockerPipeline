#!/usr/bin/env bash
# Batch launcher for OCDockerPipeline/Snakemake.
#
# What this script does:
# - Splits the global `rule all` target set into Snakemake native `--batch all=i/N` partitions.
# - Runs one or many batch indices sequentially.
# - Propagates user Snakemake args after `--`.
# - Forces `pipeline_export_database_csv=false` during batch runs so each partition stays local.
#
# Why this wrapper exists:
# - Large datasets are easier to recover/restart when processed in bounded chunks.
# - Batch-level retries are much cheaper than re-running a giant monolithic execution.
# - It standardizes runtime temp/cache paths and optional conda invocation.
#
# Important behavior:
# - This script does NOT merge many batches into one Snakemake workflow UUID.
# - It intentionally launches one Snakemake invocation per batch index.
# - Existing completed outputs are reused by Snakemake unless rerun flags request otherwise.
set -euo pipefail
# shellcheck disable=SC2034
# SC2034 note: some variables are intentionally initialized and conditionally used.

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd -- "${script_dir}/.." && pwd -P)"

usage() {
    cat <<'USAGE'
Run OCDockerPipeline in batches via native Snakemake --batch on rule `all`.

Usage:
  scripts/run_target_batches.sh [batch mode options] [runner options] -- [snakemake args...]

Batch mode (pick exactly one):
  --total-batches N         Fractional mode (runs all=1/N, 2/N, ...)
  --batch-size N            Approximate fixed-size mode (derives total batches)

Batch range:
  --from I                  First batch index to run (1-based, default: 1)
  --to J                    Last batch index to run (inclusive)
                            Fractional mode default: N
                            Fixed-size mode default: --from value
  --all                     In fixed-size mode, run from --from through the
                            derived final batch index.

Fixed-size helpers:
  --total-targets N         Total targets to use for --batch-size conversion
                            (if omitted, inferred from .snakemake/target_discovery_cache.json)

Runner options:
  --snakefile PATH          Snakefile path (default: snakefile)
  --snakemake-bin BIN       Snakemake executable (default: snakemake)
  --conda-env NAME          Run as: conda run -n NAME snakemake
  --cache-root PATH         XDG cache root used for each run (default: /tmp)
  --tmp-root PATH           TMPDIR used for each run (default: /tmp)
  --snakemake-retries N     Default Snakemake --retries value if not passed
                            after `--` (default: 3)
  --continue-on-failed-batch[=BOOL]
                            Continue after a failed batch (default: true)
                            BOOL accepts: true/false, 1/0, yes/no
  --stop-on-failed-batch    Shortcut for --continue-on-failed-batch=false
  --dry-run                 Add -n to each run
  -h, --help                Show this help

Examples:
  # Tiny test: one small batch (approximately 5 targets)
  scripts/run_target_batches.sh --batch-size 5 --from 1 --to 1 -- --cores 8 --resources mem_mb=12000

  # Full run over 20 fractions
  scripts/run_target_batches.sh --total-batches 20 -- --cores 16 --resources mem_mb=28000 --keep-going

  # Full fixed-size run (all derived batches)
  scripts/run_target_batches.sh --batch-size 500 --all --conda-env ocdocker -- --cores 16 --resources mem_mb=28000 --keep-going

  # Same using conda env directly from script
  scripts/run_target_batches.sh --total-batches 20 --conda-env ocdocker -- --cores 16 --resources mem_mb=28000 --keep-going

Complete example:

  # Set up a stable run identifier and a matching top-level log file.
  RUN_ID="batches_$(date -u +%Y%m%dT%H%M%SZ)"
  LOG="logs/pipeline_all_batches_${RUN_ID}.log"

  # Run the pipeline with nohup, exporting OCDP_RUN_ID so all batch invocations
  # launched by this wrapper share one logical run identifier.
  nohup env \
    XDG_CACHE_HOME=/tmp \
    TMPDIR=/tmp \
    OCDP_RUN_ID="$RUN_ID" \
    ./scripts/run_target_batches.sh \
    --snakefile snakefile \
    --batch-size 500 \
    --all \
    --conda-env ocdocker \
    -- \
    --logger snkmt \
    --logger-snkmt-db /data/hd4tb/OCDocker/OCDockerPipeline/.snakemake/snkmt.db \
    --cores 18 \
    --resources mem_mb=28000 \
    --keep-going \
    --rerun-incomplete \
    --rerun-triggers mtime \
    > "$LOG" 2>&1 &

echo "Started. Tail with: tail -f $LOG"
USAGE
}

is_pos_int() {
    # Helper: strict positive integer checker used by argument validation.
    [[ "${1:-}" =~ ^[1-9][0-9]*$ ]]
}

is_nonneg_int() {
    # Helper: zero or positive integer checker.
    [[ "${1:-}" =~ ^[0-9]+$ ]]
}

parse_bool() {
    # Normalize common bool spellings to 1/0.
    case "${1,,}" in
        1|true|yes|y|on) echo 1 ;;
        0|false|no|n|off) echo 0 ;;
        *) return 1 ;;
    esac
}

normalize_forwarded_config_args() {
    # Snakemake versions differ in how repeated --config blocks are propagated
    # into subprocess jobs. Keep all config key/value pairs in one block so
    # runtime selectors such as database_sources=PDBbind are not dropped when
    # the batch runner forces pipeline_export_database_csv=false.
    local -a rebuilt_args=()
    local -a config_args=()
    local arg=""
    local token=""
    local i=0

    while (( i < ${#extra_args[@]} )); do
        arg="${extra_args[$i]}"
        if [[ "$arg" == "--config" ]]; then
            i=$(( i + 1 ))
            while (( i < ${#extra_args[@]} )) && [[ "${extra_args[$i]}" != --* ]]; do
                token="${extra_args[$i]}"
                if [[ "$token" != pipeline_export_database_csv=* ]]; then
                    config_args+=("$token")
                fi
                i=$(( i + 1 ))
            done
            continue
        fi
        if [[ "$arg" == --config=* ]]; then
            token="${arg#--config=}"
            if [[ -n "$token" && "$token" != pipeline_export_database_csv=* ]]; then
                config_args+=("$token")
            fi
            i=$(( i + 1 ))
            continue
        fi

        rebuilt_args+=("$arg")
        i=$(( i + 1 ))
    done

    config_args+=("pipeline_export_database_csv=false")
    extra_args=("${rebuilt_args[@]}" --config "${config_args[@]}")
}

fail() {
    # Consistent fatal error helper.
    echo "Error: $*" >&2
    exit 2
}

cleanup_empty_rule_failure_logs() {
    # Snakemake touches declared log files even when a rule never writes to them.
    # Prune zero-byte failure logs after each batch to avoid inode buildup.
    local run_id="${OCDP_RUN_ID:-}"
    local root=""
    local before=0
    local after=0
    local removed=0

    [[ -n "$run_id" ]] || return 0

    root="${repo_root}/logs/rule_failures/${run_id}"
    [[ -d "$root" ]] || return 0

    before="$(find "$root" -type f -empty -name '*.log' | wc -l)"

    find "$root" -type f -empty -name '*.log' -delete
    find "$root" -depth -type d -empty -delete

    if [[ -d "$root" ]]; then
        after="$(find "$root" -type f -empty -name '*.log' | wc -l)"
    fi

    removed=$(( before - after ))
    if (( removed > 0 )); then
        echo "Info: pruned ${removed} empty rule failure log(s) under ${root}"
    fi
}

infer_total_targets_from_cache() {
    # In fixed-size mode, if `--total-targets` is omitted, we infer target count
    # from Snakemake discovery cache generated by previous DAG discovery.
    #
    # Expected JSON shape:
    #   { "targets": [ ... ] }
    #
    # We only need the length, not the target payload.
    local cache_json="${1:-.snakemake/target_discovery_cache.json}"
    [[ -f "$cache_json" ]] || fail "Cannot infer target count. Cache file not found: $cache_json"
    command -v python3 >/dev/null 2>&1 || fail "python3 is required to parse $cache_json"

    local count
    count="$(python3 - "$cache_json" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
targets = payload.get("targets", [])
print(len(targets) if isinstance(targets, list) else 0)
PY
)"

    is_pos_int "$count" || fail "Invalid or empty target count in $cache_json"
    echo "$count"
}

ensure_target_discovery_cache() {
    # In fixed-size mode, bootstrap a minimal target-count cache when it is not
    # present yet. This deliberately does not call Snakemake: some Snakemake
    # dry-runs do not materialize the discovery cache before the wrapper needs
    # the count. The Snakefile will still perform its normal discovery during
    # the actual batch run and may replace this minimal cache.
    local cache_json="${1:-.snakemake/target_discovery_cache.json}"
    [[ -f "$cache_json" ]] && return 0

    [[ -f "$snakefile" ]] || fail "Snakefile not found: $snakefile"

    mkdir -p "$(dirname "$cache_json")" >/dev/null 2>&1 || true
    echo "Info: target discovery cache not found at ${cache_json}; building a filesystem target-count cache."

    command -v python3 >/dev/null 2>&1 || fail "python3 is required to build $cache_json"
    python3 - "$cache_json" "$snakefile" "${extra_args[@]}" <<'PY'
import ast
import json
import os
import re
import sys
from pathlib import Path

cache_json = Path(sys.argv[1])
snakefile = Path(sys.argv[2]).resolve()
args = sys.argv[3:]
repo_root = snakefile.parent

def parse_scalar_or_list(value):
    text = str(value).strip()
    if not text:
        return []
    try:
        parsed = ast.literal_eval(text)
    except Exception:
        parsed = text
    if isinstance(parsed, (list, tuple, set)):
        return [str(item).strip() for item in parsed if str(item).strip()]
    return [item.strip() for item in str(parsed).split(",") if item.strip()]

def parse_forwarded_config(argv):
    config = {}
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--config":
            i += 1
            while i < len(argv) and not argv[i].startswith("--"):
                token = argv[i]
                if "=" in token:
                    key, value = token.split("=", 1)
                    config[key] = value
                i += 1
            continue
        if arg.startswith("--config="):
            token = arg.split("=", 1)[1]
            if "=" in token:
                key, value = token.split("=", 1)
                config[key] = value
        i += 1
    return config

def read_ocdb_path():
    cfg_path = Path(os.environ.get("OCDOCKER_CONFIG", "")) if os.environ.get("OCDOCKER_CONFIG") else repo_root / "OCDocker.cfg"
    try:
        text = cfg_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""
    match = re.search(r"(?m)^\s*ocdb\s*=\s*(.+?)\s*$", text)
    return os.path.expanduser(match.group(1).strip()) if match else ""

def normalize_database_name(name):
    lower = str(name).strip().lower()
    if lower == "pdbbind":
        return "PDBbind"
    if lower in {"dudez", "dude-z", "dude_z"}:
        return "DUDEz"
    return str(name).strip()

def looks_like_path(value):
    text = str(value).strip()
    return text.startswith(("~", ".", "/")) or os.sep in text or (os.altsep and os.altsep in text)

def database_specs(sources, ocdb_path):
    specs = []
    for raw in sources:
        source = str(raw).strip()
        if not source:
            continue
        if "=" in source:
            alias_raw, path_raw = source.split("=", 1)
            alias = normalize_database_name(alias_raw.strip())
            root = Path(path_raw.strip()).expanduser().resolve()
        else:
            normalized = normalize_database_name(source)
            if looks_like_path(source):
                root = Path(source).expanduser().resolve()
                alias = normalize_database_name(root.name)
            else:
                alias = normalized
                root = Path(ocdb_path) / alias
        specs.append((alias, root))
    return specs

config = parse_forwarded_config(args)
sources = parse_scalar_or_list(config.get("database_sources", "")) or ["PDBbind", "DUDEz"]
kinds = [kind.lower() for kind in (parse_scalar_or_list(config.get("compound_kinds", "")) or ["ligands", "decoys", "compounds"])]
kinds = [kind for kind in kinds if kind in {"ligands", "decoys", "compounds"}]
ocdb_path = read_ocdb_path()
if not ocdb_path:
    raise SystemExit("Could not determine ocdb path from OCDocker.cfg; cannot build target-count cache.")

targets = []
for alias, root in database_specs(sources, ocdb_path):
    if not root.is_dir():
        continue
    for receptor_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        receptor = receptor_dir.name
        receptor_file = receptor_dir / "receptor.pdb"
        if not receptor_file.is_file() or receptor_file.stat().st_size <= 0:
            continue
        has_reference = any(
            (receptor_dir / name).is_file() and (receptor_dir / name).stat().st_size > 0
            for name in ("reference_ligand.pdb", "reference_ligand.sdf")
        )
        compounds_dir = receptor_dir / "compounds"
        if not compounds_dir.is_dir():
            continue
        for kind in kinds:
            kind_dir = compounds_dir / kind
            if not kind_dir.is_dir():
                continue
            for target_dir in sorted(path for path in kind_dir.iterdir() if path.is_dir()):
                ligand = target_dir / "ligand.smi"
                box = target_dir / "boxes" / "box0.pdb"
                has_box = box.is_file() and box.stat().st_size > 0
                if not ligand.is_file() or ligand.stat().st_size <= 0:
                    continue
                if not has_box and not has_reference:
                    continue
                targets.append(str(Path(ocdb_path) / alias / receptor / "compounds" / kind / target_dir.name / "payload.pkl"))

payload = {
    "schema_version": "wrapper-target-count-only",
    "generated_by": "scripts/run_target_batches.sh",
    "targets": sorted(set(targets)),
}
cache_json.parent.mkdir(parents=True, exist_ok=True)
cache_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(f"Info: wrote {cache_json} with {len(payload['targets'])} target(s).")
PY

    [[ -f "$cache_json" ]] || fail "Filesystem target-count cache was not created: $cache_json"
}

total_batches=""
batch_size=""
total_targets=""
from_idx=1
to_idx=""
snakefile="snakefile"
snakemake_bin="snakemake"
conda_env=""
cache_root="/tmp"
tmp_root="/tmp"
dry_run=0
run_all=0
snakemake_retries=3
continue_on_failed_batch=1
declare -a extra_args=()

# ---------------------------------------------------------------------------
# 1) Parse wrapper arguments until `--`.
#    Everything after `--` is forwarded verbatim to Snakemake.
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --total-batches)
            [[ $# -ge 2 ]] || fail "Missing value for --total-batches"
            total_batches="$2"
            shift 2
            ;;
        --batch-size)
            [[ $# -ge 2 ]] || fail "Missing value for --batch-size"
            batch_size="$2"
            shift 2
            ;;
        --total-targets)
            [[ $# -ge 2 ]] || fail "Missing value for --total-targets"
            total_targets="$2"
            shift 2
            ;;
        --from|--start)
            [[ $# -ge 2 ]] || fail "Missing value for --from"
            from_idx="$2"
            shift 2
            ;;
        --to|--end)
            [[ $# -ge 2 ]] || fail "Missing value for --to"
            to_idx="$2"
            shift 2
            ;;
        --snakefile)
            [[ $# -ge 2 ]] || fail "Missing value for --snakefile"
            snakefile="$2"
            shift 2
            ;;
        --snakemake-bin)
            [[ $# -ge 2 ]] || fail "Missing value for --snakemake-bin"
            snakemake_bin="$2"
            shift 2
            ;;
        --conda-env)
            [[ $# -ge 2 ]] || fail "Missing value for --conda-env"
            conda_env="$2"
            shift 2
            ;;
        --cache-root)
            [[ $# -ge 2 ]] || fail "Missing value for --cache-root"
            cache_root="$2"
            shift 2
            ;;
        --tmp-root)
            [[ $# -ge 2 ]] || fail "Missing value for --tmp-root"
            tmp_root="$2"
            shift 2
            ;;
        --snakemake-retries)
            [[ $# -ge 2 ]] || fail "Missing value for --snakemake-retries"
            snakemake_retries="$2"
            shift 2
            ;;
        --snakemake-retries=*)
            snakemake_retries="${1#*=}"
            shift
            ;;
        --continue-on-failed-batch)
            if [[ $# -ge 2 ]] && [[ ! "$2" =~ ^- ]]; then
                continue_on_failed_batch="$(parse_bool "$2")" || fail "Invalid bool for --continue-on-failed-batch: $2"
                shift 2
            else
                continue_on_failed_batch=1
                shift
            fi
            ;;
        --continue-on-failed-batch=*)
            continue_on_failed_batch="$(parse_bool "${1#*=}")" || fail "Invalid bool for --continue-on-failed-batch: ${1#*=}"
            shift
            ;;
        --stop-on-failed-batch)
            continue_on_failed_batch=0
            shift
            ;;
        --dry-run)
            dry_run=1
            shift
            ;;
        --all)
            run_all=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            extra_args=("$@")
            break
            ;;
        *)
            fail "Unknown option: $1 (use --help)"
            ;;
    esac
done

# ---------------------------------------------------------------------------
# 2) Validate mode selection and numeric inputs.
# ---------------------------------------------------------------------------
if [[ -n "$total_batches" && -n "$batch_size" ]]; then
    fail "Use exactly one batching mode: --total-batches OR --batch-size"
fi
if [[ -z "$total_batches" && -z "$batch_size" ]]; then
    fail "Missing batching mode: provide --total-batches or --batch-size"
fi

is_pos_int "$from_idx" || fail "--from must be a positive integer"
is_nonneg_int "$snakemake_retries" || fail "--snakemake-retries must be a non-negative integer"
if [[ -n "$to_idx" ]]; then
    is_pos_int "$to_idx" || fail "--to must be a positive integer"
fi

mode=""
if [[ -n "$total_batches" ]]; then
    # Fractional mode:
    # User already defines N batches. We run all=i/N.
    mode="fraction"
    is_pos_int "$total_batches" || fail "--total-batches must be a positive integer"
    if [[ -z "$to_idx" ]]; then
        to_idx="$total_batches"
    fi
    (( from_idx <= total_batches )) || fail "--from must be <= --total-batches"
    (( to_idx <= total_batches )) || fail "--to must be <= --total-batches"
else
    # Fixed-size mode:
    # Derive N from total target count and requested batch size.
    # Formula: ceil(total_targets / batch_size).
    mode="fixed_size"
    is_pos_int "$batch_size" || fail "--batch-size must be a positive integer"

    if [[ -z "$total_targets" ]]; then
        ensure_target_discovery_cache
        total_targets="$(infer_total_targets_from_cache)"
    fi
    is_pos_int "$total_targets" || fail "--total-targets must be a positive integer"

    total_batches="$(( (total_targets + batch_size - 1) / batch_size ))"
    if [[ -z "$to_idx" ]]; then
        # Default range behavior in fixed-size mode:
        # - with --all: run through the final derived batch
        # - without --all: run a single batch (--from only)
        if (( run_all )); then
            to_idx="$total_batches"
        else
            to_idx="$from_idx"
        fi
    fi
    (( from_idx <= total_batches )) || fail "--from must be <= derived total batches (${total_batches})"
    (( to_idx <= total_batches )) || fail "--to must be <= derived total batches (${total_batches})"
fi

(( from_idx <= to_idx )) || fail "--from must be <= --to"
[[ -f "$snakefile" ]] || fail "Snakefile not found: $snakefile"

# ---------------------------------------------------------------------------
# 3) Build the Snakemake executable prefix.
#    - If --conda-env is provided: use `conda run -n <env> snakemake`
#    - Else: execute the given snakemake binary directly.
# ---------------------------------------------------------------------------
declare -a snakemake_cmd=()
if [[ -n "$conda_env" ]]; then
    command -v conda >/dev/null 2>&1 || fail "conda not found but --conda-env was provided"
    snakemake_cmd=(conda run -n "$conda_env" snakemake)
else
    command -v "$snakemake_bin" >/dev/null 2>&1 || fail "Snakemake binary not found: $snakemake_bin"
    snakemake_cmd=("$snakemake_bin")
fi

# ---------------------------------------------------------------------------
# 4) Runtime path normalization.
#    Many tools in this stack write temp/cache files; using explicit roots helps
#    avoid stale locks/permission drift from prior sessions.
# ---------------------------------------------------------------------------
mkdir -p "$cache_root" "$tmp_root" >/dev/null 2>&1 || true
export XDG_CACHE_HOME="$cache_root"
export TMPDIR="$tmp_root"

# snkmt requires both flags. If DB path is provided without --logger,
# prepend --logger snkmt automatically for convenience.
has_logger=0
has_logger_snkmt_db=0
has_retries=0
for arg in "${extra_args[@]}"; do
    case "$arg" in
        --logger|--logger=*)
            has_logger=1
            ;;
        --logger-snkmt-db|--logger-snkmt-db=*)
            has_logger_snkmt_db=1
            ;;
        --retries|--retries=*)
            has_retries=1
            ;;
    esac
done
if (( has_logger_snkmt_db )) && (( ! has_logger )); then
    extra_args=(--logger snkmt "${extra_args[@]}")
    echo "Info: auto-added '--logger snkmt' because '--logger-snkmt-db' was provided."
fi
if (( ! has_retries )); then
    extra_args=(--retries "$snakemake_retries" "${extra_args[@]}")
    echo "Info: auto-added '--retries ${snakemake_retries}' (use --snakemake-retries or pass --retries after '--' to override)."
fi
normalize_forwarded_config_args

# ---------------------------------------------------------------------------
# 5) User-facing run summary before execution starts.
# ---------------------------------------------------------------------------
echo "Batch runner: mode=${mode}, range=${from_idx}-${to_idx}, snakefile=${snakefile}"
echo "Runtime paths: XDG_CACHE_HOME=${XDG_CACHE_HOME}, TMPDIR=${TMPDIR}"
echo "Failure policy: continue_on_failed_batch=${continue_on_failed_batch}"
if [[ "$mode" == "fixed_size" ]]; then
    echo "Derived batch conversion: total_targets=${total_targets}, batch_size=${batch_size}, total_batches=${total_batches}"
fi
if [[ ${#extra_args[@]} -gt 0 ]]; then
    echo "Forwarded Snakemake args: ${extra_args[*]}"
fi

# ---------------------------------------------------------------------------
# 6) Sequential batch execution loop.
#    Each iteration launches one Snakemake process:
#      snakemake -s <snakefile> --batch all=i/N [forwarded args...]
#
#    Notes:
#    - We always append `--config pipeline_export_database_csv=false`.
#      This prevents each partition from depending on a full-database CSV fan-in.
#    - Batch failures can either stop immediately or be recorded and skipped,
#      depending on continue_on_failed_batch.
# ---------------------------------------------------------------------------
declare -a failed_batches=()
for (( idx=from_idx; idx<=to_idx; idx++ )); do
    batch_spec="all=${idx}/${total_batches}"

    echo
    echo "=== Running batch ${idx} (${batch_spec}) ($(date '+%Y-%m-%d %H:%M:%S')) ==="

    declare -a cmd=(
        "${snakemake_cmd[@]}"
        -s "$snakefile"
        --batch "$batch_spec"
    )
    if (( dry_run )); then
        cmd+=(-n)
    fi
    if [[ ${#extra_args[@]} -gt 0 ]]; then
        cmd+=("${extra_args[@]}")
    fi

    batch_rc=0
    if "${cmd[@]}"; then
        batch_rc=0
    else
        batch_rc=$?
    fi

    cleanup_empty_rule_failure_logs

    if (( batch_rc != 0 )); then
        failed_batches+=("$idx")
        if (( continue_on_failed_batch )); then
            echo "Batch ${idx} failed. Continuing to next batch (--continue-on-failed-batch=true)." >&2
            continue
        fi
        echo "Batch ${idx} failed. Stopping (--continue-on-failed-batch=false)." >&2
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# 7) Finished all requested batch indices successfully.
# ---------------------------------------------------------------------------
if (( ${#failed_batches[@]} > 0 )); then
    echo
    echo "Completed with failed batch(es): ${failed_batches[*]}" >&2
    exit 1
fi

echo
echo "All requested batches finished successfully."
