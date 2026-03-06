#!/usr/bin/env bash
set -euo pipefail

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

Fixed-size helpers:
  --total-targets N         Total targets to use for --batch-size conversion
                            (if omitted, inferred from .snakemake/target_discovery_cache.json)

Runner options:
  --snakefile PATH          Snakefile path (default: snakefile)
  --snakemake-bin BIN       Snakemake executable (default: snakemake)
  --conda-env NAME          Run as: conda run -n NAME snakemake
  --cache-root PATH         XDG cache root used for each run (default: /tmp)
  --tmp-root PATH           TMPDIR used for each run (default: /tmp)
  --dry-run                 Add -n to each run
  -h, --help                Show this help

Examples:
  # Tiny test: one small batch (approximately 5 targets)
  scripts/run_target_batches.sh --batch-size 5 --from 1 --to 1 -- --cores 8 --resources mem_mb=12000

  # Full run over 20 fractions
  scripts/run_target_batches.sh --total-batches 20 -- --cores 16 --resources mem_mb=28000 --keep-going

  # Same using conda env directly from script
  scripts/run_target_batches.sh --total-batches 20 --conda-env ocdocker -- --cores 16 --resources mem_mb=28000 --keep-going
USAGE
}

is_pos_int() {
    [[ "${1:-}" =~ ^[1-9][0-9]*$ ]]
}

fail() {
    echo "Error: $*" >&2
    exit 2
}

infer_total_targets_from_cache() {
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
declare -a extra_args=()

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
        --dry-run)
            dry_run=1
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

if [[ -n "$total_batches" && -n "$batch_size" ]]; then
    fail "Use exactly one batching mode: --total-batches OR --batch-size"
fi
if [[ -z "$total_batches" && -z "$batch_size" ]]; then
    fail "Missing batching mode: provide --total-batches or --batch-size"
fi

is_pos_int "$from_idx" || fail "--from must be a positive integer"
if [[ -n "$to_idx" ]]; then
    is_pos_int "$to_idx" || fail "--to must be a positive integer"
fi

mode=""
if [[ -n "$total_batches" ]]; then
    mode="fraction"
    is_pos_int "$total_batches" || fail "--total-batches must be a positive integer"
    if [[ -z "$to_idx" ]]; then
        to_idx="$total_batches"
    fi
    (( from_idx <= total_batches )) || fail "--from must be <= --total-batches"
    (( to_idx <= total_batches )) || fail "--to must be <= --total-batches"
else
    mode="fixed_size"
    is_pos_int "$batch_size" || fail "--batch-size must be a positive integer"

    if [[ -z "$total_targets" ]]; then
        total_targets="$(infer_total_targets_from_cache)"
    fi
    is_pos_int "$total_targets" || fail "--total-targets must be a positive integer"

    total_batches="$(( (total_targets + batch_size - 1) / batch_size ))"
    if [[ -z "$to_idx" ]]; then
        to_idx="$from_idx"
    fi
    (( from_idx <= total_batches )) || fail "--from must be <= derived total batches (${total_batches})"
    (( to_idx <= total_batches )) || fail "--to must be <= derived total batches (${total_batches})"
fi

(( from_idx <= to_idx )) || fail "--from must be <= --to"
[[ -f "$snakefile" ]] || fail "Snakefile not found: $snakefile"

declare -a snakemake_cmd=()
if [[ -n "$conda_env" ]]; then
    command -v conda >/dev/null 2>&1 || fail "conda not found but --conda-env was provided"
    snakemake_cmd=(conda run -n "$conda_env" snakemake)
else
    command -v "$snakemake_bin" >/dev/null 2>&1 || fail "Snakemake binary not found: $snakemake_bin"
    snakemake_cmd=("$snakemake_bin")
fi

mkdir -p "$cache_root" "$tmp_root" >/dev/null 2>&1 || true
export XDG_CACHE_HOME="$cache_root"
export TMPDIR="$tmp_root"

echo "Batch runner: mode=${mode}, range=${from_idx}-${to_idx}, snakefile=${snakefile}"
echo "Runtime paths: XDG_CACHE_HOME=${XDG_CACHE_HOME}, TMPDIR=${TMPDIR}"
if [[ "$mode" == "fixed_size" ]]; then
    echo "Derived batch conversion: total_targets=${total_targets}, batch_size=${batch_size}, total_batches=${total_batches}"
fi
if [[ ${#extra_args[@]} -gt 0 ]]; then
    echo "Forwarded Snakemake args: ${extra_args[*]}"
fi

for (( idx=from_idx; idx<=to_idx; idx++ )); do
    batch_spec="all=${idx}/${total_batches}"

    echo
    echo "=== Running batch ${idx} (${batch_spec}) ($(date '+%Y-%m-%d %H:%M:%S')) ==="

    # Disable per-database CSV export in batch runs to avoid all-target fan-in
    # dependencies being reintroduced for each partition.
    declare -a cmd=(
        "${snakemake_cmd[@]}"
        -s "$snakefile"
        --batch "$batch_spec"
        --config "pipeline_export_database_csv=false"
    )
    if (( dry_run )); then
        cmd+=(-n)
    fi
    if [[ ${#extra_args[@]} -gt 0 ]]; then
        cmd+=("${extra_args[@]}")
    fi

    if ! "${cmd[@]}"; then
        echo "Batch ${idx} failed. Stopping." >&2
        exit 1
    fi
done

echo
echo "All requested batches finished successfully."
