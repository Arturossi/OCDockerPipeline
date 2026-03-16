#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Run LAN queue worker(s) against the existing batch runner.

Usage:
  scripts/lan_worker.sh --queue-dir DIR [--worker-id ID] [--runner PATH] -- [snakemake args...]

The worker claims one batch at a time via atomic moves:
  pending -> running -> done|failed

Everything after `--` is forwarded to:
  scripts/run_target_batches.sh --total-batches N --from i --to i
USAGE
}

fail() {
    echo "Error: $*" >&2
    exit 2
}

is_pos_int() {
    [[ "${1:-}" =~ ^[1-9][0-9]*$ ]]
}

resolve_path() {
    local raw_path="$1"
    local dir_path base_name
    dir_path="$(cd -- "$(dirname -- "$raw_path")" && pwd -P)"
    base_name="$(basename -- "$raw_path")"
    printf '%s/%s\n' "$dir_path" "$base_name"
}

default_worker_id() {
    local raw
    raw="$(hostname 2>/dev/null || uname -n 2>/dev/null || echo worker)"
    printf '%s' "$raw" | tr -c 'A-Za-z0-9._-' '_'
}

queue_dir=""
worker_id="$(default_worker_id)"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
runner_path="$script_dir/run_target_batches.sh"
declare -a forwarded_args=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --queue-dir)
            [[ $# -ge 2 ]] || fail "Missing value for --queue-dir"
            queue_dir="$2"
            shift 2
            ;;
        --worker-id)
            [[ $# -ge 2 ]] || fail "Missing value for --worker-id"
            worker_id="$(printf '%s' "$2" | tr -c 'A-Za-z0-9._-' '_')"
            shift 2
            ;;
        --runner)
            [[ $# -ge 2 ]] || fail "Missing value for --runner"
            runner_path="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            forwarded_args=("$@")
            break
            ;;
        *)
            fail "Unknown option: $1"
            ;;
    esac
done

[[ -n "$queue_dir" ]] || fail "--queue-dir is required"
[[ -f "$queue_dir/queue.env" ]] || fail "Queue metadata not found: $queue_dir/queue.env"

# shellcheck source=/dev/null
source "$queue_dir/queue.env"
is_pos_int "${TOTAL_BATCHES:-}" || fail "Invalid TOTAL_BATCHES in $queue_dir/queue.env"

runner_abs="$(resolve_path "$runner_path")"
[[ -x "$runner_abs" ]] || fail "Runner is not executable: $runner_abs"

pipeline_root="$(cd -- "$(dirname -- "$runner_abs")/.." && pwd -P)"
[[ -f "$pipeline_root/snakefile" ]] || fail "snakefile not found under pipeline root: $pipeline_root"

mkdir -p \
    "$queue_dir/pending" \
    "$queue_dir/running" \
    "$queue_dir/done" \
    "$queue_dir/failed" \
    "$queue_dir/logs"

claimed_path=""
claimed_token=""

move_claimed_to_state() {
    local state_dir="$1"
    local destination="$queue_dir/$state_dir/${claimed_token}.batch"
    [[ -n "$claimed_path" ]] || return 0
    [[ -e "$claimed_path" ]] || return 0
    [[ ! -e "$destination" ]] || fail "Queue state collision: $destination already exists"
    mv "$claimed_path" "$destination"
    claimed_path=""
    claimed_token=""
}

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    if [[ -n "$claimed_path" && -e "$claimed_path" ]]; then
        local destination="$queue_dir/failed/${claimed_token}.batch"
        if [[ ! -e "$destination" ]]; then
            mv "$claimed_path" "$destination" || true
            echo "Marked batch ${claimed_token} as failed during worker shutdown." >&2
        fi
    fi
    exit "$status"
}

claim_next_batch() {
    local pending_path base_name claim_suffix
    claim_suffix="${worker_id}.$$"
    shopt -s nullglob
    for pending_path in "$queue_dir"/pending/*.batch; do
        base_name="$(basename -- "$pending_path")"
        claimed_token="${base_name%.batch}"
        claimed_path="$queue_dir/running/${claimed_token}.${claim_suffix}.running"
        if mv "$pending_path" "$claimed_path" 2>/dev/null; then
            shopt -u nullglob
            return 0
        fi
        claimed_path=""
        claimed_token=""
    done
    shopt -u nullglob
    return 1
}

trap cleanup EXIT INT TERM

claimed_batches=0

while claim_next_batch; do
    batch_index=$((10#$claimed_token))
    log_stamp="$(date '+%Y-%m-%d_%H-%M-%S')"
    log_path="$queue_dir/logs/batch_${claimed_token}_${worker_id}_${log_stamp}.log"

    {
        printf 'worker_id=%s\n' "$worker_id"
        printf 'batch_index=%s\n' "$batch_index"
        printf 'total_batches=%s\n' "$TOTAL_BATCHES"
        printf 'pipeline_root=%s\n' "$pipeline_root"
        printf 'runner=%s\n' "$runner_abs"
        printf 'started_at_utc=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        printf '\n'
    } > "$log_path"

    cmd=(
        "$runner_abs"
        --total-batches "$TOTAL_BATCHES"
        --from "$batch_index"
        --to "$batch_index"
        --stop-on-failed-batch
    )
    if (( ${#forwarded_args[@]} > 0 )); then
        cmd+=(-- "${forwarded_args[@]}")
    fi

    echo "Claimed batch ${batch_index}/${TOTAL_BATCHES} as ${worker_id}. Log: $log_path"
    if (
        cd "$pipeline_root"
        "${cmd[@]}"
    ) >>"$log_path" 2>&1; then
        move_claimed_to_state done
        claimed_batches=$((claimed_batches + 1))
        echo "Completed batch ${batch_index}/${TOTAL_BATCHES}"
    else
        move_claimed_to_state failed
        echo "Failed batch ${batch_index}/${TOTAL_BATCHES}. See $log_path" >&2
    fi
done

echo "No pending batches remain for worker ${worker_id}. Completed ${claimed_batches} batch(es)."
