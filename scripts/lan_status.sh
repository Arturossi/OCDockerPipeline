#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Show shared-folder LAN queue status.

Usage:
  scripts/lan_status.sh --queue-dir DIR
USAGE
}

fail() {
    echo "Error: $*" >&2
    exit 2
}

queue_dir=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --queue-dir)
            [[ $# -ge 2 ]] || fail "Missing value for --queue-dir"
            queue_dir="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "Unknown option: $1"
            ;;
    esac
done

[[ -n "$queue_dir" ]] || fail "--queue-dir is required"

total_batches=""
seeded_at_utc=""
if [[ -f "$queue_dir/queue.env" ]]; then
    # shellcheck source=/dev/null
    source "$queue_dir/queue.env"
    total_batches="${TOTAL_BATCHES:-}"
    seeded_at_utc="${SEEDED_AT_UTC:-}"
fi

shopt -s nullglob
pending_files=("$queue_dir"/pending/*.batch)
running_files=("$queue_dir"/running/*.running)
done_files=("$queue_dir"/done/*.batch)
failed_files=("$queue_dir"/failed/*.batch)
shopt -u nullglob

pending_count=${#pending_files[@]}
running_count=${#running_files[@]}
done_count=${#done_files[@]}
failed_count=${#failed_files[@]}

echo "Queue: $queue_dir"
if [[ -n "$total_batches" ]]; then
    echo "Total batches: $total_batches"
fi
if [[ -n "$seeded_at_utc" ]]; then
    echo "Seeded at: $seeded_at_utc"
fi
echo "Pending: $pending_count"
echo "Running: $running_count"
echo "Done: $done_count"
echo "Failed: $failed_count"

if [[ -n "$total_batches" && "$total_batches" =~ ^[1-9][0-9]*$ ]]; then
    progress=$((100 * done_count / total_batches))
    echo "Progress: ${done_count}/${total_batches} done (${progress}%)"
fi

if (( running_count > 0 )); then
    printf 'Running batches:'
    for path in "${running_files[@]}"; do
        name="$(basename -- "$path")"
        token="${name%%.*}"
        worker="${name#${token}.}"
        worker="${worker%.running}"
        printf ' %s(%s)' "$token" "$worker"
    done
    printf '\n'
fi

if (( failed_count > 0 )); then
    printf 'Failed batches:'
    for path in "${failed_files[@]}"; do
        printf ' %s' "$(basename -- "${path%.batch}")"
    done
    printf '\n'
fi
