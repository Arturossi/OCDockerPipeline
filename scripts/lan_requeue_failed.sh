#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Move all failed LAN queue batches back to pending.

Usage:
  scripts/lan_requeue_failed.sh --queue-dir DIR
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
mkdir -p "$queue_dir/pending" "$queue_dir/failed"

shopt -s nullglob
failed_files=("$queue_dir"/failed/*.batch)
shopt -u nullglob

requeued=0
for failed_path in "${failed_files[@]}"; do
    base_name="$(basename -- "$failed_path")"
    destination="$queue_dir/pending/$base_name"
    [[ ! -e "$destination" ]] || fail "Pending already contains $destination"
    mv "$failed_path" "$destination"
    requeued=$((requeued + 1))
done

echo "Requeued $requeued failed batch(es) into $queue_dir/pending"
