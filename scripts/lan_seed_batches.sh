#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Seed a shared-folder LAN batch queue.

Usage:
  scripts/lan_seed_batches.sh --queue-dir DIR --total-batches N

This creates the queue folders:
  pending, running, done, failed, logs

and seeds one pending batch file per batch index.
USAGE
}

fail() {
    echo "Error: $*" >&2
    exit 2
}

is_pos_int() {
    [[ "${1:-}" =~ ^[1-9][0-9]*$ ]]
}

queue_dir=""
total_batches=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --queue-dir)
            [[ $# -ge 2 ]] || fail "Missing value for --queue-dir"
            queue_dir="$2"
            shift 2
            ;;
        --total-batches)
            [[ $# -ge 2 ]] || fail "Missing value for --total-batches"
            total_batches="$2"
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
is_pos_int "$total_batches" || fail "--total-batches must be a positive integer"

mkdir -p \
    "$queue_dir/pending" \
    "$queue_dir/running" \
    "$queue_dir/done" \
    "$queue_dir/failed" \
    "$queue_dir/logs"

shopt -s nullglob
state_files=(
    "$queue_dir"/pending/*.batch
    "$queue_dir"/running/*.running
    "$queue_dir"/done/*.batch
    "$queue_dir"/failed/*.batch
)
shopt -u nullglob

[[ ! -f "$queue_dir/queue.env" ]] || fail "Queue metadata already exists: $queue_dir/queue.env"
(( ${#state_files[@]} == 0 )) || fail "Queue is not empty: $queue_dir"

width=${#total_batches}
if (( width < 4 )); then
    width=4
fi

seeded_at_utc="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
meta_tmp="$queue_dir/.queue.env.tmp.$$"
printf 'TOTAL_BATCHES=%s\nSEEDED_AT_UTC=%s\nBATCH_WIDTH=%s\n' \
    "$total_batches" \
    "$seeded_at_utc" \
    "$width" \
    > "$meta_tmp"
mv "$meta_tmp" "$queue_dir/queue.env"

for (( idx=1; idx<=total_batches; idx++ )); do
    token="$(printf "%0${width}d" "$idx")"
    pending_tmp="$queue_dir/pending/.${token}.batch.tmp.$$"
    printf 'batch_index=%s\ntotal_batches=%s\nseeded_at_utc=%s\n' \
        "$idx" \
        "$total_batches" \
        "$seeded_at_utc" \
        > "$pending_tmp"
    mv "$pending_tmp" "$queue_dir/pending/${token}.batch"
done

echo "Seeded $total_batches batch(es) in $queue_dir"
echo "Queue metadata: $queue_dir/queue.env"
