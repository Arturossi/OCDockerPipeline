#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Show Snakemake snkmt DB workflow status.

Usage:
  scripts/snkmt_status.sh [--db-path PATH] [--workflow-id ID] [--job-limit N] [--error-limit N]
USAGE
}

fail() {
    echo "Error: $*" >&2
    exit 2
}

db_path="${SNKMT_DB:-.snakemake/snkmt.db}"
workflow_id=""
job_limit="10"
error_limit="10"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --db-path)
            [[ $# -ge 2 ]] || fail "Missing value for --db-path"
            db_path="$2"
            shift 2
            ;;
        --workflow-id)
            [[ $# -ge 2 ]] || fail "Missing value for --workflow-id"
            workflow_id="$2"
            shift 2
            ;;
        --job-limit)
            [[ $# -ge 2 ]] || fail "Missing value for --job-limit"
            job_limit="$2"
            shift 2
            ;;
        --error-limit)
            [[ $# -ge 2 ]] || fail "Missing value for --error-limit"
            error_limit="$2"
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

[[ -f "$db_path" ]] || fail "snkmt DB not found: $db_path"
[[ "$job_limit" =~ ^[0-9]+$ ]] || fail "--job-limit must be a non-negative integer"
[[ "$error_limit" =~ ^[0-9]+$ ]] || fail "--error-limit must be a non-negative integer"

if [[ -z "$workflow_id" ]]; then
    workflow_id="$(sqlite3 "$db_path" "SELECT id FROM workflows ORDER BY started_at DESC LIMIT 1;")"
fi

[[ -n "$workflow_id" ]] || fail "No workflows found in $db_path"

workflow_row="$(sqlite3 -separator '|' "$db_path" "SELECT id, status, started_at, updated_at, COALESCE(end_time, ''), total_job_count, jobs_finished FROM workflows WHERE id='${workflow_id}';")"
[[ -n "$workflow_row" ]] || fail "Workflow not found: $workflow_id"

IFS='|' read -r wf_id wf_status wf_started wf_updated wf_end wf_total wf_finished <<<"$workflow_row"

echo "Workflow: ${wf_id}"
echo "Status:   ${wf_status}"
echo "Started:  ${wf_started}"
echo "Updated:  ${wf_updated}"
if [[ -n "$wf_end" ]]; then
    echo "Ended:    ${wf_end}"
fi
echo "Progress: ${wf_finished}/${wf_total}"
echo "DB:       ${db_path}"

if [[ "$wf_total" =~ ^[1-9][0-9]*$ ]]; then
    progress_pct=$((100 * wf_finished / wf_total))
    echo "Percent:  ${progress_pct}%"
fi
echo

echo "== Workflow Errors =="
workflow_errors="$(sqlite3 -header -column "$db_path" "SELECT id, timestamp, exception FROM errors WHERE workflow_id='${workflow_id}' ORDER BY id DESC LIMIT ${error_limit};")"
if [[ -n "${workflow_errors//$'\n'/}" ]]; then
    echo "$workflow_errors"
else
    echo "No errors recorded for this workflow."
fi
echo

echo "== Failed Jobs =="
failed_jobs="$(sqlite3 -header -column "$db_path" "
SELECT j.snakemake_id AS jobid,
       r.name AS rule,
       j.status,
       j.started_at,
       j.end_time,
       substr(COALESCE(j.reason, ''), 1, 120) AS reason,
       substr(COALESCE(j.message, ''), 1, 120) AS message
FROM jobs j
JOIN rules r ON r.id = j.rule_id
WHERE j.workflow_id='${workflow_id}' AND j.status='ERROR'
ORDER BY j.snakemake_id DESC
LIMIT ${job_limit};
")"
if [[ -n "${failed_jobs//$'\n'/}" ]]; then
    echo "$failed_jobs"
else
    echo "No failed jobs recorded for this workflow."
fi
echo

echo "== Recent Error Workflows =="
recent_error_workflows="$(sqlite3 -header -column "$db_path" "
SELECT w.id,
       w.status,
       w.started_at,
       w.updated_at,
       COUNT(e.id) AS errors,
       GROUP_CONCAT(DISTINCT e.exception) AS exceptions
FROM workflows w
JOIN errors e ON e.workflow_id = w.id
GROUP BY w.id, w.status, w.started_at, w.updated_at
ORDER BY MAX(e.timestamp) DESC
LIMIT ${error_limit};
")"
if [[ -n "${recent_error_workflows//$'\n'/}" ]]; then
    echo "$recent_error_workflows"
else
    echo "No workflows with recorded errors."
fi
