#!/usr/bin/env bash
set -euo pipefail

PIPE_ROOT="${1:-$(pwd)}"
DB_PATH="${SNKMT_DB:-${PIPE_ROOT}/.snakemake/snkmt.db}"
LOG_PATH="${2:-}"

if [[ ! -f "${DB_PATH}" ]]; then
  echo "snkmt DB not found: ${DB_PATH}" >&2
  exit 1
fi

wf_id=$(sqlite3 "${DB_PATH}" "SELECT id FROM workflows ORDER BY started_at DESC LIMIT 1;")
if [[ -z "${wf_id}" ]]; then
  echo "No workflows found in ${DB_PATH}" >&2
  exit 1
fi

wf_info=$(sqlite3 -separator $'	' "${DB_PATH}" "SELECT id, status, started_at, updated_at, total_job_count, jobs_finished FROM workflows WHERE id='${wf_id}';")
IFS=$'	' read -r wf_id wf_status wf_started wf_updated wf_total wf_finished <<<"${wf_info}"

if [[ -z "${LOG_PATH}" ]]; then
  LOG_PATH=$(ls -t "${PIPE_ROOT}"/.snakemake/log/*.snakemake.log 2>/dev/null | head -n 1 || true)
fi

echo "Workflow: ${wf_id}"
echo "Status:   ${wf_status}"
echo "Started:  ${wf_started}"
echo "Updated:  ${wf_updated}"
echo "Progress: ${wf_finished}/${wf_total}"
echo "DB:       ${DB_PATH}"
if [[ -n "${LOG_PATH}" ]]; then
  echo "Log:      ${LOG_PATH}"
else
  echo "Log:      (not found)"
fi
echo

echo "== Error Events =="
sqlite3 -header -column "${DB_PATH}" "
SELECT id, timestamp, exception
FROM errors
WHERE workflow_id='${wf_id}'
ORDER BY id DESC;
"
echo

echo "== Failed Jobs =="
failed_rows=$(sqlite3 -separator $'	' "${DB_PATH}" "
SELECT j.snakemake_id, r.name, COALESCE(j.reason,''), COALESCE(j.message,'')
FROM jobs j
JOIN rules r ON r.id = j.rule_id
WHERE j.workflow_id='${wf_id}' AND j.status='ERROR'
ORDER BY j.snakemake_id;
")

if [[ -z "${failed_rows}" ]]; then
  echo "No failed jobs recorded in jobs table."
  exit 0
fi

printf "%s
" "${failed_rows}" | while IFS=$'	' read -r jobid rule reason message; do
  echo
  echo "--- Job ${jobid} (${rule}) ---"
  if [[ -n "${message}" ]]; then
    echo "message: ${message}"
  else
    echo "message: <empty>"
  fi
  if [[ -n "${reason}" ]]; then
    echo "reason:  ${reason}"
  else
    echo "reason:  <empty>"
  fi

  if [[ -n "${LOG_PATH}" && -f "${LOG_PATH}" ]]; then
    line=$(rg -n "jobid:\s*${jobid}\b" "${LOG_PATH}" -S | tail -n 1 | cut -d: -f1 || true)
    if [[ -n "${line}" ]]; then
      start=$(( line > 12 ? line - 12 : 1 ))
      end=$(( line + 70 ))
      echo "log_excerpt:"
      sed -n "${start},${end}p" "${LOG_PATH}"
    else
      echo "log_excerpt: <job id not found in selected log>"
    fi
  else
    echo "log_excerpt: <log file unavailable>"
  fi
done
