#!/usr/bin/env bash
# Waits for the currently-running batch to finish, repairs stale mtimes via
# `snakemake --touch` (fixes the false-invalidation cascade without redoing
# real work), then resumes the sequential batch loop from wherever it left
# off (reads the last "Starting batch" line from loop_progress.log).
set -u
cd /data/hd4tb/OCDocker/OCDockerPipeline
LOG_DIR="/data/hd4tb/OCDocker/OCDockerPipeline/batch_loop_logs"

echo "$(date '+%Y-%m-%d %H:%M:%S') touch_repair_and_resume: waiting for batch 176 to finish" >> "$LOG_DIR/loop_progress.log"
while ! grep -q "Batch 176/268 completed OK\|Batch 176/268 FAILED" "$LOG_DIR/loop_progress.log" 2>/dev/null; do
    sleep 15
done

# Kill the still-running outer loop (run_batches_loop.sh 1) immediately so it
# can't race us into starting batch 177 on its own, then make sure no
# snakemake process is left before running --touch.
echo "$(date '+%Y-%m-%d %H:%M:%S') touch_repair_and_resume: batch 176 finished, stopping the outer loop before it starts 177" >> "$LOG_DIR/loop_progress.log"
pkill -f "run_batches_loop.sh 1" 2>/dev/null
sleep 2
pkill -f "snakemake -s snakefile --batch all=" 2>/dev/null
while pgrep -f "snakemake -s snakefile" > /dev/null; do
    sleep 5
done

echo "$(date '+%Y-%m-%d %H:%M:%S') touch_repair_and_resume: running snakemake --touch to repair stale timestamps" >> "$LOG_DIR/loop_progress.log"
/home/artur/miniconda3/bin/conda run -n ocdocker snakemake -s snakefile --touch \
    --config database_sources=LITPCBA=/home/artur/ocdocker_fast/LITPCBA pipeline_export_database_csv=false \
    > "$LOG_DIR/touch_repair.out" 2>&1
touch_rc=$?
echo "$(date '+%Y-%m-%d %H:%M:%S') touch_repair_and_resume: --touch finished (exit ${touch_rc})" >> "$LOG_DIR/loop_progress.log"

# Resume from the batch number after the last one that actually completed.
last_completed=$(grep "completed OK\|FAILED" "$LOG_DIR/loop_progress.log" | tail -1 | grep -oE "Batch [0-9]+" | grep -oE "[0-9]+")
if [ -z "$last_completed" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') touch_repair_and_resume: could not determine resume point, aborting" >> "$LOG_DIR/loop_progress.log"
    exit 1
fi
resume_at=$((last_completed + 1))

echo "$(date '+%Y-%m-%d %H:%M:%S') touch_repair_and_resume: resuming loop at batch ${resume_at}" >> "$LOG_DIR/loop_progress.log"
exec /data/hd4tb/OCDocker/OCDockerPipeline/run_batches_loop.sh "$resume_at"
