#!/usr/bin/env bash
# Runs LIT-PCBA batches sequentially from START_BATCH through 268. Does NOT
# stop on a batch's nonzero exit -- WorkflowError from --keep-going exhausting
# retries on a handful of permanently-unparseable molecules is expected and
# common (dozens of known cases across VDR's 20,000 decoys), so every batch
# failure is only logged, never fatal to the loop.
set -u
cd /data/hd4tb/OCDocker/OCDockerPipeline

START_BATCH="${1:?usage: run_batches_loop.sh START_BATCH}"
END_BATCH=268
LOG_DIR="/data/hd4tb/OCDocker/OCDockerPipeline/batch_loop_logs"
mkdir -p "$LOG_DIR"

for ((i=START_BATCH; i<=END_BATCH; i++)); do
    LOG_FILE="$LOG_DIR/batch_${i}.out"
    echo "$(date '+%Y-%m-%d %H:%M:%S') Starting batch ${i}/268" >> "$LOG_DIR/loop_progress.log"

    /home/artur/miniconda3/bin/conda run -n ocdocker snakemake -s snakefile \
        --batch "all=${i}/268" \
        --retries 3 --cores 18 --resources mem_mb=26000 \
        --keep-going --rerun-triggers mtime \
        --config database_sources=LITPCBA=/home/artur/ocdocker_fast/LITPCBA pipeline_export_database_csv=false \
        > "$LOG_FILE" 2>&1
    rc=$?

    if [ $rc -ne 0 ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') Batch ${i}/268 FAILED (exit ${rc}) -- continuing to next batch, see ${LOG_FILE}" >> "$LOG_DIR/loop_progress.log"
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S') Batch ${i}/268 completed OK" >> "$LOG_DIR/loop_progress.log"
    fi
done

echo "$(date '+%Y-%m-%d %H:%M:%S') All batches ${START_BATCH}-${END_BATCH} completed" >> "$LOG_DIR/loop_progress.log"
