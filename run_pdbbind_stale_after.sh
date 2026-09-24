#!/usr/bin/env bash
# Waits for the regenerated-SMILES PDBbind run to finish, then re-docks the
# complexes whose engine status still pointed at the old data location.
set -u
cd /data/hd4tb/OCDocker/OCDockerPipeline
LOG_DIR=/data/hd4tb/OCDocker/OCDockerPipeline/batch_loop_logs
while pgrep -f "run_pdbbind_regen_after.sh" > /dev/null; do sleep 60; done
echo "$(date '+%F %T') starting stale-status PDBbind rerun" >> "$LOG_DIR/pdbbind_progress.log"
/home/artur/miniconda3/bin/conda run -n ocdocker snakemake -s snakefile \
  $(cat "$LOG_DIR/pdbbind_stale_rerun_targets.txt") \
  --retries 3 --cores 18 --resources mem_mb=26000 --keep-going --rerun-triggers mtime \
  --config database_sources=PDBbind pipeline_export_database_csv=false \
  > "$LOG_DIR/pdbbind_stale_rerun.out" 2>&1
echo "$(date '+%F %T') stale-status PDBbind rerun exited ($?)" >> "$LOG_DIR/pdbbind_progress.log"
