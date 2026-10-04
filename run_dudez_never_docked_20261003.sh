#!/usr/bin/env bash
# Docks the DUDEz compounds whose folders were prepared (ligand.smi present) but never docked.
set -u
cd /data/hd4tb/OCDocker/OCDockerPipeline
LOG_DIR=/data/hd4tb/OCDocker/OCDockerPipeline/batch_loop_logs
TARGETS=/data/hd4tb/OCDocker/data/ocdb2/OCScore/analysis/work/dudez_dock_now_targets.txt
echo "$(date '+%F %T') starting DUDEz never-docked run ($(wc -l < "$TARGETS") targets)" >> "$LOG_DIR/dudez_progress.log"
/home/artur/miniconda3/bin/conda run -n ocdocker snakemake -s snakefile \
  $(cat "$TARGETS") \
  --retries 3 --cores 18 --resources mem_mb=20000 --keep-going --rerun-triggers mtime --rerun-incomplete \
  --config database_sources=DUDEz pipeline_export_database_csv=false \
  > "$LOG_DIR/dudez_never_docked_run.out" 2>&1
echo "$(date '+%F %T') DUDEz never-docked run exited ($?)" >> "$LOG_DIR/dudez_progress.log"
