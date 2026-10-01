#!/usr/bin/env bash
# Resumes the PDBbind runs after the 2026-09-29 freeze: the regenerated-SMILES run, then the
# stale-status rerun. Vina runs through the memory-capped wrapper set in OCDocker.cfg, and ligands
# too large to dock were removed from both target lists beforehand.
set -u
cd /data/hd4tb/OCDocker/OCDockerPipeline
LOG_DIR=/data/hd4tb/OCDocker/OCDockerPipeline/batch_loop_logs
SNAKEMAKE=(/home/artur/miniconda3/bin/conda run -n ocdocker snakemake -s snakefile)
COMMON=(--retries 3 --cores 18 --resources mem_mb=20000 --keep-going --rerun-triggers mtime --rerun-incomplete
        --config database_sources=PDBbind pipeline_export_database_csv=false)

echo "$(date '+%F %T') unlocking after the 2026-09-29 freeze" >> "$LOG_DIR/pdbbind_progress.log"
"${SNAKEMAKE[@]}" --unlock --config database_sources=PDBbind > "$LOG_DIR/pdbbind_unlock_20260930.out" 2>&1

for run in regen_smi stale_rerun; do
  targets="$LOG_DIR/pdbbind_${run}_targets.txt"
  echo "$(date '+%F %T') starting $run ($(wc -l < "$targets") targets)" >> "$LOG_DIR/pdbbind_progress.log"
  "${SNAKEMAKE[@]}" $(cat "$targets") "${COMMON[@]}" > "$LOG_DIR/pdbbind_${run}_resume_20260930.out" 2>&1
  echo "$(date '+%F %T') $run exited ($?)" >> "$LOG_DIR/pdbbind_progress.log"
done
