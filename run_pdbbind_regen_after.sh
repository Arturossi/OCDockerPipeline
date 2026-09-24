#!/usr/bin/env bash
# Waits for the running PDBbind (valid-SMILES) job to finish, then docks the
# complexes whose ligand.smi was regenerated from the crystal structure.
set -u
cd /data/hd4tb/OCDocker/OCDockerPipeline
LOG_DIR=/data/hd4tb/OCDocker/OCDockerPipeline/batch_loop_logs
while pgrep -f "snakemake -s snakefile.*database_sources=PDBbind" > /dev/null; do sleep 60; done
echo "$(date '+%F %T') starting regenerated-SMILES PDBbind run" >> "$LOG_DIR/pdbbind_progress.log"
/home/artur/miniconda3/bin/conda run -n ocdocker snakemake -s snakefile \
  $(cat "$LOG_DIR/pdbbind_regen_smi_targets.txt") \
  --retries 3 --cores 18 --resources mem_mb=26000 --keep-going --rerun-triggers mtime \
  --config database_sources=PDBbind pipeline_export_database_csv=false \
  > "$LOG_DIR/pdbbind_regen_smi_run.out" 2>&1
echo "$(date '+%F %T') regenerated-SMILES PDBbind run exited ($?)" >> "$LOG_DIR/pdbbind_progress.log"
