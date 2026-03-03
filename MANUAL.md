# OCDockerPipeline Manual

This manual documents practical execution and monitoring commands for the
Snakemake-based `OCDockerPipeline`.

## Prerequisites

- Conda environment with Snakemake and OCDocker available.
- `OCDocker.cfg` configured for tools and database.
- `config.yaml` configured for selected datasets/engines/resources.

## Run

Dry-run:

```bash
snakemake -s snakefile -n --cores 1
```

Full execution:

```bash
snakemake -s snakefile --cores 16 --resources mem_mb=28000 --use-conda --conda-frontend mamba --keep-going
```

Notes:

- Pose generation (docking) is restricted to `vina`, `gnina`, and `plants`.
- Docking uses each engine's default scoring function only.
- Rescoring always includes `vina,smina,gnina,plants,oddt` with each engine's configured scoring-function set.
- With `pipeline_store_db: true`, post-processing persists both mapped numeric scores (`complexes`) and rich run metadata (`pipelineruns`) including selected representative pose and full rescoring JSON.
- With `pipeline_store_db_mid_execution: true`, `run_engine` also writes per-engine progress snapshots into `pipelineruns` (`<job>__progress__<engine>`).
- GPU scheduling is resource-aware: `run_engine` requests `gpu` per engine (typically `gnina: 1`) and uses `pipeline_total_gpus` as global scheduler limit.
- Mixed-engine fairness is configurable: use `pipeline_engine_priority` and `pipeline_engine_max_parallel` in `config.yaml` to prevent single-engine queue flooding.

Single-target execution example:

```bash
snakemake -s snakefile --cores 8 --resources mem_mb=12000 \
  /data/hd4tb/OCDocker/data/ocdb2/PDBbind/2wlz/compounds/ligands/ligand/payload.pkl
```

## Monitoring with snkmt

Run with Snakemake logger plugin enabled:

```bash
snakemake -s snakefile \
  --logger snkmt \
  --logger-snkmt-db .snakemake/snkmt.db \
  --cores 16 --resources mem_mb=28000 --use-conda --conda-frontend mamba --keep-going
```

Open Snakemate console in another terminal:

```bash
snkmt console --db-path .snakemake/snkmt.db
```

Install plugin if needed:

```bash
pip install snakemake-logger-plugin-snkmt
```

## Key Outputs

For each target under `<ocdb>/<database>/<receptor>/compounds/<kind>/<target>/`:

- `engine_status/<engine>.json`
- `summary.json` (or `box*/summary.json` when `pipeline_all_boxes: true`)
- `payload.pkl`
- `run_report.json`

For each selected database under `<ocdb>/<database>/`:

- `pipeline_results.csv` (consolidated per-target export)
