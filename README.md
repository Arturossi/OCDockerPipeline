# OCDockerPipeline

Snakemake workflow for running OCDocker using the Python API (no CLI subprocess).
Each ligand entry is processed with:

- in-process engine execution via `OCDocker.Docking` classes
- per-engine Snakemake jobs that can run concurrently
- shared, lock-safe preparation artifacts reused across engines
- clustering/rescoring + PostgreSQL persistence in a downstream aggregation step

Outputs are written under the `ocdb` root defined in `OCDocker.cfg`.

## Prerequisites

- Python environment with Snakemake.
- OCDocker installed in the same environment.
- External tools configured in `OCDocker.cfg` (Vina, Smina, Gnina, PLANTS, MGLTools/OpenBabel, etc.).
- PostgreSQL configured in `OCDocker.cfg` (`HOST`, `USER`, `PASSWORD`, `DATABASE`, `PORT`).

## Installation

1. Create the pipeline environment:

```bash
conda env create -f envs/ocdocker.yaml
conda activate ocdocker
```

2. Install OCDocker in the same environment:

```bash
pip install -e ../OCDocker
```

## Configuration

### `config.yaml`

- `db_backend`: must be `postgresql` (the Snakefile enforces this).
- `run_databases`: databases to process (`PDBbind`, `DUDEz`).
- `compound_kinds`: kinds to process (`ligands`, `decoys`, `compounds`).
- `pipeline_cluster_min`, `pipeline_cluster_max`, `pipeline_cluster_step`.
- `pipeline_all_boxes`, `pipeline_timeout` (optional).
- `pipeline_engine_threads*`, `pipeline_engine_mem_mb*` to tune per-engine scheduling.
- `pipeline_postprocess_threads`, `pipeline_postprocess_mem_mb` for aggregation stage resources.
- Optional:
  - `pipeline_engines`: explicit docking engines.
  - `pipeline_rescoring_engines`: explicit rescoring engines.

If `pipeline_engines` is omitted, engines are auto-detected from executable paths in `OCDocker.cfg`.
If `pipeline_rescoring_engines` is omitted, it defaults to docking engines plus `oddt`.

### `OCDocker.cfg`

- `ocdb`: root directory for dataset storage and results.
- Tool paths: `vina`, `smina`, `gnina`, `plants`, `prepare_ligand`, `prepare_receptor`, etc.
- Scoring function lists as needed by OCDocker.
- PostgreSQL connection fields (`HOST`, `USER`, `PASSWORD`, `DATABASE`, `PORT`).

If `OCDocker.cfg` is not in the pipeline root, set:

```bash
export OCDOCKER_CONFIG=/path/to/OCDocker.cfg
```

## Workflow

For each `{database}/{receptor}/compounds/{kind}/{target}` entry, the workflow:

1. Validates and caches receptor preparation (`prepare_receptor_cache`).
2. Prepares and caches shared ligand artifacts (`prepare_ligand_cache`).
3. Runs one `run_engine` job per selected engine (`vina`, `smina`, `gnina`, `plants`).
4. Writes per-engine status files to `engine_status/{engine}.json`.
5. Runs `run_pipeline` to aggregate engine outputs, cluster poses, select representative pose, and rescore.
6. Stores descriptors/scores in PostgreSQL and writes `summary.json` + `payload.pkl`.

## Execution model

DAG for one target:

- `prepare_receptor_cache`
- `prepare_ligand_cache`
- `run_engine[vina]`, `run_engine[smina]`, `run_engine[gnina]`, `run_engine[plants]` (subset depends on `pipeline_engines`)
- `run_pipeline` (fan-in from all generated `engine_status/*.json`)

Because engines are separate jobs, they can run at the same time when `--cores` allows it.

## Cache and concurrency

- Receptor preparation cache is keyed by pipeline settings and receptor metadata.
- Shared preparation files use lock files (`.prepared_*.lock`) to avoid race conditions.
- Each engine writes to isolated engine output folders and its own `engine_status/{engine}.json`.
- This avoids duplicated preparation work while keeping engine execution parallel.

## Run

Dry run:

```bash
snakemake -s snakefile -n --cores 1
```

Execute:

```bash
snakemake -s snakefile --cores 16 --use-conda --conda-frontend mamba --keep-going
```

Recommended production run (this host profile, GNINA GPU enabled):

```bash
snakemake -s snakefile --cores 16 --resources mem_mb=28000 --use-conda --conda-frontend mamba --keep-going
```

Why `--resources mem_mb=28000`: rule-level `mem_mb` values are applied only when a global
resource budget is provided; this keeps concurrent engine jobs within RAM capacity.

Database-only target preparation:

```bash
snakemake -s snakefile db_pdbbind --cores 8
snakemake -s snakefile db_dudez --cores 8
```

CI dry-run DAG check:

```bash
bash ci/test_engine_dag_dryrun.sh
```

## Notes

- Pipeline execution is fully API-driven; no CLI command invocation is required.
- Receptor preparation is cached per receptor and reused across ligand targets.
- Per-engine summaries are explicit pipeline artifacts and can be inspected independently.
- Database writes are done through OCDocker DB models during post-processing.
