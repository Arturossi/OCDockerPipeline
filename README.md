# OCDockerPipeline

Snakemake workflow for running OCDocker through the Python API (without per-target CLI subprocess calls).
The pipeline is structured for multi-engine fan-out, shared preparation caching, and traceable post-processing.

## Overview

For each `{database}/{receptor}/compounds/{kind}/{target}` entry, the workflow:

1. Validates and caches receptor preparation (`prepare_receptor_cache`).
2. Prepares and caches ligand artifacts shared across engines (`prepare_ligand_cache`).
3. Runs one independent `run_engine` job per selected docking engine.
4. Aggregates in `run_pipeline`, performs clustering/rescoring, and optionally writes DB rows.
5. Writes `summary.json`, `payload.pkl`, and `run_report.json`.

Outputs are written under the `ocdb` root configured in `OCDocker.cfg`.

## Requirements

- Python environment with Snakemake.
- OCDocker installed in the same environment.
- External tools configured in `OCDocker.cfg` (`vina`, `smina`, `gnina`, `plants`, OpenBabel/MGLTools, etc.).
- PostgreSQL configured in `OCDocker.cfg` (`HOST`, `USER`, `PASSWORD`, `DATABASE`, `PORT`).

## Installation

1. Create the environment:

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

Core keys:

- `db_backend`: must be `postgresql`.
- `run_databases`: databases to process (`PDBbind`, `DUDEz`).
- `compound_kinds`: kinds to process (`ligands`, `decoys`, `compounds`).
- `target_discovery_mode`: `index`, `filesystem`, or `hybrid`.
- `pipeline_engines`: optional explicit docking engines.
- `pipeline_rescoring_engines`: optional explicit rescoring engines.
- `pipeline_store_db`: enable/disable DB writes.
- `pipeline_cluster_min`, `pipeline_cluster_max`, `pipeline_cluster_step`.
- `pipeline_all_boxes`: when `true`, process all `box*.pdb` files for each target.
- `pipeline_timeout`: optional timeout propagated to OCDocker API calls.
- `pipeline_engine_threads*`, `pipeline_engine_mem_mb*`: per-engine resource settings.
- `pipeline_postprocess_threads`, `pipeline_postprocess_mem_mb`: post-processing resources.
- `pipeline_report_include_python_packages`: include full installed package list in `run_report.json`.

Behavior:

- If `pipeline_engines` is omitted, engines are auto-detected from `OCDocker.cfg` executables.
- If `pipeline_rescoring_engines` is omitted, it defaults to `pipeline_engines + oddt`.

### `OCDocker.cfg`

Important keys:

- `ocdb`: root directory for dataset storage and results.
- executable paths for docking/preparation tools.
- PostgreSQL connection fields.

If `OCDocker.cfg` is not in the pipeline root, set:

```bash
export OCDOCKER_CONFIG=/path/to/OCDocker.cfg
```

## Execution Model

DAG per target:

- `prepare_receptor_cache`
- `prepare_ligand_cache`
- `run_engine[...]` (one job per selected engine)
- `run_pipeline` (fan-in from `engine_status/*.json`)

Because engines are independent jobs, they can run concurrently when `--cores` allows.

## Run

Dry-run:

```bash
snakemake -s snakefile -n --cores 1
```

Execute:

```bash
snakemake -s snakefile --cores 16 --use-conda --conda-frontend mamba --keep-going
```

Recommended production shape on this host:

```bash
snakemake -s snakefile --cores 16 --resources mem_mb=28000 --use-conda --conda-frontend mamba --keep-going
```

Why `--resources mem_mb=28000`: rule-level `mem_mb` limits are enforced only when a global resource budget is provided.

Database-only target preparation:

```bash
snakemake -s snakefile db_pdbbind --cores 8
snakemake -s snakefile db_dudez --cores 8
```

CI dry-run check:

```bash
bash ci/test_engine_dag_dryrun.sh
```

## Outputs

Per target (`ocdb/<database>/<receptor>/compounds/<kind>/<target>/`):

- `engine_status/<engine>.json`: per-engine execution summary.
- `summary.json` (or `box*/summary.json` with `pipeline_all_boxes: true`): post-processing summary.
- `payload.pkl`: main target artifact used by Snakemake rules.
- `run_report.json`: reproducibility report for the target.

## Reproducibility Report

`run_report.json` is generated in `run_pipeline` for every target and records:

- job identity and pipeline version/cache key.
- workflow/config fingerprints and effective runtime config hash.
- Python/platform/runtime environment and pipeline git metadata.
- fingerprints of key inputs/outputs (size, mtime, SHA256 where applicable).
- canonical SHA256 digest of the final `summary` payload.
- OCDocker reproducibility manifest (tools/runtime/git), with optional full package list.

This report is meant to support provenance tracking and reproducible reruns.

## Versioning

- Pipeline version is defined in `OCDP/_version.py` (`__version__`).
- The same value is propagated to `engine_status/*.json`, `summary.json`, `payload.pkl`, and `run_report.json`.

## Notes

- Pipeline execution is fully API-driven.
- Receptor preparation is cached per receptor and reused across targets.
- Shared preparation files use lock files (`.prepared_*.lock`) to avoid race conditions.
- Database writes are handled through OCDocker DB models during post-processing.
