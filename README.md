# OCDockerPipeline

Snakemake workflow for running the OCDocker docking/rescoring pipeline against
PDBbind and DUDEz datasets. Outputs are written under the `ocdb` root defined
in `OCDocker.cfg`.

## Prerequisites
- Python environment with Snakemake.
- OCDocker installed as a Python package.
- External tools configured in `OCDocker.cfg` (MGLTools, Vina, Smina, PLANTS, etc.).
- Database backend: MySQL (default) or SQLite (optional, via `USE_SQLITE`).

## Installation
1) Create the pipeline environment:
```bash
conda env create -f envs/ocdocker.yaml
conda activate ocdocker
```

2) Install OCDocker into the same environment (editable recommended):
```bash
pip install -e ../OCDocker
```
If you do not have the repo locally:
```bash
git clone https://github.com/Arturossi/OCDocker.git
pip install -e OCDocker
```

## Configuration
### Pipeline settings (`config.yaml`)
- `cpu_cores`: total cores available to Snakemake.
- `logDir`: location for pipeline logs.
- `pdbbindTarGzPath`, `pdb_database_index`, `ignored_pdb_database_index`.
- `dudez_database_index`, `ignored_dudez_database_index`.

### OCDocker settings (`OCDocker.cfg`)
- `ocdb`: root directory for dataset storage and results.
- Tool paths: `vina`, `smina`, `plants`, `prepare_ligand`, `prepare_receptor`, etc.
- Scoring function lists: `vina_scoring_functions`, `smina_scoring_functions`, `plants_scoring_functions`.
- Database: set `HOST/USER/PASSWORD/DATABASE/PORT` or enable SQLite with:
```
USE_SQLITE = yes
SQLITE_PATH = /path/to/ocdocker.db
```

If `OCDocker.cfg` is not in the pipeline root, set:
```bash
export OCDOCKER_CONFIG=/path/to/OCDocker.cfg
```

## Workflow Overview
The pipeline performs, in order:
1) Prepare database folders (`DUDEz`, `PDBbind`) under `ocdb`.
2) Prepare input structures and ligand sets for each target.
3) Dock with Vina and PLANTS.
4) Cluster poses and rescore with Vina, Smina, and PLANTS.
5) Store outputs under `ocdb` and emit `payload.pkl` per ligand.

## Run
Dry run:
```bash
snakemake -s snakefile -n --cores 1
```

Execute:
```bash
snakemake -s snakefile --cores 16 --use-conda --conda-frontend mamba --keep-going
```

Targeted runs:
```bash
snakemake -s snakefile db_pdbbind --cores 8
snakemake -s snakefile db_dudez --cores 8
```

## Snakemake Monitoring (WMS)
Monitoring is provided via plugins and enabled with `--logger`.

### Live monitoring (recommended)
1) Install the logger plugin:
```bash
pip install snakemake-logger-plugin-snkmt
```

2) Run Snakemake with the logger enabled:
```bash
snakemake -s snakefile --cores 20 --use-conda --conda-frontend mamba --keep-going \
  --logger snkmt
```

3) Open the snkmt dashboard URL printed in the Snakemake output.

If running remotely, forward the port:
```bash
ssh -L 5000:127.0.0.1:5000 user@remote-host
```

### Companion CLI views (quick status and graphs)
These are optional but useful alongside live monitoring:
```bash
snakemake -s snakefile --summary
snakemake -s snakefile --detailed-summary
snakemake -s snakefile --dag | dot -Tpng > dag.png
snakemake -s snakefile --rulegraph | dot -Tpng > rulegraph.png
```
