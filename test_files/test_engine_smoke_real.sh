#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE_DIR="${ROOT_DIR}/test_files/test_ptn1"
CONDA_ENV="${OCDOCKER_CONDA_ENV:-ocdocker}"
ENGINE="plants"
WORK_ROOT=""
KEEP_WORKDIR=false
CLEAN_AFTER=false

usage() {
    cat <<'USAGE'
Usage:
  bash test_files/test_engine_smoke_real.sh [options]

Options:
  --engine <name>      Docking engine for smoke run (default: plants)
  --work-root <dir>    Keep run workspace in this directory
  --keep-workdir       Keep temporary workspace instead of auto-deleting it
  --clean              Remove generated artifacts after run (uses clean_smoke_outputs.sh)
  -h, --help           Show help

Notes:
  - This runs a real (non-mocked) smoke test using fixture files in:
    test_files/test_ptn1
  - Default engine is plants because it is stable for this fixture.
USAGE
}

while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --engine)
            ENGINE="${2:-}"
            shift 2
            ;;
        --work-root)
            WORK_ROOT="${2:-}"
            shift 2
            ;;
        --keep-workdir)
            KEEP_WORKDIR=true
            shift
            ;;
        --clean)
            CLEAN_AFTER=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

case "${ENGINE}" in
    vina|gnina|plants)
        ;;
    *)
        echo "Unsupported engine '${ENGINE}'. Use one of: vina, gnina, plants." >&2
        exit 1
        ;;
esac

if [[ ! -d "${FIXTURE_DIR}" ]]; then
    echo "Fixture directory not found: ${FIXTURE_DIR}" >&2
    exit 1
fi

AUTO_REMOVE=false
if [[ -z "${WORK_ROOT}" ]]; then
    WORK_ROOT="$(mktemp -d)"
    AUTO_REMOVE=true
else
    mkdir -p "${WORK_ROOT}"
fi

if [[ "${KEEP_WORKDIR}" == true ]]; then
    AUTO_REMOVE=false
fi

cleanup() {
    if [[ "${AUTO_REMOVE}" == true ]]; then
        rm -rf "${WORK_ROOT}"
    fi
}
trap cleanup EXIT

OCDB_DIR="${WORK_ROOT}/ocdb"
TARGET_RECEPTOR_ROOT="${OCDB_DIR}/PDBbind/test_ptn1"
TARGET_DIR="${TARGET_RECEPTOR_ROOT}/compounds/ligands/ligand"
PAYLOAD_PATH="${TARGET_DIR}/payload.pkl"
WORK_DIR="${WORK_ROOT}/workdir"
LOG_DIR="${WORK_ROOT}/logs"
RUN_CONFIG="${WORK_ROOT}/OCDocker.cfg"
RUN_CONFIG_YAML="${WORK_ROOT}/config.yaml"

mkdir -p "${OCDB_DIR}/PDBbind" "${WORK_DIR}" "${LOG_DIR}"
rm -rf "${TARGET_RECEPTOR_ROOT}"
cp -a "${FIXTURE_DIR}" "${TARGET_RECEPTOR_ROOT}"

cp "${ROOT_DIR}/OCDocker.cfg" "${RUN_CONFIG}"
sed -i "s|^ocdb = .*|ocdb = ${OCDB_DIR}|" "${RUN_CONFIG}"

cat > "${RUN_CONFIG_YAML}" <<EOF_CFG
cpu_cores: 1
overwrite: true
db_backend: "postgresql"

database_sources:
  - "PDBbind"
compound_kinds:
  - "ligands"
target_discovery_mode: "filesystem"
enable_legacy_database_rules: false

pipeline_engines: ["${ENGINE}"]
pipeline_rescoring_engines: ["${ENGINE}"]
pipeline_store_db: false
pipeline_report_include_python_packages: false

pipeline_engine_threads_default: 1
pipeline_engine_mem_mb_default: 2000
pipeline_postprocess_threads: 1
pipeline_postprocess_mem_mb: 3000

pipeline_cluster_min: 10.0
pipeline_cluster_max: 20.0
pipeline_cluster_step: 0.1
pipeline_all_boxes: false

logprefix: ""
logDir: "${LOG_DIR}"
log_level: "info"
log_handler: "console"

pdbbindTarGzPath: ""
pdb_database_index: ""
ignored_pdb_database_index: ""
dudez_database_index: ""
ignored_dudez_database_index: ""
EOF_CFG

echo "Running real smoke test (engine=${ENGINE}) using fixture test_files/test_ptn1 ..."

OCDOCKER_CONFIG="${RUN_CONFIG}" \
XDG_CACHE_HOME="${WORK_ROOT}/.cache" \
TMPDIR="${WORK_ROOT}" \
conda run -n "${CONDA_ENV}" snakemake \
    -s "${ROOT_DIR}/snakefile" \
    --directory "${WORK_DIR}" \
    --cores 1 \
    --configfile "${RUN_CONFIG_YAML}" \
    -- \
    "${PAYLOAD_PATH}"

ENGINE_STATUS="${TARGET_DIR}/engine_status/${ENGINE}.json"
SUMMARY_JSON="${TARGET_DIR}/summary.json"
RUN_REPORT_JSON="${TARGET_DIR}/run_report.json"

for required in "${ENGINE_STATUS}" "${SUMMARY_JSON}" "${PAYLOAD_PATH}" "${RUN_REPORT_JSON}"; do
    if [[ ! -f "${required}" ]]; then
        echo "Missing expected output: ${required}" >&2
        exit 1
    fi
done

python - <<'PY' "${TARGET_DIR}" "${ENGINE}"
import json
import pathlib
import pickle
import sys

target_dir = pathlib.Path(sys.argv[1])
engine = sys.argv[2]

engine_status = json.loads((target_dir / "engine_status" / f"{engine}.json").read_text(encoding="utf-8"))
summary = json.loads((target_dir / "summary.json").read_text(encoding="utf-8"))
run_report = json.loads((target_dir / "run_report.json").read_text(encoding="utf-8"))

with (target_dir / "payload.pkl").open("rb") as handle:
    payload = pickle.load(handle)

box0 = engine_status.get("boxes", {}).get("box0", {})
rescoring = summary.get("rescoring", {}).get(engine, {})

print("Smoke test completed successfully.")
print(f"Target directory: {target_dir}")
print(f"Engine box0 success: {box0.get('success')}")
print(f"Representative pose: {summary.get('representative_pose')}")
print(f"Clustered poses: {summary.get('clustering', {}).get('total_poses')}")
if rescoring:
    print("Rescoring:")
    for key, value in sorted(rescoring.items()):
        print(f"  - {key}: {value}")
print(f"summary_sha256: {run_report.get('summary_sha256')}")
print(f"payload keys: {sorted(payload.keys())}")
PY

if [[ "${CLEAN_AFTER}" == true ]]; then
    bash "${ROOT_DIR}/test_files/clean_smoke_outputs.sh" "${TARGET_RECEPTOR_ROOT}"
fi

if [[ "${AUTO_REMOVE}" == false ]]; then
    echo
    echo "Workspace kept at: ${WORK_ROOT}"
    if [[ "${CLEAN_AFTER}" == true ]]; then
        echo "Generated artifacts were removed. Fixture inputs remain under: ${TARGET_RECEPTOR_ROOT}"
    else
        echo "Main outputs:"
        echo "  - ${ENGINE_STATUS}"
        echo "  - ${SUMMARY_JSON}"
        echo "  - ${PAYLOAD_PATH}"
        echo "  - ${RUN_REPORT_JSON}"
        echo "To clean generated files later:"
        echo "  bash ${ROOT_DIR}/test_files/clean_smoke_outputs.sh ${TARGET_RECEPTOR_ROOT}"
    fi
fi
