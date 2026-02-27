#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_ROOT="$(mktemp -d)"

cleanup() {
    rm -rf "${TMP_ROOT}"
}
trap cleanup EXIT

OCDB_DIR="${TMP_ROOT}/ocdb"
TARGET_DIR="${OCDB_DIR}/PDBbind/TEST1/compounds/ligands/ligand"
BOX_DIR="${TARGET_DIR}/boxes"
WORK_DIR="${TMP_ROOT}/workdir"

mkdir -p "${BOX_DIR}" "${WORK_DIR}" "${TMP_ROOT}/logs"

cat > "${OCDB_DIR}/PDBbind/TEST1/receptor.pdb" <<'EOF'
ATOM      1  N   ALA A   1       0.000   0.000   0.000  1.00  0.00           N
TER
END
EOF

cat > "${TARGET_DIR}/ligand.smi" <<'EOF'
CCO
EOF

cat > "${BOX_DIR}/box0.pdb" <<'EOF'
HEADER    TEST BOX
REMARK    CENTER (X Y Z)      0.000   0.000   0.000
REMARK    DIMENSIONS (X Y Z)  20.000  20.000  20.000
END
EOF

cp "${ROOT_DIR}/OCDocker.cfg" "${TMP_ROOT}/OCDocker.cfg"
sed -i "s|^ocdb = .*|ocdb = ${OCDB_DIR}|" "${TMP_ROOT}/OCDocker.cfg"
sed -i "s|^vina = .*|vina = /bin/true|" "${TMP_ROOT}/OCDocker.cfg"
sed -i "s|^smina = .*|smina = /bin/true|" "${TMP_ROOT}/OCDocker.cfg"
sed -i "s|^gnina = .*|gnina = /bin/true|" "${TMP_ROOT}/OCDocker.cfg"
sed -i "s|^plants = .*|plants = /bin/true|" "${TMP_ROOT}/OCDocker.cfg"

cat > "${TMP_ROOT}/config.yaml" <<EOF
cpu_cores: 1
overwrite: false
db_backend: "postgresql"

run_databases:
  - "PDBbind"
compound_kinds:
  - "ligands"
target_discovery_mode: "filesystem"
enable_legacy_database_rules: false

pipeline_engines: ["vina", "gnina"]
pipeline_rescoring_engines: ["vina", "smina", "oddt"]
pipeline_store_db: false

pipeline_engine_threads_default: 1
pipeline_engine_mem_mb_default: 2000
pipeline_postprocess_threads: 1
pipeline_postprocess_mem_mb: 4000

pipeline_cluster_min: 10.0
pipeline_cluster_max: 20.0
pipeline_cluster_step: 0.1
pipeline_all_boxes: false

logprefix: ""
logDir: "${TMP_ROOT}/logs"
log_level: "info"
log_handler: "console"

pdbbindTarGzPath: ""
pdb_database_index: ""
ignored_pdb_database_index: ""
dudez_database_index: ""
ignored_dudez_database_index: ""
EOF

PAYLOAD_PATH="${TARGET_DIR}/payload.pkl"

set +e
OUTPUT="$(
    OCDOCKER_CONFIG="${TMP_ROOT}/OCDocker.cfg" \
    XDG_CACHE_HOME="${TMP_ROOT}/.cache" \
    TMPDIR="${TMP_ROOT}" \
    snakemake \
        -s "${ROOT_DIR}/snakefile" \
        --directory "${WORK_DIR}" \
        -n \
        --cores 1 \
        --configfile "${TMP_ROOT}/config.yaml" \
        -- \
        "${PAYLOAD_PATH}" \
        2>&1
)"
STATUS=$?
set -e

printf '%s\n' "${OUTPUT}"

if [[ "${STATUS}" -ne 0 ]]; then
    echo "Dry-run command failed unexpectedly."
    exit "${STATUS}"
fi

if ! printf '%s\n' "${OUTPUT}" | grep -q "^rule prepare_receptor_cache:"; then
    echo "Expected rule prepare_receptor_cache was not scheduled."
    exit 1
fi

if ! printf '%s\n' "${OUTPUT}" | grep -q "^rule prepare_ligand_cache:"; then
    echo "Expected rule prepare_ligand_cache was not scheduled."
    exit 1
fi

RUN_ENGINE_COUNT="$(printf '%s\n' "${OUTPUT}" | grep -c "^rule run_engine:")"
if [[ "${RUN_ENGINE_COUNT}" -ne 2 ]]; then
    echo "Expected exactly 2 run_engine jobs, got ${RUN_ENGINE_COUNT}."
    exit 1
fi

if ! printf '%s\n' "${OUTPUT}" | grep -q "^rule run_pipeline:"; then
    echo "Expected rule run_pipeline was not scheduled."
    exit 1
fi

if ! printf '%s\n' "${OUTPUT}" | grep -q "engine_status/vina.json"; then
    echo "Expected vina engine summary dependency not found in dry-run output."
    exit 1
fi

if ! printf '%s\n' "${OUTPUT}" | grep -q "engine_status/gnina.json"; then
    echo "Expected gnina engine summary dependency not found in dry-run output."
    exit 1
fi

echo "Engine DAG dry-run check passed."
