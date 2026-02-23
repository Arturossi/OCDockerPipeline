#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  bash test_files/clean_smoke_outputs.sh <receptor_root>

Example:
  bash test_files/clean_smoke_outputs.sh /tmp/oc_smoke/ocdb/PDBbind/test_ptn1

This removes generated pipeline outputs while keeping fixture inputs
(e.g., receptor.pdb, ligand.smi, boxes/box0.pdb).
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

if [[ "$#" -ne 1 ]]; then
    usage >&2
    exit 1
fi

TARGET_ROOT="${1%/}"

if [[ ! -d "${TARGET_ROOT}" ]]; then
    echo "Target root not found: ${TARGET_ROOT}" >&2
    exit 1
fi

if [[ "${TARGET_ROOT}" == "/" || "${TARGET_ROOT}" == "." ]]; then
    echo "Refusing to clean unsafe target root: ${TARGET_ROOT}" >&2
    exit 1
fi

echo "Cleaning generated artifacts under: ${TARGET_ROOT}"

find "${TARGET_ROOT}" -type f \
    \( \
        -name 'payload.pkl' -o \
        -name 'summary.json' -o \
        -name 'run_report.json' -o \
        -name 'cluster_assignments.csv' -o \
        -name 'clustering_dendrogram.png' -o \
        -name 'clustering_dendrogram_labels.txt' -o \
        -name 'clustering_info.json' -o \
        -name 'pose_list_single.txt' -o \
        -name 'rmsd_matrix.csv' -o \
        -name 'representative.mol2' -o \
        -name 'representative.pdbqt' -o \
        -name 'representative_for_vina_smina.pdbqt' -o \
        -name 'prepared_ligand.pdbqt' -o \
        -name 'prepared_ligand.mol2' -o \
        -name 'ligand.mol2' -o \
        -name 'prepared_receptor.pdbqt' -o \
        -name 'prepared_receptor.mol2' -o \
        -name '.prepared_receptor_cache.*.json' -o \
        -name '.prepared_ligand_cache.*.json' -o \
        -name '.prepared_ligand.pdbqt.lock' -o \
        -name '.prepared_ligand.mol2.lock' -o \
        -name '.prepared_receptor.pdbqt.lock' -o \
        -name '.prepared_receptor.mol2.lock' \
    \) \
    -print -delete

find "${TARGET_ROOT}" -type d \
    \( \
        -name '.ligand_prep' -o \
        -name 'engine_status' -o \
        -name 'vinaFiles' -o \
        -name 'sminaFiles' -o \
        -name 'gninaFiles' -o \
        -name 'plantsFiles' \
    \) \
    -print -exec rm -rf {} +

echo "Cleanup finished."
