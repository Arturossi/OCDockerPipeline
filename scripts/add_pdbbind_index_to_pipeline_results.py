#!/usr/bin/env python3
"""Merge PDBbind index metadata into an OCDocker pipeline_results.csv."""

import argparse
import csv
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from OCDocker.Config import OCDockerConfig, set_config
import OCDocker.DB.PDBbind as ocpdbbind

INDEX_COLUMNS = [
    "Protein",
    "resolution",
    "release_year",
    "-logKd/Ki",
    "Ki/Kd",
    "Ki/Kd_relation",
    "Ki/Kd_value",
    "Ki/Kd_order",
    "Ki/Kd_raw_value",
    "Ki/Kd_raw_unit",
    "dG",
    "dG_kcal_mol",
    "reference",
    "ligand_name",
    "index_comment",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Append parsed PDBbind INDEX_refined_data fields to pipeline_results.csv.",
    )
    parser.add_argument("pipeline_csv", type=Path, help="Path to pipeline_results.csv.")
    parser.add_argument("index_file", type=Path, help="Path to INDEX_refined_data.* file.")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output CSV path. Defaults to <pipeline_csv stem>_with_pdbbind_index.csv unless --in-place is used.",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Replace pipeline_csv atomically after writing a temporary merged file.",
    )
    parser.add_argument(
        "--key-column",
        default="receptor",
        help="Pipeline CSV column containing the PDB code. Default: receptor.",
    )
    parser.add_argument(
        "--kdki-order",
        default="M",
        help="Output unit for Ki/Kd_value. Use M for molar, u for uM, n for nM, etc. Default: M.",
    )
    parser.add_argument(
        "--no-experimental",
        action="store_true",
        help="Do not add the experimental column from dG_kcal_mol when it is absent.",
    )
    parser.add_argument(
        "--overwrite-existing-columns",
        action="store_true",
        help="Overwrite existing index/experimental columns if they already exist.",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="When --in-place is used, skip creating a timestamped .bak copy first.",
    )
    return parser


def configure_parser(kdki_order: str) -> None:
    config = OCDockerConfig()
    config.paths.pdbbind_kdki_order = kdki_order
    set_config(config)


def load_index(index_file: Path, kdki_order: str) -> dict[str, dict[str, str | float]]:
    configure_parser(kdki_order)
    parsed = ocpdbbind.read_index(str(index_file))
    if parsed is None:
        raise FileNotFoundError(f"Could not read PDBbind index file: {index_file}")
    return {pdb_id.lower(): entry for pdb_id, entry in parsed.items()}


def stringify(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def default_output_path(pipeline_csv: Path) -> Path:
    return pipeline_csv.with_name(f"{pipeline_csv.stem}_with_pdbbind_index{pipeline_csv.suffix}")


def backup_path(pipeline_csv: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return pipeline_csv.with_name(f"{pipeline_csv.name}.bak.{timestamp}")


def merge_csv(
    pipeline_csv: Path,
    output_csv: Path,
    index_data: dict[str, dict[str, str | float]],
    key_column: str,
    add_experimental: bool,
    overwrite_existing_columns: bool,
) -> tuple[int, int]:
    with pipeline_csv.open("r", newline="") as in_handle:
        reader = csv.DictReader(in_handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {pipeline_csv}")
        if key_column not in reader.fieldnames:
            raise KeyError(f"Key column '{key_column}' not found in {pipeline_csv}")

        existing_columns = list(reader.fieldnames)
        fieldnames = existing_columns[:]
        for column in INDEX_COLUMNS:
            if column not in fieldnames:
                fieldnames.append(column)
        if add_experimental and "experimental" not in fieldnames:
            fieldnames.append("experimental")

        output_csv.parent.mkdir(parents=True, exist_ok=True)
        with output_csv.open("w", newline="") as out_handle:
            writer = csv.DictWriter(out_handle, fieldnames=fieldnames)
            writer.writeheader()
            matched = 0
            missing = 0

            for row in reader:
                pdb_id = str(row.get(key_column, "")).lower()
                entry = index_data.get(pdb_id)
                if entry is None:
                    missing += 1
                else:
                    matched += 1
                    for column in INDEX_COLUMNS:
                        if column in existing_columns and not overwrite_existing_columns:
                            continue
                        row[column] = stringify(entry.get(column, ""))
                    if add_experimental and (
                        "experimental" not in existing_columns or overwrite_existing_columns
                    ):
                        row["experimental"] = stringify(entry.get("dG_kcal_mol", ""))
                writer.writerow(row)

    return matched, missing


def main() -> int:
    args = build_parser().parse_args()
    pipeline_csv = args.pipeline_csv.resolve()
    index_file = args.index_file.resolve()

    if not pipeline_csv.is_file():
        raise FileNotFoundError(f"Pipeline CSV not found: {pipeline_csv}")
    if not index_file.is_file():
        raise FileNotFoundError(f"PDBbind index file not found: {index_file}")

    if args.in_place:
        final_output = pipeline_csv
    else:
        final_output = (args.output or default_output_path(pipeline_csv)).resolve()

    temp_output = final_output
    if final_output == pipeline_csv:
        temp_output = pipeline_csv.with_name(f".{pipeline_csv.name}.tmp")

    index_data = load_index(index_file, args.kdki_order)
    matched, missing = merge_csv(
        pipeline_csv=pipeline_csv,
        output_csv=temp_output,
        index_data=index_data,
        key_column=args.key_column,
        add_experimental=not args.no_experimental,
        overwrite_existing_columns=args.overwrite_existing_columns,
    )

    backup = None
    if final_output == pipeline_csv:
        if not args.no_backup:
            backup = backup_path(pipeline_csv)
            shutil.copy2(pipeline_csv, backup)
        os.replace(temp_output, pipeline_csv)

    print(f"Parsed index entries: {len(index_data)}")
    print(f"Matched CSV rows: {matched}")
    print(f"Rows without index match: {missing}")
    print(f"Output CSV: {final_output}")
    if backup is not None:
        print(f"Backup CSV: {backup}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
