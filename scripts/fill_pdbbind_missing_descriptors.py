#!/usr/bin/env python3
"""Fill missing PDBbind molecular descriptors and patch pipeline_results.csv."""

from __future__ import annotations

import argparse
import contextlib
import csv
import json
import os
import shutil
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate missing OCDocker molecular descriptor JSONs for PDBbind rows and patch the CSV.",
    )
    parser.add_argument("pipeline_csv", type=Path)
    parser.add_argument("pdbbind_root", type=Path)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N targets, for testing.")
    parser.add_argument("--targets", nargs="*", default=None, help="Specific PDB IDs to process.")
    parser.add_argument("--log-dir", type=Path, default=Path("/tmp/ocdocker_pdbbind_descriptor_fill"))
    parser.add_argument("--status-jsonl", type=Path, default=None)
    parser.add_argument("--no-patch-csv", action="store_true")
    parser.add_argument("--overwrite-csv-columns", action="store_true")
    parser.add_argument("--include-unscored", action="store_true", help="Also target rows without score values.")
    parser.add_argument("--overwrite-descriptors", action="store_true", help="Pass overwrite=True to OCDocker prepare.")
    return parser


def descriptor_columns(fieldnames: list[str]) -> tuple[list[str], list[str], list[str]]:
    score_prefixes = ("smina_", "vina_", "gnina_", "plants_", "oddt_")
    first_score_idx = min(
        (idx for idx, col in enumerate(fieldnames) if col.startswith(score_prefixes)),
        default=len(fieldnames),
    )
    receptor_cols = [col for col in fieldnames if col.startswith("receptor_")]
    ligand_cols = [
        col
        for idx, col in enumerate(fieldnames)
        if idx < first_score_idx and col.startswith("ligand_")
    ]
    score_cols = [col for col in fieldnames if col.startswith(score_prefixes)]
    return receptor_cols, ligand_cols, score_cols


def nonempty_count(row: dict[str, str], columns: list[str]) -> int:
    return sum(1 for col in columns if row.get(col, "") != "")


def find_targets(
    pipeline_csv: Path,
    requested_targets: set[str] | None,
    include_unscored: bool,
) -> list[str]:
    with pipeline_csv.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {pipeline_csv}")
        receptor_cols, ligand_cols, score_cols = descriptor_columns(reader.fieldnames)
        targets: list[str] = []
        seen: set[str] = set()
        for row in reader:
            pdb_id = row.get("receptor", "").lower()
            if not pdb_id or pdb_id in seen:
                continue
            if requested_targets is not None and pdb_id not in requested_targets:
                continue
            scored = nonempty_count(row, score_cols) > 0
            if not include_unscored and not scored:
                continue
            receptor_complete = nonempty_count(row, receptor_cols) == len(receptor_cols)
            ligand_complete = nonempty_count(row, ligand_cols) == len(ligand_cols)
            if not receptor_complete or not ligand_complete:
                targets.append(pdb_id)
                seen.add(pdb_id)
    return targets


def descriptor_paths(pdbbind_root: Path, pdb_id: str) -> tuple[Path, Path]:
    base = pdbbind_root / pdb_id
    receptor_json = base / "receptor_descriptors.json"
    ligand_json = base / "compounds" / "ligands" / "ligand" / "ligand_descriptors.json"
    return receptor_json, ligand_json


def prepare_one(
    pdb_id: str,
    pdbbind_root: str,
    log_dir: str,
    overwrite_descriptors: bool,
) -> dict[str, Any]:
    pdbbind_path = Path(pdbbind_root) / pdb_id
    log_path = Path(log_dir) / f"{pdb_id}.log"
    result: dict[str, Any] = {
        "pdb_id": pdb_id,
        "ok": False,
        "receptor_json": False,
        "ligand_json": False,
        "error": "",
        "log": str(log_path),
    }
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as log_handle:
            with contextlib.redirect_stdout(log_handle), contextlib.redirect_stderr(log_handle):
                print(f"START {datetime.now().isoformat()} {pdb_id}")
                from OCDocker.Config import OCDockerConfig, set_config
                import OCDocker.Processing.Preprocessing.Prepare as ocprepare

                config = OCDockerConfig()
                config.multiprocess = False
                config.available_cores = 1
                config.logdir = str(log_path.parent)
                config.tmp_dir = "/tmp"
                set_config(config)

                ocprepare.prepare(
                    str(pdbbind_path),
                    overwrite=overwrite_descriptors,
                    archive="pdbbind",
                    sanitize=True,
                    spacing=0.33,
                    all_boxes=False,
                )
                receptor_json, ligand_json = descriptor_paths(Path(pdbbind_root), pdb_id)
                result["receptor_json"] = receptor_json.is_file() and receptor_json.stat().st_size > 0
                result["ligand_json"] = ligand_json.is_file() and ligand_json.stat().st_size > 0
                result["ok"] = bool(result["receptor_json"] and result["ligand_json"])
                print(f"END {datetime.now().isoformat()} {pdb_id} ok={result['ok']}")
    except Exception as exc:  # pragma: no cover - operational script
        result["error"] = f"{type(exc).__name__}: {exc}"
        with log_path.open("a", encoding="utf-8") as log_handle:
            traceback.print_exc(file=log_handle)
    return result


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def patch_csv(
    pipeline_csv: Path,
    pdbbind_root: Path,
    successes: set[str],
    overwrite_csv_columns: bool,
) -> tuple[int, Path]:
    backup = pipeline_csv.with_name(
        f"{pipeline_csv.name}.bak.descriptors_all.{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    )
    tmp_path = pipeline_csv.with_name(f".{pipeline_csv.name}.descriptors_all.tmp")
    shutil.copy2(pipeline_csv, backup)

    payloads: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for pdb_id in sorted(successes):
        receptor_json, ligand_json = descriptor_paths(pdbbind_root, pdb_id)
        if receptor_json.is_file() and ligand_json.is_file():
            payloads[pdb_id] = (load_json(receptor_json), load_json(ligand_json))

    updated = 0
    with pipeline_csv.open("r", newline="") as in_handle, tmp_path.open("w", newline="") as out_handle:
        reader = csv.DictReader(in_handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {pipeline_csv}")
        writer = csv.DictWriter(out_handle, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            pdb_id = row.get("receptor", "").lower()
            payload = payloads.get(pdb_id)
            if payload:
                receptor_payload, ligand_payload = payload
                for key, value in receptor_payload.items():
                    col = f"receptor_{key}"
                    if col in row and (overwrite_csv_columns or row.get(col, "") == ""):
                        row[col] = "" if value is None else str(value)
                for key, value in ligand_payload.items():
                    col = f"ligand_{key}"
                    if col in row and (overwrite_csv_columns or row.get(col, "") == ""):
                        row[col] = "" if value is None else str(value)
                updated += 1
            writer.writerow(row)

    os.replace(tmp_path, pipeline_csv)
    return updated, backup


def main() -> int:
    args = build_parser().parse_args()
    pipeline_csv = args.pipeline_csv.resolve()
    pdbbind_root = args.pdbbind_root.resolve()
    args.log_dir.mkdir(parents=True, exist_ok=True)
    status_jsonl = args.status_jsonl or args.log_dir / "status.jsonl"

    requested_targets = {target.lower() for target in args.targets} if args.targets is not None else None
    targets = find_targets(pipeline_csv, requested_targets, args.include_unscored)
    if args.limit > 0:
        targets = targets[:args.limit]

    print(f"Targets: {len(targets)}")
    print(f"Workers: {args.workers}")
    print(f"Log dir: {args.log_dir}")
    print(f"Status: {status_jsonl}")

    successes: set[str] = set()
    failures: list[dict[str, Any]] = []
    completed = 0

    with status_jsonl.open("a", encoding="utf-8") as status_handle:
        with ProcessPoolExecutor(max_workers=max(1, args.workers)) as executor:
            futures = {
                executor.submit(
                    prepare_one,
                    pdb_id,
                    str(pdbbind_root),
                    str(args.log_dir),
                    args.overwrite_descriptors,
                ): pdb_id
                for pdb_id in targets
            }
            for future in as_completed(futures):
                result = future.result()
                completed += 1
                status_handle.write(json.dumps(result, sort_keys=True) + "\n")
                status_handle.flush()
                if result.get("ok"):
                    successes.add(str(result["pdb_id"]))
                else:
                    failures.append(result)
                if completed % 25 == 0 or completed == len(targets):
                    print(
                        f"Completed {completed}/{len(targets)}; "
                        f"ok={len(successes)} failed={len(failures)}",
                        flush=True,
                    )

    print(f"Generated complete descriptor sets: {len(successes)}")
    print(f"Failed/incomplete descriptor sets: {len(failures)}")
    if failures:
        fail_path = args.log_dir / "failures.json"
        fail_path.write_text(json.dumps(failures, indent=2, sort_keys=True), encoding="utf-8")
        print(f"Failures file: {fail_path}")

    if not args.no_patch_csv and successes:
        updated, backup = patch_csv(
            pipeline_csv,
            pdbbind_root,
            successes,
            overwrite_csv_columns=args.overwrite_csv_columns,
        )
        print(f"CSV rows patched: {updated}")
        print(f"CSV backup: {backup}")

    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
