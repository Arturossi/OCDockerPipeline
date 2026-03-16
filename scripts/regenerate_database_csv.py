#!/usr/bin/env python3
"""Regenerate per-database pipeline_results.csv from existing payloads.

This mirrors the Snakefile CSV schema and column ordering, but scans only
payloads that already exist on disk so it can be run safely during a live
pipeline execution for snapshot reporting.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import numbers
import os
import pickle
import re
import sys

from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Union

try:
    import yaml
except Exception as exc:  # pragma: no cover - environment issue
    raise SystemExit(f"PyYAML is required to run this script: {type(exc).__name__}: {exc}")


os.environ.setdefault("OCDOCKER_NO_AUTO_BOOTSTRAP", "1")

_PRESET_DATABASES = {"PDBbind", "DUDEz"}


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off", ""}:
        return False
    return default


def _parse_list(value: Any, default: Any) -> List[str]:
    if value is None:
        value = default

    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]

    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]

    return [str(value).strip()] if str(value).strip() else []


def _normalize_database_name(name: Any) -> str:
    lower = str(name).strip().lower()
    if lower == "pdbbind":
        return "PDBbind"
    if lower in {"dudez", "dude-z", "dude_z"}:
        return "DUDEz"
    return str(name).strip()


def _looks_like_path(value: str) -> bool:
    text = str(value).strip()
    if not text:
        return False
    if text.startswith(("~", ".", "/")):
        return True
    if os.sep in text:
        return True
    if os.altsep and os.altsep in text:
        return True
    return False


def _validate_database_alias(alias: str, source: str) -> None:
    if not alias:
        raise RuntimeError(f"Invalid database source '{source}': empty alias.")
    if os.sep in alias or (os.altsep and os.altsep in alias):
        raise RuntimeError(
            f"Invalid database alias '{alias}' from source '{source}'. "
            "Aliases cannot contain path separators."
        )


def _parse_database_sources(sources: Sequence[str], ocdb_path: str) -> Dict[str, Dict[str, Any]]:
    specs: Dict[str, Dict[str, Any]] = {}
    seen_aliases: Dict[str, str] = {}

    for raw_source in sources:
        source = str(raw_source).strip()
        if not source:
            continue

        if "=" in source:
            raw_alias, raw_path = source.split("=", 1)
            alias = _normalize_database_name(raw_alias.strip())
            _validate_database_alias(alias, source)

            raw_path = raw_path.strip()
            if not raw_path:
                raise RuntimeError(
                    f"Invalid database source '{source}'. Expected '<alias>=<path>' with a non-empty path."
                )
            target_path = Path(raw_path).expanduser().resolve()
        else:
            normalized = _normalize_database_name(source)
            if normalized in _PRESET_DATABASES:
                alias = normalized
                target_path = Path(ocdb_path) / normalized
            elif _looks_like_path(source):
                target_path = Path(source).expanduser().resolve()
                alias = _normalize_database_name(target_path.name)
            else:
                alias = _normalize_database_name(source)
                target_path = Path(ocdb_path) / alias

            _validate_database_alias(alias, source)

        alias_key = alias.lower()
        if alias_key in seen_aliases:
            raise RuntimeError(
                f"Duplicate database alias '{alias}' from source '{source}'. "
                f"Already defined by '{seen_aliases[alias_key]}'."
            )
        seen_aliases[alias_key] = source

        preset = alias if alias in _PRESET_DATABASES else None
        if not target_path.exists():
            raise RuntimeError(
                f"Database source '{source}' resolved to '{target_path}', but this directory does not exist."
            )
        if not target_path.is_dir():
            raise RuntimeError(
                f"Database source '{source}' resolved to '{target_path}', but it is not a directory."
            )

        specs[alias] = {
            "alias": alias,
            "root": str(target_path),
            "preset": preset,
            "source": source,
        }

    if not specs:
        raise RuntimeError(
            "No valid database sources configured. "
            "Set 'database_sources' (preferred) or 'run_databases' in config.yaml."
        )

    return specs


def _prepare_database_mounts(database_specs: Dict[str, Dict[str, Any]], database_rule_root: Path) -> None:
    database_rule_root.mkdir(parents=True, exist_ok=True)
    for database, spec in database_specs.items():
        source_root = Path(str(spec["root"])).resolve()
        mount_path = database_rule_root / database

        if mount_path.exists():
            try:
                if mount_path.resolve() == source_root:
                    continue
            except OSError:
                pass

        if mount_path.is_symlink():
            try:
                current_target = mount_path.resolve()
            except OSError:
                current_target = None
            if current_target == source_root:
                continue
            mount_path.unlink()
        elif mount_path.exists():
            raise RuntimeError(
                f"Database mount path '{mount_path}' already exists and is not compatible with source '{source_root}'. "
                "Remove it or choose a different database alias."
            )

        mount_path.symlink_to(source_root, target_is_directory=True)


def _is_integer_descriptor_name(descriptor: str) -> bool:
    name = descriptor.strip()
    return (
        name.startswith("fr_")
        or name.startswith("Num")
        or name.startswith("count")
        or name in {"HeavyAtomCount", "NHOHCount", "NOCount", "RingCount", "TotalAALength"}
    )


def _to_numeric(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return float(int(value))
    if not isinstance(value, numbers.Real):
        return None

    numeric_value = float(value)
    if math.isnan(numeric_value) or math.isinf(numeric_value):
        return None
    return numeric_value


def _descriptor_attribute_candidates(descriptor: str) -> List[str]:
    base = descriptor.strip()
    if not base:
        return []

    candidates: List[str] = [base]
    lower_first = base[0].lower() + base[1:]
    for candidate in (lower_first, base.lower()):
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _collect_numeric_descriptors(obj: Any, descriptor_names: List[str]) -> Dict[str, Union[int, float]]:
    payload: Dict[str, Union[int, float]] = {}
    for descriptor in descriptor_names:
        raw_value: Any = None
        found = False
        for attr_name in _descriptor_attribute_candidates(descriptor):
            if hasattr(obj, attr_name):
                raw_value = getattr(obj, attr_name)
                found = True
                break
        if not found:
            continue
        numeric_value = _to_numeric(raw_value)
        if numeric_value is None:
            continue
        if _is_integer_descriptor_name(descriptor):
            payload[descriptor] = int(numeric_value)
        else:
            payload[descriptor] = numeric_value
    return payload


def _canonicalize_rescore_key(engine: str, raw_key: str) -> str:
    engine_key = str(engine).strip().lower()
    key = str(raw_key).strip().lower().replace("-", "_").replace(" ", "_")
    while "__" in key:
        key = key.replace("__", "_")

    if key.endswith("_rescoring"):
        key = key[: -len("_rescoring")]

    if key.startswith(f"{engine_key}_"):
        canonical = key
    elif key.startswith("rescoring_"):
        rescoring_key = key[len("rescoring_"):]
        rescoring_key = re.sub(r"_\d+$", "", rescoring_key)
        canonical = f"{engine_key}_{rescoring_key}" if rescoring_key else f"{engine_key}_{key}"
    else:
        canonical = f"{engine_key}_{key}"

    if engine_key == "gnina":
        if canonical in {"gnina_ad4", "gnina_scoring_ad4"}:
            return "gnina_ad4_scoring"
        if canonical in {"gnina_dkoes", "gnina_scoring_dkoes"}:
            return "gnina_dkoes_scoring"
        if canonical.startswith("gnina_cnn_"):
            return "gnina_default"
    elif engine_key == "smina":
        if canonical in {"smina_ad4", "smina_scoring_ad4"}:
            return "smina_ad4_scoring"
        if canonical in {"smina_dkoes", "smina_scoring_dkoes"}:
            return "smina_dkoes_scoring"
        if canonical in {"smina_old_scoring_dkoes"}:
            return "smina_dkoes_scoring_old"
        if canonical in {"smina_fast_dkoes"}:
            return "smina_dkoes_fast"

    return canonical


def _to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, dict):
        return {str(key): _to_jsonable(inner) for key, inner in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(inner) for inner in value]

    return str(value)


def _csv_scalar(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    return json.dumps(_to_jsonable(value), sort_keys=True)


def _csv_key(value: Any) -> str:
    key = str(value).strip().lower()
    key = re.sub(r"[^0-9a-zA-Z_]+", "_", key)
    key = re.sub(r"_+", "_", key).strip("_")
    return key or "score"


def _flatten_summary_rescoring_for_csv(summary: Dict[str, Any]) -> Dict[str, float]:
    flattened: Dict[str, float] = {}

    def _ingest_rescoring(rescoring_data: Any, prefix: str = "") -> None:
        if not isinstance(rescoring_data, dict):
            return
        for engine, engine_scores in rescoring_data.items():
            if not isinstance(engine_scores, dict):
                continue
            for raw_key, raw_value in engine_scores.items():
                numeric = _to_numeric(raw_value if not isinstance(raw_value, (list, tuple)) else raw_value[0])
                if numeric is None:
                    continue
                canonical = _canonicalize_rescore_key(str(engine), str(raw_key))
                col = f"{prefix}{_csv_key(canonical)}"
                flattened[col] = float(numeric)

    _ingest_rescoring(summary.get("rescoring"), prefix="")

    box_summaries = summary.get("box_summaries")
    if isinstance(box_summaries, dict):
        for box_name, box_data in box_summaries.items():
            if not isinstance(box_data, dict):
                continue
            box_prefix = f"{_csv_key(box_name)}__"
            _ingest_rescoring(box_data.get("rescoring"), prefix=box_prefix)

    return flattened


def _config_score_column_for_csv(engine: str, scoring_function: str) -> Optional[str]:
    engine_key = str(engine).strip().lower()
    sf_key = _csv_key(scoring_function)
    if not sf_key:
        return None

    if engine_key in {"vina", "smina", "gnina", "plants"}:
        raw_key = f"{engine_key}_{sf_key}"
        return _csv_key(_canonicalize_rescore_key(engine_key, raw_key))

    if engine_key == "oddt":
        if sf_key.startswith("rfscore_v1"):
            return "oddt_rfscore_v1"
        if sf_key.startswith("rfscore_v2"):
            return "oddt_rfscore_v2"
        if sf_key.startswith("rfscore_v3"):
            return "oddt_rfscore_v3"
        if sf_key.startswith("nnscore"):
            return "oddt_nnscore"
        if sf_key.startswith("plecrf"):
            return "oddt_plecrf_p5_l1_s65536"
        return f"oddt_{sf_key}"

    return None


def _configured_score_columns_for_csv(oc_config: Any) -> List[str]:
    ordered: List[str] = []

    def _add(column: Optional[str]) -> None:
        if not column:
            return
        if column not in ordered:
            ordered.append(column)

    engine_to_functions = {
        "vina": list(getattr(getattr(oc_config, "vina", None), "scoring_functions", []) or []),
        "smina": list(getattr(getattr(oc_config, "smina", None), "scoring_functions", []) or []),
        "gnina": list(getattr(getattr(oc_config, "gnina", None), "scoring_functions", []) or []),
        "plants": list(getattr(getattr(oc_config, "plants", None), "scoring_functions", []) or []),
        "oddt": list(getattr(getattr(oc_config, "oddt", None), "scoring_functions", []) or []),
    }

    for engine in ("vina", "smina", "gnina", "plants", "oddt"):
        for scoring_function in engine_to_functions.get(engine, []):
            _add(_config_score_column_for_csv(engine, str(scoring_function)))

    return ordered


def _load_pipeline_config(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        raise RuntimeError(f"Pipeline config file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise RuntimeError(f"Invalid YAML object in {path}: expected a mapping at top level.")
    return payload


def _resolve_selected_databases(
    requested: Sequence[str],
    available: Sequence[str],
) -> List[str]:
    if not requested:
        return list(available)

    available_map = {name.lower(): name for name in available}
    resolved: List[str] = []
    for raw_name in requested:
        normalized = _normalize_database_name(raw_name)
        candidate = available_map.get(normalized.lower()) or available_map.get(str(raw_name).strip().lower())
        if not candidate:
            raise RuntimeError(
                f"Unknown database alias '{raw_name}'. Available aliases: {', '.join(available)}"
            )
        if candidate not in resolved:
            resolved.append(candidate)
    return resolved


def _collect_existing_payloads_for_database(
    database_rule_root: Path,
    database: str,
    selected_kinds: Sequence[str],
) -> List[str]:
    db_root = (database_rule_root / database).resolve()
    if not db_root.is_dir():
        return []

    payloads: List[str] = []
    for receptor_dir in sorted(path for path in db_root.iterdir() if path.is_dir()):
        compounds_dir = receptor_dir / "compounds"
        if not compounds_dir.is_dir():
            continue
        for kind in selected_kinds:
            kind_dir = compounds_dir / kind
            if not kind_dir.is_dir():
                continue
            for target_dir in sorted(path for path in kind_dir.iterdir() if path.is_dir()):
                payload_path = target_dir / "payload.pkl"
                if payload_path.is_file():
                    payloads.append(str(payload_path.resolve()))

    return sorted(set(payloads))


def _write_database_results_csv(
    *,
    database: str,
    payload_paths: Sequence[str],
    csv_path: Union[str, Path],
    oc_config: Any,
) -> int:
    output_path = Path(csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    receptor_descriptor_names: List[str] = []
    ligand_descriptor_names: List[str] = []
    receptor_module = None
    ligand_module = None
    try:
        import OCDocker.Receptor as ocr
        import OCDocker.Ligand as ocl

        receptor_module = ocr
        ligand_module = ocl
        receptor_descriptor_names = list(getattr(ocr.Receptor, "allDescriptors", []))
        ligand_descriptor_names = list(getattr(ocl.Ligand, "allDescriptors", []))
    except Exception as exc:
        print(
            "Warning: failed to import receptor/ligand descriptor modules for CSV export: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )

    receptor_descriptor_columns = [f"receptor_{name}" for name in receptor_descriptor_names]
    ligand_descriptor_columns = [f"ligand_{name}" for name in ligand_descriptor_names]

    receptor_descriptor_cache: Dict[str, Dict[str, Union[int, float]]] = {}
    ligand_descriptor_cache: Dict[str, Dict[str, Union[int, float]]] = {}

    def _load_receptor_descriptors(receptor_path: Path, receptor_name: str) -> Dict[str, Union[int, float]]:
        cache_key = str(receptor_path.resolve())
        if cache_key in receptor_descriptor_cache:
            return receptor_descriptor_cache[cache_key]

        if receptor_module is None or not receptor_path.is_file():
            receptor_descriptor_cache[cache_key] = {}
            return receptor_descriptor_cache[cache_key]

        descriptor_json = receptor_path.with_name("receptor_descriptors.json")
        kwargs: Dict[str, Any] = {
            "name": f"{receptor_name}_csv_receptor",
            "allow_missing_surface": True,
        }
        if descriptor_json.is_file():
            kwargs["from_json_descriptors"] = str(descriptor_json)

        try:
            receptor_obj = receptor_module.Receptor(str(receptor_path), **kwargs)
            payload = _collect_numeric_descriptors(receptor_obj, receptor_descriptor_names)
        except Exception as exc:
            print(
                f"Warning: failed to compute receptor descriptors for '{receptor_path}': "
                f"{type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            payload = {}

        receptor_descriptor_cache[cache_key] = payload
        return payload

    def _load_ligand_descriptors(ligand_paths: Sequence[Path], target_name: str) -> Dict[str, Union[int, float]]:
        if ligand_module is None:
            return {}

        for ligand_path in ligand_paths:
            if not ligand_path.is_file():
                continue

            cache_key = str(ligand_path.resolve())
            if cache_key in ligand_descriptor_cache:
                cached = ligand_descriptor_cache[cache_key]
                if cached:
                    return cached
                continue

            descriptor_json = ligand_path.parent / "ligand_descriptors.json"
            kwargs: Dict[str, Any] = {"name": f"{target_name}_csv_ligand"}
            if descriptor_json.is_file():
                kwargs["from_json_descriptors"] = str(descriptor_json)

            try:
                ligand_obj = ligand_module.Ligand(str(ligand_path), **kwargs)
                payload = _collect_numeric_descriptors(ligand_obj, ligand_descriptor_names)
            except Exception as exc:
                print(
                    f"Warning: failed to compute ligand descriptors for '{ligand_path}': "
                    f"{type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                payload = {}

            ligand_descriptor_cache[cache_key] = payload
            if payload:
                return payload

        return {}

    excluded_score_columns = {"gnina_default"}
    rows: List[Dict[str, Any]] = []
    score_columns: Set[str] = set()

    for payload_path in sorted(set(payload_paths)):
        payload_file = Path(payload_path)
        if not payload_file.is_file():
            continue

        try:
            with payload_file.open("rb") as handle:
                payload = pickle.load(handle)
        except Exception as exc:
            print(
                f"Warning: failed to read payload '{payload_file}' during CSV export: "
                f"{type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            continue

        if not isinstance(payload, dict):
            continue

        target_dir = payload_file.parent
        summary_path = target_dir / "summary.json"
        summary = payload.get("summary", {})
        if not isinstance(summary, dict):
            summary = {}

        if summary_path.is_file():
            try:
                loaded_summary = json.loads(summary_path.read_text(encoding="utf-8"))
                if isinstance(loaded_summary, dict):
                    summary = loaded_summary
            except Exception as exc:
                print(
                    f"Warning: failed to parse summary '{summary_path}' during CSV export: "
                    f"{type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )

        receptor_name = str(payload.get("receptor", ""))
        target_name = str(payload.get("target", ""))
        receptor_path = target_dir.parents[2] / "receptor.pdb"
        ligand_candidates: List[Path] = []
        for candidate in (
            target_dir / "ligand.mol2",
            target_dir / "ligand.smi",
            target_dir / "prepared_ligand.mol2",
        ):
            if candidate.is_file() and candidate not in ligand_candidates:
                ligand_candidates.append(candidate)

        receptor_descriptors = _load_receptor_descriptors(receptor_path, receptor_name)
        ligand_descriptors = _load_ligand_descriptors(ligand_candidates, target_name)

        row: Dict[str, Any] = {
            "database": str(payload.get("database", database)),
            "receptor": receptor_name,
            "kind": str(payload.get("kind", "")),
            "target": target_name,
            "name": str(payload.get("name", "")),
        }

        for descriptor_name in receptor_descriptor_names:
            row[f"receptor_{descriptor_name}"] = receptor_descriptors.get(descriptor_name)

        for descriptor_name in ligand_descriptor_names:
            row[f"ligand_{descriptor_name}"] = ligand_descriptors.get(descriptor_name)

        flattened_scores = _flatten_summary_rescoring_for_csv(summary)
        for score_key, score_value in flattened_scores.items():
            if score_key in excluded_score_columns:
                continue
            row[score_key] = score_value
            score_columns.add(score_key)

        rows.append(row)

    base_columns = ["database", "receptor", "kind", "target", "name"]
    configured_score_columns = [
        score for score in _configured_score_columns_for_csv(oc_config) if score not in excluded_score_columns
    ]
    configured_score_set = set(configured_score_columns)
    ordered_score_columns = configured_score_columns + sorted(
        score for score in score_columns if score not in configured_score_set
    )
    fieldnames = base_columns + receptor_descriptor_columns + ligand_descriptor_columns + ordered_score_columns

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_scalar(row.get(key)) for key in fieldnames})

    return len(rows)


def _build_bootstrap_namespace(config: Dict[str, Any], config_file: Path) -> argparse.Namespace:
    import OCDocker.Error as ocerror

    log_level = str(config.get("log_level", "info")).lower()
    log_level_map = {
        "debug": ocerror.ReportLevel.DEBUG,
        "info": ocerror.ReportLevel.INFO,
        "warning": ocerror.ReportLevel.WARNING,
        "error": ocerror.ReportLevel.ERROR,
        "none": ocerror.ReportLevel.NONE,
    }
    output_level = log_level_map.get(log_level, ocerror.ReportLevel.INFO)
    return argparse.Namespace(
        multiprocess=bool(int(config.get("cpu_cores", 1)) > 1),
        update=False,
        config_file=str(config_file),
        output_level=output_level,
        overwrite=_as_bool(config.get("overwrite", False), default=False),
        no_splash=True,
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    script_path = Path(__file__).resolve()
    pipeline_root = script_path.parents[1]

    parser = argparse.ArgumentParser(
        description=(
            "Regenerate pipeline_results.csv snapshots from existing payload.pkl files "
            "using the same CSV schema and column ordering as the Snakefile."
        )
    )
    parser.add_argument(
        "--config",
        default=str(pipeline_root / "config.yaml"),
        help="Path to the pipeline config.yaml (default: %(default)s)",
    )
    parser.add_argument(
        "--ocdocker-config",
        default=os.getenv("OCDOCKER_CONFIG", str(pipeline_root / "OCDocker.cfg")),
        help="Path to OCDocker.cfg (default: OCDOCKER_CONFIG or %(default)s)",
    )
    parser.add_argument(
        "--database",
        action="append",
        default=[],
        help="Database alias to regenerate. Repeat to select multiple aliases. Default: all selected databases.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Custom CSV output path. Only valid when exactly one database is selected.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    pipeline_config_path = Path(args.config).expanduser().resolve()
    ocdocker_config_path = Path(args.ocdocker_config).expanduser().resolve()

    pipeline_config = _load_pipeline_config(pipeline_config_path)

    os.environ["OCDOCKER_CONFIG"] = str(ocdocker_config_path)
    os.environ.setdefault("OCDOCKER_DB_BACKEND", "postgresql")
    os.environ.setdefault("DB_BACKEND", "postgresql")

    import OCDocker.Initialise as ocinit
    from OCDocker.Config import get_config

    ocinit.bootstrap(_build_bootstrap_namespace(pipeline_config, ocdocker_config_path), init_db=False)
    oc_config = get_config()

    ocdb_path = str(getattr(getattr(oc_config, "paths", None), "ocdb_path", "") or "").strip()
    if not ocdb_path:
        raise RuntimeError("OCDocker ocdb path is not set. Update OCDocker.cfg and rerun.")

    raw_database_sources = _parse_list(pipeline_config.get("database_sources"), [])
    if not raw_database_sources:
        raw_database_sources = _parse_list(pipeline_config.get("run_databases"), ["PDBbind", "DUDEz"])
    database_specs = _parse_database_sources(raw_database_sources, ocdb_path)
    selected_databases = _resolve_selected_databases(args.database, list(database_specs.keys()))

    selected_kinds = [kind.lower() for kind in _parse_list(pipeline_config.get("compound_kinds"), ["ligands", "decoys", "compounds"])]
    selected_kinds = [kind for kind in selected_kinds if kind in {"ligands", "decoys", "compounds"}]
    if not selected_kinds:
        raise RuntimeError("No valid compound_kinds configured. Use one or more of: ligands, decoys, compounds")

    database_rule_root = Path(ocdb_path).resolve()
    _prepare_database_mounts(database_specs, database_rule_root)

    if args.output and len(selected_databases) != 1:
        raise RuntimeError("--output may only be used when exactly one --database is selected.")

    for database in selected_databases:
        payload_paths = _collect_existing_payloads_for_database(database_rule_root, database, selected_kinds)
        csv_path = Path(args.output).expanduser().resolve() if args.output else database_rule_root / database / "pipeline_results.csv"
        row_count = _write_database_results_csv(
            database=database,
            payload_paths=payload_paths,
            csv_path=csv_path,
            oc_config=oc_config,
        )
        print(f"Wrote {csv_path} from {len(payload_paths)} payload(s); rows={row_count}; database={database}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
