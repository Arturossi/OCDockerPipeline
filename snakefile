"""
Module name: main

this is the main file from snakemake pipeline. It is responsible for the
execution of the pipeline.

Author: Artur Duque Rossi

Created: 06-11-2023
Last modified: 17-02-2026
"""

# Initial directives
###############################################################################
configfile: "config.yaml"


# Python functions and imports
###############################################################################
import argparse
import copy
import hashlib
import json
import math
import numbers
import os
import pickle
import platform
import re
import shutil
import socket
import subprocess
import sys
import threading

from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import lru_cache
from glob import glob
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

try:
    import fcntl as _fcntl
except ImportError:  # pragma: no cover - non-POSIX fallback
    _fcntl = None

# Disable auto-bootstrap so we can explicitly load the pipeline config.
os.environ.setdefault("OCDOCKER_NO_AUTO_BOOTSTRAP", "1")

# Enforce PostgreSQL backend for pipeline DB writes.
db_backend = str(config.get("db_backend", "postgresql")).strip().lower() or "postgresql"
if db_backend != "postgresql":
    raise RuntimeError(
        "OCDockerPipeline requires PostgreSQL backend for DB storage. "
        "Set 'db_backend: postgresql' in config.yaml."
    )
os.environ["OCDOCKER_DB_BACKEND"] = "postgresql"
os.environ["DB_BACKEND"] = "postgresql"

import OCDocker.Error as ocerror
import OCDocker.Initialise as ocinit
from OCDocker.Config import get_config

try:
    from OCDP._version import __version__ as pipeline_version
except Exception:
    pipeline_version = "0+unknown"

# Bootstrap OCDocker using the pipeline config to populate the shared Config object.
if "workflow" in globals() and getattr(workflow, "basedir", None):
    pipeline_root = str(Path(workflow.basedir).resolve())
else:
    pipeline_root = os.path.dirname(os.path.abspath(__file__))
pipeline_source_root = Path(pipeline_root).resolve()
config_file = os.getenv("OCDOCKER_CONFIG", os.path.join(pipeline_root, "OCDocker.cfg"))
os.environ["OCDOCKER_CONFIG"] = config_file
log_level = str(config.get("log_level", "info")).lower()
log_level_map = {
    "debug": ocerror.ReportLevel.DEBUG,
    "info": ocerror.ReportLevel.INFO,
    "warning": ocerror.ReportLevel.WARNING,
    "error": ocerror.ReportLevel.ERROR,
    "none": ocerror.ReportLevel.NONE,
}
output_level = log_level_map.get(log_level, ocerror.ReportLevel.INFO)
bootstrap_ns = argparse.Namespace(
    multiprocess=config.get("cpu_cores", 1) > 1,
    update=False,
    config_file=config_file,
    output_level=output_level,
    overwrite=bool(config.get("overwrite", False)),
)
# Do a lightweight bootstrap during DAG parsing; DB is initialized lazily at store time.
ocinit.bootstrap(bootstrap_ns, init_db=False)

oc_config = get_config()
ocdb_path = oc_config.paths.ocdb_path or ""
if not ocdb_path:
    raise RuntimeError("OCDocker ocdb path is not set. Update OCDocker.cfg (ocdb) and rerun.")

# Python definitions
###############################################################################

cpu_cores = config["cpu_cores"]
_db_tables_initialized = False
_db_tables_init_lock = threading.Lock()
_db_write_lock = threading.Lock()
_PIPELINE_DB_SCHEMA_VERSION = "2026-02-27.pipeline-runs-v1"


def _as_bool(value: Any, default: bool = False) -> bool:
    """Parse booleans from bool/int/string config values."""

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


overwrite = _as_bool(config.get("overwrite", False), default=False)


def _parse_list(value, default):
    """Parse config values accepting list/tuple/set or comma-separated string."""

    if value is None:
        value = default

    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]

    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]

    return [str(value).strip()] if str(value).strip() else []


def _load_ignored_targets(index_path: str) -> Set[str]:
    """Load ignored receptor IDs from a line-based text file."""

    text_path = str(index_path or "").strip()
    if not text_path:
        return set()

    path = Path(text_path)
    if not path.is_file():
        return set()

    ignored: Set[str] = set()
    try:
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if line and not line.startswith("#"):
                    ignored.add(line)
    except OSError:
        return set()

    return ignored


def _normalize_database_name(name):
    lower = str(name).strip().lower()
    if lower == "pdbbind":
        return "PDBbind"
    if lower in {"dudez", "dude-z", "dude_z"}:
        return "DUDEz"
    return str(name).strip()


def _is_valid_file(path: Union[str, Path]) -> bool:
    p = Path(path)
    return p.is_file() and p.stat().st_size > 0


def _binary_available(executable):
    if not executable:
        return False

    executable = str(executable).strip()
    if not executable:
        return False

    if os.path.isabs(executable):
        return os.path.isfile(executable) and os.access(executable, os.X_OK)

    return shutil.which(executable) is not None


def _normalize_exit_code(result):
    """Normalize command/API return value into a numeric exit code."""

    if isinstance(result, tuple):
        if not result:
            return 1
        result = result[0]

    if result is None:
        return 0

    if isinstance(result, bool):
        return 0 if result else 1

    if isinstance(result, int):
        return result

    try:
        return int(result)
    except (TypeError, ValueError):
        return 1


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc_iso_from_timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256_file(path: Union[str, Path]) -> Optional[str]:
    file_path = Path(path)
    if not file_path.is_file():
        return None

    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, dict):
        return {str(key): _to_jsonable(inner) for key, inner in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(inner) for inner in value]

    return str(value)


def _json_sha256(payload: Any) -> str:
    normalized = _to_jsonable(payload)
    text = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return _sha256_text(text)


def _file_fingerprint(path: Union[str, Path], include_sha256: bool = True) -> Dict[str, Any]:
    file_path = Path(path)
    payload: Dict[str, Any] = {
        "path": str(file_path),
        "exists": file_path.exists(),
    }
    if not file_path.exists():
        return payload

    stat_info = file_path.stat()
    payload["is_file"] = file_path.is_file()
    payload["size_bytes"] = int(stat_info.st_size)
    payload["mtime_utc"] = _utc_iso_from_timestamp(stat_info.st_mtime)
    if include_sha256 and file_path.is_file():
        payload["sha256"] = _sha256_file(file_path)
    return payload


def _run_git(repo_root: Union[str, Path], args: List[str]) -> Optional[str]:
    root = Path(repo_root)
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=3,
        )
    except (OSError, ValueError, subprocess.SubprocessError):
        return None

    if completed.returncode != 0:
        return None

    output = completed.stdout.strip()
    return output if output else None


def _collect_git_manifest(repo_root: Union[str, Path]) -> Dict[str, Optional[Union[str, bool]]]:
    commit = _run_git(repo_root, ["rev-parse", "HEAD"])
    branch = _run_git(repo_root, ["rev-parse", "--abbrev-ref", "HEAD"])
    status = _run_git(repo_root, ["status", "--porcelain"])
    return {
        "commit": commit,
        "branch": branch,
        "dirty": bool(status) if status is not None else None,
    }


def _runtime_cache_root() -> Path:
    """Return a writable cache root for runtime metadata/sentinels."""

    primary = Path(os.getcwd()) / ".snakemake"
    try:
        primary.mkdir(parents=True, exist_ok=True)
        return primary
    except OSError:
        fallback = Path("/tmp") / "ocdockerpipeline_snakemake_cache"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


def _set_command_option(cmd: Any, flag: str, value: Union[str, int]) -> None:
    """Set or append CLI option ``flag value`` on command lists."""

    if not isinstance(cmd, list):
        return

    value_text = str(value)
    idx = 0
    while idx < len(cmd):
        if cmd[idx] == flag:
            if idx + 1 < len(cmd):
                cmd[idx + 1] = value_text
            else:
                cmd.append(value_text)
            return
        idx += 1

    cmd.extend([flag, value_text])


def _apply_engine_cpu_hint(engine: str, runner: Any, threads_hint: int) -> None:
    """Align engine ``--cpu`` arguments with Snakemake thread scheduling."""

    cpu_threads = max(1, int(threads_hint))
    if engine == "vina":
        _set_command_option(getattr(runner, "vina_cmd", None), "--cpu", cpu_threads)
    elif engine == "smina":
        _set_command_option(getattr(runner, "smina_cmd", None), "--cpu", cpu_threads)
    elif engine == "gnina":
        _set_command_option(getattr(runner, "gnina_cmd", None), "--cpu", cpu_threads)


@lru_cache(maxsize=2)
def _cached_reproducibility_manifest(include_python_packages: bool) -> Tuple[Dict[str, Any], Optional[str]]:
    """Collect reproducibility manifest once per interpreter process."""

    try:
        import OCDocker.Toolbox.Reproducibility as ocrepro

        manifest = ocrepro.generate_reproducibility_manifest(include_python_packages=include_python_packages)
        return copy.deepcopy(_to_jsonable(manifest)), None
    except Exception as exc:
        return {}, f"{type(exc).__name__}: {exc}"


def _generate_run_report(
    *,
    job_name: str,
    database: str,
    receptor: str,
    kind: str,
    target: str,
    receptor_path: Union[str, Path],
    ligand_path: Union[str, Path],
    box_path: Union[str, Path],
    engine_summary_paths: List[str],
    summary: Dict[str, Any],
    summary_path: Optional[Union[str, Path]],
    per_box_summary_paths: List[Union[str, Path]],
    payload_path: Union[str, Path],
    report_path: Union[str, Path],
) -> Dict[str, Any]:
    ocdocker_manifest, ocdocker_manifest_error = _cached_reproducibility_manifest(
        pipeline_report_include_python_packages
    )

    repo_root = pipeline_source_root
    snakefile_path = repo_root / "snakefile"
    pipeline_config_path = repo_root / "config.yaml"
    ocdocker_config_path = Path(config_file)
    config_snapshot = _to_jsonable(config)
    git_manifest = _collect_git_manifest(repo_root)
    if isinstance(ocdocker_manifest, dict):
        manifest_git = ocdocker_manifest.get("git")
        if isinstance(manifest_git, dict) and manifest_git:
            git_manifest = manifest_git

    report_payload = {
        "schema_version": 1,
        "generated_at_utc": _utc_now_iso(),
        "job": {
            "name": job_name,
            "database": database,
            "database_root": str(_database_root_path(database)),
            "receptor": receptor,
            "kind": kind,
            "target": target,
        },
        "pipeline": {
            "name": "OCDockerPipeline",
            "version": pipeline_version,
            "workflow_root": str(repo_root),
            "cache_key": pipeline_cache_key,
            "snakefile": _file_fingerprint(snakefile_path),
            "pipeline_config_yaml": _file_fingerprint(pipeline_config_path),
            "ocdocker_config": _file_fingerprint(ocdocker_config_path),
            "effective_config_sha256": _json_sha256(config_snapshot),
            "effective_config": config_snapshot,
            "settings": {
                "engines": list(pipeline_engines),
                "rescoring_engines": list(pipeline_rescoring_engines),
                "cluster": {
                    "min": pipeline_cluster_min,
                    "max": pipeline_cluster_max,
                    "step": pipeline_cluster_step,
                },
                "all_boxes": pipeline_all_boxes,
                "timeout": pipeline_timeout,
                "store_db": pipeline_store_db,
                "report_include_python_packages": pipeline_report_include_python_packages,
                "database_source": _to_jsonable(database_specs.get(database, {})),
            },
        },
        "runtime": {
            "python": {
                "version": platform.python_version(),
                "implementation": platform.python_implementation(),
                "executable": sys.executable,
            },
            "platform": {
                "system": platform.system(),
                "release": platform.release(),
                "machine": platform.machine(),
                "processor": platform.processor(),
            },
            "host": socket.gethostname(),
            "working_directory": os.getcwd(),
            "git": git_manifest,
            "environment": {
                "OCDOCKER_CONFIG": os.getenv("OCDOCKER_CONFIG"),
                "OCDOCKER_DB_BACKEND": os.getenv("OCDOCKER_DB_BACKEND"),
                "DB_BACKEND": os.getenv("DB_BACKEND"),
                "OCDOCKER_SQLITE_PATH": os.getenv("OCDOCKER_SQLITE_PATH"),
                "OCDOCKER_TIMEOUT": os.getenv("OCDOCKER_TIMEOUT"),
            },
        },
        "inputs": {
            "receptor": _file_fingerprint(receptor_path),
            "ligand": _file_fingerprint(ligand_path),
            "box": _file_fingerprint(box_path),
            "engine_summaries": [_file_fingerprint(path) for path in sorted(engine_summary_paths)],
        },
        "outputs": {
            "summary": _file_fingerprint(summary_path) if summary_path is not None else None,
            "box_summaries": [_file_fingerprint(path) for path in sorted(per_box_summary_paths)],
            "payload": _file_fingerprint(payload_path),
            "run_report": {
                "path": str(report_path),
            },
        },
        "summary_sha256": _json_sha256(summary),
        "ocdocker_manifest": _to_jsonable(ocdocker_manifest),
    }

    if ocdocker_manifest_error:
        report_payload["ocdocker_manifest_error"] = ocdocker_manifest_error

    return report_payload


@contextmanager
def _file_lock(lock_path: Union[str, Path]):
    lock_path = Path(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        if _fcntl is not None:
            _fcntl.flock(handle.fileno(), _fcntl.LOCK_EX)
        yield
    finally:
        if _fcntl is not None:
            try:
                _fcntl.flock(handle.fileno(), _fcntl.LOCK_UN)
            except OSError:
                pass
        handle.close()


def _box_sort_key(path: Path) -> Tuple[int, object]:
    stem = path.stem.lower()
    if stem.startswith("box"):
        suffix = stem[3:]
        if suffix.isdigit():
            return (0, int(suffix))
    return (1, stem)


def _list_boxes(ligand_dir: Path, box_path: Path, all_boxes: bool) -> List[Path]:
    if not all_boxes:
        return [box_path]

    candidates: List[Path] = []
    for directory in {ligand_dir, box_path.parent}:
        candidates.extend(Path(p) for p in glob(str(directory / "box*.pdb")))
    if box_path.is_file():
        candidates.append(box_path)

    unique: Dict[str, Path] = {}
    for path in candidates:
        try:
            unique[str(path.resolve())] = path
        except OSError:
            unique[str(path)] = path

    boxes = list(unique.values())
    boxes.sort(key=_box_sort_key)
    return boxes


def _ensure_mol2_poses(
    pose_paths: List[str],
    dest_dir: Path,
    pose_engine_map: Optional[Dict[str, str]] = None,
) -> Tuple[List[str], Dict[str, str]]:
    """Ensure poses are available in MOL2 format and keep source mapping."""

    import OCDocker.Toolbox.Conversion as occonversion

    dest_dir.mkdir(parents=True, exist_ok=True)
    mol2_paths: List[str] = []
    mapping: Dict[str, str] = {}

    for pose in pose_paths:
        src = Path(pose)
        if src.suffix.lower() == ".mol2":
            src_txt = str(src)
            mol2_paths.append(src_txt)
            mapping[src_txt] = src_txt
            continue

        engine = pose_engine_map.get(str(src), "unknown") if pose_engine_map else "unknown"
        out = dest_dir / f"{engine}_{src.stem}.mol2"
        _ = occonversion.convert_mols(str(src), str(out), overwrite=True)
        out_txt = str(out)
        mol2_paths.append(out_txt)
        mapping[out_txt] = str(src)

    return mol2_paths, mapping


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


def _collect_numeric_descriptors(obj: Any, descriptor_names: List[str]) -> Dict[str, Union[int, float]]:
    payload: Dict[str, Union[int, float]] = {}
    for descriptor in descriptor_names:
        if not hasattr(obj, descriptor):
            continue
        numeric_value = _to_numeric(getattr(obj, descriptor))
        if numeric_value is None:
            continue
        if _is_integer_descriptor_name(descriptor):
            payload[descriptor] = int(numeric_value)
        else:
            payload[descriptor] = numeric_value
    return payload


def _map_score_to_complex_column(raw_key: str) -> Optional[str]:
    key = raw_key.strip().lower().replace("-", "_").replace(" ", "_")
    while "__" in key:
        key = key.replace("__", "_")

    direct_map = {
        "vina_vina": "VINA_VINA",
        "vina_vinardo": "VINA_VINARDO",
        "smina_vina": "SMINA_VINA",
        "smina_vinardo": "SMINA_VINARDO",
        "smina_scoring_dkoes": "SMINA_SCORING_DKOES",
        "smina_dkoes_scoring": "SMINA_SCORING_DKOES",
        "smina_old_scoring_dkoes": "SMINA_OLD_SCORING_DKOES",
        "smina_dkoes_scoring_old": "SMINA_OLD_SCORING_DKOES",
        "smina_fast_dkoes": "SMINA_FAST_DKOES",
        "smina_dkoes_fast": "SMINA_FAST_DKOES",
        "smina_scoring_ad4": "SMINA_SCORING_AD4",
        "smina_ad4_scoring": "SMINA_SCORING_AD4",
        "plants_chemplp": "PLANTS_CHEMPLP",
        "plants_plp": "PLANTS_PLP",
        "plants_plp95": "PLANTS_PLP95",
        "oddt_plecrf_p5_l1_s65536": "ODDT_PLECRF_P5_L1_S65536",
        "oddt_nnscore": "ODDT_NNSCORE",
        "oddt_rfscore_v1": "ODDT_RFSCORE_V1",
        "oddt_rfscore_v2": "ODDT_RFSCORE_V2",
        "oddt_rfscore_v3": "ODDT_RFSCORE_V3",
    }
    if key in direct_map:
        return direct_map[key]

    oddt_key = key[5:] if key.startswith("oddt_") else key
    if "rfscore_v1" in oddt_key or oddt_key.endswith("rfscore1"):
        return "ODDT_RFSCORE_V1"
    if "rfscore_v2" in oddt_key or oddt_key.endswith("rfscore2"):
        return "ODDT_RFSCORE_V2"
    if "rfscore_v3" in oddt_key or oddt_key.endswith("rfscore3"):
        return "ODDT_RFSCORE_V3"
    if "plec" in oddt_key:
        return "ODDT_PLECRF_P5_L1_S65536"
    if "nnscore" in oddt_key:
        return "ODDT_NNSCORE"

    return None


def _flatten_rescoring_to_complex_payload(rescoring: Dict[str, Dict[str, float]]) -> Tuple[Dict[str, float], List[str]]:
    payload: Dict[str, float] = {}
    ignored_keys: List[str] = []

    for engine_scores in rescoring.values():
        if not isinstance(engine_scores, dict):
            continue
        for raw_key, raw_value in engine_scores.items():
            numeric_value = _to_numeric(raw_value)
            if numeric_value is None:
                continue
            column = _map_score_to_complex_column(str(raw_key))
            if not column:
                ignored_keys.append(str(raw_key))
                continue
            payload[column] = numeric_value

    return payload, sorted(set(ignored_keys))


def _ensure_db_runtime() -> None:
    import OCDocker.Initialise as ocinit_runtime
    from OCDocker.DB.DB import create_tables
    from OCDocker.DB.DBMinimal import create_database_if_not_exists, create_engine, create_session
    from sqlalchemy.engine import URL

    global _db_tables_initialized

    if getattr(ocinit_runtime, "session", None) is None:
        runtime_config = get_config()
        backend = str(getattr(runtime_config.database, "backend", "postgresql") or "postgresql").strip().lower()
        backend = "postgresql" if backend in {"postgres", "postgresql", "psql"} else backend

        if backend == "sqlite":
            sqlite_path = str(getattr(runtime_config.database, "sqlite_path", "") or "").strip()
            if not sqlite_path:
                sqlite_path = str(pipeline_source_root / "ocdocker_pipeline.sqlite")
            db_url = URL.create(drivername="sqlite", database=sqlite_path)
        else:
            if backend == "mysql":
                drivername = "mysql+pymysql"
                default_port = 3306
            else:
                drivername = "postgresql+psycopg"
                default_port = 5432

            host = str(getattr(runtime_config.database, "host", "") or "").strip()
            user = str(getattr(runtime_config.database, "user", "") or "").strip()
            password = str(getattr(runtime_config.database, "password", "") or "").strip()
            database = str(getattr(runtime_config.database, "database", "") or "").strip()
            port = int(getattr(runtime_config.database, "port", 0) or default_port)

            if not host or not user or not password or not database:
                raise RuntimeError(
                    "Database settings are incomplete in OCDocker.cfg. "
                    "Required: host, user, password, database."
                )

            db_url = URL.create(
                drivername=drivername,
                host=host,
                username=user,
                password=password,
                database=database,
                port=port,
            )

        engine = create_engine(db_url)
        create_database_if_not_exists(engine.url)
        session_factory = create_session(engine)

        ocinit_runtime.db_url = db_url
        ocinit_runtime.engine = engine
        ocinit_runtime.session = session_factory

    if _db_tables_initialized:
        return

    schema_dir = _runtime_cache_root()
    schema_dir.mkdir(parents=True, exist_ok=True)
    schema_lock_path = schema_dir / "pipeline_db_schema.lock"
    schema_ready_path = schema_dir / "pipeline_db_schema.ready"
    db_signature = _sha256_text(f"{getattr(ocinit_runtime, 'db_url', '')}|{_PIPELINE_DB_SCHEMA_VERSION}")

    with _db_tables_init_lock:
        if _db_tables_initialized:
            return

        with _file_lock(schema_lock_path):
            if schema_ready_path.is_file():
                try:
                    cached_signature = schema_ready_path.read_text(encoding="utf-8").splitlines()[0].strip()
                except Exception:
                    cached_signature = ""
                if cached_signature == db_signature:
                    _db_tables_initialized = True
                    return

            create_tables()
            schema_ready_path.write_text(f"{db_signature}\n{_utc_now_iso()}\n", encoding="utf-8")

        _db_tables_initialized = True


def _store_pipeline_results_in_db(
    job_name: str,
    receptor: Any,
    ligand: Any,
    rescoring: Dict[str, Dict[str, float]],
    box_label: Optional[str] = None,
    representative_pose: Optional[str] = None,
    representative_engine: Optional[str] = None,
    summary: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str, List[str]]:
    _ensure_db_runtime()

    from OCDocker.DB.Models.Complexes import Complexes
    from OCDocker.DB.Models.Ligands import Ligands
    from OCDocker.DB.Models.PipelineRuns import PipelineRuns
    from OCDocker.DB.Models.Receptors import Receptors

    receptor_name = str(getattr(receptor, "name", "") or f"{job_name}_receptor")
    ligand_name = str(getattr(ligand, "name", "") or f"{job_name}_ligand")
    complex_name = f"{job_name}_{box_label}" if box_label else job_name

    with _db_write_lock:
        receptor_payload: Dict[str, Union[str, int, float]] = {"name": receptor_name}
        receptor_payload.update(_collect_numeric_descriptors(receptor, list(getattr(Receptors, "allDescriptors", []))))

        ligand_payload: Dict[str, Union[str, int, float]] = {"name": ligand_name}
        ligand_payload.update(_collect_numeric_descriptors(ligand, list(getattr(Ligands, "allDescriptors", []))))

        receptor_ok = Receptors.insert_or_update(receptor_payload)
        ligand_ok = Ligands.insert_or_update(ligand_payload)
        if not receptor_ok or not ligand_ok:
            return False, "", []

        receptor_row = Receptors.find_first(receptor_name)
        ligand_row = Ligands.find_first(ligand_name)

        receptor_id = getattr(receptor_row, "id", None)
        ligand_id = getattr(ligand_row, "id", None)

        complex_payload: Dict[str, Union[str, int, float]] = {"name": complex_name}
        if isinstance(receptor_id, int):
            complex_payload["receptor_id"] = receptor_id
        if isinstance(ligand_id, int):
            complex_payload["ligand_id"] = ligand_id

        score_payload, ignored_keys = _flatten_rescoring_to_complex_payload(rescoring)
        complex_payload.update(score_payload)

        complex_ok = Complexes.insert_or_update(complex_payload)
        complex_row = Complexes.find_first(complex_name)
        complex_id = getattr(complex_row, "id", None)

        pipeline_run_payload: Dict[str, Union[str, int, None]] = {
            "name": complex_name,
            "representative_pose": str(representative_pose) if representative_pose else None,
            "representative_engine": str(representative_engine) if representative_engine else None,
            "rescoring_json": json.dumps(_to_jsonable(rescoring), sort_keys=True),
            "summary_json": json.dumps(_to_jsonable(summary or {}), sort_keys=True),
        }
        if isinstance(complex_id, int):
            pipeline_run_payload["complex_id"] = complex_id

        pipeline_run_ok = PipelineRuns.insert_or_update(pipeline_run_payload)
        return bool(complex_ok and pipeline_run_ok), complex_name, ignored_keys


def _canonicalize_rescore_key(engine: str, raw_key: str) -> str:
    key = str(raw_key).strip().lower().replace("-", "_").replace(" ", "_")
    while "__" in key:
        key = key.replace("__", "_")

    if key.endswith("_rescoring"):
        key = key[: -len("_rescoring")]

    if key.startswith(f"{engine}_"):
        return key

    if key.startswith("rescoring_"):
        parts = key.split("_")
        if len(parts) >= 2:
            if len(parts) >= 3 and parts[1] == "cnn":
                return f"{engine}_cnn_{parts[2]}"
            return f"{engine}_{parts[1]}"

    return f"{engine}_{key}"


def _prepare_cached_receptors_for_receptor(receptor_path):
    """Prepare receptor artifacts once per receptor and reuse across ligand jobs."""

    import OCDocker.Docking.Gnina as ocgnina
    import OCDocker.Docking.PLANTS as ocplants
    import OCDocker.Docking.Smina as ocsmina
    import OCDocker.Docking.Vina as ocvina

    receptor_path = str(receptor_path)
    receptor_dir = Path(receptor_path).resolve().parent

    if pipeline_requires_pdbqt:
        prepared_pdbqt = receptor_dir / "prepared_receptor.pdbqt"
        # Drop stale zero-byte artifacts even when overwrite=False.
        if prepared_pdbqt.is_file() and prepared_pdbqt.stat().st_size == 0:
            prepared_pdbqt.unlink()
        elif overwrite and prepared_pdbqt.exists():
            prepared_pdbqt.unlink()

        if not prepared_pdbqt.exists() or prepared_pdbqt.stat().st_size == 0:
            rc = None
            pdbqt_preparers = [
                ("vina", lambda: ocvina.run_prepare_receptor(receptor_path, str(prepared_pdbqt), logFile="", overwrite=overwrite)),
                ("smina", lambda: ocsmina.run_prepare_receptor(receptor_path, str(prepared_pdbqt), overwrite=overwrite)),
                ("gnina", lambda: ocgnina.run_prepare_receptor(receptor_path, str(prepared_pdbqt), overwrite=overwrite)),
            ]

            for prep_name, prep_fn in pdbqt_preparers:
                if prep_name not in pipeline_pdbqt_preparer_priority:
                    continue
                rc = _normalize_exit_code(prep_fn())
                if rc == 0 and prepared_pdbqt.exists() and prepared_pdbqt.stat().st_size > 0:
                    break

            if rc != 0 or not prepared_pdbqt.exists() or prepared_pdbqt.stat().st_size == 0:
                raise RuntimeError(
                    f"Failed to prepare cached PDBQT receptor for '{receptor_path}'. "
                    "Checked Vina/Smina/Gnina preparers."
                )

    if pipeline_requires_mol2:
        prepared_mol2 = receptor_dir / "prepared_receptor.mol2"
        # Drop stale zero-byte artifacts even when overwrite=False.
        if prepared_mol2.is_file() and prepared_mol2.stat().st_size == 0:
            prepared_mol2.unlink()
        elif overwrite and prepared_mol2.exists():
            prepared_mol2.unlink()

        if not prepared_mol2.exists() or prepared_mol2.stat().st_size == 0:
            rc = _normalize_exit_code(
                ocplants.run_prepare_receptor(receptor_path, str(prepared_mol2), log_file="", overwrite=overwrite)
            )
            if rc != 0 or not prepared_mol2.exists() or prepared_mol2.stat().st_size == 0:
                raise RuntimeError(f"Failed to prepare cached MOL2 receptor for '{receptor_path}' using PLANTS/SPORES.")


def _cache_settings_signature() -> str:
    signature_payload = {
        "engines": sorted(pipeline_engines_set),
        "rescoring": sorted(pipeline_rescoring_engines_set),
        "requires_pdbqt": pipeline_requires_pdbqt,
        "requires_mol2": pipeline_requires_mol2,
        "preparer_priority": pipeline_pdbqt_preparer_priority,
    }
    encoded = json.dumps(signature_payload, sort_keys=True).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


def _build_receptor_cache_manifest(receptor_path: Union[str, Path]) -> Dict[str, Any]:
    receptor_path = Path(receptor_path).resolve()
    receptor_stat = receptor_path.stat()
    receptor_dir = receptor_path.parent

    manifest: Dict[str, Any] = {
        "settings_signature": _cache_settings_signature(),
        "receptor": {
            "path": str(receptor_path),
            "size": int(receptor_stat.st_size),
            "mtime_ns": int(receptor_stat.st_mtime_ns),
        },
        "prepared": {},
    }

    if pipeline_requires_pdbqt:
        pdbqt_path = receptor_dir / "prepared_receptor.pdbqt"
        manifest["prepared"]["pdbqt"] = {
            "exists": pdbqt_path.exists(),
            "size": int(pdbqt_path.stat().st_size) if pdbqt_path.exists() else 0,
            "mtime_ns": int(pdbqt_path.stat().st_mtime_ns) if pdbqt_path.exists() else 0,
        }

    if pipeline_requires_mol2:
        mol2_path = receptor_dir / "prepared_receptor.mol2"
        manifest["prepared"]["mol2"] = {
            "exists": mol2_path.exists(),
            "size": int(mol2_path.stat().st_size) if mol2_path.exists() else 0,
            "mtime_ns": int(mol2_path.stat().st_mtime_ns) if mol2_path.exists() else 0,
        }

    return manifest


def _cache_manifest_is_valid(cache_manifest_path: Union[str, Path], receptor_path: Union[str, Path]) -> bool:
    cache_manifest_path = Path(cache_manifest_path)
    if not cache_manifest_path.is_file():
        return False

    try:
        current = _build_receptor_cache_manifest(receptor_path)
        stored = json.loads(cache_manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return False

    if stored.get("settings_signature") != current.get("settings_signature"):
        return False
    if stored.get("receptor") != current.get("receptor"):
        return False

    for required in ("pdbqt", "mol2"):
        if required not in current["prepared"]:
            continue
        current_prep = current["prepared"].get(required, {})
        stored_prep = stored.get("prepared", {}).get(required, {})
        if not current_prep.get("exists") or current_prep.get("size", 0) <= 0:
            return False
        if stored_prep != current_prep:
            return False

    return True


def _write_cache_manifest(cache_manifest_path: Union[str, Path], receptor_path: Union[str, Path]) -> None:
    cache_manifest_path = Path(cache_manifest_path)
    manifest = _build_receptor_cache_manifest(receptor_path)
    cache_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    cache_manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _ensure_receptor_cache_ready(receptor_path: Union[str, Path], cache_manifest_path: Union[str, Path]) -> None:
    receptor_path = Path(receptor_path)
    cache_manifest_path = Path(cache_manifest_path)
    if _cache_manifest_is_valid(cache_manifest_path, receptor_path):
        return

    _prepare_cached_receptors_for_receptor(str(receptor_path))
    _write_cache_manifest(cache_manifest_path, receptor_path)


def _cached_receptor_files_present(receptor_path: Union[str, Path]) -> bool:
    receptor_dir = Path(receptor_path).resolve().parent
    if pipeline_requires_pdbqt and not _is_valid_file(receptor_dir / "prepared_receptor.pdbqt"):
        return False
    if pipeline_requires_mol2 and not _is_valid_file(receptor_dir / "prepared_receptor.mol2"):
        return False
    return True


def _ligand_cache_manifest_path(database: str, receptor: str, kind: str, target: str) -> str:
    """Build the ligand preparation cache manifest path for one target."""

    return str(_target_dir_path(database, receptor, kind, target) / f".prepared_ligand_cache.{pipeline_cache_key}.json")


def _build_ligand_cache_manifest(ligand_path: Union[str, Path], target_dir: Union[str, Path]) -> Dict[str, Any]:
    ligand_path = Path(ligand_path).resolve()
    ligand_stat = ligand_path.stat()
    target_dir = Path(target_dir).resolve()

    manifest: Dict[str, Any] = {
        "settings_signature": _cache_settings_signature(),
        "ligand": {
            "path": str(ligand_path),
            "size": int(ligand_stat.st_size),
            "mtime_ns": int(ligand_stat.st_mtime_ns),
        },
        "prepared": {},
    }

    if pipeline_requires_pdbqt:
        pdbqt_path = target_dir / "prepared_ligand.pdbqt"
        manifest["prepared"]["pdbqt"] = {
            "exists": pdbqt_path.exists(),
            "size": int(pdbqt_path.stat().st_size) if pdbqt_path.exists() else 0,
            "mtime_ns": int(pdbqt_path.stat().st_mtime_ns) if pdbqt_path.exists() else 0,
        }

    if pipeline_requires_mol2:
        mol2_path = target_dir / "prepared_ligand.mol2"
        manifest["prepared"]["mol2"] = {
            "exists": mol2_path.exists(),
            "size": int(mol2_path.stat().st_size) if mol2_path.exists() else 0,
            "mtime_ns": int(mol2_path.stat().st_mtime_ns) if mol2_path.exists() else 0,
        }

    return manifest


def _ligand_cache_manifest_is_valid(
    cache_manifest_path: Union[str, Path],
    ligand_path: Union[str, Path],
    target_dir: Union[str, Path],
) -> bool:
    cache_manifest_path = Path(cache_manifest_path)
    if not cache_manifest_path.is_file():
        return False

    try:
        current = _build_ligand_cache_manifest(ligand_path, target_dir)
        stored = json.loads(cache_manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return False

    if stored.get("settings_signature") != current.get("settings_signature"):
        return False
    if stored.get("ligand") != current.get("ligand"):
        return False

    for required in ("pdbqt", "mol2"):
        if required not in current["prepared"]:
            continue
        current_prep = current["prepared"].get(required, {})
        stored_prep = stored.get("prepared", {}).get(required, {})
        if not current_prep.get("exists") or current_prep.get("size", 0) <= 0:
            return False
        if stored_prep != current_prep:
            return False

    return True


def _write_ligand_cache_manifest(
    cache_manifest_path: Union[str, Path],
    ligand_path: Union[str, Path],
    target_dir: Union[str, Path],
) -> None:
    cache_manifest_path = Path(cache_manifest_path)
    manifest = _build_ligand_cache_manifest(ligand_path, target_dir)
    cache_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    cache_manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _prepare_cached_ligands_for_target(
    receptor_path: Union[str, Path],
    ligand_path: Union[str, Path],
    box_path: Union[str, Path],
    target_dir: Union[str, Path],
    job_name: str,
) -> None:
    """Prepare shared ligand artifacts once per target entry."""

    import OCDocker.Docking.Gnina as ocgnina
    import OCDocker.Docking.PLANTS as ocplants
    import OCDocker.Docking.Smina as ocsmina
    import OCDocker.Docking.Vina as ocvina
    import OCDocker.Ligand as ocl
    import OCDocker.Receptor as ocr

    receptor_path = Path(receptor_path).resolve()
    ligand_path = Path(ligand_path).resolve()
    box_path = Path(box_path).resolve()
    target_dir = Path(target_dir).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    if pipeline_timeout:
        os.environ["OCDOCKER_TIMEOUT"] = str(pipeline_timeout)

    receptor_dir = receptor_path.parent
    receptor_obj = ocr.Receptor(str(receptor_path), name=f"{job_name}_receptor")
    ligand_obj = ocl.Ligand(str(ligand_path), name=job_name)

    if pipeline_requires_pdbqt:
        prep_receptor = receptor_dir / "prepared_receptor.pdbqt"
        prep_ligand = target_dir / "prepared_ligand.pdbqt"
        if not _is_valid_file(prep_receptor):
            raise RuntimeError(
                f"Missing prepared receptor PDBQT at '{prep_receptor}'. "
                "Run prepare_receptor_cache first."
            )

        pdbqt_prepared = False
        for engine in pipeline_pdbqt_preparer_priority:
            prep_tmp_dir = target_dir / ".ligand_prep"
            prep_tmp_dir.mkdir(parents=True, exist_ok=True)

            if engine == "vina":
                runner = ocvina.Vina(
                    str(prep_tmp_dir / "conf_prepare_vina.txt"),
                    str(box_path),
                    receptor_obj,
                    str(prep_receptor),
                    ligand_obj,
                    str(prep_ligand),
                    str(prep_tmp_dir / "prepare_ligand_vina.log"),
                    str(prep_tmp_dir / "prepare_ligand_vina.pdbqt"),
                    name=f"VINA_PREP {job_name}",
                    overwrite_config=overwrite,
                )
            elif engine == "smina":
                runner = ocsmina.Smina(
                    str(prep_tmp_dir / "conf_prepare_smina.txt"),
                    str(box_path),
                    receptor_obj,
                    str(prep_receptor),
                    ligand_obj,
                    str(prep_ligand),
                    str(prep_tmp_dir / "prepare_ligand_smina.log"),
                    str(prep_tmp_dir / "prepare_ligand_smina.pdbqt"),
                    name=f"SMINA_PREP {job_name}",
                    overwrite_config=overwrite,
                )
            elif engine == "gnina":
                runner = ocgnina.Gnina(
                    str(prep_tmp_dir / "conf_prepare_gnina.conf"),
                    str(box_path),
                    receptor_obj,
                    str(prep_receptor),
                    ligand_obj,
                    str(prep_ligand),
                    str(prep_tmp_dir / "prepare_ligand_gnina.log"),
                    str(prep_tmp_dir / "prepare_ligand_gnina.pdbqt"),
                    name=f"GNINA_PREP {job_name}",
                    overwrite_config=overwrite,
                )
            else:
                continue

            if _ensure_prepared_file_with_lock(prep_ligand, lambda: runner.run_prepare_ligand(overwrite=overwrite)):
                pdbqt_prepared = True
                break

        if not pdbqt_prepared:
            raise RuntimeError(
                f"Failed to prepare cached PDBQT ligand for '{ligand_path}' using "
                f"{','.join(pipeline_pdbqt_preparer_priority)} preparers."
            )

    if pipeline_requires_mol2:
        prep_receptor = receptor_dir / "prepared_receptor.mol2"
        prep_ligand = target_dir / "prepared_ligand.mol2"
        if not _is_valid_file(prep_receptor):
            raise RuntimeError(
                f"Missing prepared receptor MOL2 at '{prep_receptor}'. "
                "Run prepare_receptor_cache first."
            )

        prep_tmp_dir = target_dir / ".ligand_prep"
        prep_tmp_dir.mkdir(parents=True, exist_ok=True)
        plants_runner = ocplants.PLANTS(
            str(prep_tmp_dir / "conf_prepare_plants.txt"),
            str(box_path),
            receptor_obj,
            str(prep_receptor),
            ligand_obj,
            str(prep_ligand),
            str(prep_tmp_dir / "prepare_ligand_plants.log"),
            str(prep_tmp_dir / "prepare_ligand_plants"),
            name=f"PLANTS_PREP {job_name}",
            overwrite_config=overwrite,
        )
        if not _ensure_prepared_file_with_lock(prep_ligand, lambda: plants_runner.run_prepare_ligand(overwrite=overwrite)):
            raise RuntimeError(f"Failed to prepare cached MOL2 ligand for '{ligand_path}' using PLANTS/SPORES.")


def _ensure_ligand_cache_ready(
    receptor_path: Union[str, Path],
    ligand_path: Union[str, Path],
    box_path: Union[str, Path],
    target_dir: Union[str, Path],
    cache_manifest_path: Union[str, Path],
    job_name: str,
) -> None:
    target_dir = Path(target_dir)
    cache_manifest_path = Path(cache_manifest_path)
    if _ligand_cache_manifest_is_valid(cache_manifest_path, ligand_path, target_dir):
        return

    _prepare_cached_ligands_for_target(receptor_path, ligand_path, box_path, target_dir, job_name)
    _write_ligand_cache_manifest(cache_manifest_path, ligand_path, target_dir)


# Pipeline engine and rescoring selection
engine_executables = {
    "vina": getattr(getattr(oc_config, "vina", None), "executable", None),
    "smina": getattr(getattr(oc_config, "smina", None), "executable", None),
    "gnina": getattr(getattr(oc_config, "gnina", None), "executable", None),
    "plants": getattr(getattr(oc_config, "plants", None), "executable", None),
}
auto_engines = [
    engine for engine in ("vina", "gnina", "plants") if _binary_available(engine_executables.get(engine))
]
default_engines = auto_engines or ["vina", "gnina", "plants"]

pipeline_engines = [
    engine.lower() for engine in _parse_list(config.get("pipeline_engines"), default_engines)
]
valid_docking_engines = {"vina", "gnina", "plants"}
pipeline_engines = [engine for engine in pipeline_engines if engine in valid_docking_engines]
pipeline_engines = list(dict.fromkeys(pipeline_engines))
if not pipeline_engines:
    raise RuntimeError(
        "No valid docking engines configured for pipeline execution. "
        "Set pipeline_engines in config.yaml with at least one of: vina,gnina,plants"
    )

unavailable_requested_engines = [
    engine for engine in pipeline_engines if not _binary_available(engine_executables.get(engine))
]
if unavailable_requested_engines:
    missing_bins = ", ".join(
        f"{engine} ({engine_executables.get(engine) or 'not set'})" for engine in unavailable_requested_engines
    )
    raise RuntimeError(
        "Configured docking engines are unavailable in OCDocker.cfg/PATH: "
        f"{missing_bins}. Fix executable paths or remove unavailable engines from pipeline_engines."
    )
pipeline_rescoring_default = ["vina", "smina", "gnina", "plants", "oddt"]
requested_rescoring_engines = [
    engine.lower() for engine in _parse_list(config.get("pipeline_rescoring_engines"), pipeline_rescoring_default)
]
valid_rescoring_engines = {"vina", "smina", "gnina", "plants", "oddt"}
requested_rescoring_engines = [engine for engine in requested_rescoring_engines if engine in valid_rescoring_engines]
pipeline_rescoring_engines = list(
    dict.fromkeys(pipeline_rescoring_default + requested_rescoring_engines)
)
pipeline_engines_set = set(pipeline_engines)
pipeline_rescoring_engines_set = set(pipeline_rescoring_engines)
pipeline_engines_pattern = "|".join(pipeline_engines)
pipeline_effective_engines = pipeline_engines_set | pipeline_rescoring_engines_set
pipeline_requires_pdbqt = bool(pipeline_effective_engines & {"vina", "smina", "gnina"})
pipeline_requires_mol2 = bool(pipeline_effective_engines & {"plants"})
pipeline_pdbqt_preparer_priority = [engine for engine in ("vina", "smina", "gnina") if engine in pipeline_effective_engines]

pipeline_cluster_min = float(config.get("pipeline_cluster_min", 10.0))
pipeline_cluster_max = float(config.get("pipeline_cluster_max", 20.0))
pipeline_cluster_step = float(config.get("pipeline_cluster_step", 0.1))
pipeline_all_boxes = _as_bool(config.get("pipeline_all_boxes", False), default=False)
pipeline_store_db = _as_bool(config.get("pipeline_store_db", True), default=True)
pipeline_discovery_cache = _as_bool(config.get("pipeline_discovery_cache", True), default=True)
pipeline_report_include_python_packages = _as_bool(
    config.get("pipeline_report_include_python_packages", False),
    default=False,
)


def _parse_engine_int_map(value: Any) -> Dict[str, int]:
    """Parse engine->integer maps from dicts or comma-separated key:value text."""

    mapping: Dict[str, int] = {}
    items = None
    if isinstance(value, dict):
        items = value.items()
    elif isinstance(value, str):
        pairs = [pair.strip() for pair in value.split(",") if pair.strip()]
        items = []
        for pair in pairs:
            if ":" not in pair:
                continue
            key, raw = pair.split(":", 1)
            items.append((key.strip(), raw.strip()))

    if items is None:
        return mapping

    for raw_key, raw_value in items:
        key = str(raw_key).strip().lower()
        if key not in valid_docking_engines:
            continue
        try:
            number = int(raw_value)
        except (TypeError, ValueError):
            continue
        if number > 0:
            mapping[key] = number

    return mapping


pipeline_engine_threads_default = max(1, int(config.get("pipeline_engine_threads_default", 1)))
pipeline_engine_threads_map = _parse_engine_int_map(config.get("pipeline_engine_threads", {}))

pipeline_engine_mem_mb_default = max(1, int(config.get("pipeline_engine_mem_mb_default", 2000)))
pipeline_engine_mem_mb_map = _parse_engine_int_map(config.get("pipeline_engine_mem_mb", {"gnina": 8000}))

pipeline_postprocess_threads = max(1, int(config.get("pipeline_postprocess_threads", 1)))
pipeline_postprocess_mem_mb = max(1, int(config.get("pipeline_postprocess_mem_mb", 4000)))


def _engine_threads(engine: str) -> int:
    """Return configured CPU threads for a given engine rule instance."""

    return max(1, int(pipeline_engine_threads_map.get(engine, pipeline_engine_threads_default)))


def _engine_mem_mb(engine: str) -> int:
    """Return configured memory budget in MB for a given engine rule instance."""

    return max(1, int(pipeline_engine_mem_mb_map.get(engine, pipeline_engine_mem_mb_default)))


_timeout_raw = config.get("pipeline_timeout", None)
if _timeout_raw in (None, ""):
    pipeline_timeout = None
else:
    pipeline_timeout = int(_timeout_raw)

pipeline_cache_key = hashlib.sha1(
    json.dumps(
        {
            "engines": sorted(pipeline_engines_set),
            "rescoring": sorted(pipeline_rescoring_engines_set),
            "requires_pdbqt": pipeline_requires_pdbqt,
            "requires_mol2": pipeline_requires_mol2,
        },
        sort_keys=True,
    ).encode("utf-8")
).hexdigest()[:12]

_PRESET_DATABASES = {"PDBbind", "DUDEz"}


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


def _parse_database_sources(sources: List[str]) -> Dict[str, Dict[str, Any]]:
    specs: Dict[str, Dict[str, Any]] = {}
    seen_aliases: Dict[str, str] = {}

    for raw_source in sources:
        source = str(raw_source).strip()
        if not source:
            continue

        alias: str
        target_path: Path

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


raw_database_sources = _parse_list(config.get("database_sources"), [])
if not raw_database_sources:
    raw_database_sources = _parse_list(config.get("run_databases"), ["PDBbind", "DUDEz"])
database_specs = _parse_database_sources(raw_database_sources)
selected_databases = list(database_specs.keys())
pipeline_databases_pattern = "|".join(re.escape(database) for database in selected_databases)

selected_kinds = [kind.lower() for kind in _parse_list(config.get("compound_kinds"), ["ligands", "decoys", "compounds"])]
selected_kinds = [kind for kind in selected_kinds if kind in {"ligands", "decoys", "compounds"}]
if not selected_kinds:
    raise RuntimeError("No valid compound_kinds configured. Use one or more of: ligands, decoys, compounds")

target_discovery_mode = str(config.get("target_discovery_mode", "hybrid")).strip().lower() or "hybrid"
valid_discovery_modes = {"index", "filesystem", "hybrid"}
if target_discovery_mode not in valid_discovery_modes:
    raise RuntimeError(
        f"Invalid target_discovery_mode='{target_discovery_mode}'. "
        "Valid values are: index, filesystem, hybrid."
    )

enable_legacy_database_rules = _as_bool(config.get("enable_legacy_database_rules", False), default=False)

preset_database_aliases: Dict[str, List[str]] = {"PDBbind": [], "DUDEz": []}
for database, spec in database_specs.items():
    preset_name = str(spec.get("preset", "") or "")
    if preset_name in preset_database_aliases:
        preset_database_aliases[preset_name].append(database)


def _database_root_path(database: str) -> Path:
    spec = database_specs.get(database)
    if spec is None:
        raise RuntimeError(f"Unknown database alias '{database}'. Check database_sources configuration.")
    return Path(str(spec["root"]))


database_rule_root = Path(ocdb_path).resolve()
database_rule_root.mkdir(parents=True, exist_ok=True)
database_rule_root_str = str(database_rule_root)


def _prepare_database_mounts() -> None:
    for database in selected_databases:
        source_root = _database_root_path(database).resolve()
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


_prepare_database_mounts()


def _database_rule_root_path(database: str) -> Path:
    return database_rule_root / database


def _source_receptor_path(database: str, receptor: str) -> Path:
    return _database_root_path(database) / receptor / "receptor.pdb"


def _receptor_path(database: str, receptor: str) -> Path:
    return _database_rule_root_path(database) / receptor / "receptor.pdb"


def _receptor_cache_manifest_path(database: str, receptor: str) -> Path:
    return _database_rule_root_path(database) / receptor / f".prepared_receptor_cache.{pipeline_cache_key}.json"


def _target_dir_path(database: str, receptor: str, kind: str, target: str) -> Path:
    return _database_rule_root_path(database) / receptor / "compounds" / kind / target


def _ligand_path(database: str, receptor: str, kind: str, target: str) -> Path:
    return _target_dir_path(database, receptor, kind, target) / "ligand.smi"


def _box_path(database: str, receptor: str, kind: str, target: str) -> Path:
    return _target_dir_path(database, receptor, kind, target) / "boxes" / "box0.pdb"


def _payload_path(database: str, receptor: str, kind: str, target: str) -> Path:
    return _target_dir_path(database, receptor, kind, target) / "payload.pkl"


def _run_report_path(database: str, receptor: str, kind: str, target: str) -> Path:
    return _target_dir_path(database, receptor, kind, target) / "run_report.json"


ignored_pdb_index = str(config.get("ignored_pdb_database_index", "") or "").strip()
ignored_dudez_index = str(config.get("ignored_dudez_database_index", "") or "").strip()
ignored_pdb_targets = _load_ignored_targets(ignored_pdb_index)
ignored_dudez_targets = _load_ignored_targets(ignored_dudez_index)
ignored_receptors_by_database: Dict[str, Set[str]] = {database: set() for database in selected_databases}
for _database in preset_database_aliases["PDBbind"]:
    ignored_receptors_by_database[_database] = set(ignored_pdb_targets)
for _database in preset_database_aliases["DUDEz"]:
    ignored_receptors_by_database[_database] = set(ignored_dudez_targets)


custom_database_aliases = [db for db, spec in database_specs.items() if not spec.get("preset")]
if target_discovery_mode == "index" and custom_database_aliases:
    raise RuntimeError(
        "target_discovery_mode=index is supported only for preset databases (PDBbind/DUDEz). "
        "Custom database sources require target_discovery_mode=filesystem or hybrid. "
        f"Custom sources: {', '.join(custom_database_aliases)}"
    )

index_targets: Dict[str, List[str]] = {database: [] for database in selected_databases}
if target_discovery_mode in {"index", "hybrid"}:
    import OCDP.preload as OCDPpre

    pdb_database_index = str(config.get("pdb_database_index", "") or "").strip()
    dudez_database_index = str(config.get("dudez_database_index", "") or "").strip()

    if not pdb_database_index and target_discovery_mode == "index" and preset_database_aliases["PDBbind"]:
        raise RuntimeError("pdb_database_index is required when target_discovery_mode=index for PDBbind.")
    if not dudez_database_index and target_discovery_mode == "index" and preset_database_aliases["DUDEz"]:
        raise RuntimeError("dudez_database_index is required when target_discovery_mode=index for DUDEz.")

    pdb_index_targets: List[str] = []
    dudez_index_targets: List[str] = []

    if pdb_database_index and preset_database_aliases["PDBbind"]:
        try:
            pdb_index_targets = OCDPpre.preload_PDBbind(pdb_database_index, ignored_pdb_index)
        except Exception as exc:
            if target_discovery_mode == "index":
                raise RuntimeError(f"Failed loading PDBbind index targets: {exc}") from exc
            print(f"Warning: failed loading PDBbind index targets ({exc}). Falling back to filesystem discovery.")

    if dudez_database_index and preset_database_aliases["DUDEz"]:
        try:
            dudez_index_targets = OCDPpre.preload_DUDEz(dudez_database_index, ignored_dudez_index)
        except Exception as exc:
            if target_discovery_mode == "index":
                raise RuntimeError(f"Failed loading DUDEz index targets: {exc}") from exc
            print(f"Warning: failed loading DUDEz index targets ({exc}). Falling back to filesystem discovery.")

    for database in preset_database_aliases["PDBbind"]:
        index_targets[database] = list(pdb_index_targets)
    for database in preset_database_aliases["DUDEz"]:
        index_targets[database] = list(dudez_index_targets)


def _discover_receptors_from_filesystem(database: str) -> List[str]:
    db_dir = _database_root_path(database)
    if not db_dir.exists():
        return []

    receptors: List[str] = []
    for receptor_file in db_dir.glob("*/receptor.pdb"):
        if receptor_file.is_file():
            receptors.append(receptor_file.parent.name)
    return sorted(set(receptors))


def _collect_database_receptors(database: str) -> List[str]:
    receptors: List[str] = []

    if target_discovery_mode in {"index", "hybrid"}:
        receptors.extend(index_targets.get(database, []))
    if target_discovery_mode in {"filesystem", "hybrid"}:
        receptors.extend(_discover_receptors_from_filesystem(database))

    ignored = ignored_receptors_by_database.get(database, set())
    if ignored:
        receptors = [receptor for receptor in receptors if receptor not in ignored]

    result = sorted(set(receptors))
    if database in selected_databases and not result:
        source = database_specs.get(database, {}).get("source", database)
        raise RuntimeError(
            f"No receptors discovered for database '{database}' ({source}) "
            f"with target_discovery_mode={target_discovery_mode}."
        )
    return result


database_to_receptors: Dict[str, List[str]] = {
    database: _collect_database_receptors(database) for database in selected_databases
}


def _target_discovery_cache_path() -> Path:
    return _runtime_cache_root() / "target_discovery_cache.json"


def _target_discovery_signature(database_to_receptors: Dict[str, List[str]]) -> str:
    layout: List[Dict[str, Any]] = []
    for database in selected_databases:
        database_root = _database_root_path(database)
        for receptor in database_to_receptors.get(database, []):
            receptor_path = database_root / receptor / "receptor.pdb"
            receptor_exists = receptor_path.is_file()
            receptor_stat = receptor_path.stat() if receptor_exists else None
            receptor_entry: Dict[str, Any] = {
                "database": database,
                "database_root": str(database_root),
                "receptor": receptor,
                "receptor_exists": receptor_exists,
                "receptor_size": int(receptor_stat.st_size) if receptor_stat else 0,
                "receptor_mtime_ns": int(receptor_stat.st_mtime_ns) if receptor_stat else 0,
                "kinds": [],
            }

            compounds_dir = database_root / receptor / "compounds"
            for kind in selected_kinds:
                kind_dir = compounds_dir / kind
                kind_exists = kind_dir.is_dir()
                kind_stat = kind_dir.stat() if kind_exists else None
                receptor_entry["kinds"].append(
                    {
                        "kind": kind,
                        "path": str(kind_dir),
                        "exists": kind_exists,
                        "mtime_ns": int(kind_stat.st_mtime_ns) if kind_stat else 0,
                    }
                )

            layout.append(receptor_entry)

    payload = {
        "schema_version": 1,
        "ocdb_path": str(Path(ocdb_path).resolve()),
        "selected_databases": list(selected_databases),
        "database_roots": {database: str(_database_root_path(database)) for database in selected_databases},
        "selected_kinds": list(selected_kinds),
        "target_discovery_mode": target_discovery_mode,
        "layout": layout,
    }
    return _json_sha256(payload)


def _load_target_discovery_cache(signature: str) -> Optional[Tuple[List[str], int]]:
    cache_path = _target_discovery_cache_path()
    if not cache_path.is_file():
        return None

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    if payload.get("schema_version") != 1:
        return None
    if payload.get("signature") != signature:
        return None

    targets_payload = payload.get("targets")
    if not isinstance(targets_payload, list) or not targets_payload:
        return None

    targets = [str(path) for path in targets_payload if str(path).strip()]
    if not targets:
        return None

    try:
        scanned = int(payload.get("scanned", 0))
    except (TypeError, ValueError):
        scanned = 0

    return sorted(set(targets)), max(0, scanned)


def _write_target_discovery_cache(signature: str, targets: List[str], scanned: int) -> None:
    cache_path = _target_discovery_cache_path()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "generated_at_utc": _utc_now_iso(),
        "signature": signature,
        "scanned": int(scanned),
        "targets": sorted(set(targets)),
    }

    tmp_path = cache_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(cache_path)


def collect_payload_targets():
    targets = []
    scanned = 0

    if pipeline_discovery_cache:
        discovery_signature = _target_discovery_signature(database_to_receptors)
        cached = _load_target_discovery_cache(discovery_signature)
        if cached is not None:
            cached_targets, cached_scanned = cached
            print(
                "Discovery summary: "
                f"mode={target_discovery_mode}, scanned={cached_scanned}, valid_targets={len(cached_targets)}, cache=hit"
            )
            return cached_targets
    else:
        discovery_signature = ""

    for database in selected_databases:
        for receptor in database_to_receptors.get(database, []):
            receptor_path = _source_receptor_path(database, receptor)
            if not _is_valid_file(receptor_path):
                continue

            compounds_dir = _database_root_path(database) / receptor / "compounds"
            if not compounds_dir.is_dir():
                continue

            for kind in selected_kinds:
                kind_dir = compounds_dir / kind
                if not kind_dir.is_dir():
                    continue

                for target_dir in sorted(path for path in kind_dir.iterdir() if path.is_dir()):
                    scanned += 1
                    ligand_path = target_dir / "ligand.smi"
                    box_path = target_dir / "boxes" / "box0.pdb"
                    if not _is_valid_file(ligand_path) or not _is_valid_file(box_path):
                        continue

                    targets.append(
                        str(_payload_path(database, receptor, kind, target_dir.name))
                    )

    unique_targets = sorted(set(targets))
    if not unique_targets:
        raise RuntimeError(
            "No valid targets found to process. "
            "Checked selected databases/kinds and required files: receptor.pdb, ligand.smi, boxes/box0.pdb."
        )

    if pipeline_discovery_cache:
        _write_target_discovery_cache(discovery_signature, unique_targets, scanned)

    print(
        "Discovery summary: "
        f"mode={target_discovery_mode}, scanned={scanned}, valid_targets={len(unique_targets)}, cache=miss"
    )
    return unique_targets


def _engine_summary_path(database: str, receptor: str, kind: str, target: str, engine: str) -> str:
    '''Build the per-engine summary output path for a target entry.

    Parameters
    ----------
    database : str
        Dataset name (for example ``PDBbind`` or ``DUDEz``).
    receptor : str
        Receptor identifier.
    kind : str
        Compound subset name (``ligands``, ``decoys``, or ``compounds``).
    target : str
        Target/molecule folder name under the selected ``kind``.
    engine : str
        Docking engine identifier.

    Returns
    -------
    str
        Absolute path to ``engine_status/{engine}.json`` for the target.
    '''

    return str(_target_dir_path(database, receptor, kind, target) / "engine_status" / f"{engine}.json")


def _engine_summary_inputs(wildcards) -> List[str]:
    '''Resolve required engine summaries for ``run_pipeline``.

    Parameters
    ----------
    wildcards : snakemake.io.Wildcards
        Wildcards from the ``run_pipeline`` rule.

    Returns
    -------
    List[str]
        Ordered list of per-engine summary JSON files expected for the target.
    '''

    return [
        _engine_summary_path(
            wildcards.database,
            wildcards.receptor,
            wildcards.kind,
            wildcards.target,
            engine,
        )
        for engine in pipeline_engines
    ]


def _preset_receptor_inputs(preset_name: str) -> List[str]:
    paths: List[str] = []
    for database in preset_database_aliases.get(preset_name, []):
        for receptor in database_to_receptors.get(database, []):
            paths.append(str(_receptor_path(database, receptor)))
    return sorted(set(paths))


def _wc_receptor_path(wildcards) -> str:
    return str(_receptor_path(wildcards.database, wildcards.receptor))


def _wc_receptor_cache_manifest_path(wildcards) -> str:
    return str(_receptor_cache_manifest_path(wildcards.database, wildcards.receptor))


def _wc_ligand_path(wildcards) -> str:
    return str(_ligand_path(wildcards.database, wildcards.receptor, wildcards.kind, wildcards.target))


def _wc_box_path(wildcards) -> str:
    return str(_box_path(wildcards.database, wildcards.receptor, wildcards.kind, wildcards.target))


def _wc_ligand_cache_manifest_path(wildcards) -> str:
    return _ligand_cache_manifest_path(wildcards.database, wildcards.receptor, wildcards.kind, wildcards.target)


def _ensure_prepared_file_with_lock(path: Union[str, Path], prepare_fn) -> bool:
    '''Create a preparation artifact once, safely under parallel execution.

    This helper uses a lock file next to the output to avoid multiple engine jobs
    preparing the same receptor/ligand artifact at the same time.

    Parameters
    ----------
    path : Union[str, Path]
        Output file that must exist and be non-empty after preparation.
    prepare_fn : Callable[[], Any]
        Function that executes the preparation command/API call.

    Returns
    -------
    bool
        ``True`` if the prepared file exists and is valid; ``False`` otherwise.
    '''

    prep_path = Path(path)
    if _is_valid_file(prep_path):
        return True

    lock_file = prep_path.parent / f".{prep_path.name}.lock"
    with _file_lock(lock_file):
        if _is_valid_file(prep_path):
            return True
        # Some preparers skip when file already exists and overwrite is disabled.
        # Remove stale zero-byte artifacts so preparation can proceed.
        if prep_path.is_file():
            try:
                if prep_path.stat().st_size == 0:
                    prep_path.unlink()
            except OSError:
                pass
        rc = _normalize_exit_code(prepare_fn())
        if rc != 0:
            return False

    return _is_valid_file(prep_path)


def _run_single_engine_for_box(
    engine: str,
    receptor: Any,
    ligand: Any,
    box_path: Path,
    outdir: Path,
    job_name: str,
    receptor_prepare_dir: Path,
    ligand_prepare_dir: Path,
    engine_cpu_threads: int = 1,
) -> Dict[str, Any]:
    '''Run one docking engine for one box using OCDocker API objects.

    Parameters
    ----------
    engine : str
        Engine name (``vina``, ``smina``, ``gnina``, ``plants``).
    receptor : Any
        ``OCDocker.Receptor.Receptor`` instance.
    ligand : Any
        ``OCDocker.Ligand.Ligand`` instance.
    box_path : Path
        Docking box file path.
    outdir : Path
        Base output directory for this engine/box execution.
    job_name : str
        Pipeline job label used for logs and output naming.
    receptor_prepare_dir : Path
        Directory holding shared receptor preparation artifacts.
    ligand_prepare_dir : Path
        Directory holding shared ligand preparation artifacts.

    Returns
    -------
    Dict[str, Any]
        Structured status payload with preparation paths, produced poses,
        success flag, and error message when applicable.
    '''

    import OCDocker.Docking.Gnina as ocgnina
    import OCDocker.Docking.PLANTS as ocplants
    import OCDocker.Docking.Smina as ocsmina
    import OCDocker.Docking.Vina as ocvina

    outdir.mkdir(parents=True, exist_ok=True)
    receptor_prepare_dir.mkdir(parents=True, exist_ok=True)
    ligand_prepare_dir.mkdir(parents=True, exist_ok=True)

    engine_dir = outdir / f"{engine}Files"
    engine_dir.mkdir(parents=True, exist_ok=True)

    result: Dict[str, Any] = {
        "success": False,
        "engine": engine,
        "box": box_path.stem,
        "dir": str(engine_dir),
        "conf": "",
        "prep_rec": "",
        "prep_lig": "",
        "poses": [],
        "error": "",
    }

    try:
        if engine == "vina":
            conf = engine_dir / "conf_vina.txt"
            prep_receptor = receptor_prepare_dir / "prepared_receptor.pdbqt"
            prep_ligand = ligand_prepare_dir / "prepared_ligand.pdbqt"
            log = engine_dir / f"{job_name}.log"
            output_pose = engine_dir / f"{job_name}.pdbqt"
            runner = ocvina.Vina(
                str(conf),
                str(box_path),
                receptor,
                str(prep_receptor),
                ligand,
                str(prep_ligand),
                str(log),
                str(output_pose),
                name=f"VINA {job_name}",
                overwrite_config=overwrite,
            )
        elif engine == "smina":
            conf = engine_dir / "conf_smina.txt"
            prep_receptor = receptor_prepare_dir / "prepared_receptor.pdbqt"
            prep_ligand = ligand_prepare_dir / "prepared_ligand.pdbqt"
            log = engine_dir / f"{job_name}.log"
            output_pose = engine_dir / f"{job_name}.pdbqt"
            runner = ocsmina.Smina(
                str(conf),
                str(box_path),
                receptor,
                str(prep_receptor),
                ligand,
                str(prep_ligand),
                str(log),
                str(output_pose),
                name=f"SMINA {job_name}",
                overwrite_config=overwrite,
            )
        elif engine == "gnina":
            conf = engine_dir / "conf_gnina.conf"
            prep_receptor = receptor_prepare_dir / "prepared_receptor.pdbqt"
            prep_ligand = ligand_prepare_dir / "prepared_ligand.pdbqt"
            log = engine_dir / f"{job_name}.log"
            output_pose = engine_dir / f"{job_name}.pdbqt"
            runner = ocgnina.Gnina(
                str(conf),
                str(box_path),
                receptor,
                str(prep_receptor),
                ligand,
                str(prep_ligand),
                str(log),
                str(output_pose),
                name=f"GNINA {job_name}",
                overwrite_config=overwrite,
            )
        else:
            conf = engine_dir / "conf_plants.txt"
            prep_receptor = receptor_prepare_dir / "prepared_receptor.mol2"
            prep_ligand = ligand_prepare_dir / "prepared_ligand.mol2"
            log = engine_dir / f"{job_name}.log"
            output_pose = engine_dir
            runner = ocplants.PLANTS(
                str(conf),
                str(box_path),
                receptor,
                str(prep_receptor),
                ligand,
                str(prep_ligand),
                str(log),
                str(output_pose),
                name=f"PLANTS {job_name}",
                overwrite_config=overwrite,
            )

        result["conf"] = str(conf)
        result["prep_rec"] = str(prep_receptor)
        result["prep_lig"] = str(prep_ligand)
        _apply_engine_cpu_hint(engine, runner, engine_cpu_threads)

        if not _ensure_prepared_file_with_lock(prep_receptor, lambda: runner.run_prepare_receptor(overwrite=overwrite)):
            result["error"] = f"receptor preparation failed for {engine}"
            return result

        if not _ensure_prepared_file_with_lock(prep_ligand, lambda: runner.run_prepare_ligand(overwrite=overwrite)):
            result["error"] = f"ligand preparation failed for {engine}"
            return result

        dock_rc = _normalize_exit_code(runner.run_docking())
        if dock_rc != 0:
            result["error"] = f"docking failed with code {dock_rc}"
            return result

        if engine in {"vina", "smina", "gnina"}:
            _ = runner.split_poses(str(engine_dir))

        poses = [str(p) for p in runner.get_docked_poses() if _is_valid_file(p)]
        if not poses:
            result["error"] = "no poses generated"
            return result

        result["poses"] = poses
        result["success"] = True
        return result
    except Exception as exc:
        result["error"] = str(exc)
        return result


def _run_single_engine_via_api(
    engine: str,
    receptor_path: str,
    ligand_path: str,
    box_path: str,
    outdir_path: str,
    job_name: str,
    max_workers: int = 1,
) -> Dict[str, Any]:
    '''Run one engine across one or many boxes and return summary payload.

    Parameters
    ----------
    engine : str
        Engine name to execute.
    receptor_path : str
        Path to receptor input file.
    ligand_path : str
        Path to ligand input file.
    box_path : str
        Path to default box file (``box0.pdb``).
    outdir_path : str
        Target output directory for this molecule entry.
    job_name : str
        Stable pipeline job identifier.

    Returns
    -------
    Dict[str, Any]
        Summary dictionary written by ``run_engine`` rule as JSON.
    '''

    import OCDocker.Ligand as ocl
    import OCDocker.Receptor as ocr

    if pipeline_timeout:
        os.environ["OCDOCKER_TIMEOUT"] = str(pipeline_timeout)

    receptor_obj = ocr.Receptor(str(receptor_path), name=f"{job_name}_receptor")
    ligand_name = job_name[:-7] if job_name.endswith("_ligand") else job_name
    ligand_obj = ocl.Ligand(str(ligand_path), name=ligand_name)

    base_outdir = Path(outdir_path).resolve()
    ligand_prepare_dir = base_outdir
    receptor_prepare_dir = Path(receptor_path).resolve().parent

    ligand_dir = Path(ligand_path).parent
    box_path_obj = Path(box_path)
    boxes = _list_boxes(ligand_dir, box_path_obj, pipeline_all_boxes)
    if pipeline_all_boxes and not boxes:
        return {
            "engine": engine,
            "job": job_name,
            "pipeline_version": pipeline_version,
            "boxes": {},
            "error": "no box*.pdb files found",
        }

    use_multi_boxes = pipeline_all_boxes and len(boxes) > 1
    summary: Dict[str, Any] = {
        "engine": engine,
        "job": job_name,
        "pipeline_version": pipeline_version,
        "boxes": {},
    }
    requested_workers = max(1, int(max_workers))
    box_workers = min(len(boxes), requested_workers)
    engine_cpu_threads = max(1, requested_workers // box_workers)

    def _run_box(box: Path, receptor: Any, ligand: Any) -> Tuple[str, Dict[str, Any]]:
        box_id = box.stem
        box_outdir = base_outdir / box_id if use_multi_boxes else base_outdir
        box_result = _run_single_engine_for_box(
            engine=engine,
            receptor=receptor,
            ligand=ligand,
            box_path=box,
            outdir=box_outdir,
            job_name=job_name,
            receptor_prepare_dir=receptor_prepare_dir,
            ligand_prepare_dir=ligand_prepare_dir,
            engine_cpu_threads=engine_cpu_threads,
        )
        return box_id, box_result

    if box_workers <= 1:
        for box in boxes:
            box_id, box_result = _run_box(box, receptor_obj, ligand_obj)
            summary["boxes"][box_id] = box_result
    else:
        results_by_box: Dict[str, Dict[str, Any]] = {}

        def _run_box_isolated(box: Path) -> Tuple[str, Dict[str, Any]]:
            isolated_receptor = ocr.Receptor(str(receptor_path), name=f"{job_name}_receptor")
            isolated_ligand = ocl.Ligand(str(ligand_path), name=ligand_name)
            return _run_box(box, isolated_receptor, isolated_ligand)

        with ThreadPoolExecutor(max_workers=box_workers) as executor:
            future_to_box = {executor.submit(_run_box_isolated, box): box for box in boxes}
            for future in as_completed(future_to_box):
                box = future_to_box[future]
                box_id = box.stem
                try:
                    result_box_id, box_result = future.result()
                    results_by_box[result_box_id] = box_result
                except Exception as exc:
                    results_by_box[box_id] = {
                        "success": False,
                        "engine": engine,
                        "box": box_id,
                        "dir": str(base_outdir / box_id if use_multi_boxes else base_outdir / f"{engine}Files"),
                        "conf": "",
                        "prep_rec": "",
                        "prep_lig": "",
                        "poses": [],
                        "error": f"{type(exc).__name__}: {exc}",
                    }

        for box in boxes:
            box_id = box.stem
            summary["boxes"][box_id] = results_by_box.get(
                box_id,
                {
                    "success": False,
                    "engine": engine,
                    "box": box_id,
                    "dir": str(base_outdir / box_id if use_multi_boxes else base_outdir / f"{engine}Files"),
                    "conf": "",
                    "prep_rec": "",
                    "prep_lig": "",
                    "poses": [],
                    "error": "internal error: missing parallel result",
                },
            )

    return summary


def _postprocess_pipeline_box(
    receptor: Any,
    ligand: Any,
    box_path: Path,
    outdir: Path,
    job_name: str,
    box_label: Optional[str],
    engine_box_results: Dict[str, Dict[str, Any]],
) -> int:
    '''Aggregate engine outputs for one box and perform clustering/rescoring/DB.

    Parameters
    ----------
    receptor : Any
        ``OCDocker.Receptor.Receptor`` instance.
    ligand : Any
        ``OCDocker.Ligand.Ligand`` instance.
    box_path : Path
        Path to the processed box file.
    outdir : Path
        Output directory for clustering/rescoring artifacts.
    job_name : str
        Stable pipeline job identifier.
    box_label : str, optional
        Box identifier used when ``pipeline_all_boxes`` is enabled.
    engine_box_results : Dict[str, Dict[str, Any]]
        Per-engine execution payloads loaded from ``engine_status/*.json``.

    Returns
    -------
    int
        ``0`` on success, non-zero when no valid poses were available.
    '''

    import numpy as np
    import pandas as pd

    import OCDocker.Docking.Gnina as ocgnina
    import OCDocker.Docking.PLANTS as ocplants
    import OCDocker.Docking.Smina as ocsmina
    import OCDocker.Docking.Vina as ocvina
    import OCDocker.Processing.Preprocessing.RmsdClustering as ocrmsd
    import OCDocker.Toolbox.Conversion as occonversion
    import OCDocker.Toolbox.MoleculeProcessing as ocmolproc

    outdir.mkdir(parents=True, exist_ok=True)
    all_poses: List[str] = []
    pose_engine_map: Dict[str, str] = {}
    ctx: Dict[str, Dict[str, str]] = {}
    engine_errors: Dict[str, str] = {}

    for engine in pipeline_engines:
        box_result = engine_box_results.get(engine, {})
        if not isinstance(box_result, dict):
            continue
        if not box_result.get("success", False):
            if box_result.get("error"):
                engine_errors[engine] = str(box_result["error"])
            continue

        poses = [str(p) for p in box_result.get("poses", []) if _is_valid_file(p)]
        if not poses:
            engine_errors[engine] = "no valid poses"
            continue

        all_poses.extend(poses)
        for pose in poses:
            pose_engine_map[pose] = engine
        ctx[engine] = {
            "conf": str(box_result.get("conf", "")),
            "dir": str(box_result.get("dir", "")),
            "prep_rec": str(box_result.get("prep_rec", "")),
        }

    if engine_errors:
        for engine, message in sorted(engine_errors.items()):
            print(f"Warning: {engine} failed for {job_name}: {message}")

    if not all_poses:
        return 2

    mol2_dir = outdir / "poses_mol2"
    mol2_list, mol2_map = _ensure_mol2_poses(all_poses, mol2_dir, pose_engine_map)
    if not mol2_list:
        return 2

    rmsd = ocmolproc.get_rmsd_matrix(mol2_list)
    rmsd_df = pd.DataFrame(rmsd).loc[mol2_list, mol2_list]
    rmsd_df.to_csv(outdir / "rmsd_matrix.csv")

    clusters = ocrmsd.cluster_rmsd(
        rmsd_df,
        min_distance_threshold=pipeline_cluster_min,
        max_distance_threshold=pipeline_cluster_max,
        threshold_step=pipeline_cluster_step,
        outputPlot=str(outdir / "clustering_dendrogram.png"),
        molecule_name=job_name,
        pose_engine_map=pose_engine_map,
    )

    clustering_info: Dict[str, Any] = {
        "method": "rmsd_based_clustering",
        "total_poses": len(mol2_list),
        "representative_selection": None,
        "cluster_sizes": None,
        "medoids": None,
    }

    if isinstance(clusters, int) or getattr(clusters, "size", 0) == 0:
        representative_mol2 = mol2_list[0]
        clustering_info["representative_selection"] = "first_pose_fallback"
        clustering_info["reason"] = "clustering_failed_or_no_labels"
    else:
        cluster_assignments = pd.DataFrame({"pose_path": mol2_list, "cluster_id": clusters})
        cluster_assignments.to_csv(outdir / "cluster_assignments.csv", index=False)

        cluster_sizes: Dict[int, int] = {}
        unique_clusters, counts = np.unique(clusters, return_counts=True)
        for cluster_id, size in zip(unique_clusters, counts):
            cluster_sizes[int(cluster_id)] = int(size)

        medoids = ocrmsd.get_medoids(rmsd_df, clusters, onlyBiggest=True)
        if medoids:
            representative_mol2 = medoids[0]
            clustering_info["representative_selection"] = "medoid_of_largest_cluster"
            clustering_info["medoids"] = [str(medoid) for medoid in medoids]
        else:
            representative_mol2 = mol2_list[0]
            clustering_info["representative_selection"] = "first_pose_fallback"
            clustering_info["reason"] = "no_medoid_found"

        clustering_info["cluster_sizes"] = cluster_sizes
        rep_idx = mol2_list.index(representative_mol2)
        rep_cluster = int(clusters[rep_idx])
        clustering_info["representative_cluster_id"] = rep_cluster
        clustering_info["representative_cluster_size"] = cluster_sizes.get(rep_cluster, 0)

    representative_original = mol2_map.get(representative_mol2, representative_mol2)
    representative_engine = pose_engine_map.get(str(representative_original), "")
    representative_pdbqt: Optional[Path] = None
    representative_mol2_final: Optional[Path] = None

    if representative_original.lower().endswith(".pdbqt"):
        representative_pdbqt = Path(representative_original)
        representative_mol2_final = outdir / "representative_for_plants.mol2"
        _ = occonversion.convert_mols(str(representative_pdbqt), str(representative_mol2_final), overwrite=True)
    elif representative_original.lower().endswith(".mol2"):
        representative_mol2_final = Path(representative_original)
        representative_pdbqt = outdir / "representative_for_vina_smina.pdbqt"
        _ = occonversion.convert_mols(str(representative_mol2_final), str(representative_pdbqt), overwrite=True)
    else:
        representative_mol2_final = Path(representative_mol2)
        representative_pdbqt = outdir / "representative_for_vina_smina.pdbqt"
        _ = occonversion.convert_mols(str(representative_mol2_final), str(representative_pdbqt), overwrite=True)

    representative_pose_path = outdir / "representative.mol2"
    source_rep = representative_mol2_final if representative_mol2_final and representative_mol2_final.exists() else Path(representative_mol2)
    shutil.copyfile(str(source_rep), str(representative_pose_path))
    (outdir / "clustering_info.json").write_text(json.dumps(clustering_info, indent=2) + "\n", encoding="utf-8")

    rescoring: Dict[str, Dict[str, float]] = {}
    runtime_config = get_config()

    if "vina" in pipeline_rescoring_engines_set and "vina" in ctx and representative_pdbqt and representative_pdbqt.exists():
        vina_scores: Dict[str, float] = {}
        vina_scoring_functions = runtime_config.vina.scoring_functions or ["vina"]
        for scoring_function in vina_scoring_functions:
            try:
                ocvina.run_rescore(
                    ctx["vina"]["conf"],
                    str(representative_pdbqt),
                    ctx["vina"]["dir"],
                    scoring_function,
                    splitLigand=False,
                    overwrite=True,
                )
            except Exception:
                continue
        try:
            log_paths = ocvina.get_rescore_log_paths(ctx["vina"]["dir"])
            raw_scores = ocvina.read_rescore_logs(log_paths, onlyBest=True) if log_paths else {}
            for raw_key, raw_value in raw_scores.items():
                numeric = _to_numeric(raw_value)
                if numeric is None:
                    continue
                canonical = _canonicalize_rescore_key("vina", str(raw_key))
                vina_scores[canonical] = float(numeric)
        except Exception:
            pass
        if vina_scores:
            rescoring["vina"] = vina_scores

    if "smina" in pipeline_rescoring_engines_set and "smina" in ctx and representative_pdbqt and representative_pdbqt.exists():
        smina_scores: Dict[str, float] = {}
        smina_scoring_functions = runtime_config.smina.scoring_functions or ["vina", "vinardo", "dkoes_scoring"]
        for scoring_function in smina_scoring_functions:
            try:
                ocsmina.run_rescore(
                    ctx["smina"]["conf"],
                    str(representative_pdbqt),
                    ctx["smina"]["dir"],
                    scoring_function,
                    splitLigand=False,
                    overwrite=True,
                )
            except Exception:
                continue
        try:
            log_paths = ocsmina.get_rescore_log_paths(ctx["smina"]["dir"])
            raw_scores = ocsmina.read_rescore_logs(log_paths, onlyBest=True) if log_paths else {}
            for raw_key, raw_value in raw_scores.items():
                numeric = _to_numeric(raw_value)
                if numeric is None:
                    continue
                canonical = _canonicalize_rescore_key("smina", str(raw_key))
                smina_scores[canonical] = float(numeric)
        except Exception:
            pass
        if smina_scores:
            rescoring["smina"] = smina_scores

    if "gnina" in pipeline_rescoring_engines_set and "gnina" in ctx and representative_pdbqt and representative_pdbqt.exists():
        gnina_scores: Dict[str, float] = {}
        gnina_default_scoring = str(getattr(runtime_config.gnina, "scoring", "default") or "default").strip() or "default"
        gnina_scoring_functions = runtime_config.gnina.scoring_functions or [gnina_default_scoring]
        gnina_cnn_models = runtime_config.gnina.cnn_models or [str(getattr(runtime_config.gnina, "cnn", "default") or "default")]
        for scoring_function in gnina_scoring_functions:
            try:
                ocgnina.run_rescore(
                    ctx["gnina"]["conf"],
                    str(representative_pdbqt),
                    ctx["gnina"]["dir"],
                    scoring_function,
                    splitLigand=False,
                    overwrite=True,
                    disable_cnn=True,
                )
            except Exception:
                continue
        for cnn_model in gnina_cnn_models:
            try:
                ocgnina.run_rescore(
                    ctx["gnina"]["conf"],
                    str(representative_pdbqt),
                    ctx["gnina"]["dir"],
                    gnina_default_scoring,
                    splitLigand=False,
                    overwrite=True,
                    cnn_model=cnn_model,
                    disable_cnn=False,
                )
            except Exception:
                continue
        try:
            log_paths = ocgnina.get_rescore_log_paths(ctx["gnina"]["dir"])
            raw_scores = ocgnina.read_rescore_logs(log_paths, onlyBest=True) if log_paths else {}
            for raw_key, raw_value in raw_scores.items():
                numeric = _to_numeric(raw_value)
                if numeric is None:
                    continue
                canonical = _canonicalize_rescore_key("gnina", str(raw_key))
                gnina_scores[canonical] = float(numeric)
        except Exception:
            pass
        if gnina_scores:
            rescoring["gnina"] = gnina_scores

    if (
        "plants" in pipeline_rescoring_engines_set
        and "plants" in ctx
        and representative_mol2_final
        and representative_mol2_final.exists()
    ):
        plants_scores: Dict[str, float] = {}
        binding_site = ocplants.get_binding_site(str(box_path))
        if not isinstance(binding_site, int):
            center, radius = binding_site
            pose_list = outdir / "pose_list_single.txt"
            pose_list.write_text(str(representative_mol2_final) + "\n", encoding="utf-8")
            plants_scoring_functions = runtime_config.plants.scoring_functions or ["chemplp", "plp", "plp95"]
            for scoring_function in plants_scoring_functions:
                try:
                    output_path = Path(ctx["plants"]["dir"]) / f"run_{scoring_function}"
                    conf_path = Path(ctx["plants"]["dir"]) / f"{job_name}_rescoring_{scoring_function}.txt"
                    ocplants.write_rescoring_config_file(
                        str(conf_path),
                        ctx["plants"]["prep_rec"],
                        str(pose_list),
                        str(output_path),
                        center[0],
                        center[1],
                        center[2],
                        radius,
                        scoringFunction=scoring_function,
                    )
                    ocplants.run_rescore(
                        str(conf_path),
                        str(pose_list),
                        str(output_path),
                        ctx["plants"]["prep_rec"],
                        scoring_function,
                        center[0],
                        center[1],
                        center[2],
                        radius,
                        overwrite=True,
                    )
                    ranking_file = output_path / "bestranking.csv"
                    if ranking_file.is_file():
                        log_data = ocplants.read_log(str(ranking_file), onlyBest=True)
                        for _, score_map in log_data.items():
                            for _, score_value in score_map.items():
                                numeric = _to_numeric(score_value if not isinstance(score_value, list) else score_value[0])
                                if numeric is not None:
                                    plants_scores[f"plants_{scoring_function}"] = float(numeric)
                                    break
                            if f"plants_{scoring_function}" in plants_scores:
                                break
                except Exception:
                    continue
        if plants_scores:
            rescoring["plants"] = plants_scores

    if "oddt" in pipeline_rescoring_engines_set:
        try:
            from OCDocker.Rescoring.ODDT import df_to_dict, run_oddt

            prepared_receptor: Optional[Path] = None
            for engine_name in ("vina", "smina", "gnina", "plants"):
                prep_path = Path(ctx.get(engine_name, {}).get("prep_rec", ""))
                if _is_valid_file(prep_path):
                    prepared_receptor = prep_path
                    break

            oddt_ligand = representative_mol2_final if representative_mol2_final and representative_mol2_final.exists() else representative_pose_path
            if prepared_receptor is not None and oddt_ligand.exists():
                oddt_output = outdir / "oddt_rescoring"
                oddt_output.mkdir(parents=True, exist_ok=True)
                oddt_result = run_oddt(
                    str(prepared_receptor),
                    str(oddt_ligand),
                    job_name,
                    str(oddt_output),
                    overwrite=True,
                    returnData=True,
                )
                if oddt_result is not None and not isinstance(oddt_result, int):
                    oddt_dict = df_to_dict(oddt_result)
                    if isinstance(oddt_dict, dict) and oddt_dict:
                        oddt_scores: Dict[str, float] = {}
                        first_key = list(oddt_dict.keys())[0]
                        for score_name, score_value in oddt_dict[first_key].items():
                            if str(score_name).strip().lower() in {"ligand_name", "name"}:
                                continue
                            numeric = _to_numeric(score_value if not isinstance(score_value, (list, tuple)) else score_value[0])
                            if numeric is not None:
                                oddt_scores[f"oddt_{score_name}"] = float(numeric)
                        if oddt_scores:
                            rescoring["oddt"] = oddt_scores
        except Exception:
            pass

    summary = {
        "job": job_name if box_label is None else f"{job_name}_{box_label}",
        "pipeline_version": pipeline_version,
        "engines": pipeline_engines,
        "rescoring_engines": sorted(rescoring.keys()),
        "representative_pose": str(representative_pose_path),
        "representative_engine": representative_engine,
        "clustering": clustering_info,
        "rescoring": rescoring,
    }
    (outdir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    if pipeline_store_db:
        try:
            stored, stored_name, ignored_keys = _store_pipeline_results_in_db(
                job_name=job_name,
                receptor=receptor,
                ligand=ligand,
                rescoring=rescoring,
                box_label=box_label,
                representative_pose=str(representative_pose_path),
                representative_engine=representative_engine,
                summary=summary,
            )
            if stored and ignored_keys:
                print(
                    "Warning: some score keys were not mapped to Complexes columns and were skipped: "
                    + ", ".join(ignored_keys)
                )
            if not stored:
                print(f"Warning: DB upsert failed for job {job_name}.")
        except Exception as exc:
            print(f"Warning: failed to store pipeline result in DB for {job_name}: {exc}")

    return 0


def _run_pipeline_postprocess_from_summaries(
    receptor_path: str,
    ligand_path: str,
    box_path: str,
    outdir_path: str,
    job_name: str,
    engine_summary_paths: List[str],
    max_workers: int = 1,
) -> int:
    '''Run the post-processing stage from per-engine summaries.

    This is the aggregation path used by Snakemake ``run_pipeline`` after all
    ``run_engine`` jobs are completed for a given target.

    Parameters
    ----------
    receptor_path : str
        Path to receptor input file.
    ligand_path : str
        Path to ligand input file.
    box_path : str
        Path to default box file (``box0.pdb``).
    outdir_path : str
        Target output directory for the processed entry.
    job_name : str
        Stable job identifier used in output files and DB records.
    engine_summary_paths : List[str]
        Paths to ``engine_status/{engine}.json`` files to aggregate.

    Returns
    -------
    int
        ``0`` on success, non-zero if post-processing fails for any box.
    '''

    import OCDocker.Ligand as ocl
    import OCDocker.Receptor as ocr

    receptor_obj = ocr.Receptor(str(receptor_path), name=f"{job_name}_receptor")
    ligand_name = job_name[:-7] if job_name.endswith("_ligand") else job_name
    ligand_obj = ocl.Ligand(str(ligand_path), name=ligand_name)

    base_outdir = Path(outdir_path).resolve()
    ligand_dir = Path(ligand_path).parent
    box_path_obj = Path(box_path)
    boxes = _list_boxes(ligand_dir, box_path_obj, pipeline_all_boxes)
    if pipeline_all_boxes and not boxes:
        print(f"Warning: no box*.pdb files found for {job_name}.")
        return 2

    loaded_summaries: Dict[str, Dict[str, Any]] = {}
    for summary_path in engine_summary_paths:
        path = Path(summary_path)
        if not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        engine = str(data.get("engine", path.stem)).strip().lower()
        loaded_summaries[engine] = data

    use_multi_boxes = pipeline_all_boxes and len(boxes) > 1
    requested_workers = max(1, int(max_workers))
    box_workers = min(len(boxes), requested_workers)

    def _process_box(box: Path, receptor: Any, ligand: Any) -> Tuple[str, int]:
        box_id = box.stem
        box_outdir = base_outdir / box_id if use_multi_boxes else base_outdir
        box_engine_results: Dict[str, Dict[str, Any]] = {}
        for engine in pipeline_engines:
            engine_data = loaded_summaries.get(engine, {})
            box_map = engine_data.get("boxes", {}) if isinstance(engine_data, dict) else {}
            if isinstance(box_map, dict) and box_id in box_map:
                box_engine_results[engine] = box_map[box_id]

        rc = _postprocess_pipeline_box(
            receptor=receptor,
            ligand=ligand,
            box_path=box,
            outdir=box_outdir,
            job_name=job_name,
            box_label=box_id if use_multi_boxes else None,
            engine_box_results=box_engine_results,
        )
        return box_id, rc

    overall_rc = 0
    if box_workers <= 1:
        for box in boxes:
            _, rc = _process_box(box, receptor_obj, ligand_obj)
            if rc != 0:
                overall_rc = rc
    else:
        results_by_box: Dict[str, int] = {}

        def _process_box_isolated(box: Path) -> Tuple[str, int]:
            isolated_receptor = ocr.Receptor(str(receptor_path), name=f"{job_name}_receptor")
            isolated_ligand = ocl.Ligand(str(ligand_path), name=ligand_name)
            return _process_box(box, isolated_receptor, isolated_ligand)

        with ThreadPoolExecutor(max_workers=box_workers) as executor:
            future_to_box = {executor.submit(_process_box_isolated, box): box for box in boxes}
            for future in as_completed(future_to_box):
                box = future_to_box[future]
                box_id = box.stem
                try:
                    result_box_id, rc = future.result()
                    results_by_box[result_box_id] = rc
                except Exception as exc:
                    print(f"Warning: post-processing failed for {job_name}/{box_id}: {type(exc).__name__}: {exc}")
                    results_by_box[box_id] = 2

        for box in boxes:
            rc = int(results_by_box.get(box.stem, 2))
            if rc != 0:
                overall_rc = rc

    return overall_rc


if enable_legacy_database_rules:
    include: "system/fileSystem.smk"
    include: "system/database/pdbbind.smk"
    include: "system/database/dudez.smk"


# Wildcards
###############################################################################

# Keep engine wildcard constrained to user-selected/auto-detected engines.
wildcard_constraints:
    database=pipeline_databases_pattern,
    engine=pipeline_engines_pattern,


# License
###############################################################################
'''
OCDocker pipeline
Authors: Rossi, A.D.; Pascutti, P.G.; Torres, P.H.M;
[Federal University of Rio de Janeiro, UFRJ, Brazil]
Contact info:
Carlos Chagas Filho Institute of Biophysics (IBCCF),
Modeling and Molecular Dynamics Laboratory,
Av. Carlos Chagas Filho 373 - CCS - bloco G1-19, Cidade Universitária - Rio de Janeiro, RJ - Brazil
E-mail address: arturossi10@gmail.com
This project is licensed under the GNU General Public License v3.0
'''

# Rules
###############################################################################

rule db_pdbbind:
    """
    Set up the PDBbind database.
    """
    input:
        lambda wildcards: _preset_receptor_inputs("PDBbind"),

rule db_dudez:
    """
    Set up the DUDEz database.
    """
    input:
        lambda wildcards: _preset_receptor_inputs("DUDEz"),


rule prepare_receptor_cache:
    """
    Prepare receptor artifacts once per receptor.

    Generated files are tracked by a cache manifest whose hash depends on
    active engines/rescoring settings, so cache invalidates automatically when
    preparation requirements change.
    """
    input:
        receptor=_wc_receptor_path,
    output:
        cache=os.path.join(
            database_rule_root_str,
            "{database}",
            "{receptor}",
            f".prepared_receptor_cache.{pipeline_cache_key}.json",
        ),
    threads: 1
    run:
        _ensure_receptor_cache_ready(str(input.receptor), str(output.cache))


rule prepare_ligand_cache:
    """
    Prepare ligand artifacts once per target entry.

    Produces shared ligand preparation files (PDBQT and/or MOL2, depending on
    active engines/rescoring) and writes a cache manifest for DAG tracking.
    """
    input:
        receptor=_wc_receptor_path,
        receptor_cache=_wc_receptor_cache_manifest_path,
        ligand=_wc_ligand_path,
        box=_wc_box_path,
    output:
        cache=os.path.join(
            database_rule_root_str,
            "{database}",
            "{receptor}",
            "compounds",
            "{kind}",
            "{target}",
            f".prepared_ligand_cache.{pipeline_cache_key}.json",
        ),
    threads: 1
    run:
        target_dir = Path(os.path.dirname(input.ligand))
        target_dir.mkdir(parents=True, exist_ok=True)

        if not _cached_receptor_files_present(str(input.receptor)):
            _ensure_receptor_cache_ready(str(input.receptor), str(input.receptor_cache))

        job_name = f"{wildcards.database}_{wildcards.receptor}_{wildcards.kind}_{wildcards.target}"
        _ensure_ligand_cache_ready(
            receptor_path=str(input.receptor),
            ligand_path=str(input.ligand),
            box_path=str(input.box),
            target_dir=str(target_dir),
            cache_manifest_path=str(output.cache),
            job_name=job_name,
        )


rule run_engine:
    """
    Run one docking engine (full API path) for one target entry.

    This rule is engine-scoped by wildcard and can run in parallel with other
    engines for the same molecule. Ligand preparation is consumed from
    ``prepare_ligand_cache`` outputs. Engine status is persisted in
    ``engine_status/{engine}.json``.
    """
    input:
        receptor=_wc_receptor_path,
        receptor_cache=_wc_receptor_cache_manifest_path,
        ligand=_wc_ligand_path,
        box=_wc_box_path,
        ligand_cache=_wc_ligand_cache_manifest_path,
    output:
        summary=os.path.join(
            database_rule_root_str,
            "{database}",
            "{receptor}",
            "compounds",
            "{kind}",
            "{target}",
            "engine_status",
            "{engine}.json",
        ),
    threads:
        lambda wildcards: _engine_threads(wildcards.engine)
    resources:
        mem_mb=lambda wildcards: _engine_mem_mb(wildcards.engine)
    run:
        # Keep BLAS/OMP consumers aligned with Snakemake scheduling for each engine job.
        for env_name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            os.environ[env_name] = str(threads)

        target_dir = Path(os.path.dirname(input.ligand))
        target_dir.mkdir(parents=True, exist_ok=True)
        if not _cached_receptor_files_present(str(input.receptor)):
            _ensure_receptor_cache_ready(str(input.receptor), str(input.receptor_cache))

        job_name = f"{wildcards.database}_{wildcards.receptor}_{wildcards.kind}_{wildcards.target}"
        summary = _run_single_engine_via_api(
            engine=wildcards.engine,
            receptor_path=str(input.receptor),
            ligand_path=str(input.ligand),
            box_path=str(input.box),
            outdir_path=str(target_dir),
            job_name=job_name,
            max_workers=int(threads),
        )

        out_path = Path(str(output.summary))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")


rule run_pipeline:
    """
    Aggregate per-engine outputs, run clustering/rescoring, and write payload/report.

    The docking stage is intentionally delegated to ``run_engine`` jobs so this
    rule only performs post-processing and DB persistence.
    """
    input:
        receptor=_wc_receptor_path,
        receptor_cache=_wc_receptor_cache_manifest_path,
        ligand=_wc_ligand_path,
        box=_wc_box_path,
        engine_summaries=_engine_summary_inputs,
    output:
        payload=os.path.join(
            database_rule_root_str,
            "{database}",
            "{receptor}",
            "compounds",
            "{kind}",
            "{target}",
            "payload.pkl",
        ),
        run_report=os.path.join(
            database_rule_root_str,
            "{database}",
            "{receptor}",
            "compounds",
            "{kind}",
            "{target}",
            "run_report.json",
        ),
    threads: pipeline_postprocess_threads
    resources:
        mem_mb=pipeline_postprocess_mem_mb
    run:
        for env_name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            os.environ[env_name] = str(threads)

        target_dir = Path(os.path.dirname(input.ligand))
        target_dir.mkdir(parents=True, exist_ok=True)

        job_name = f"{wildcards.database}_{wildcards.receptor}_{wildcards.kind}_{wildcards.target}"
        if not _cached_receptor_files_present(str(input.receptor)):
            _ensure_receptor_cache_ready(str(input.receptor), str(input.receptor_cache))
        rc = _run_pipeline_postprocess_from_summaries(
            receptor_path=str(input.receptor),
            ligand_path=str(input.ligand),
            box_path=str(input.box),
            outdir_path=str(target_dir),
            job_name=job_name,
            engine_summary_paths=list(input.engine_summaries),
            max_workers=int(threads),
        )
        if rc != 0:
            raise RuntimeError(
                f"OCDocker pipeline failed for {wildcards.database}/{wildcards.receptor}/{wildcards.kind}/{wildcards.target} "
                f"with return code {rc}."
            )

        summary_path = target_dir / "summary.json"
        summary_output_path: Optional[Path] = None
        per_box_summary_paths: List[Path] = []
        if summary_path.exists():
            with summary_path.open("r", encoding="utf-8") as handle:
                summary = json.load(handle)
            summary_output_path = summary_path
        elif pipeline_all_boxes:
            per_box_summary = {}
            for box_summary_path in sorted(target_dir.glob("box*/summary.json")):
                per_box_summary_paths.append(box_summary_path)
                with box_summary_path.open("r", encoding="utf-8") as handle:
                    per_box_summary[box_summary_path.parent.name] = json.load(handle)

            if not per_box_summary:
                raise RuntimeError(
                    "Pipeline output missing summary.json and no per-box summaries were found under "
                    f"{target_dir}."
                )

            summary = {
                "job": job_name,
                "pipeline_version": pipeline_version,
                "all_boxes": True,
                "box_summaries": per_box_summary,
            }
        else:
            raise RuntimeError(f"Pipeline output missing summary.json at: {summary_path}")

        representative_pose = summary.get("representative_pose")
        representative_engine = summary.get("representative_engine")
        if representative_pose is None and isinstance(summary.get("box_summaries"), dict):
            representative_pose = {
                box_name: box_data.get("representative_pose")
                for box_name, box_data in summary["box_summaries"].items()
                if isinstance(box_data, dict)
            }
        if representative_engine is None and isinstance(summary.get("box_summaries"), dict):
            representative_engine = {
                box_name: box_data.get("representative_engine")
                for box_name, box_data in summary["box_summaries"].items()
                if isinstance(box_data, dict)
            }

        payload = {
            "name": str(summary.get("job", job_name)),
            "pipeline_version": summary.get("pipeline_version", pipeline_version),
            "database": wildcards.database,
            "receptor": wildcards.receptor,
            "kind": wildcards.kind,
            "target": wildcards.target,
            "representative_pose": representative_pose,
            "representative_engine": representative_engine,
            "run_report": str(output.run_report),
            "summary": summary,
        }

        with open(output.payload, "wb") as handle:
            pickle.dump(payload, handle)

        run_report = _generate_run_report(
            job_name=job_name,
            database=wildcards.database,
            receptor=wildcards.receptor,
            kind=wildcards.kind,
            target=wildcards.target,
            receptor_path=str(input.receptor),
            ligand_path=str(input.ligand),
            box_path=str(input.box),
            engine_summary_paths=list(input.engine_summaries),
            summary=summary,
            summary_path=summary_output_path,
            per_box_summary_paths=[str(path) for path in per_box_summary_paths],
            payload_path=str(output.payload),
            report_path=str(output.run_report),
        )
        report_path = Path(str(output.run_report))
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(run_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


rule all:
    """
    Execute OCDocker pipeline over selected databases and kinds.

    Example usage:
        snakemake -s snakefile --cores 20 --use-conda --conda-frontend mamba --keep-going
    """
    default_target: True
    input:
        allkinds=collect_payload_targets(),
    run:
        print(f"All done! Processed {len(input.allkinds)} entries.")
