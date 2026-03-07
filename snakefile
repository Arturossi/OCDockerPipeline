"""
OCDockerPipeline Snakemake workflow.

This Snakefile orchestrates target discovery, preparation caching, per-engine
docking, post-processing (clustering and rescoring), optional ODDT rescoring,
payload/report generation, and per-database CSV export.

Author: Artur Duque Rossi
Created: 2023-11-06
Last modified: 2026-02-28
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
import multiprocessing as mp
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
_TARGET_DISCOVERY_CACHE_SCHEMA_VERSION = 3
_REFERENCE_LIGAND_FILENAMES = ("reference_ligand.pdb", "reference_ligand.sdf")


def _as_bool(value: Any, default: bool = False) -> bool:
    '''Convert a configuration token into a boolean value.

    Parameters
    ----------
    value : Any
        Raw configuration value to parse.
    default : bool, optional
        Fallback value used when ``value`` cannot be interpreted.

    Returns
    -------
    bool
        Parsed boolean value.
    '''

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
    '''Normalize a config field into a list of non-empty strings.

    Parameters
    ----------
    value : Any
        Runtime value provided by Snakemake config.
    default : Any
        Value used when ``value`` is ``None``.

    Returns
    -------
    List[str]
        Parsed list of stripped tokens.
    '''

    if value is None:
        value = default

    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]

    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]

    return [str(value).strip()] if str(value).strip() else []


def _load_ignored_targets(index_path: str) -> Set[str]:
    '''Load ignored target identifiers from a line-based file.

    Parameters
    ----------
    index_path : str
        Path to a text file where each non-comment line contains one target
        identifier.

    Returns
    -------
    Set[str]
        Set of ignored target identifiers. Empty when the file is missing or
        unreadable.
    '''

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
    '''Normalize database aliases to canonical names used in the pipeline.

    Parameters
    ----------
    name : Any
        User-provided database token.

    Returns
    -------
    str
        Canonical database name.
    '''

    lower = str(name).strip().lower()
    if lower == "pdbbind":
        return "PDBbind"
    if lower in {"dudez", "dude-z", "dude_z"}:
        return "DUDEz"
    return str(name).strip()


def _is_valid_file(path: Union[str, Path]) -> bool:
    '''Check whether a path exists and points to a non-empty file.

    Parameters
    ----------
    path : Union[str, Path]
        Candidate file path.

    Returns
    -------
    bool
        ``True`` when the file exists and has non-zero size.
    '''

    p = Path(path)
    return p.is_file() and p.stat().st_size > 0


def _binary_available(executable):
    '''Check whether an executable can be resolved and executed.

    Parameters
    ----------
    executable : Any
        Absolute path or command name.

    Returns
    -------
    bool
        ``True`` when the executable exists and is runnable.
    '''

    if not executable:
        return False

    executable = str(executable).strip()
    if not executable:
        return False

    if os.path.isabs(executable):
        return os.path.isfile(executable) and os.access(executable, os.X_OK)

    return shutil.which(executable) is not None


def _normalize_exit_code(result):
    '''Normalize command results into an integer exit code.

    Parameters
    ----------
    result : Any
        Return payload from subprocess/API helper.

    Returns
    -------
    int
        Exit code compatible with shell semantics.
    '''

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
    '''Return the current UTC timestamp in ISO-8601 format.

    Parameters
    ----------
    None

    Returns
    -------
    str
        UTC timestamp string with ``Z`` suffix.
    '''

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc_iso_from_timestamp(value: float) -> str:
    '''Convert a UNIX timestamp into a UTC ISO-8601 string.

    Parameters
    ----------
    value : float
        UNIX timestamp.

    Returns
    -------
    str
        UTC timestamp string with ``Z`` suffix.
    '''

    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256_text(value: str) -> str:
    '''Compute SHA-256 digest for UTF-8 text.

    Parameters
    ----------
    value : str
        Input text.

    Returns
    -------
    str
        Hexadecimal SHA-256 digest.
    '''

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256_file(path: Union[str, Path]) -> Optional[str]:
    '''Compute SHA-256 digest for a file.

    Parameters
    ----------
    path : Union[str, Path]
        File path to hash.

    Returns
    -------
    Optional[str]
        Hexadecimal SHA-256 digest, or ``None`` when file is missing.
    '''

    file_path = Path(path)
    if not file_path.is_file():
        return None

    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _to_jsonable(value: Any) -> Any:
    '''Recursively convert values into JSON-serializable primitives.

    Parameters
    ----------
    value : Any
        Input object to normalize.

    Returns
    -------
    Any
        JSON-serializable representation.
    '''

    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, dict):
        return {str(key): _to_jsonable(inner) for key, inner in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(inner) for inner in value]

    return str(value)


def _json_sha256(payload: Any) -> str:
    '''Compute a stable SHA-256 digest for a JSON-normalized payload.

    Parameters
    ----------
    payload : Any
        Arbitrary object to hash after JSON normalization.

    Returns
    -------
    str
        Hexadecimal SHA-256 digest.
    '''

    normalized = _to_jsonable(payload)
    text = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return _sha256_text(text)


def _file_fingerprint(path: Union[str, Path], include_sha256: bool = True) -> Dict[str, Any]:
    '''Collect reproducibility metadata for one filesystem path.

    Parameters
    ----------
    path : Union[str, Path]
        Target file path.
    include_sha256 : bool, optional
        Whether to include content digest for regular files.

    Returns
    -------
    Dict[str, Any]
        Metadata payload with path, existence, timestamps, size, and optional
        digest fields.
    '''

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
    '''Run a git command in a repository and return stdout.

    Parameters
    ----------
    repo_root : Union[str, Path]
        Repository root path.
    args : List[str]
        Git command arguments after ``git -C <repo_root>``.

    Returns
    -------
    Optional[str]
        Stripped stdout content when command succeeds, else ``None``.
    '''

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
    '''Collect lightweight VCS metadata for reproducibility reports.

    Parameters
    ----------
    repo_root : Union[str, Path]
        Repository root path.

    Returns
    -------
    Dict[str, Optional[Union[str, bool]]]
        Commit hash, branch name, and dirty-status fields.
    '''

    commit = _run_git(repo_root, ["rev-parse", "HEAD"])
    branch = _run_git(repo_root, ["rev-parse", "--abbrev-ref", "HEAD"])
    status = _run_git(repo_root, ["status", "--porcelain"])
    return {
        "commit": commit,
        "branch": branch,
        "dirty": bool(status) if status is not None else None,
    }


def _runtime_cache_root() -> Path:
    '''Return a writable runtime cache directory for workflow metadata.

    Parameters
    ----------
    None

    Returns
    -------
    Path
        Cache directory path.
    '''

    primary = Path(os.getcwd()) / ".snakemake"
    try:
        primary.mkdir(parents=True, exist_ok=True)
        return primary
    except OSError:
        fallback = Path("/tmp") / "ocdockerpipeline_snakemake_cache"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


def _set_command_option(cmd: Any, flag: str, value: Union[str, int]) -> None:
    '''Set or append a CLI option pair on a mutable command list.

    Parameters
    ----------
    cmd : Any
        Command container expected to be a ``list``.
    flag : str
        CLI flag name (for example ``--cpu``).
    value : Union[str, int]
        Value associated with ``flag``.

    Returns
    -------
    None
        This function mutates ``cmd`` in place.
    '''

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
    '''Align engine ``--cpu`` CLI options with Snakemake thread limits.

    Parameters
    ----------
    engine : str
        Engine identifier (for example ``vina``, ``smina``, ``gnina``).
    runner : Any
        Runner object that holds per-engine command lists.
    threads_hint : int
        Thread count requested by Snakemake for this job.

    Returns
    -------
    None
        The function mutates runner command lists in place.
    '''

    cpu_threads = max(1, int(threads_hint))
    if engine == "vina":
        _set_command_option(getattr(runner, "vina_cmd", None), "--cpu", cpu_threads)
    elif engine == "smina":
        _set_command_option(getattr(runner, "smina_cmd", None), "--cpu", cpu_threads)
    elif engine == "gnina":
        _set_command_option(getattr(runner, "gnina_cmd", None), "--cpu", cpu_threads)


def _apply_thread_limit_env(threads_hint: int) -> int:
    '''Export thread-related environment variables for subprocess consistency.

    Parameters
    ----------
    threads_hint : int
        Thread count requested by Snakemake for this job.

    Returns
    -------
    int
        Normalized thread count applied to environment variables.
    '''

    threads_count = max(1, int(threads_hint))
    for env_name in (
        "OCDOCKER_GNINA_CPU",
        "SNK_THREADS",
        "SNAKEMAKE_THREADS",
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        os.environ[env_name] = str(threads_count)
    return threads_count


@lru_cache(maxsize=2)
def _cached_reproducibility_manifest(include_python_packages: bool) -> Tuple[Dict[str, Any], Optional[str]]:
    '''Return cached reproducibility metadata for reporting.

    Parameters
    ----------
    include_python_packages : bool
        Whether to include installed Python package inventory.

    Returns
    -------
    Tuple[Dict[str, Any], Optional[str]]
        Manifest payload and optional error message.
    '''

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
    '''Build the structured run-report payload for one target execution.

    Parameters
    ----------
    job_name : str
        Canonical pipeline job identifier.
    database : str
        Database alias for the target.
    receptor : str
        Receptor identifier.
    kind : str
        Compound subset name.
    target : str
        Target identifier.
    receptor_path : Union[str, Path]
        Receptor file path.
    ligand_path : Union[str, Path]
        Ligand file path.
    box_path : Union[str, Path]
        Docking box path.
    engine_summary_paths : List[str]
        Per-engine status JSON paths.
    summary : Dict[str, Any]
        Final target summary payload.
    summary_path : Optional[Union[str, Path]]
        On-disk summary path, when present.
    per_box_summary_paths : List[Union[str, Path]]
        Per-box summary paths.
    payload_path : Union[str, Path]
        Final payload pickle path.
    report_path : Union[str, Path]
        Output run report path.

    Returns
    -------
    Dict[str, Any]
        Structured run-report payload.
    '''

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
                "store_db_mid_execution": pipeline_store_db_mid_execution,
                "export_database_csv": pipeline_export_database_csv,
                "engine_gpu": {
                    "default": pipeline_engine_gpu_default,
                    "map": dict(pipeline_engine_gpu_map),
                    "total_gpus": pipeline_total_gpus,
                },
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
    '''Provide an inter-process file lock for shared artifact writes.

    Parameters
    ----------
    lock_path : Union[str, Path]
        Lockfile path.

    Returns
    -------
    contextmanager
        Context manager that acquires and releases the lock.
    '''

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
    '''Build a stable sorting key for docking box files.

    Parameters
    ----------
    path : Path
        Candidate box file path.

    Returns
    -------
    Tuple[int, object]
        Sorting tuple preferring numeric ``boxN`` ordering.
    '''

    stem = path.stem.lower()
    if stem.startswith("box"):
        suffix = stem[3:]
        if suffix.isdigit():
            return (0, int(suffix))
    return (1, stem)


def _list_boxes(ligand_dir: Path, box_path: Path, all_boxes: bool) -> List[Path]:
    '''Resolve one or many docking box files for a target.

    Parameters
    ----------
    ligand_dir : Path
        Ligand directory to scan for ``box*.pdb`` files.
    box_path : Path
        Default box path.
    all_boxes : bool
        Whether to use all discovered boxes instead of only default box.

    Returns
    -------
    List[Path]
        Sorted unique list of box paths.
    '''

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
    '''Ensure pose files exist in MOL2 format for downstream scoring.

    Parameters
    ----------
    pose_paths : List[str]
        Input pose file paths.
    dest_dir : Path
        Output directory for converted MOL2 files.
    pose_engine_map : Optional[Dict[str, str]], optional
        Mapping from pose path to engine name for output naming.

    Returns
    -------
    Tuple[List[str], Dict[str, str]]
        Converted MOL2 paths and mapping from MOL2 path to original source path.
    '''

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
    '''Check whether a descriptor should be stored as an integer.

    Parameters
    ----------
    descriptor : str
        Descriptor name.

    Returns
    -------
    bool
        ``True`` when descriptor is integer-like.
    '''

    name = descriptor.strip()
    return (
        name.startswith("fr_")
        or name.startswith("Num")
        or name.startswith("count")
        or name in {"HeavyAtomCount", "NHOHCount", "NOCount", "RingCount", "TotalAALength"}
    )


def _to_numeric(value: Any) -> Optional[float]:
    '''Safely coerce numeric values to finite ``float``.

    Parameters
    ----------
    value : Any
        Candidate value.

    Returns
    -------
    Optional[float]
        Finite float value, otherwise ``None``.
    '''

    if isinstance(value, bool):
        return float(int(value))
    if not isinstance(value, numbers.Real):
        return None

    numeric_value = float(value)
    if math.isnan(numeric_value) or math.isinf(numeric_value):
        return None
    return numeric_value


def _descriptor_attribute_candidates(descriptor: str) -> List[str]:
    '''Generate candidate attribute names for one descriptor key.

    Parameters
    ----------
    descriptor : str
        Descriptor key.

    Returns
    -------
    List[str]
        Candidate attribute names in lookup order.
    '''

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
    '''Extract numeric descriptor values from an object.

    Parameters
    ----------
    obj : Any
        Object exposing descriptor attributes.
    descriptor_names : List[str]
        Descriptor names to collect.

    Returns
    -------
    Dict[str, Union[int, float]]
        Descriptor dictionary with numeric values.
    '''

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


def _map_score_to_complex_column(raw_key: str) -> Optional[str]:
    '''Map raw rescoring keys to ``Complexes`` model columns.

    Parameters
    ----------
    raw_key : str
        Raw score key from rescoring payload.

    Returns
    -------
    Optional[str]
        Destination column name, or ``None`` when unmapped.
    '''

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
        # Keep Gnina mapping hardcoded like other engines, matching gnina_scoring_functions defaults.
        "gnina_ad4_scoring": "GNINA_AD4_SCORING",
        "gnina_scoring_ad4": "GNINA_AD4_SCORING",
        "gnina_default": "GNINA_DEFAULT",
        "gnina_dkoes_fast": "GNINA_DKOES_FAST",
        "gnina_fast_dkoes": "GNINA_DKOES_FAST",
        "gnina_dkoes_scoring": "GNINA_DKOES_SCORING",
        "gnina_scoring_dkoes": "GNINA_DKOES_SCORING",
        "gnina_dkoes_scoring_old": "GNINA_DKOES_SCORING_OLD",
        "gnina_old_scoring_dkoes": "GNINA_DKOES_SCORING_OLD",
        "gnina_vina": "GNINA_VINA",
        "gnina_vinardo": "GNINA_VINARDO",
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
    '''Flatten nested rescoring map into DB-ready score payload.

    Parameters
    ----------
    rescoring : Dict[str, Dict[str, float]]
        Engine-scoped rescoring values.

    Returns
    -------
    Tuple[Dict[str, float], List[str]]
        Mapped score payload and list of ignored score keys.
    '''

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
    '''Initialize DB runtime state lazily before any write operation.

    Parameters
    ----------
    None

    Returns
    -------
    None
        Initializes global DB engine/session and schema markers as needed.
    '''

    import OCDocker.Initialise as ocinit_runtime
    import OCDocker.DB.DB as ocdb_runtime
    import OCDocker.DB.Models.Base as ocdb_models_base
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

    # DB model modules import `Initialise.session` at import-time and cache it.
    # Refresh cached references after runtime session initialization so model
    # methods (insert_or_update/find_first/etc.) do not see a stale None value.
    runtime_session = getattr(ocinit_runtime, "session", None)
    if runtime_session is not None:
        ocdb_runtime.session = runtime_session
        ocdb_models_base.session = runtime_session

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

            ocdb_runtime.create_tables()
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
    '''
    Upsert receptor/ligand/complex/pipeline-run records for one result.

    Parameters
    ----------
    job_name : str
        Pipeline job identifier.
    receptor : Any
        Receptor object with descriptor attributes.
    ligand : Any
        Ligand object with descriptor attributes.
    rescoring : Dict[str, Dict[str, float]]
        Rescoring payload keyed by engine and score name.
    box_label : Optional[str], default=None
        Optional box label appended to complex name.
    representative_pose : Optional[str], default=None
        Path to representative pose used in aggregation.
    representative_engine : Optional[str], default=None
        Engine name of representative pose.
    summary : Optional[Dict[str, Any]], default=None
        Optional summary payload persisted with pipeline run record.

    Returns
    -------
    Tuple[bool, str, List[str]]
        Success flag, stored complex name, and ignored score keys.
    '''

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


def _pipeline_progress_row_name(job_name: str, engine: str) -> str:
    '''
    Build a deterministic DB row name for engine progress records.

    Parameters
    ----------
    job_name : str
        Pipeline job identifier.
    engine : str
        Engine name associated with the progress event.

    Returns
    -------
    str
        Stable ``PipelineRuns.name`` value for progress upserts.
    '''

    return f"{job_name}__progress__{engine}"


def _store_engine_progress_in_db(
    *,
    job_name: str,
    database: str,
    receptor: str,
    kind: str,
    target: str,
    engine: str,
    phase: str,
    summary_path: Optional[str] = None,
    summary: Optional[Dict[str, Any]] = None,
) -> None:
    '''
    Persist mid-execution engine progress events into ``PipelineRuns``.

    Parameters
    ----------
    job_name : str
        Pipeline job identifier.
    database : str
        Database alias for the running target.
    receptor : str
        Receptor identifier.
    kind : str
        Target kind (for example ``ligands`` or ``decoys``).
    target : str
        Target identifier.
    engine : str
        Engine responsible for the progress event.
    phase : str
        Execution phase label (for example ``started`` or ``finished``).
    summary_path : Optional[str], default=None
        Optional path to the engine summary JSON.
    summary : Optional[Dict[str, Any]], default=None
        Optional summary payload to embed into the DB event record.

    Returns
    -------
    None
        This function writes side effects only.
    '''

    if not (pipeline_store_db and pipeline_store_db_mid_execution):
        return

    try:
        _ensure_db_runtime()
        from OCDocker.DB.Models.PipelineRuns import PipelineRuns

        event_payload: Dict[str, Any] = {
            "schema_version": 1,
            "record_type": "engine_progress",
            "phase": str(phase),
            "updated_at_utc": _utc_now_iso(),
            "job": {
                "name": str(job_name),
                "database": str(database),
                "receptor": str(receptor),
                "kind": str(kind),
                "target": str(target),
            },
            "engine": str(engine),
            "engine_summary_path": str(summary_path) if summary_path else None,
        }
        if isinstance(summary, dict) and summary:
            event_payload["engine_summary"] = _to_jsonable(summary)

        payload: Dict[str, Union[str, int, None]] = {
            "name": _pipeline_progress_row_name(job_name, engine),
            "representative_engine": str(engine),
            "summary_json": json.dumps(_to_jsonable(event_payload), sort_keys=True),
        }
        ok = PipelineRuns.insert_or_update(payload)
        if not ok:
            print(
                "Warning: failed to upsert engine progress in DB "
                f"for {job_name}/{engine} ({phase})."
            )
    except Exception as exc:
        print(
            "Warning: failed to store engine progress in DB "
            f"for {job_name}/{engine} ({phase}): {type(exc).__name__}: {exc}"
        )


def _canonicalize_rescore_key(engine: str, raw_key: str) -> str:
    '''
    Normalize rescoring keys to a canonical ``engine_metric`` form.

    Parameters
    ----------
    engine : str
        Engine namespace used to prefix normalized keys.
    raw_key : str
        Raw rescoring key produced by engine output.

    Returns
    -------
    str
        Canonical key name used across summaries and CSV export.
    '''

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

    # Collapse legacy/new Gnina aliases so CSV columns stay stable across runs.
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


def _prepare_cached_receptors_for_receptor(receptor_path):
    '''
    Prepare receptor artifacts once per receptor and reuse across ligand jobs.

    Parameters
    ----------
    receptor_path : str or PathLike
        Path to the receptor structure file.

    Returns
    -------
    None
        This function materializes prepared receptor files on disk.

    Raises
    ------
    RuntimeError
        If a required prepared receptor artifact cannot be created.
    '''

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
    '''
    Hash cache-relevant pipeline settings into a short invalidation signature.

    Returns
    -------
    str
        SHA1 digest representing cache-relevant configuration.
    '''

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
    '''
    Create the receptor-preparation cache manifest for validation.

    Parameters
    ----------
    receptor_path : Union[str, Path]
        Path to the receptor file associated with the cache entry.

    Returns
    -------
    Dict[str, Any]
        Manifest dictionary with input fingerprint and prepared artifacts.
    '''

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
    '''
    Validate receptor cache manifest against current receptor/artifact state.

    Parameters
    ----------
    cache_manifest_path : Union[str, Path]
        Path to a persisted receptor cache manifest.
    receptor_path : Union[str, Path]
        Path to the receptor file to validate against.

    Returns
    -------
    bool
        ``True`` when manifest matches current settings and files.
    '''

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
    '''
    Write receptor cache manifest JSON for a receptor entry.

    Parameters
    ----------
    cache_manifest_path : Union[str, Path]
        Destination path for the manifest JSON.
    receptor_path : Union[str, Path]
        Receptor path used to build current manifest content.

    Returns
    -------
    None
        This function writes the manifest file on disk.
    '''

    cache_manifest_path = Path(cache_manifest_path)
    manifest = _build_receptor_cache_manifest(receptor_path)
    cache_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    cache_manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _ensure_receptor_cache_ready(receptor_path: Union[str, Path], cache_manifest_path: Union[str, Path]) -> None:
    '''
    Prepare receptor artifacts and refresh cache manifest when stale.

    Parameters
    ----------
    receptor_path : Union[str, Path]
        Path to receptor input structure.
    cache_manifest_path : Union[str, Path]
        Path to receptor cache manifest file.

    Returns
    -------
    None
        This function ensures cache artifacts and manifest consistency.
    '''

    receptor_path = Path(receptor_path)
    cache_manifest_path = Path(cache_manifest_path)
    if _cache_manifest_is_valid(cache_manifest_path, receptor_path):
        return

    _prepare_cached_receptors_for_receptor(str(receptor_path))
    _write_cache_manifest(cache_manifest_path, receptor_path)


def _cached_receptor_files_present(receptor_path: Union[str, Path]) -> bool:
    '''
    Check whether required prepared receptor files are already present.

    Parameters
    ----------
    receptor_path : Union[str, Path]
        Path to receptor input structure.

    Returns
    -------
    bool
        ``True`` when all required prepared receptor files exist and are valid.
    '''

    receptor_dir = Path(receptor_path).resolve().parent
    if pipeline_requires_pdbqt and not _is_valid_file(receptor_dir / "prepared_receptor.pdbqt"):
        return False
    if pipeline_requires_mol2 and not _is_valid_file(receptor_dir / "prepared_receptor.mol2"):
        return False
    return True


def _ligand_cache_manifest_path(database: str, receptor: str, kind: str, target: str) -> str:
    '''
    Build the ligand preparation cache manifest path for one target.

    Parameters
    ----------
    database : str
        Database alias.
    receptor : str
        Receptor identifier.
    kind : str
        Target kind (for example ``ligands`` or ``decoys``).
    target : str
        Target identifier.

    Returns
    -------
    str
        Path to ligand cache manifest JSON.
    '''

    return str(_target_dir_path(database, receptor, kind, target) / f".prepared_ligand_cache.{pipeline_cache_key}.json")


def _build_ligand_cache_manifest(ligand_path: Union[str, Path], target_dir: Union[str, Path]) -> Dict[str, Any]:
    '''
    Create the ligand-preparation cache manifest for one target.

    Parameters
    ----------
    ligand_path : Union[str, Path]
        Path to ligand input file.
    target_dir : Union[str, Path]
        Target directory that stores prepared ligand artifacts.

    Returns
    -------
    Dict[str, Any]
        Manifest dictionary with ligand fingerprint and prepared artifacts.
    '''

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
    '''
    Validate ligand cache manifest against current target artifact state.

    Parameters
    ----------
    cache_manifest_path : Union[str, Path]
        Path to ligand cache manifest JSON.
    ligand_path : Union[str, Path]
        Ligand input path.
    target_dir : Union[str, Path]
        Target directory containing prepared ligand artifacts.

    Returns
    -------
    bool
        ``True`` when cached manifest matches current ligand and artifacts.
    '''

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
    '''
    Write ligand cache manifest JSON for one target entry.

    Parameters
    ----------
    cache_manifest_path : Union[str, Path]
        Destination path for ligand cache manifest JSON.
    ligand_path : Union[str, Path]
        Ligand input path used to fingerprint the cache.
    target_dir : Union[str, Path]
        Target directory containing prepared ligand artifacts.

    Returns
    -------
    None
        This function writes the manifest file on disk.
    '''

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
    '''
    Prepare shared ligand artifacts once per target entry.

    Parameters
    ----------
    receptor_path : Union[str, Path]
        Receptor input path.
    ligand_path : Union[str, Path]
        Ligand input path.
    box_path : Union[str, Path]
        Docking box definition path.
    target_dir : Union[str, Path]
        Target directory where prepared ligand files are written.
    job_name : str
        Pipeline job identifier used in generated artifact names.

    Returns
    -------
    None
        This function writes prepared ligand artifacts on disk.

    Raises
    ------
    RuntimeError
        If required prepared artifacts cannot be created.
    '''

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
    receptor_obj = ocr.Receptor(
        str(receptor_path),
        name=f"{job_name}_receptor",
        allow_missing_surface=True,
    )
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
    '''
    Prepare ligand artifacts and refresh cache manifest when stale.

    Parameters
    ----------
    receptor_path : Union[str, Path]
        Receptor input path.
    ligand_path : Union[str, Path]
        Ligand input path.
    box_path : Union[str, Path]
        Docking box definition path.
    target_dir : Union[str, Path]
        Target directory that stores prepared ligand artifacts.
    cache_manifest_path : Union[str, Path]
        Path to ligand cache manifest JSON.
    job_name : str
        Pipeline job identifier used in logs/artifacts.

    Returns
    -------
    None
        This function ensures ligand cache artifacts and manifest consistency.
    '''

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
pipeline_store_db_mid_execution = _as_bool(
    config.get("pipeline_store_db_mid_execution", pipeline_store_db),
    default=pipeline_store_db,
)
pipeline_export_database_csv = _as_bool(config.get("pipeline_export_database_csv", True), default=True)
pipeline_discovery_cache = _as_bool(config.get("pipeline_discovery_cache", True), default=True)
pipeline_report_include_python_packages = _as_bool(
    config.get("pipeline_report_include_python_packages", False),
    default=False,
)


def _parse_engine_int_map(value: Any) -> Dict[str, int]:
    '''
    Parse engine->integer maps from dicts or comma-separated key:value text.

    Parameters
    ----------
    value : Any
        Raw mapping value from config, either dict-like or ``"k:v,k:v"`` text.

    Returns
    -------
    Dict[str, int]
        Normalized engine-to-integer mapping with positive values only.
    '''

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

# GPU scheduling for docking jobs.
# By default, Gnina requires one GPU job slot when gnina_no_gpu is not enabled.
gnina_gpu_default = 0 if _as_bool(getattr(getattr(oc_config, "gnina", None), "no_gpu", "no"), default=False) else 1
pipeline_engine_gpu_default = max(0, int(config.get("pipeline_engine_gpu_default", 0)))
pipeline_engine_gpu_map = _parse_engine_int_map(config.get("pipeline_engine_gpu", {"gnina": gnina_gpu_default}))

# Optional rule priority map to bias mixed-engine scheduling.
pipeline_engine_priority_default = max(1, int(config.get("pipeline_engine_priority_default", 50)))
pipeline_engine_priority_map = _parse_engine_int_map(config.get("pipeline_engine_priority", {"gnina": 100}))

# Optional per-engine parallel caps. When set, each engine consumes one dedicated slot.
pipeline_engine_max_parallel_map = _parse_engine_int_map(config.get("pipeline_engine_max_parallel", {}))

pipeline_postprocess_threads = max(1, int(config.get("pipeline_postprocess_threads", 1)))
pipeline_postprocess_mem_mb = max(1, int(config.get("pipeline_postprocess_mem_mb", 4000)))
pipeline_oddt_threads = max(1, int(config.get("pipeline_oddt_threads", 1)))
pipeline_oddt_mem_mb = max(1, int(config.get("pipeline_oddt_mem_mb", pipeline_postprocess_mem_mb)))
pipeline_oddt_timeout = max(0, int(config.get("pipeline_oddt_timeout", config.get("pipeline_timeout", 0) or 0) or 0))


def _engine_threads(engine: str) -> int:
    '''
    Return configured CPU threads for a given engine rule instance.

    Parameters
    ----------
    engine : str
        Engine name.

    Returns
    -------
    int
        Thread count for the engine.
    '''

    return max(1, int(pipeline_engine_threads_map.get(engine, pipeline_engine_threads_default)))


def _engine_mem_mb(engine: str) -> int:
    '''
    Return configured memory budget in MB for a given engine rule instance.

    Parameters
    ----------
    engine : str
        Engine name.

    Returns
    -------
    int
        Memory budget in megabytes for the engine.
    '''

    return max(1, int(pipeline_engine_mem_mb_map.get(engine, pipeline_engine_mem_mb_default)))


def _engine_gpu(engine: str) -> int:
    '''
    Return configured GPU slots required for a given engine rule instance.

    Parameters
    ----------
    engine : str
        Engine name.

    Returns
    -------
    int
        Number of GPU slots consumed by one engine job.
    '''

    return max(0, int(pipeline_engine_gpu_map.get(engine, pipeline_engine_gpu_default)))


def _engine_priority(engine: str) -> int:
    '''
    Return configured Snakemake priority for a given engine rule instance.

    Parameters
    ----------
    engine : str
        Engine name.

    Returns
    -------
    int
        Snakemake priority value for scheduling.
    '''

    return max(1, int(pipeline_engine_priority_map.get(engine, pipeline_engine_priority_default)))


pipeline_total_gpus_default = 1 if _engine_gpu("gnina") > 0 else 0
pipeline_total_gpus = max(0, int(config.get("pipeline_total_gpus", pipeline_total_gpus_default)))

if "workflow" in globals():
    # Enforce a global GPU pool directly from config, so users don't need to pass
    # `--resources gpu=...` for common local runs.
    workflow.global_resources["gpu"] = pipeline_total_gpus
    for _engine_name, _limit in pipeline_engine_max_parallel_map.items():
        workflow.global_resources[f"engine_slots_{_engine_name}"] = max(1, int(_limit))


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
    '''
    Heuristically detect whether a config token looks like a filesystem path.

    Parameters
    ----------
    value : str
        Candidate text token from configuration.

    Returns
    -------
    bool
        ``True`` when token resembles a filesystem path.
    '''

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
    '''
    Validate a database alias used in ``database_sources`` configuration.

    Parameters
    ----------
    alias : str
        Normalized database alias.
    source : str
        Original source string from configuration.

    Returns
    -------
    None
        This function validates and raises on invalid aliases.

    Raises
    ------
    RuntimeError
        If alias is empty or contains path separators.
    '''

    if not alias:
        raise RuntimeError(f"Invalid database source '{source}': empty alias.")
    if os.sep in alias or (os.altsep and os.altsep in alias):
        raise RuntimeError(
            f"Invalid database alias '{alias}' from source '{source}'. "
            "Aliases cannot contain path separators."
        )


def _parse_database_sources(sources: List[str]) -> Dict[str, Dict[str, Any]]:
    '''
    Parse and validate configured database sources into normalized specs.

    Parameters
    ----------
    sources : List[str]
        Raw ``database_sources`` entries from config.

    Returns
    -------
    Dict[str, Dict[str, Any]]
        Mapping from alias to normalized source metadata.

    Raises
    ------
    RuntimeError
        If configuration entries are malformed or resolve to invalid paths.
    '''

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
    '''
    Return source-root path for a configured database alias.

    Parameters
    ----------
    database : str
        Database alias.

    Returns
    -------
    Path
        Resolved source root for the alias.
    '''

    spec = database_specs.get(database)
    if spec is None:
        raise RuntimeError(f"Unknown database alias '{database}'. Check database_sources configuration.")
    return Path(str(spec["root"]))


database_rule_root = Path(ocdb_path).resolve()
database_rule_root.mkdir(parents=True, exist_ok=True)
database_rule_root_str = str(database_rule_root)


def _prepare_database_mounts() -> None:
    '''
    Create/update per-alias symlink mounts under the pipeline database root.

    Returns
    -------
    None
        This function ensures alias mount points are consistent.

    Raises
    ------
    RuntimeError
        If an existing mount path conflicts with required source layout.
    '''

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
    '''
    Return mounted rule root path for a database alias.

    Parameters
    ----------
    database : str
        Database alias.

    Returns
    -------
    Path
        Mounted database path used by workflow rules.
    '''

    return database_rule_root / database


def _source_receptor_path(database: str, receptor: str) -> Path:
    '''
    Return receptor source file path in the original database root.

    Parameters
    ----------
    database : str
        Database alias.
    receptor : str
        Receptor identifier.

    Returns
    -------
    Path
        Path to source ``receptor.pdb``.
    '''

    return _database_root_path(database) / receptor / "receptor.pdb"


def _receptor_path(database: str, receptor: str) -> Path:
    '''
    Return receptor file path through the mounted database rule root.

    Parameters
    ----------
    database : str
        Database alias.
    receptor : str
        Receptor identifier.

    Returns
    -------
    Path
        Mounted path to ``receptor.pdb``.
    '''

    return _database_rule_root_path(database) / receptor / "receptor.pdb"


def _receptor_cache_manifest_path(database: str, receptor: str) -> Path:
    '''
    Return receptor cache manifest path for a receptor entry.

    Parameters
    ----------
    database : str
        Database alias.
    receptor : str
        Receptor identifier.

    Returns
    -------
    Path
        Path to receptor cache manifest JSON.
    '''

    return _database_rule_root_path(database) / receptor / f".prepared_receptor_cache.{pipeline_cache_key}.json"


def _target_dir_path(database: str, receptor: str, kind: str, target: str) -> Path:
    '''
    Return directory path for one target entry (database/receptor/kind/target).

    Parameters
    ----------
    database : str
        Database alias.
    receptor : str
        Receptor identifier.
    kind : str
        Target kind.
    target : str
        Target identifier.

    Returns
    -------
    Path
        Target directory path.
    '''

    return _database_rule_root_path(database) / receptor / "compounds" / kind / target


def _ligand_path(database: str, receptor: str, kind: str, target: str) -> Path:
    '''
    Return ligand input path (``ligand.smi``) for one target entry.

    Parameters
    ----------
    database : str
        Database alias.
    receptor : str
        Receptor identifier.
    kind : str
        Target kind.
    target : str
        Target identifier.

    Returns
    -------
    Path
        Ligand input path.
    '''

    return _target_dir_path(database, receptor, kind, target) / "ligand.smi"


def _box_path(database: str, receptor: str, kind: str, target: str) -> Path:
    '''
    Return default docking box path (``boxes/box0.pdb``) for one target entry.

    Parameters
    ----------
    database : str
        Database alias.
    receptor : str
        Receptor identifier.
    kind : str
        Target kind.
    target : str
        Target identifier.

    Returns
    -------
    Path
        Default docking box path.
    '''

    return _target_dir_path(database, receptor, kind, target) / "boxes" / "box0.pdb"


def _reference_ligand_paths(database: str, receptor: str) -> List[Path]:
    '''
    Return receptor-level reference ligand candidate paths for one entry.

    Parameters
    ----------
    database : str
        Database alias.
    receptor : str
        Receptor identifier.

    Returns
    -------
    List[Path]
        Ordered candidate paths for receptor reference ligands.
    '''

    receptor_root = _database_rule_root_path(database) / receptor
    return [receptor_root / name for name in _REFERENCE_LIGAND_FILENAMES]


def _resolve_reference_ligand_path(database: str, receptor: str) -> Optional[Path]:
    '''
    Return first valid receptor-level reference ligand path, if available.

    Parameters
    ----------
    database : str
        Database alias.
    receptor : str
        Receptor identifier.

    Returns
    -------
    Optional[Path]
        First valid reference ligand path, else ``None``.
    '''

    for candidate in _reference_ligand_paths(database, receptor):
        if _is_valid_file(candidate):
            return candidate
    return None


def _ensure_target_box_from_reference_ligand(
    *,
    database: str,
    receptor: str,
    kind: str,
    target: str,
    ligand_path: Union[str, Path],
    box_path: Union[str, Path],
) -> None:
    '''
    Ensure ``boxes/box0.pdb`` exists, generating it from reference-ligand centroid when missing.

    Parameters
    ----------
    database : str
        Database alias.
    receptor : str
        Receptor identifier.
    kind : str
        Target kind.
    target : str
        Target identifier.
    ligand_path : Union[str, Path]
        Candidate ligand path used to infer box size.
    box_path : Union[str, Path]
        Expected box output path.

    Returns
    -------
    None
        This function materializes box file when absent.

    Raises
    ------
    RuntimeError
        If no valid reference ligand is available or box generation fails.
    '''

    import OCDocker.Ligand as ocl

    ligand_path = Path(ligand_path).resolve()
    box_path = Path(box_path).resolve()
    if _is_valid_file(box_path):
        return

    # Remove stale zero-byte artifacts before creating a fresh box.
    if box_path.is_file():
        try:
            if box_path.stat().st_size <= 0:
                box_path.unlink()
        except OSError:
            pass

    reference_ligand_raw = _resolve_reference_ligand_path(database, receptor)
    if reference_ligand_raw is None:
        expected_names = ", ".join(_REFERENCE_LIGAND_FILENAMES)
        receptor_root = _database_rule_root_path(database) / receptor
        raise RuntimeError(
            f"Missing docking box at '{box_path}' and missing reference ligand under '{receptor_root}'. "
            f"Expected one of: {expected_names}. Provide boxes/box0.pdb or a reference ligand file."
        )
    reference_ligand = reference_ligand_raw.resolve()

    centroid: Optional[Tuple[float, float, float]] = None
    centroid_error: Optional[Exception] = None
    for sanitize in (True, False):
        try:
            centroid_raw = ocl.get_centroid(str(reference_ligand), sanitize=sanitize)
        except Exception as exc:
            centroid_error = exc
            continue

        if centroid_raw is None:
            continue

        if hasattr(centroid_raw, "x") and hasattr(centroid_raw, "y") and hasattr(centroid_raw, "z"):
            centroid = (float(centroid_raw.x), float(centroid_raw.y), float(centroid_raw.z))
        else:
            try:
                centroid_values = tuple(float(value) for value in centroid_raw)
            except Exception:
                centroid = None
                continue
            if len(centroid_values) == 3:
                centroid = (
                    float(centroid_values[0]),
                    float(centroid_values[1]),
                    float(centroid_values[2]),
                )

        if centroid is not None:
            break

    if centroid is None:
        error_suffix = f" Last error: {centroid_error}" if centroid_error is not None else ""
        raise RuntimeError(
            f"Failed to compute centroid from reference ligand '{reference_ligand}'.{error_suffix}"
        )

    ligand_obj = None
    ligand_error: Optional[Exception] = None
    ligand_name = f"{database}_{receptor}_{kind}_{target}"
    for sanitize in (True, False):
        try:
            ligand_obj = ocl.Ligand(str(ligand_path), name=ligand_name, sanitize=sanitize)
            break
        except Exception as exc:
            ligand_error = exc

    if ligand_obj is None:
        error_suffix = f" Last error: {ligand_error}" if ligand_error is not None else ""
        raise RuntimeError(
            f"Failed to parse candidate ligand '{ligand_path}' to infer box boundaries.{error_suffix}"
        )

    if ligand_obj.RadiusOfGyration is None:
        raise RuntimeError(
            f"RadiusOfGyration is unavailable for ligand '{ligand_path}'. "
            "Cannot infer box boundaries from candidate ligand."
        )

    box_path.parent.mkdir(parents=True, exist_ok=True)
    create_result = ligand_obj.create_box(
        centroid=centroid,
        save_path=str(box_path.parent),
        overwrite=False,
    )
    if not _is_valid_file(box_path):
        if create_result is None:
            raise RuntimeError(f"Failed to generate docking box at '{box_path}'.")
        raise RuntimeError(
            f"Failed to generate docking box at '{box_path}' (create_box returned code {create_result})."
        )


def _payload_path(database: str, receptor: str, kind: str, target: str) -> Path:
    '''
    Return final payload pickle path for one target entry.

    Parameters
    ----------
    database : str
        Database alias.
    receptor : str
        Receptor identifier.
    kind : str
        Target kind.
    target : str
        Target identifier.

    Returns
    -------
    Path
        Final payload pickle path.
    '''

    return _target_dir_path(database, receptor, kind, target) / "payload.pkl"


def _run_report_path(database: str, receptor: str, kind: str, target: str) -> Path:
    '''
    Return run-report JSON path for one target entry.

    Parameters
    ----------
    database : str
        Database alias.
    receptor : str
        Receptor identifier.
    kind : str
        Target kind.
    target : str
        Target identifier.

    Returns
    -------
    Path
        Run report JSON path.
    '''

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
    '''
    Discover receptor IDs by scanning ``*/receptor.pdb`` on disk.

    Parameters
    ----------
    database : str
        Database alias.

    Returns
    -------
    List[str]
        Sorted receptor identifiers found on disk.
    '''

    db_dir = _database_root_path(database)
    if not db_dir.exists():
        return []

    receptors: List[str] = []
    for receptor_file in db_dir.glob("*/receptor.pdb"):
        if receptor_file.is_file():
            receptors.append(receptor_file.parent.name)
    return sorted(set(receptors))


def _collect_database_receptors(database: str) -> List[str]:
    '''
    Collect receptor IDs for one database using selected discovery mode.

    Parameters
    ----------
    database : str
        Database alias.

    Returns
    -------
    List[str]
        Sorted discovered receptor identifiers for the database.

    Raises
    ------
    RuntimeError
        If no receptors are discovered for a selected database.
    '''

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
    '''
    Return filesystem path for target discovery cache metadata.

    Returns
    -------
    Path
        Cache file path for discovered targets metadata.
    '''

    return _runtime_cache_root() / "target_discovery_cache.json"


def _target_discovery_signature(database_to_receptors: Dict[str, List[str]]) -> str:
    '''
    Build a content signature used to validate discovery-cache reuse.

    Parameters
    ----------
    database_to_receptors : Dict[str, List[str]]
        Receptors discovered per database alias.

    Returns
    -------
    str
        Deterministic JSON hash for current discovery-relevant layout.
    '''

    layout: List[Dict[str, Any]] = []
    for database in selected_databases:
        database_root = _database_root_path(database)
        for receptor in database_to_receptors.get(database, []):
            receptor_path = database_root / receptor / "receptor.pdb"
            receptor_exists = receptor_path.is_file()
            receptor_stat = receptor_path.stat() if receptor_exists else None
            reference_ligand_entries: List[Dict[str, Any]] = []
            for reference_name in _REFERENCE_LIGAND_FILENAMES:
                reference_path = database_root / receptor / reference_name
                reference_exists = reference_path.is_file()
                reference_stat = reference_path.stat() if reference_exists else None
                reference_size = int(reference_stat.st_size) if reference_stat else 0
                reference_ligand_entries.append(
                    {
                        "name": reference_name,
                        "path": str(reference_path),
                        "exists": reference_exists,
                        "size": reference_size,
                        "mtime_ns": int(reference_stat.st_mtime_ns) if reference_stat else 0,
                    }
                )
            has_reference_ligand = any(entry["exists"] and int(entry["size"]) > 0 for entry in reference_ligand_entries)
            receptor_entry: Dict[str, Any] = {
                "database": database,
                "database_root": str(database_root),
                "receptor": receptor,
                "receptor_exists": receptor_exists,
                "receptor_size": int(receptor_stat.st_size) if receptor_stat else 0,
                "receptor_mtime_ns": int(receptor_stat.st_mtime_ns) if receptor_stat else 0,
                "reference_ligand_exists": has_reference_ligand,
                "reference_ligands": reference_ligand_entries,
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
        "schema_version": _TARGET_DISCOVERY_CACHE_SCHEMA_VERSION,
        "ocdb_path": str(Path(ocdb_path).resolve()),
        "selected_databases": list(selected_databases),
        "database_roots": {database: str(_database_root_path(database)) for database in selected_databases},
        "selected_kinds": list(selected_kinds),
        "target_discovery_mode": target_discovery_mode,
        "layout": layout,
    }
    return _json_sha256(payload)


def _load_target_discovery_cache(signature: str) -> Optional[Tuple[List[str], int]]:
    '''
    Load cached discovered targets when signature and schema match.

    Parameters
    ----------
    signature : str
        Expected discovery signature for current run context.

    Returns
    -------
    Optional[Tuple[List[str], int]]
        Cached targets and scanned count, or ``None`` when cache is invalid.
    '''

    cache_path = _target_discovery_cache_path()
    if not cache_path.is_file():
        return None

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    if payload.get("schema_version") != _TARGET_DISCOVERY_CACHE_SCHEMA_VERSION:
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
    '''
    Persist discovered target list and scan statistics to cache.

    Parameters
    ----------
    signature : str
        Discovery signature associated with the cache payload.
    targets : List[str]
        Discovered payload target paths.
    scanned : int
        Number of candidate directories scanned during discovery.

    Returns
    -------
    None
        This function writes the discovery cache file.
    '''

    cache_path = _target_discovery_cache_path()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": _TARGET_DISCOVERY_CACHE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now_iso(),
        "signature": signature,
        "scanned": int(scanned),
        "targets": sorted(set(targets)),
    }

    lock_path = cache_path.with_suffix(".lock")
    with _file_lock(lock_path):
        # Snakemake can parse this snakefile concurrently in subprocess mode.
        # Use a unique tmp file per writer and serialize replace() operations.
        tmp_path = cache_path.with_name(
            f"{cache_path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp_path.replace(cache_path)


def collect_payload_targets():
    '''
    Discover and validate all pipeline payload targets across databases.

    Returns
    -------
    List[str]
        Sorted payload target paths for workflow execution.

    Raises
    ------
    RuntimeError
        If no valid targets are found after discovery and validation.
    '''

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
            reference_ligand_path = _resolve_reference_ligand_path(database, receptor)
            has_reference_ligand = reference_ligand_path is not None

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
                    has_box = _is_valid_file(box_path)
                    if not _is_valid_file(ligand_path) or (not has_box and not has_reference_ligand):
                        continue

                    targets.append(
                        str(_payload_path(database, receptor, kind, target_dir.name))
                    )

    unique_targets = sorted(set(targets))
    if not unique_targets:
        raise RuntimeError(
            "No valid targets found to process. "
            "Checked selected databases/kinds and required files: receptor.pdb, ligand.smi, "
            "and either boxes/box0.pdb or reference_ligand.pdb/.sdf."
        )

    if pipeline_discovery_cache:
        _write_target_discovery_cache(discovery_signature, unique_targets, scanned)

    print(
        "Discovery summary: "
        f"mode={target_discovery_mode}, scanned={scanned}, valid_targets={len(unique_targets)}, cache=miss"
    )
    return unique_targets


all_payload_targets = collect_payload_targets()


def _database_results_csv_path(database: str) -> str:
    '''
    Return consolidated CSV export path for one database alias.

    Parameters
    ----------
    database : str
        Database alias.

    Returns
    -------
    str
        Path to per-database consolidated CSV file.
    '''

    return str(_database_rule_root_path(database) / "pipeline_results.csv")


def _collect_database_csv_targets() -> List[str]:
    '''
    Return CSV outputs requested by ``rule all`` based on config.

    Returns
    -------
    List[str]
        CSV output paths requested by current configuration.
    '''

    if not pipeline_export_database_csv:
        return []
    return [_database_results_csv_path(database) for database in selected_databases]


def _payload_targets_for_database(database: str) -> List[str]:
    '''
    Filter global payload targets to only those under one database root.

    Parameters
    ----------
    database : str
        Database alias.

    Returns
    -------
    List[str]
        Sorted payload paths belonging to the given database.
    '''

    db_root = _database_rule_root_path(database).resolve()
    targets: List[str] = []
    for payload in all_payload_targets:
        payload_path = Path(payload).resolve()
        try:
            payload_path.relative_to(db_root)
        except ValueError:
            continue
        targets.append(str(payload_path))
    return sorted(set(targets))


def _csv_scalar(value: Any) -> str:
    '''
    Convert values to stable scalar strings for CSV serialization.

    Parameters
    ----------
    value : Any
        Value to normalize into CSV-safe scalar text.

    Returns
    -------
    str
        Normalized scalar string representation.
    '''

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
    '''
    Normalize free-form keys into safe lowercase CSV column identifiers.

    Parameters
    ----------
    value : Any
        Raw key to normalize.

    Returns
    -------
    str
        Sanitized lowercase identifier suitable for CSV columns.
    '''

    key = str(value).strip().lower()
    key = re.sub(r"[^0-9a-zA-Z_]+", "_", key)
    key = re.sub(r"_+", "_", key).strip("_")
    return key or "score"


def _flatten_summary_rescoring_for_csv(summary: Dict[str, Any]) -> Dict[str, float]:
    '''
    Flatten summary rescoring payloads into tabular CSV columns.

    Parameters
    ----------
    summary : Dict[str, Any]
        Pipeline summary payload containing rescoring data.

    Returns
    -------
    Dict[str, float]
        Flattened mapping from CSV column names to numeric scores.
    '''

    flattened: Dict[str, float] = {}

    def _ingest_rescoring(rescoring_data: Any, prefix: str = "") -> None:
        '''
        Ingest one rescoring dictionary into flattened CSV score columns.

        Parameters
        ----------
        rescoring_data : Any
            Rescoring payload expected to be engine->score mapping.
        prefix : str, default=""
            Optional column prefix (for example per-box prefix).

        Returns
        -------
        None
            This function mutates ``flattened`` in place.
        '''

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
    '''
    Map one scoring function from OCDocker.cfg into a CSV column name.

    Parameters
    ----------
    engine : str
        Scoring engine name.
    scoring_function : str
        Raw scoring function name from configuration.

    Returns
    -------
    Optional[str]
        Normalized CSV column name, or ``None`` when unmapped.
    '''

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


def _configured_score_columns_for_csv() -> List[str]:
    '''
    Return score columns ordered by scoring_functions in OCDocker.cfg.

    Returns
    -------
    List[str]
        Ordered score column names derived from OCDocker configuration.
    '''

    ordered: List[str] = []

    def _add(column: Optional[str]) -> None:
        '''
        Append a column to ordered output if it is valid and unseen.

        Parameters
        ----------
        column : Optional[str]
            Candidate column name.

        Returns
        -------
        None
            This function mutates ``ordered`` in place.
        '''

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


def _write_database_results_csv(database: str, payload_paths: List[str], csv_path: Union[str, Path]) -> None:
    '''
    Write a consolidated per-database CSV from target payloads and summaries.

    Parameters
    ----------
    database : str
        Database alias being exported.
    payload_paths : List[str]
        Payload pickle paths included in the CSV.
    csv_path : Union[str, Path]
        Destination CSV path.

    Returns
    -------
    None
        This function writes consolidated CSV output to disk.
    '''

    import csv

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
            f"{type(exc).__name__}: {exc}"
        )

    receptor_descriptor_columns = [f"receptor_{name}" for name in receptor_descriptor_names]
    ligand_descriptor_columns = [f"ligand_{name}" for name in ligand_descriptor_names]

    receptor_descriptor_cache: Dict[str, Dict[str, Union[int, float]]] = {}
    ligand_descriptor_cache: Dict[str, Dict[str, Union[int, float]]] = {}

    def _load_receptor_descriptors(receptor_path: Path, receptor_name: str) -> Dict[str, Union[int, float]]:
        '''
        Load or compute receptor descriptor values for CSV output.

        Parameters
        ----------
        receptor_path : Path
            Receptor structure path.
        receptor_name : str
            Receptor identifier used for object naming.

        Returns
        -------
        Dict[str, Union[int, float]]
            Numeric receptor descriptor mapping.
        '''

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
                f"{type(exc).__name__}: {exc}"
            )
            payload = {}

        receptor_descriptor_cache[cache_key] = payload
        return payload

    def _load_ligand_descriptors(ligand_paths: List[Path], target_name: str) -> Dict[str, Union[int, float]]:
        '''
        Load or compute ligand descriptor values for CSV output.

        Parameters
        ----------
        ligand_paths : List[Path]
            Candidate ligand paths to probe for descriptors.
        target_name : str
            Target identifier used for object naming.

        Returns
        -------
        Dict[str, Union[int, float]]
            Numeric ligand descriptor mapping.
        '''

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
                    f"{type(exc).__name__}: {exc}"
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
                f"{type(exc).__name__}: {exc}"
            )
            continue

        if not isinstance(payload, dict):
            continue

        target_dir = payload_file.parent
        summary_path = target_dir / "summary.json"
        summary = payload.get("summary", {})
        if not isinstance(summary, dict):
            summary = {}

        # Prefer on-disk summary when present (payload can be stale after partial reruns).
        if summary_path.is_file():
            try:
                loaded_summary = json.loads(summary_path.read_text(encoding="utf-8"))
                if isinstance(loaded_summary, dict):
                    summary = loaded_summary
            except Exception as exc:
                print(
                    f"Warning: failed to parse summary '{summary_path}' during CSV export: "
                    f"{type(exc).__name__}: {exc}"
                )

        receptor_name = str(payload.get("receptor", ""))
        target_name = str(payload.get("target", ""))
        receptor_path = target_dir.parents[2] / "receptor.pdb"
        ligand_mol2 = target_dir / "ligand.mol2"
        ligand_smi = target_dir / "ligand.smi"
        prepared_ligand_mol2 = target_dir / "prepared_ligand.mol2"
        ligand_candidates: List[Path] = []
        for candidate in (ligand_mol2, ligand_smi, prepared_ligand_mol2):
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

    base_columns = [
        "database",
        "receptor",
        "kind",
        "target",
        "name",
    ]

    configured_score_columns = [
        score for score in _configured_score_columns_for_csv() if score not in excluded_score_columns
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


def _pipeline_core_summary_path(database: str, receptor: str, kind: str, target: str) -> str:
    '''
    Build the intermediate core-summary path for one target entry.

    Parameters
    ----------
    database : str
        Database alias.
    receptor : str
        Receptor identifier.
    kind : str
        Target kind.
    target : str
        Target identifier.

    Returns
    -------
    str
        Absolute path to ``pipeline_core_summary.json``.
    '''

    return str(_target_dir_path(database, receptor, kind, target) / "pipeline_core_summary.json")


def _oddt_status_path(database: str, receptor: str, kind: str, target: str) -> str:
    '''
    Build the intermediate ODDT status path for one target entry.

    Parameters
    ----------
    database : str
        Database alias.
    receptor : str
        Receptor identifier.
    kind : str
        Target kind.
    target : str
        Target identifier.

    Returns
    -------
    str
        Absolute path to ``oddt_status.json``.
    '''

    return str(_target_dir_path(database, receptor, kind, target) / "oddt_status.json")


def _wc_core_summary_path(wildcards) -> str:
    '''
    Resolve core-summary intermediate path from Snakemake wildcards.

    Parameters
    ----------
    wildcards : Any
        Snakemake wildcards carrying ``database/receptor/kind/target`` fields.

    Returns
    -------
    str
        Absolute core-summary path for the wildcard tuple.
    '''

    return _pipeline_core_summary_path(wildcards.database, wildcards.receptor, wildcards.kind, wildcards.target)


def _wc_oddt_status_path(wildcards) -> str:
    '''
    Resolve ODDT-status intermediate path from Snakemake wildcards.

    Parameters
    ----------
    wildcards : Any
        Snakemake wildcards carrying ``database/receptor/kind/target`` fields.

    Returns
    -------
    str
        Absolute ODDT status path for the wildcard tuple.
    '''

    return _oddt_status_path(wildcards.database, wildcards.receptor, wildcards.kind, wildcards.target)


def _collect_pipeline_summary(
    target_dir: Path,
    job_name: str,
) -> Tuple[Dict[str, Any], Optional[Path], List[Path]]:
    '''
    Load summary outputs generated by post-processing for one target.

    Parameters
    ----------
    target_dir : Path
        Target directory containing summary artifacts.
    job_name : str
        Pipeline job identifier.

    Returns
    -------
    Tuple[Dict[str, Any], Optional[Path], List[Path]]
        Summary payload, optional summary file path, and per-box summary paths.

    Raises
    ------
    RuntimeError
        If required summary outputs are missing.
    '''

    summary_path = target_dir / "summary.json"
    summary_output_path: Optional[Path] = None
    per_box_summary_paths: List[Path] = []

    if summary_path.exists():
        with summary_path.open("r", encoding="utf-8") as handle:
            summary = json.load(handle)
        summary_output_path = summary_path
        return summary, summary_output_path, per_box_summary_paths

    if pipeline_all_boxes:
        per_box_summary: Dict[str, Any] = {}
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
        return summary, None, per_box_summary_paths

    raise RuntimeError(f"Pipeline output missing summary.json at: {summary_path}")


def _write_json(path: Union[str, Path], payload: Any) -> None:
    '''
    Write JSON payload with stable formatting.

    Parameters
    ----------
    path : Union[str, Path]
        Destination JSON path.
    payload : Any
        Payload to serialize as JSON.

    Returns
    -------
    None
        This function writes JSON to disk.
    '''

    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(_to_jsonable(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _find_prepared_receptor_for_oddt(receptor_path: Union[str, Path]) -> Optional[Path]:
    '''
    Locate a prepared receptor suitable for ODDT rescoring.

    Parameters
    ----------
    receptor_path : Union[str, Path]
        Primary receptor input path.

    Returns
    -------
    Optional[Path]
        First valid prepared receptor candidate, else ``None``.
    '''

    receptor_path = Path(receptor_path)
    receptor_dir = receptor_path.resolve().parent
    candidates = [
        receptor_dir / "prepared_receptor.pdbqt",
        receptor_dir / "prepared_receptor.mol2",
        receptor_path,
    ]
    for candidate in candidates:
        if _is_valid_file(candidate):
            return candidate
    return None


def _extract_oddt_scores_from_dataframe(data: Any) -> Dict[str, float]:
    '''
    Convert the first-row ODDT API DataFrame into canonical `oddt_*` keys.

    Parameters
    ----------
    data : Any
        ODDT API dataframe-like object.

    Returns
    -------
    Dict[str, float]
        Canonical ODDT score mapping extracted from the first row.
    '''

    scores: Dict[str, float] = {}
    if data is None or not hasattr(data, "empty") or bool(getattr(data, "empty", True)):
        return scores

    try:
        first_row = data.iloc[0].to_dict()
    except Exception:
        return scores

    def _coerce_numeric(value: Any) -> Optional[float]:
        '''
        Coerce nested ODDT cell payloads into finite floats.

        Parameters
        ----------
        value : Any
            Raw score cell value.

        Returns
        -------
        Optional[float]
            Parsed finite float, or ``None`` when conversion fails.
        '''

        numeric = _to_numeric(value)
        if numeric is not None:
            return float(numeric)
        if isinstance(value, str):
            try:
                parsed = float(value.strip())
                return parsed if not math.isnan(parsed) and not math.isinf(parsed) else None
            except (TypeError, ValueError):
                return None
        if isinstance(value, (list, tuple)) and value:
            return _coerce_numeric(value[0])
        return None

    for raw_key, raw_value in first_row.items():
        key = str(raw_key or "").strip()
        if not key or key.lower() in {"ligand_name", "name", "title"}:
            continue
        numeric_value = _coerce_numeric(raw_value)
        if numeric_value is None:
            continue
        canonical = _canonicalize_rescore_key("oddt", key)
        scores[canonical] = float(numeric_value)

    return scores


def _run_oddt_api_once(
    *,
    receptor_path: str,
    ligand_path: str,
    run_name: str,
    output_dir: str,
    threads_hint: int,
    chunksize: int,
) -> Dict[str, Any]:
    '''
    Execute ODDT API once and return parsed status payload.

    Parameters
    ----------
    receptor_path : str
        Prepared receptor path used for ODDT scoring.
    ligand_path : str
        Ligand pose path used for ODDT scoring.
    run_name : str
        Run identifier for generated ODDT output files.
    output_dir : str
        Directory for ODDT output files.
    threads_hint : int
        CPU thread hint passed to ODDT.
    chunksize : int
        ODDT processing chunk size.

    Returns
    -------
    Dict[str, Any]
        Structured ODDT execution status payload.
    '''

    try:
        import OCDocker.Rescoring.ODDT as ocoddt

        completed = ocoddt.run_oddt(
            receptor_path,
            ligand_path,
            run_name,
            output_dir,
            returnData=True,
            overwrite=True,
            n_cpu=max(1, int(threads_hint)),
            chunksize=max(1, int(chunksize)),
        )
        if isinstance(completed, int):
            return {
                "success": False,
                "scores": {},
                "returncode": int(completed),
                "error": f"ODDT API returned non-zero code: {completed}",
            }

        scores = _extract_oddt_scores_from_dataframe(completed)
        if not scores:
            return {
                "success": False,
                "scores": {},
                "returncode": 0,
                "error": "ODDT API completed but produced no numeric scores.",
            }

        return {"success": True, "scores": scores, "returncode": 0, "error": ""}
    except Exception as exc:
        return {
            "success": False,
            "scores": {},
            "returncode": 1,
            "error": f"ODDT API execution failed: {type(exc).__name__}: {exc}",
        }


def _run_oddt_api_worker(
    queue: Any,
    *,
    receptor_path: str,
    ligand_path: str,
    run_name: str,
    output_dir: str,
    threads_hint: int,
    chunksize: int,
) -> None:
    '''
    Execute ODDT API in a worker process and publish parsed payload to a queue.

    Parameters
    ----------
    queue : Any
        Multiprocessing queue used to return status payload.
    receptor_path : str
        Prepared receptor path used for ODDT scoring.
    ligand_path : str
        Ligand pose path used for ODDT scoring.
    run_name : str
        Run identifier for generated ODDT output files.
    output_dir : str
        Directory for ODDT output files.
    threads_hint : int
        CPU thread hint passed to ODDT.
    chunksize : int
        ODDT processing chunk size.

    Returns
    -------
    None
        This function writes one payload into ``queue``.
    '''

    queue.put(
        _run_oddt_api_once(
            receptor_path=receptor_path,
            ligand_path=ligand_path,
            run_name=run_name,
            output_dir=output_dir,
            threads_hint=threads_hint,
            chunksize=chunksize,
        )
    )


def _run_oddt_api_for_pose(
    *,
    receptor_path: Path,
    ligand_path: Path,
    output_dir: Path,
    run_name: str,
    timeout_seconds: int,
    threads_hint: int,
) -> Dict[str, Any]:
    '''
    Run ODDT API for one representative pose and return status payload.

    Parameters
    ----------
    receptor_path : Path
        Prepared receptor path used for ODDT scoring.
    ligand_path : Path
        Representative ligand pose path used for ODDT scoring.
    output_dir : Path
        Output directory for ODDT files.
    run_name : str
        Run identifier for generated outputs.
    timeout_seconds : int
        Timeout in seconds for ODDT execution (``<=0`` disables timeout).
    threads_hint : int
        CPU thread hint passed to ODDT.

    Returns
    -------
    Dict[str, Any]
        Structured ODDT execution result payload.
    '''

    result: Dict[str, Any] = {
        "success": False,
        "scores": {},
        "error": "",
        "output_csv": "",
        "returncode": None,
        "mode": "ocdocker_api",
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / f"{run_name}.csv"
    result["output_csv"] = str(output_csv)

    chunksize_raw = getattr(getattr(get_config(), "oddt", None), "chunk_size", 100)
    try:
        chunksize = max(1, int(chunksize_raw))
    except (TypeError, ValueError):
        chunksize = 100

    try:
        if timeout_seconds > 0:
            ctx = mp.get_context("fork" if hasattr(os, "fork") else "spawn")
            queue = ctx.Queue(maxsize=1)
            process = ctx.Process(
                target=_run_oddt_api_worker,
                kwargs={
                    "queue": queue,
                    "receptor_path": str(receptor_path),
                    "ligand_path": str(ligand_path),
                    "run_name": run_name,
                    "output_dir": str(output_dir),
                    "threads_hint": max(1, int(threads_hint)),
                    "chunksize": chunksize,
                },
            )
            process.start()
            process.join(timeout=timeout_seconds)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
                result["error"] = (
                    f"ODDT timed out after {timeout_seconds}s while scoring {ligand_path.name}."
                    if timeout_seconds > 0
                    else f"ODDT timed out while scoring {ligand_path.name}."
                )
                return result

            completed = queue.get_nowait() if not queue.empty() else None
            queue.close()
            queue.join_thread()
        else:
            completed = _run_oddt_api_once(
                receptor_path=str(receptor_path),
                ligand_path=str(ligand_path),
                run_name=run_name,
                output_dir=str(output_dir),
                threads_hint=max(1, int(threads_hint)),
                chunksize=chunksize,
            )
    except Exception as exc:
        result["error"] = f"ODDT API execution failed: {type(exc).__name__}: {exc}"
        return result

    if not isinstance(completed, dict):
        result["error"] = "ODDT API returned no data."
        return result

    result["returncode"] = int(completed.get("returncode", 0) or 0)
    result["error"] = str(completed.get("error", "") or "")
    scores = completed.get("scores", {})
    if not isinstance(scores, dict):
        scores = {}

    if not bool(completed.get("success", False)):
        if not result["error"]:
            result["error"] = "ODDT API execution failed."
        return result

    if not scores:
        result["error"] = "ODDT API completed but produced no numeric scores."
        return result

    result["scores"] = scores
    result["success"] = True
    return result


def _apply_oddt_status_to_summary(summary: Dict[str, Any], oddt_status: Dict[str, Any]) -> Dict[str, Any]:
    '''
    Merge dedicated ODDT rule scores into a pipeline summary payload.

    Parameters
    ----------
    summary : Dict[str, Any]
        Existing pipeline summary payload.
    oddt_status : Dict[str, Any]
        ODDT status payload produced by the dedicated ODDT rule.

    Returns
    -------
    Dict[str, Any]
        Updated summary payload including ODDT status and scores.
    '''

    def _merge_scores(target_summary: Dict[str, Any], scores: Dict[str, Any]) -> None:
        '''
        Merge ODDT numeric scores into one summary object.

        Parameters
        ----------
        target_summary : Dict[str, Any]
            Summary payload (root or box-level) to update.
        scores : Dict[str, Any]
            ODDT score mapping to merge.

        Returns
        -------
        None
            This function mutates ``target_summary`` in place.
        '''

        if not isinstance(scores, dict) or not scores:
            return

        existing_rescoring = target_summary.get("rescoring")
        if not isinstance(existing_rescoring, dict):
            existing_rescoring = {}
        existing_rescoring["oddt"] = {
            str(key): float(value) for key, value in scores.items() if _to_numeric(value) is not None
        }
        target_summary["rescoring"] = existing_rescoring

        existing_engines = target_summary.get("rescoring_engines")
        if isinstance(existing_engines, list):
            engine_set = {str(engine).strip().lower() for engine in existing_engines if str(engine).strip()}
        else:
            engine_set = {str(engine).strip().lower() for engine in existing_rescoring.keys()}
        engine_set.add("oddt")
        target_summary["rescoring_engines"] = sorted(engine_set)

    entries = oddt_status.get("entries", {}) if isinstance(oddt_status, dict) else {}
    if not isinstance(entries, dict):
        entries = {}

    box_summaries = summary.get("box_summaries")
    if isinstance(box_summaries, dict):
        for box_name, box_entry in entries.items():
            if not isinstance(box_entry, dict):
                continue
            if not bool(box_entry.get("success", False)):
                continue
            if box_name not in box_summaries or not isinstance(box_summaries[box_name], dict):
                continue
            _merge_scores(box_summaries[box_name], box_entry.get("scores", {}))
    else:
        root_entry = entries.get("__root__")
        if isinstance(root_entry, dict) and bool(root_entry.get("success", False)):
            _merge_scores(summary, root_entry.get("scores", {}))

    summary["oddt_status"] = oddt_status
    return summary


def _preset_receptor_inputs(preset_name: str) -> List[str]:
    '''
    Collect receptor rule inputs for a preset dataset family alias.

    Parameters
    ----------
    preset_name : str
        Preset dataset family name (for example ``PDBbind`` or ``DUDEz``).

    Returns
    -------
    List[str]
        Sorted receptor input paths for matching preset aliases.
    '''

    paths: List[str] = []
    for database in preset_database_aliases.get(preset_name, []):
        for receptor in database_to_receptors.get(database, []):
            paths.append(str(_receptor_path(database, receptor)))
    return sorted(set(paths))


def _wc_receptor_path(wildcards) -> str:
    '''
    Resolve receptor input path from Snakemake wildcards.

    Parameters
    ----------
    wildcards : Any
        Snakemake wildcard object.

    Returns
    -------
    str
        Receptor input path for current wildcard tuple.
    '''

    return str(_receptor_path(wildcards.database, wildcards.receptor))


def _wc_receptor_cache_manifest_path(wildcards) -> str:
    '''
    Resolve receptor cache-manifest path from Snakemake wildcards.

    Parameters
    ----------
    wildcards : Any
        Snakemake wildcard object.

    Returns
    -------
    str
        Receptor cache manifest path for current wildcard tuple.
    '''

    return str(_receptor_cache_manifest_path(wildcards.database, wildcards.receptor))


def _wc_ligand_path(wildcards) -> str:
    '''
    Resolve ligand input path from Snakemake wildcards.

    Parameters
    ----------
    wildcards : Any
        Snakemake wildcard object.

    Returns
    -------
    str
        Ligand input path for current wildcard tuple.
    '''

    return str(_ligand_path(wildcards.database, wildcards.receptor, wildcards.kind, wildcards.target))


def _wc_box_path(wildcards) -> str:
    '''
    Resolve default box input path from Snakemake wildcards.

    Parameters
    ----------
    wildcards : Any
        Snakemake wildcard object.

    Returns
    -------
    str
        Box input path for current wildcard tuple.
    '''

    return str(_box_path(wildcards.database, wildcards.receptor, wildcards.kind, wildcards.target))


def _wc_ligand_cache_manifest_path(wildcards) -> str:
    '''
    Resolve ligand cache-manifest path from Snakemake wildcards.

    Parameters
    ----------
    wildcards : Any
        Snakemake wildcard object.

    Returns
    -------
    str
        Ligand cache manifest path for current wildcard tuple.
    '''

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

        if engine == "plants":
            # Reruns can leave stale PLANTS work directories; clear them to avoid
            # "[Errno 39] Directory not empty" failures on restart.
            plants_run_dir = engine_dir / "run"
            if plants_run_dir.exists():
                try:
                    shutil.rmtree(plants_run_dir)
                except OSError:
                    pass

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
        '''
        Execute one engine box run and return per-box result payload.

        Parameters
        ----------
        box : Path
            Box file path.
        receptor : Any
            Receptor object instance.
        ligand : Any
            Ligand object instance.

        Returns
        -------
        Tuple[str, Dict[str, Any]]
            Box identifier and structured engine result payload.
        '''

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
            '''
            Run one box using isolated receptor/ligand objects for threading.

            Parameters
            ----------
            box : Path
                Box file path.

            Returns
            -------
            Tuple[str, Dict[str, Any]]
                Box identifier and structured engine result payload.
            '''

            isolated_receptor = ocr.Receptor(
                str(receptor_path),
                name=f"{job_name}_receptor",
                allow_missing_surface=True,
            )
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
        ``0`` on success, non-zero when strict clustering prerequisites fail.
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
    engine_pose_counts: Dict[str, int] = {}

    for engine in pipeline_engines:
        box_result = engine_box_results.get(engine, {})
        if not isinstance(box_result, dict):
            engine_errors[engine] = "missing engine status payload"
            continue
        if not box_result.get("success", False):
            engine_errors[engine] = str(box_result.get("error", "engine reported unsuccessful status"))
            continue

        poses = [str(p) for p in box_result.get("poses", []) if _is_valid_file(p)]
        if not poses:
            engine_errors[engine] = "no valid poses"
            continue

        engine_pose_counts[engine] = len(poses)
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
            print(f"Error: {engine} failed for {job_name}: {message}")

    missing_pose_engines = [engine for engine in pipeline_engines if engine_pose_counts.get(engine, 0) < 1]
    if missing_pose_engines:
        box_suffix = f" ({box_label})" if box_label else ""
        print(
            f"Error: strict clustering requires at least one valid pose from every configured docking engine for "
            f"{job_name}{box_suffix}. Missing/empty engines: {', '.join(sorted(missing_pose_engines))}"
        )
        return 2

    if not all_poses:
        return 2

    mol2_dir = outdir / "poses_mol2"
    mol2_list, mol2_map = _ensure_mol2_poses(all_poses, mol2_dir, pose_engine_map)
    if not mol2_list:
        return 2

    rmsd = ocmolproc.get_rmsd_matrix(mol2_list)
    rmsd_df = pd.DataFrame(rmsd).loc[mol2_list, mol2_list]
    rmsd_df.to_csv(outdir / "rmsd_matrix.csv")

    try:
        clusters = ocrmsd.cluster_rmsd(
            rmsd_df,
            min_distance_threshold=pipeline_cluster_min,
            max_distance_threshold=pipeline_cluster_max,
            threshold_step=pipeline_cluster_step,
            outputPlot=str(outdir / "clustering_dendrogram.png"),
            molecule_name=job_name,
            pose_engine_map=pose_engine_map,
        )
    except Exception as exc:
        box_suffix = f" ({box_label})" if box_label else ""
        print(f"Error: clustering failed for {job_name}{box_suffix}: {type(exc).__name__}: {exc}")
        return 3

    clustering_info: Dict[str, Any] = {
        "method": "rmsd_based_clustering",
        "total_poses": len(mol2_list),
        "representative_selection": None,
        "cluster_sizes": None,
        "medoids": None,
    }

    if isinstance(clusters, int) or getattr(clusters, "size", 0) == 0:
        box_suffix = f" ({box_label})" if box_label else ""
        print(f"Error: clustering produced no labels for {job_name}{box_suffix}.")
        return 3

    cluster_assignments = pd.DataFrame({"pose_path": mol2_list, "cluster_id": clusters})
    cluster_assignments.to_csv(outdir / "cluster_assignments.csv", index=False)

    cluster_sizes: Dict[int, int] = {}
    unique_clusters, counts = np.unique(clusters, return_counts=True)
    for cluster_id, size in zip(unique_clusters, counts):
        cluster_sizes[int(cluster_id)] = int(size)

    try:
        medoids = ocrmsd.get_medoids(rmsd_df, clusters, onlyBiggest=True)
    except Exception as exc:
        box_suffix = f" ({box_label})" if box_label else ""
        print(f"Error: medoid selection failed for {job_name}{box_suffix}: {type(exc).__name__}: {exc}")
        return 3
    if not medoids:
        box_suffix = f" ({box_label})" if box_label else ""
        print(f"Error: clustering returned no medoids for {job_name}{box_suffix}.")
        return 3

    representative_mol2 = medoids[0]
    clustering_info["representative_selection"] = "medoid_of_largest_cluster"
    clustering_info["medoids"] = [str(medoid) for medoid in medoids]
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
            raw_scores = ocvina.read_rescore_logs(log_paths, onlyBest=False) if log_paths else {}
            for raw_key, raw_value in raw_scores.items():
                numeric = _to_numeric(raw_value)
                if numeric is None:
                    continue
                canonical = _canonicalize_rescore_key("vina", str(raw_key))
                vina_scores[canonical] = float(numeric)

            vina_scoring_set = {str(fn).strip().lower() for fn in (vina_scoring_functions or [])}
            if "vinardo" in vina_scoring_set and "vina_vinardo" not in vina_scores:
                fallback_keys = sorted(k for k in vina_scores if re.fullmatch(r"vina_\d+", k))
                if fallback_keys:
                    vina_scores["vina_vinardo"] = vina_scores[fallback_keys[0]]
            for fallback_key in [k for k in list(vina_scores) if re.fullmatch(r"vina_\d+", k)]:
                vina_scores.pop(fallback_key, None)
        except Exception:
            pass
        if vina_scores:
            rescoring["vina"] = vina_scores

    if "smina" in pipeline_rescoring_engines_set and representative_pdbqt and representative_pdbqt.exists():
        smina_ctx: Optional[Dict[str, str]] = ctx.get("smina")
        if smina_ctx is None:
            # Reuse Vina/Gnina config for rescoring-only Smina runs.
            for fallback_engine in ("vina", "gnina"):
                fallback_ctx = ctx.get(fallback_engine)
                if not fallback_ctx:
                    continue
                fallback_conf = str(fallback_ctx.get("conf", "")).strip()
                if not fallback_conf:
                    continue
                smina_rescore_dir = outdir / "sminaRescoreFiles"
                smina_rescore_dir.mkdir(parents=True, exist_ok=True)
                smina_ctx = {
                    "conf": fallback_conf,
                    "dir": str(smina_rescore_dir),
                }
                break

        smina_conf = str((smina_ctx or {}).get("conf", "")).strip()
        smina_dir = str((smina_ctx or {}).get("dir", "")).strip()
        if smina_conf and Path(smina_conf).is_file():
            if not smina_dir:
                smina_dir = str(outdir / "sminaRescoreFiles")
            Path(smina_dir).mkdir(parents=True, exist_ok=True)

            smina_scores: Dict[str, float] = {}
            smina_scoring_functions = runtime_config.smina.scoring_functions or ["vina", "vinardo", "dkoes_scoring"]
            for scoring_function in smina_scoring_functions:
                try:
                    ocsmina.run_rescore(
                        smina_conf,
                        str(representative_pdbqt),
                        smina_dir,
                        scoring_function,
                        splitLigand=False,
                        overwrite=True,
                    )
                except Exception:
                    continue
            try:
                log_paths = ocsmina.get_rescore_log_paths(smina_dir)
                raw_scores = ocsmina.read_rescore_logs(log_paths, onlyBest=False) if log_paths else {}
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
            raw_scores = ocgnina.read_rescore_logs(log_paths, onlyBest=False) if log_paths else {}
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

    # ODDT rescoring runs in a dedicated Snakemake rule (`run_oddt`) so that
    # failures/timeouts are isolated from core post-processing.

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

    receptor_obj = ocr.Receptor(
        str(receptor_path),
        name=f"{job_name}_receptor",
        allow_missing_surface=True,
    )
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
        '''
        Post-process one box using loaded engine summaries.

        Parameters
        ----------
        box : Path
            Box file path.
        receptor : Any
            Receptor object instance.
        ligand : Any
            Ligand object instance.

        Returns
        -------
        Tuple[str, int]
            Box identifier and post-processing return code.
        '''

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
            '''
            Post-process one box with isolated objects for thread safety.

            Parameters
            ----------
            box : Path
                Box file path.

            Returns
            -------
            Tuple[str, int]
                Box identifier and post-processing return code.
            '''

            isolated_receptor = ocr.Receptor(
                str(receptor_path),
                name=f"{job_name}_receptor",
                allow_missing_surface=True,
            )
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


rule prepare_target_box:
    """
    Ensure per-target default docking box exists.

    When ``boxes/box0.pdb`` is missing, infer a new box using:
    - centroid from receptor-level ``reference_ligand.pdb``/``reference_ligand.sdf``
    - box size from candidate ligand ``RadiusOfGyration``
    """
    input:
        ligand=_wc_ligand_path,
    output:
        box=os.path.join(
            database_rule_root_str,
            "{database}",
            "{receptor}",
            "compounds",
            "{kind}",
            "{target}",
            "boxes",
            "box0.pdb",
        ),
    threads: 1
    run:
        _ensure_target_box_from_reference_ligand(
            database=wildcards.database,
            receptor=wildcards.receptor,
            kind=wildcards.kind,
            target=wildcards.target,
            ligand_path=str(input.ligand),
            box_path=str(output.box),
        )


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


def _run_engine_job(*, wildcards, rule_input, rule_output, threads_count: int, engine_name: str) -> None:
    '''
    Execute one engine job and persist status/progress metadata.

    Parameters
    ----------
    wildcards : Any
        Snakemake wildcards object for the current target.
    rule_input : Any
        Snakemake input namespace for the engine rule.
    rule_output : Any
        Snakemake output namespace for the engine rule.
    threads_count : int
        Threads allocated by Snakemake for this rule.
    engine_name : str
        Engine identifier for the running rule.

    Returns
    -------
    None
        This function executes the engine and writes status files/DB events.
    '''

    threads_count = _apply_thread_limit_env(int(threads_count))

    target_dir = Path(os.path.dirname(str(rule_input.ligand)))
    target_dir.mkdir(parents=True, exist_ok=True)
    if not _cached_receptor_files_present(str(rule_input.receptor)):
        _ensure_receptor_cache_ready(str(rule_input.receptor), str(rule_input.receptor_cache))

    job_name = f"{wildcards.database}_{wildcards.receptor}_{wildcards.kind}_{wildcards.target}"
    _store_engine_progress_in_db(
        job_name=job_name,
        database=wildcards.database,
        receptor=wildcards.receptor,
        kind=wildcards.kind,
        target=wildcards.target,
        engine=engine_name,
        phase="running",
    )

    summary = _run_single_engine_via_api(
        engine=engine_name,
        receptor_path=str(rule_input.receptor),
        ligand_path=str(rule_input.ligand),
        box_path=str(rule_input.box),
        outdir_path=str(target_dir),
        job_name=job_name,
        max_workers=threads_count,
    )

    out_path = Path(str(rule_output.summary))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    _store_engine_progress_in_db(
        job_name=job_name,
        database=wildcards.database,
        receptor=wildcards.receptor,
        kind=wildcards.kind,
        target=wildcards.target,
        engine=engine_name,
        phase="completed" if bool(summary.get("success", False)) else "failed",
        summary_path=str(rule_output.summary),
        summary=summary,
    )


rule run_engine_vina:
    """Run Vina for one target entry and persist engine status."""

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
            "vina.json",
        ),
    threads: _engine_threads("vina")
    priority: _engine_priority("vina")
    resources:
        mem_mb=_engine_mem_mb("vina"),
        gpu=_engine_gpu("vina"),
        engine_slots_vina=1
    run:
        _run_engine_job(
            wildcards=wildcards,
            rule_input=input,
            rule_output=output,
            threads_count=int(threads),
            engine_name="vina",
        )


rule run_engine_gnina:
    """Run Gnina for one target entry and persist engine status."""

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
            "gnina.json",
        ),
    threads: _engine_threads("gnina")
    priority: _engine_priority("gnina")
    resources:
        mem_mb=_engine_mem_mb("gnina"),
        gpu=_engine_gpu("gnina"),
        engine_slots_gnina=1
    run:
        _run_engine_job(
            wildcards=wildcards,
            rule_input=input,
            rule_output=output,
            threads_count=int(threads),
            engine_name="gnina",
        )


rule run_engine_plants:
    """Run PLANTS for one target entry and persist engine status."""

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
            "plants.json",
        ),
    threads: _engine_threads("plants")
    priority: _engine_priority("plants")
    resources:
        mem_mb=_engine_mem_mb("plants"),
        gpu=_engine_gpu("plants"),
        engine_slots_plants=1
    run:
        _run_engine_job(
            wildcards=wildcards,
            rule_input=input,
            rule_output=output,
            threads_count=int(threads),
            engine_name="plants",
        )


# Pipeline finalization stages:
# 1) run_pipeline_core: postprocess/clustering + non-ODDT rescoring.
# 2) run_oddt: isolated ODDT API execution with timeout handling.
# 3) run_pipeline: merge results and write payload/report artifacts.
rule run_pipeline_core:
    """
    Aggregate per-engine outputs and write an intermediate core summary.

    This stage performs clustering and non-ODDT rescoring only. ODDT runs in a
    dedicated downstream rule for better isolation.
    """
    input:
        receptor=_wc_receptor_path,
        receptor_cache=_wc_receptor_cache_manifest_path,
        ligand=_wc_ligand_path,
        box=_wc_box_path,
        engine_summaries=_engine_summary_inputs,
    output:
        core_summary=os.path.join(
            database_rule_root_str,
            "{database}",
            "{receptor}",
            "compounds",
            "{kind}",
            "{target}",
            "pipeline_core_summary.json",
        ),
    threads: pipeline_postprocess_threads
    resources:
        mem_mb=pipeline_postprocess_mem_mb
    run:
        threads_count = _apply_thread_limit_env(int(threads))

        target_dir = Path(os.path.dirname(input.ligand))
        target_dir.mkdir(parents=True, exist_ok=True)

        job_name = f"{wildcards.database}_{wildcards.receptor}_{wildcards.kind}_{wildcards.target}"
        if not _cached_receptor_files_present(str(input.receptor)):
            _ensure_receptor_cache_ready(str(input.receptor), str(input.receptor_cache))
        rc = 0
        core_error = ""

        try:
            rc = _run_pipeline_postprocess_from_summaries(
                receptor_path=str(input.receptor),
                ligand_path=str(input.ligand),
                box_path=str(input.box),
                outdir_path=str(target_dir),
                job_name=job_name,
                engine_summary_paths=list(input.engine_summaries),
                max_workers=threads_count,
            )
        except Exception as exc:
            rc = 90
            core_error = f"postprocess crashed: {type(exc).__name__}: {exc}"

        summary_output_path: Optional[Path] = None
        per_box_summary_paths: List[Path] = []

        if rc == 0:
            try:
                summary, summary_output_path, per_box_summary_paths = _collect_pipeline_summary(
                    target_dir=target_dir,
                    job_name=job_name,
                )
            except Exception as exc:
                rc = 91
                core_error = f"summary collection failed: {type(exc).__name__}: {exc}"

        if rc != 0:
            if not core_error:
                core_error = f"pipeline core returned non-zero status ({rc})"
            print(
                f"Warning: run_pipeline_core marked as failed for "
                f"{wildcards.database}/{wildcards.receptor}/{wildcards.kind}/{wildcards.target}: {core_error}"
            )
            summary = {
                "job": job_name,
                "pipeline_version": pipeline_version,
                "status": "failed",
                "success": False,
                "error": core_error,
                "return_code": rc,
                "rescoring": {},
                "rescoring_engines": [engine for engine in pipeline_rescoring_engines if engine != "oddt"],
            }

        core_payload = {
            "job_name": job_name,
            "success": (rc == 0),
            "return_code": rc,
            "error": core_error,
            "summary": summary,
            "summary_output_path": str(summary_output_path) if summary_output_path else "",
            "per_box_summary_paths": [str(path) for path in per_box_summary_paths],
        }
        _write_json(output.core_summary, core_payload)


rule run_oddt:
    """
    Run ODDT rescoring through OCDocker API and persist status for final aggregation.
    """
    input:
        receptor=_wc_receptor_path,
        ligand=_wc_ligand_path,
        core_summary=_wc_core_summary_path,
    output:
        oddt_status=os.path.join(
            database_rule_root_str,
            "{database}",
            "{receptor}",
            "compounds",
            "{kind}",
            "{target}",
            "oddt_status.json",
        ),
    threads: pipeline_oddt_threads
    resources:
        mem_mb=pipeline_oddt_mem_mb
    run:
        threads_count = _apply_thread_limit_env(int(threads))

        target_dir = Path(os.path.dirname(input.ligand))
        target_dir.mkdir(parents=True, exist_ok=True)
        job_name = f"{wildcards.database}_{wildcards.receptor}_{wildcards.kind}_{wildcards.target}"

        # `run_pipeline_core` already produced the representative pose(s) and
        # non-ODDT rescoring; this rule only adds ODDT outcomes.
        with Path(str(input.core_summary)).open("r", encoding="utf-8") as handle:
            core_payload = json.load(handle)
        summary = core_payload.get("summary", {})
        if not isinstance(summary, dict):
            summary = {}

        oddt_status: Dict[str, Any] = {
            "enabled": "oddt" in pipeline_rescoring_engines_set,
            "success": False,
            "phase": "pending",
            "timeout_seconds": pipeline_oddt_timeout,
            "entries": {},
        }

        if "oddt" not in pipeline_rescoring_engines_set:
            oddt_status["phase"] = "skipped"
            oddt_status["reason"] = "oddt not enabled in pipeline_rescoring_engines"
            _write_json(output.oddt_status, oddt_status)
            return

        prepared_receptor = _find_prepared_receptor_for_oddt(str(input.receptor))
        if prepared_receptor is None:
            oddt_status["phase"] = "failed"
            oddt_status["reason"] = (
                "No prepared receptor file found for ODDT. Checked prepared_receptor.pdbqt, "
                "prepared_receptor.mol2, and receptor input path."
            )
            _write_json(output.oddt_status, oddt_status)
            return

        box_summaries = summary.get("box_summaries")
        if isinstance(box_summaries, dict):
            # Multi-box mode: score each box representative independently.
            for box_name, box_data in sorted(box_summaries.items()):
                if not isinstance(box_data, dict):
                    oddt_status["entries"][box_name] = {
                        "success": False,
                        "scores": {},
                        "error": "invalid box summary payload",
                    }
                    continue

                representative_pose = Path(str(box_data.get("representative_pose", "")))
                if not _is_valid_file(representative_pose):
                    oddt_status["entries"][box_name] = {
                        "success": False,
                        "scores": {},
                        "error": f"representative pose missing: {representative_pose}",
                    }
                    continue

                oddt_status["entries"][box_name] = _run_oddt_api_for_pose(
                    receptor_path=prepared_receptor,
                    ligand_path=representative_pose,
                    output_dir=(target_dir / box_name / "oddt_rescoring"),
                    run_name=f"{job_name}_{box_name}",
                    timeout_seconds=pipeline_oddt_timeout,
                    threads_hint=threads_count,
                )
        else:
            # Single-box/default mode: score one representative pose.
            representative_pose = Path(str(summary.get("representative_pose", "")))
            if _is_valid_file(representative_pose):
                oddt_status["entries"]["__root__"] = _run_oddt_api_for_pose(
                    receptor_path=prepared_receptor,
                    ligand_path=representative_pose,
                    output_dir=(target_dir / "oddt_rescoring"),
                    run_name=job_name,
                    timeout_seconds=pipeline_oddt_timeout,
                    threads_hint=threads_count,
                )
            else:
                oddt_status["entries"]["__root__"] = {
                    "success": False,
                    "scores": {},
                    "error": f"representative pose missing: {representative_pose}",
                }

        oddt_status["success"] = any(
            bool(entry.get("success", False))
            for entry in oddt_status["entries"].values()
            if isinstance(entry, dict)
        )
        oddt_status["phase"] = "completed" if oddt_status["success"] else "failed"
        _write_json(output.oddt_status, oddt_status)


rule run_pipeline:
    """
    Finalize payload/report from core summary plus dedicated ODDT status.
    """
    input:
        receptor=_wc_receptor_path,
        receptor_cache=_wc_receptor_cache_manifest_path,
        ligand=_wc_ligand_path,
        box=_wc_box_path,
        engine_summaries=_engine_summary_inputs,
        core_summary=_wc_core_summary_path,
        oddt_status=_wc_oddt_status_path,
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
    threads: 1
    resources:
        mem_mb=pipeline_postprocess_mem_mb
    run:
        target_dir = Path(os.path.dirname(input.ligand))
        target_dir.mkdir(parents=True, exist_ok=True)

        job_name = f"{wildcards.database}_{wildcards.receptor}_{wildcards.kind}_{wildcards.target}"
        if not _cached_receptor_files_present(str(input.receptor)):
            _ensure_receptor_cache_ready(str(input.receptor), str(input.receptor_cache))

        with Path(str(input.core_summary)).open("r", encoding="utf-8") as handle:
            core_payload = json.load(handle)
        summary = core_payload.get("summary", {})
        if not isinstance(summary, dict):
            raise RuntimeError(f"Invalid core summary payload at {input.core_summary}")

        summary_output_path_raw = str(core_payload.get("summary_output_path", "") or "").strip()
        summary_output_path: Optional[Path] = Path(summary_output_path_raw) if summary_output_path_raw else None
        per_box_summary_paths = [Path(str(path)) for path in core_payload.get("per_box_summary_paths", []) if str(path).strip()]

        oddt_status: Dict[str, Any] = {}
        oddt_status_path = Path(str(input.oddt_status))
        if oddt_status_path.is_file():
            with oddt_status_path.open("r", encoding="utf-8") as handle:
                loaded_status = json.load(handle)
            if isinstance(loaded_status, dict):
                oddt_status = loaded_status
        # Merge ODDT scores/status into the summary generated in core stage.
        summary = _apply_oddt_status_to_summary(summary, oddt_status)

        if summary_output_path is not None:
            _write_json(summary_output_path, summary)

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


rule export_database_csv:
    """
    Export one consolidated CSV for each selected database alias.
    """
    input:
        payloads=lambda wildcards: _payload_targets_for_database(wildcards.database),
    output:
        csv=os.path.join(
            database_rule_root_str,
            "{database}",
            "pipeline_results.csv",
        ),
    threads: 1
    run:
        _write_database_results_csv(
            database=wildcards.database,
            payload_paths=list(input.payloads),
            csv_path=str(output.csv),
        )


rule all:
    """
    Execute OCDocker pipeline over selected databases and kinds.

    Example usage:
        snakemake -s snakefile --cores 20 --use-conda --conda-frontend mamba --keep-going
    """
    default_target: True
    input:
        allkinds=all_payload_targets,
        database_csvs=_collect_database_csv_targets(),
    run:
        print(
            f"All done! Processed {len(input.allkinds)} entries. "
            f"Database CSV exports: {len(input.database_csvs)}"
        )
