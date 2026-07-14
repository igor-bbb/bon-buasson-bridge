"""Durable JSON state utilities for long-lived Runtime professional work.

Provides process-independent persistence primitives for Professional Activity,
Research Execution and Workspace state. Files are written atomically, guarded
by an OS file lock, backed up before replacement and recoverable from backup
when the primary JSON file is incomplete or corrupted.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, Optional, Tuple

try:  # Linux / Render
    import fcntl  # type: ignore
except ImportError:  # pragma: no cover
    fcntl = None


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _checksum(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


@contextmanager
def _file_lock(path: Path) -> Iterator[None]:
    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        if fcntl is not None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _decode(path: Path, expected_type: type) -> Tuple[Optional[Any], Optional[str]]:
    try:
        if not path.exists():
            return None, "not_found"
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, expected_type):
            return None, f"invalid_root_type:{type(value).__name__}"
        return value, None
    except Exception as exc:
        return None, f"{type(exc).__name__}:{exc}"


def read_json_state(path: Path, default_factory: Callable[[], Any], expected_type: type) -> Tuple[Any, Dict[str, Any]]:
    """Read state and recover from backup without hiding repository failures."""
    with _file_lock(path):
        primary, primary_error = _decode(path, expected_type)
        if primary is not None:
            return primary, {
                "status": "PASS",
                "source": "primary",
                "path": str(path),
                "recovered": False,
                "read_at": _now(),
            }
        backup_path = path.with_suffix(path.suffix + ".bak")
        backup, backup_error = _decode(backup_path, expected_type)
        if backup is not None:
            # Restore the primary file atomically so the next request uses the
            # normal path and does not depend on in-memory transport state.
            _write_json_state_locked(path, backup, create_backup=False)
            return backup, {
                "status": "RECOVERED",
                "source": "backup",
                "path": str(path),
                "recovered": True,
                "primary_error": primary_error,
                "read_at": _now(),
            }
        if primary_error == "not_found" and backup_error == "not_found":
            return default_factory(), {
                "status": "EMPTY",
                "source": "default",
                "path": str(path),
                "recovered": False,
                "read_at": _now(),
            }
        return default_factory(), {
            "status": "HOLD",
            "source": "unavailable",
            "path": str(path),
            "recovered": False,
            "primary_error": primary_error,
            "backup_error": backup_error,
            "read_at": _now(),
        }


def _write_json_state_locked(path: Path, value: Any, create_backup: bool = True) -> Dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n").encode("utf-8")
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    backup_path = path.with_suffix(path.suffix + ".bak")
    try:
        if create_backup and path.exists():
            shutil.copy2(path, backup_path)
        with temporary.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        # Best effort directory fsync protects rename durability on Linux.
        try:
            directory_fd = os.open(str(path.parent), os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except Exception:
            pass
        return {
            "status": "PASS",
            "path": str(path),
            "bytes": len(payload),
            "sha256": _checksum(payload),
            "written_at": _now(),
        }
    finally:
        if temporary.exists():
            temporary.unlink(missing_ok=True)


def write_json_state(path: Path, value: Any) -> Dict[str, Any]:
    with _file_lock(path):
        return _write_json_state_locked(path, value, create_backup=True)


def inspect_json_state(path: Path, expected_type: type) -> Dict[str, Any]:
    value, diagnostic = read_json_state(path, lambda: expected_type(), expected_type)
    count = len(value) if isinstance(value, (list, dict)) else None
    return {
        **diagnostic,
        "exists": path.exists(),
        "backup_exists": path.with_suffix(path.suffix + ".bak").exists(),
        "item_count": count,
        "transport_session_independent": True,
    }
