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


def update_json_state(path: Path, default_factory: Callable[[], Any], expected_type: type, updater: Callable[[Any], Any]) -> Tuple[Any, Dict[str, Any]]:
    """Atomically read, update, persist and read back a JSON state object.

    The entire mutation is protected by one file lock.  A successful return
    therefore means the committed state was read back from the same canonical
    repository path, rather than merely accepted in process memory.
    """
    with _file_lock(path):
        current, primary_error = _decode(path, expected_type)
        source = "primary"
        recovered = False
        if current is None:
            backup_path = path.with_suffix(path.suffix + ".bak")
            current, backup_error = _decode(backup_path, expected_type)
            if current is not None:
                source = "backup"
                recovered = True
            elif primary_error == "not_found" and backup_error == "not_found":
                current = default_factory()
                source = "default"
            else:
                raise RuntimeError(
                    f"Persistent repository unavailable: primary={primary_error}; backup={backup_error}"
                )
        updated = updater(current)
        if not isinstance(updated, expected_type):
            raise TypeError(f"updater returned {type(updated).__name__}, expected {expected_type.__name__}")
        write_diagnostic = _write_json_state_locked(path, updated, create_backup=True)
        readback, readback_error = _decode(path, expected_type)
        if readback is None:
            raise RuntimeError(f"Repository readback failed after commit: {readback_error}")
        return readback, {
            "status": "PASS",
            "source_before_commit": source,
            "recovered_before_commit": recovered,
            "path": str(path),
            "write": write_diagnostic,
            "readback_verified": True,
            "committed_at": _now(),
        }


# ---------------------------------------------------------------------------
# Unified VECTRA Runtime State foundation
# ---------------------------------------------------------------------------

UNIFIED_RUNTIME_STATE_CONTRACT_VERSION = "1.0"
UNIFIED_RUNTIME_STATE_FILE = Path("runtime") / "vectra_runtime_state.json"

RUNTIME_STATE_ROOTS = (
    "personality",
    "self_model",
    "organization",
    "professional_memory",
    "professional_behaviour",
    "business_context",
    "capabilities",
    "current_activity",
    "governance",
)


def build_default_unified_runtime_state() -> Dict[str, Any]:
    """Return the canonical empty root state for VECTRA.

    WP-001 introduces only the durable state skeleton. Existing subsystem
    repositories remain authoritative until later migration packages connect
    them to these roots. This keeps current Runtime behaviour backward
    compatible while creating one stable integration point.
    """
    state: Dict[str, Any] = {
        "contract_version": UNIFIED_RUNTIME_STATE_CONTRACT_VERSION,
        "state_type": "VECTRA_UNIFIED_RUNTIME_STATE",
        "status": "FOUNDATION_READY",
        "created_at": _now(),
        "updated_at": _now(),
        "migration_mode": "BACKWARD_COMPATIBLE_FOUNDATION",
    }
    for root_name in RUNTIME_STATE_ROOTS:
        state[root_name] = {
            "object_name": root_name,
            "status": "NOT_CONNECTED",
            "contract_version": UNIFIED_RUNTIME_STATE_CONTRACT_VERSION,
            "source_of_truth": "existing_runtime_subsystem",
            "payload": {},
        }
    return state


def _normalize_unified_runtime_state(value: Dict[str, Any]) -> Dict[str, Any]:
    """Make older or partially written state conform to the root contract."""
    state = dict(value or {})
    state.setdefault("contract_version", UNIFIED_RUNTIME_STATE_CONTRACT_VERSION)
    state.setdefault("state_type", "VECTRA_UNIFIED_RUNTIME_STATE")
    state.setdefault("status", "FOUNDATION_READY")
    state.setdefault("created_at", _now())
    state["updated_at"] = _now()
    state.setdefault("migration_mode", "BACKWARD_COMPATIBLE_FOUNDATION")
    for root_name in RUNTIME_STATE_ROOTS:
        root = state.get(root_name)
        if not isinstance(root, dict):
            root = {}
        root.setdefault("object_name", root_name)
        root.setdefault("status", "NOT_CONNECTED")
        root.setdefault("contract_version", UNIFIED_RUNTIME_STATE_CONTRACT_VERSION)
        root.setdefault("source_of_truth", "existing_runtime_subsystem")
        root.setdefault("payload", {})
        state[root_name] = root
    return state


def read_unified_runtime_state() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Read the canonical VECTRA root state without changing current subsystems."""
    state, diagnostic = read_json_state(
        UNIFIED_RUNTIME_STATE_FILE,
        build_default_unified_runtime_state,
        dict,
    )
    normalized = _normalize_unified_runtime_state(state)
    return normalized, {
        **diagnostic,
        "contract_version": UNIFIED_RUNTIME_STATE_CONTRACT_VERSION,
        "root_objects": list(RUNTIME_STATE_ROOTS),
        "backward_compatible": True,
    }


def initialize_unified_runtime_state() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Persist and verify the canonical state skeleton if it does not exist."""
    def updater(current: Dict[str, Any]) -> Dict[str, Any]:
        return _normalize_unified_runtime_state(current)

    state, diagnostic = update_json_state(
        UNIFIED_RUNTIME_STATE_FILE,
        build_default_unified_runtime_state,
        dict,
        updater,
    )
    return state, {
        **diagnostic,
        "contract_version": UNIFIED_RUNTIME_STATE_CONTRACT_VERSION,
        "root_objects": list(RUNTIME_STATE_ROOTS),
        "backward_compatible": True,
    }


def update_unified_runtime_root(
    root_name: str,
    payload: Dict[str, Any],
    *,
    status: str = "CONNECTED",
    source_of_truth: Optional[str] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Atomically update one root while preserving all other Runtime roots."""
    if root_name not in RUNTIME_STATE_ROOTS:
        raise ValueError(f"Unsupported unified runtime root: {root_name}")
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dict")

    def updater(current: Dict[str, Any]) -> Dict[str, Any]:
        state = _normalize_unified_runtime_state(current)
        root = dict(state[root_name])
        root.update({
            "object_name": root_name,
            "status": str(status or "CONNECTED"),
            "contract_version": UNIFIED_RUNTIME_STATE_CONTRACT_VERSION,
            "payload": payload,
            "updated_at": _now(),
        })
        if source_of_truth:
            root["source_of_truth"] = source_of_truth
        state[root_name] = root
        state["updated_at"] = _now()
        return state

    state, diagnostic = update_json_state(
        UNIFIED_RUNTIME_STATE_FILE,
        build_default_unified_runtime_state,
        dict,
        updater,
    )
    return state, {
        **diagnostic,
        "updated_root": root_name,
        "contract_version": UNIFIED_RUNTIME_STATE_CONTRACT_VERSION,
        "backward_compatible": True,
    }


def inspect_unified_runtime_state() -> Dict[str, Any]:
    state, diagnostic = read_unified_runtime_state()
    roots = {
        root_name: {
            "status": state[root_name].get("status"),
            "source_of_truth": state[root_name].get("source_of_truth"),
            "has_payload": bool(state[root_name].get("payload")),
        }
        for root_name in RUNTIME_STATE_ROOTS
    }
    return {
        **diagnostic,
        "status": diagnostic.get("status", "PASS"),
        "state_type": state.get("state_type"),
        "state_status": state.get("status"),
        "roots": roots,
        "transport_session_independent": True,
    }
