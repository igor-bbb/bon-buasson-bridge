"""Database-backed persistence for VECTRA Runtime Repository JSON documents.

RUNTIME-REPOSITORY-DATABASE-PERSISTENCE-001 keeps the existing repository
paths as a compatibility view while PostgreSQL becomes the durable source of
truth.  SQLite is supported only for deterministic local and CI verification.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterator, Optional, Tuple


RELEASE_ID = "RUNTIME-REPOSITORY-DATABASE-PERSISTENCE-001"
STARTUP_HOTFIX_RELEASE_ID = "RUNTIME-REPOSITORY-DATABASE-STARTUP-HOTFIX-001"
SCHEMA_VERSION = "1"
TABLE_NAME = "vectra_repository_documents"
BACKEND_ENV = "VECTRA_PERSISTENCE_BACKEND"
DATABASE_URL_ENV = "VECTRA_DATABASE_URL"
CONNECT_TIMEOUT_ENV = "VECTRA_DATABASE_CONNECT_TIMEOUT_SECONDS"
STATEMENT_TIMEOUT_ENV = "VECTRA_DATABASE_STATEMENT_TIMEOUT_SECONDS"

_SYNC_LOCK = Lock()
_INIT_LOCK = Lock()
_SYNCHRONIZED: set[Tuple[str, str, str]] = set()
_SYNCHRONIZATION_RESULTS: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
_INITIALIZED: set[Tuple[str, str]] = set()


class RepositoryPersistenceError(RuntimeError):
    """Raised when the configured durable repository cannot be used safely."""


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _database_url() -> str:
    return str(os.getenv(DATABASE_URL_ENV) or os.getenv("DATABASE_URL") or "").strip()


def configured_repository_root() -> Path:
    return Path(
        os.getenv("VECTRA_ASSISTANT_REPOSITORY_PATH", "assistant_repository")
    ).resolve()


def configured_project_root() -> Path:
    return Path(os.getenv("VECTRA_PROJECT_ROOT", str(Path.cwd()))).resolve()


def persistence_backend() -> str:
    explicit = str(os.getenv(BACKEND_ENV) or "").strip().lower()
    if explicit:
        if explicit not in {"file", "database"}:
            raise RepositoryPersistenceError(
                f"{BACKEND_ENV} must be 'file' or 'database', got {explicit!r}"
            )
        return explicit
    return "database" if _database_url() else "file"


def database_persistence_enabled() -> bool:
    return persistence_backend() == "database"


def _dialect(url: str) -> str:
    if url.startswith("sqlite:///"):
        return "sqlite"
    if url.startswith(("postgresql://", "postgres://")):
        return "postgresql"
    raise RepositoryPersistenceError(
        "Unsupported database URL. Use PostgreSQL in Runtime or sqlite:/// for tests."
    )


def _configured_database() -> Tuple[str, str]:
    url = _database_url()
    if not url:
        raise RepositoryPersistenceError(
            f"{DATABASE_URL_ENV} or DATABASE_URL is required when {BACKEND_ENV}=database"
        )
    return _dialect(url), url


def _timeout_seconds(environment_name: str, default: int, maximum: int) -> int:
    raw = str(os.getenv(environment_name) or default).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RepositoryPersistenceError(
            f"{environment_name} must be an integer number of seconds"
        ) from exc
    if value < 1 or value > maximum:
        raise RepositoryPersistenceError(
            f"{environment_name} must be between 1 and {maximum} seconds"
        )
    return value


def database_connect_timeout_seconds() -> int:
    return _timeout_seconds(CONNECT_TIMEOUT_ENV, default=5, maximum=30)


def database_statement_timeout_seconds() -> int:
    return _timeout_seconds(STATEMENT_TIMEOUT_ENV, default=15, maximum=120)


@contextmanager
def _connection() -> Iterator[Tuple[str, Any]]:
    dialect, url = _configured_database()
    connect_timeout = database_connect_timeout_seconds()
    connection = None
    try:
        if dialect == "sqlite":
            database_path = url.removeprefix("sqlite:///")
            if not database_path:
                raise RepositoryPersistenceError("SQLite database path is empty")
            path = Path(database_path).resolve()
            path.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(path, timeout=connect_timeout)
        else:
            try:
                import psycopg
            except ImportError as exc:  # pragma: no cover - exercised in production image
                raise RepositoryPersistenceError(
                    "PostgreSQL persistence requires the psycopg package"
                ) from exc
            normalized_url = "postgresql://" + url[len("postgres://") :] if url.startswith("postgres://") else url
            connection = psycopg.connect(
                normalized_url,
                connect_timeout=connect_timeout,
            )
            connection.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (f"{database_statement_timeout_seconds()}s",),
            )
        yield dialect, connection
        connection.commit()
    except Exception as exc:
        if connection is not None:
            connection.rollback()
        if isinstance(exc, RepositoryPersistenceError):
            raise
        raise RepositoryPersistenceError(
            f"Durable Runtime Repository operation failed: {type(exc).__name__}"
        ) from exc
    finally:
        if connection is not None:
            connection.close()


def _execute(connection: Any, dialect: str, query: str, parameters: tuple = ()) -> Any:
    if dialect == "sqlite":
        query = query.replace("%s", "?")
    return connection.execute(query, parameters)


def _initialize_database_on_connection(dialect: str, connection: Any) -> None:
    timestamp_type = "TEXT" if dialect == "sqlite" else "TIMESTAMPTZ"
    _execute(
        connection,
        dialect,
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            document_path TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            checksum TEXT NOT NULL,
            version BIGINT NOT NULL DEFAULT 1,
            schema_version TEXT NOT NULL,
            created_at {timestamp_type} NOT NULL,
            updated_at {timestamp_type} NOT NULL
        )
        """,
    )


def initialize_database() -> None:
    dialect, url = _configured_database()
    key = (dialect, url)
    if key in _INITIALIZED:
        return
    with _INIT_LOCK:
        if key in _INITIALIZED:
            return
        with _connection() as (active_dialect, connection):
            _initialize_database_on_connection(active_dialect, connection)
        _INITIALIZED.add(key)


def _encode(payload: Any) -> Tuple[str, str]:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _relative_document_path(path: Path, repository_root: Path) -> Optional[str]:
    try:
        return path.resolve().relative_to(repository_root.resolve()).as_posix()
    except ValueError:
        try:
            relative = path.resolve().relative_to(configured_project_root())
        except ValueError:
            return None
        if relative.parts and relative.parts[0] == "runtime":
            return f"__project_root__/{relative.as_posix()}"
        return None


def _local_path_for_document(document_path: str, repository_root: Path) -> Path:
    prefix = "__project_root__/"
    if document_path.startswith(prefix):
        return configured_project_root() / document_path[len(prefix) :]
    return repository_root / document_path


def _write_local_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
        handle.write("\n")
    temporary.replace(path)


def _write_local_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(str(content), encoding="utf-8")
    temporary.replace(path)


def _read_local_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return deepcopy(default)
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return deepcopy(default)


def _upsert_on_connection(
    connection: Any,
    dialect: str,
    document_path: str,
    payload: Any,
) -> Dict[str, Any]:
    encoded, checksum = _encode(payload)
    now = _now()
    _execute(
        connection,
        dialect,
        f"""
        INSERT INTO {TABLE_NAME}
            (document_path, payload, checksum, version, schema_version, created_at, updated_at)
        VALUES (%s, %s, %s, 1, %s, %s, %s)
        ON CONFLICT(document_path) DO UPDATE SET
            payload = excluded.payload,
            checksum = excluded.checksum,
            version = {TABLE_NAME}.version + 1,
            schema_version = excluded.schema_version,
            updated_at = excluded.updated_at
        """,
        (document_path, encoded, checksum, SCHEMA_VERSION, now, now),
    )
    row = _execute(
        connection,
        dialect,
        f"SELECT version FROM {TABLE_NAME} WHERE document_path = %s",
        (document_path,),
    ).fetchone()
    return {
        "status": "PASS",
        "document_path": document_path,
        "checksum": checksum,
        "version": int(row[0]) if row else None,
        "backend": "database",
    }


def _upsert_document(document_path: str, payload: Any) -> Dict[str, Any]:
    with _connection() as (dialect, connection):
        return _upsert_on_connection(connection, dialect, document_path, payload)


def _upsert_documents(documents: Dict[str, Any]) -> None:
    if not documents:
        return
    with _connection() as (dialect, connection):
        for document_path, payload in documents.items():
            _upsert_on_connection(connection, dialect, document_path, payload)


def _get_document(document_path: str) -> Tuple[bool, Any]:
    with _connection() as (dialect, connection):
        row = _execute(
            connection,
            dialect,
            f"SELECT payload FROM {TABLE_NAME} WHERE document_path = %s",
            (document_path,),
        ).fetchone()
    if not row:
        return False, None
    try:
        return True, json.loads(row[0])
    except Exception as exc:
        raise RepositoryPersistenceError(
            f"Database document is not valid JSON: {document_path}"
        ) from exc


def _list_documents() -> Dict[str, Any]:
    with _connection() as (dialect, connection):
        rows = _execute(
            connection,
            dialect,
            f"SELECT document_path, payload FROM {TABLE_NAME} ORDER BY document_path",
        ).fetchall()
    documents: Dict[str, Any] = {}
    for document_path, encoded in rows:
        try:
            documents[str(document_path)] = json.loads(encoded)
        except Exception as exc:
            raise RepositoryPersistenceError(
                f"Database document is not valid JSON: {document_path}"
            ) from exc
    return documents


def read_json_document(path: Path, default: Any, repository_root: Path) -> Any:
    relative = _relative_document_path(path, repository_root)
    if not database_persistence_enabled() or relative is None or path.suffix.lower() != ".json":
        return _read_local_json(path, default)
    initialize_database()
    found, payload = _get_document(relative)
    if found:
        _write_local_json(path, payload)
        return deepcopy(payload)
    local = _read_local_json(path, default)
    if path.exists():
        _upsert_document(relative, local)
    return local


def write_json_document(path: Path, payload: Any, repository_root: Path) -> Dict[str, Any]:
    relative = _relative_document_path(path, repository_root)
    persistence = {"status": "PASS", "backend": "file", "document_path": str(path)}
    if database_persistence_enabled() and relative is not None and path.suffix.lower() == ".json":
        initialize_database()
        persistence = _upsert_document(relative, payload)
    _write_local_json(path, payload)
    return persistence


def read_repository_json(path: Path, default: Any) -> Any:
    return read_json_document(path, default, configured_repository_root())


def write_repository_json(path: Path, payload: Any) -> Dict[str, Any]:
    return write_json_document(path, payload, configured_repository_root())


def read_repository_text(path: Path, default: str = "") -> str:
    root = configured_repository_root()
    relative = _relative_document_path(path, root)
    if not database_persistence_enabled() or relative is None:
        try:
            return path.read_text(encoding="utf-8") if path.exists() else default
        except Exception:
            return default
    initialize_database()
    found, payload = _get_document(relative)
    if found:
        content = str(payload)
        _write_local_text(path, content)
        return content
    content = path.read_text(encoding="utf-8") if path.exists() else default
    if path.exists():
        _upsert_document(relative, content)
    return content


def write_repository_text(path: Path, content: str) -> Dict[str, Any]:
    root = configured_repository_root()
    relative = _relative_document_path(path, root)
    persistence = {"status": "PASS", "backend": "file", "document_path": str(path)}
    if database_persistence_enabled() and relative is not None:
        initialize_database()
        persistence = _upsert_document(relative, str(content))
    _write_local_text(path, str(content))
    return persistence


def synchronize_repository(repository_root: Path) -> Dict[str, Any]:
    """Hydrate local compatibility files and import newly deployed JSON once."""
    if not database_persistence_enabled():
        return {
            "status": "PASS",
            "backend": "file",
            "durable_across_deploys": False,
            "documents_count": None,
        }
    dialect, url = _configured_database()
    key = (
        dialect,
        url,
        f"{repository_root.resolve()}|{configured_project_root()}",
    )
    with _SYNC_LOCK:
        if key in _SYNCHRONIZED:
            return deepcopy(_SYNCHRONIZATION_RESULTS[key])

        # The complete startup hydration/import deliberately uses one database
        # connection and one transaction. Repeated ensure_repository() calls
        # return the cached result above and never touch the network.
        with _INIT_LOCK:
            with _connection() as (active_dialect, connection):
                _initialize_database_on_connection(active_dialect, connection)
                rows = _execute(
                    connection,
                    active_dialect,
                    f"SELECT document_path, payload FROM {TABLE_NAME} ORDER BY document_path",
                ).fetchall()
                documents: Dict[str, Any] = {}
                for document_path, encoded in rows:
                    try:
                        documents[str(document_path)] = json.loads(encoded)
                    except Exception as exc:
                        raise RepositoryPersistenceError(
                            f"Database document is not valid JSON: {document_path}"
                        ) from exc

                for relative, payload in documents.items():
                    path = _local_path_for_document(relative, repository_root)
                    if path.suffix.lower() == ".json":
                        _write_local_json(path, payload)
                    else:
                        _write_local_text(path, str(payload))

                local_documents = []
                if repository_root.exists():
                    local_documents = sorted(
                        path
                        for path in repository_root.rglob("*")
                        if path.is_file() and path.suffix.lower() in {".json", ".md"}
                    )
                project_runtime = configured_project_root() / "runtime"
                if project_runtime.exists():
                    local_documents.extend(
                        sorted(
                            path
                            for path in project_runtime.rglob("*.json")
                            if path.is_file()
                        )
                    )
                imported_count = 0
                for path in local_documents:
                    relative = _relative_document_path(path, repository_root)
                    if relative and relative not in documents:
                        payload = (
                            _read_local_json(path, {})
                            if path.suffix.lower() == ".json"
                            else path.read_text(encoding="utf-8")
                        )
                        _upsert_on_connection(
                            connection,
                            active_dialect,
                            relative,
                            payload,
                        )
                        documents[relative] = payload
                        imported_count += 1
            _INITIALIZED.add((dialect, url))

        result = {
            "status": "PASS",
            "release": RELEASE_ID,
            "startup_hotfix_release": STARTUP_HOTFIX_RELEASE_ID,
            "backend": "database",
            "source_of_truth": "database",
            "durable_across_deploys": True,
            "database_url_configured": True,
            "database_engine": dialect,
            "schema_version": SCHEMA_VERSION,
            "documents_count": len(documents),
            "imported_documents_count": imported_count,
            "failure_reason": None,
        }
        _SYNCHRONIZATION_RESULTS[key] = deepcopy(result)
        _SYNCHRONIZED.add(key)
    return result


def get_persistence_status(repository_root: Optional[Path] = None) -> Dict[str, Any]:
    backend = persistence_backend()
    result = {
        "status": "PASS",
        "release": RELEASE_ID,
        "startup_hotfix_release": STARTUP_HOTFIX_RELEASE_ID,
        "backend": backend,
        "source_of_truth": "database" if backend == "database" else "filesystem",
        "durable_across_deploys": backend == "database",
        "database_url_configured": bool(_database_url()),
        "schema_version": SCHEMA_VERSION,
        "connect_timeout_seconds": database_connect_timeout_seconds(),
        "statement_timeout_seconds": database_statement_timeout_seconds(),
        "documents_count": None,
        "failure_reason": None,
    }
    if backend != "database":
        return result
    try:
        dialect, _ = _configured_database()
        initialize_database()
        with _connection() as (active_dialect, connection):
            row = _execute(
                connection,
                active_dialect,
                f"SELECT COUNT(*) FROM {TABLE_NAME}",
            ).fetchone()
        result["database_engine"] = dialect
        result["documents_count"] = int(row[0]) if row else 0
    except Exception as exc:
        result.update(
            status="FAIL",
            durable_across_deploys=False,
            failure_reason=str(exc),
        )
    return result


def reset_persistence_runtime_cache() -> None:
    """Test helper for simulating a new process/deployment."""
    with _SYNC_LOCK:
        _SYNCHRONIZED.clear()
        _SYNCHRONIZATION_RESULTS.clear()
    with _INIT_LOCK:
        _INITIALIZED.clear()
