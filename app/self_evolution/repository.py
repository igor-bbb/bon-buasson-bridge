"""Self Evolution Repository for Product Team Assistant.

DEV-0009A introduces a file-based repository that is independent from chat
history.  It is intentionally conservative: it stores normalized product
knowledge metadata, not raw user dialogue, and can later be replaced by a
persistent database without changing the public SEE contract.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional

REPOSITORY_ROOT = Path(os.getenv("VECTRA_PRODUCT_EVOLUTION_REPOSITORY", "/tmp/vectra_product_evolution_repository"))
REPOSITORY_FILE = REPOSITORY_ROOT / "repository.json"
JOURNAL_FILE = REPOSITORY_ROOT / "journals" / "product_evolution.json"
GRAPH_FILE = REPOSITORY_ROOT / "knowledge_graph.json"
VERSIONS_DIR = REPOSITORY_ROOT / "versions"
LOCK = Lock()

MODEL_VERSION = "SEE-0009B.1"
REPOSITORY_SCHEMA_VERSION = "1.1"

SECTION_DIRS = (
    "journals",
    "standards",
    "evolution_policy",
    "assistant_identity",
    "architecture",
    "decisions",
    "capability_map",
    "methodology",
    "research",
    "engineering",
    "versions",
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_repository() -> Dict[str, Any]:
    """Create and return the repository manifest."""
    with LOCK:
        REPOSITORY_ROOT.mkdir(parents=True, exist_ok=True)
        for dirname in SECTION_DIRS:
            (REPOSITORY_ROOT / dirname).mkdir(parents=True, exist_ok=True)

        if not REPOSITORY_FILE.exists():
            manifest = _default_manifest()
            _write_json(REPOSITORY_FILE, manifest)
        else:
            manifest = _read_json(REPOSITORY_FILE, fallback=_default_manifest())
            manifest = _upgrade_manifest(manifest)
            _write_json(REPOSITORY_FILE, manifest)

        if not JOURNAL_FILE.exists():
            _write_json(JOURNAL_FILE, {"schema_version": REPOSITORY_SCHEMA_VERSION, "entries": []})
        if not GRAPH_FILE.exists():
            _write_json(GRAPH_FILE, {"schema_version": REPOSITORY_SCHEMA_VERSION, "nodes": [], "edges": []})
        return manifest


def load_repository() -> Dict[str, Any]:
    return ensure_repository()


def load_journal() -> Dict[str, Any]:
    ensure_repository()
    return _read_json(JOURNAL_FILE, fallback={"schema_version": REPOSITORY_SCHEMA_VERSION, "entries": []})


def save_journal(payload: Dict[str, Any]) -> None:
    ensure_repository()
    _write_json(JOURNAL_FILE, payload)


def load_graph() -> Dict[str, Any]:
    ensure_repository()
    return _read_json(GRAPH_FILE, fallback={"schema_version": REPOSITORY_SCHEMA_VERSION, "nodes": [], "edges": []})


def save_graph(payload: Dict[str, Any]) -> None:
    ensure_repository()
    _write_json(GRAPH_FILE, payload)


def commit_version(snapshot: Dict[str, Any], version_id: Optional[str] = None) -> str:
    ensure_repository()
    vid = version_id or f"{MODEL_VERSION}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    version_file = VERSIONS_DIR / f"{vid}.json"
    _write_json(version_file, snapshot)
    manifest = load_repository()
    manifest["current_model_version"] = vid
    manifest["updated_at"] = now_iso()
    manifest.setdefault("versions", [])
    if vid not in manifest["versions"]:
        manifest["versions"].append(vid)
    _write_json(REPOSITORY_FILE, manifest)
    return vid


def repository_status() -> Dict[str, Any]:
    manifest = load_repository()
    journal = load_journal()
    graph = load_graph()
    return {
        "status": "ok",
        "repository_root": str(REPOSITORY_ROOT),
        "schema_version": manifest.get("schema_version"),
        "current_model_version": manifest.get("current_model_version"),
        "journal_entries": len(journal.get("entries") or []),
        "knowledge_nodes": len(graph.get("nodes") or []),
        "knowledge_edges": len(graph.get("edges") or []),
        "sections": manifest.get("sections", {}),
        "updated_at": manifest.get("updated_at"),
    }


def _default_manifest() -> Dict[str, Any]:
    return {
        "status": "active",
        "repository_name": "VECTRA Assistant Evolution Memory",
        "technical_storage_name": "VECTRA Product Evolution Repository",
        "schema_version": REPOSITORY_SCHEMA_VERSION,
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "current_model_version": MODEL_VERSION,
        "sections": {name: str(REPOSITORY_ROOT / name) for name in SECTION_DIRS},
        "knowledge_lifecycle": ["idea", "research", "confirmed", "standard", "integration", "permanent_model"],
        "self_evolution_principle": "SEE is the mechanism of continuous professional evolution of Product Team Assistant, not merely a data storage service.",
        "knowledge_classification_required": True,
        "knowledge_types": ["idea", "research_hypothesis", "local_decision", "product_decision", "architecture_principle", "methodology_change", "engineering_constraint", "assistant_behavior_change", "evolution_policy"],
        "responsibility_boundary": {
            "product_owner": "product direction and product decisions",
            "product_team_assistant": "repository, journal, standards, links, model continuity",
            "engineering_team": "platform implementation only",
        },
        "versions": [MODEL_VERSION],
    }


def _upgrade_manifest(manifest: Any) -> Dict[str, Any]:
    if not isinstance(manifest, dict):
        manifest = _default_manifest()
    manifest.setdefault("status", "active")
    manifest.setdefault("repository_name", "VECTRA Assistant Evolution Memory")
    manifest.setdefault("technical_storage_name", "VECTRA Product Evolution Repository")
    manifest.setdefault("schema_version", REPOSITORY_SCHEMA_VERSION)
    manifest.setdefault("created_at", now_iso())
    manifest["updated_at"] = now_iso()
    manifest.setdefault("current_model_version", MODEL_VERSION)
    sections = manifest.setdefault("sections", {})
    if not isinstance(sections, dict):
        sections = {}
        manifest["sections"] = sections
    for name in SECTION_DIRS:
        sections.setdefault(name, str(REPOSITORY_ROOT / name))
    manifest.setdefault("knowledge_lifecycle", ["idea", "research", "confirmed", "standard", "integration", "permanent_model"])
    manifest.setdefault("self_evolution_principle", "SEE is the mechanism of continuous professional evolution of Product Team Assistant, not merely a data storage service.")
    manifest.setdefault("knowledge_classification_required", True)
    manifest.setdefault("knowledge_types", ["idea", "research_hypothesis", "local_decision", "product_decision", "architecture_principle", "methodology_change", "engineering_constraint", "assistant_behavior_change", "evolution_policy"])
    manifest.setdefault("versions", [manifest.get("current_model_version") or MODEL_VERSION])
    return manifest


def _read_json(path: Path, fallback: Dict[str, Any]) -> Dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else fallback
    except FileNotFoundError:
        return fallback
    except Exception:
        return fallback


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
    tmp.replace(path)
