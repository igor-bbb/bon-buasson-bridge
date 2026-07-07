"""FOUNDATION-0009 read-only Repository / Deploy Package inspection.

Laboratory uses this module to inspect the deployed VECTRA project structure
without code mutation, shell execution, deploy actions, or engineering decisions.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

INSPECTION_RELEASE = "FOUNDATION-0013"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
MAX_TREE_ITEMS = 800
MAX_FILE_BYTES_FOR_DESCRIPTION = 200_000

IGNORED_DIRS = {"__pycache__", ".git", ".pytest_cache", ".mypy_cache", "venv", ".venv"}
KEY_COMPONENTS = {
    "runtime_repository": ["app/assistant_runtime/repository.py"],
    "laboratory_api": ["app/api/routes.py", "app/laboratory_processor.py"],
    "business_data_read_only": ["app/assistant_runtime/business_data.py"],
    "professional_body": ["app/assistant_runtime/vos.py", "app/assistant_runtime/execution.py", "app/assistant_runtime/reflection.py"],
    "business_domains": ["app/assistant_runtime/repository.py"],
    "data_loader": ["app/data/loader.py", "app/data/reader.py"],
    "query_runtime": ["app/query/orchestration.py", "app/workspace_runtime.py"],
    "presentation": ["app/presentation/views.py", "app/presentation/contracts.py"],
    "self_evolution": ["app/self_evolution/evolution_engine.py", "app/self_evolution/repository.py"],
    "professional_knowledge_repository": ["assistant_repository/knowledge/professional_knowledge.json"],
    "business_domain_knowledge_repository": ["assistant_repository/business_domains/bonboason/business_knowledge.json"],
}
EXPECTED_FOUNDATION_0009_ENDPOINTS = [
    "/vectra/laboratory/repository/status",
    "/vectra/laboratory/repository/manifest",
    "/vectra/laboratory/repository/tree",
    "/vectra/laboratory/repository/components",
    "/vectra/laboratory/repository/verify",
    "/vectra/knowledge/candidates",
    "/vectra/knowledge/capitalization",
    "/vectra/knowledge/capitalization/status",
    "/vectra/knowledge/capitalization/reports",
    "/vectra/knowledge/professional",
    "/vectra/knowledge/professional/overview",
    "/vectra/knowledge/professional/{knowledge_id}",
    "/vectra/knowledge/professional/{knowledge_id}/readback",
    "/vectra/domain/{domain}/knowledge",
    "/vectra/knowledge/verify",
]


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT)).replace(os.sep, "/")
    except Exception:
        return str(path)


def _iter_files() -> List[Path]:
    files: List[Path] = []
    for path in PROJECT_ROOT.rglob("*"):
        if any(part in IGNORED_DIRS for part in path.parts):
            continue
        if path.is_file():
            files.append(path)
    return sorted(files, key=lambda p: _rel(p))


def _sha256(path: Path) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def _file_description(path: Path) -> Dict[str, Any]:
    rel = _rel(path)
    suffix = path.suffix.lower()
    stat = path.stat()
    description = "Project file"
    exports: List[str] = []
    if suffix == ".py" and stat.st_size <= MAX_FILE_BYTES_FOR_DESCRIPTION:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
            for line in text.splitlines():
                stripped = line.strip()
                if stripped.startswith("def ") or stripped.startswith("class "):
                    exports.append(stripped.split(":", 1)[0])
                if len(exports) >= 20:
                    break
            doc = text.strip().splitlines()[:4]
            if doc:
                description = " ".join(x.strip().strip('"') for x in doc if x.strip())[:300]
        except Exception:
            pass
    elif suffix in {".md", ".txt"}:
        description = "Documentation or text artifact"
    elif suffix in {".json"}:
        description = "JSON runtime/configuration artifact"
    return {
        "path": rel,
        "size_bytes": stat.st_size,
        "sha256": _sha256(path),
        "description": description,
        "exports_sample": exports,
    }


def get_repository_inspection_status() -> Dict[str, Any]:
    files = _iter_files()
    routes_text = (PROJECT_ROOT / "app" / "api" / "routes.py").read_text(encoding="utf-8", errors="replace") if (PROJECT_ROOT / "app" / "api" / "routes.py").exists() else ""
    endpoint_presence = {endpoint: endpoint.replace("{domain}", "") in routes_text or endpoint in routes_text for endpoint in EXPECTED_FOUNDATION_0009_ENDPOINTS}
    knowledge_path = PROJECT_ROOT / "assistant_repository" / "knowledge" / "professional_knowledge.json"
    business_knowledge_path = PROJECT_ROOT / "assistant_repository" / "business_domains" / "bonboason" / "business_knowledge.json"
    knowledge_documents = []
    business_knowledge_documents = []
    if knowledge_path.exists():
        try:
            loaded = json.loads(knowledge_path.read_text(encoding="utf-8"))
            knowledge_documents = loaded if isinstance(loaded, list) else []
        except Exception:
            knowledge_documents = []
    if business_knowledge_path.exists():
        try:
            loaded = json.loads(business_knowledge_path.read_text(encoding="utf-8"))
            business_knowledge_documents = loaded if isinstance(loaded, list) else []
        except Exception:
            business_knowledge_documents = []
    return {
        "status": "ok",
        "render_mode": "vectra_laboratory_repository_status",
        "release": INSPECTION_RELEASE,
        "read_only": True,
        "project_root": str(PROJECT_ROOT),
        "files_count": len(files),
        "directories_count": len({p.parent for p in files}),
        "inspection_capabilities": ["manifest", "tree", "components", "verify", "key_file_descriptions"],
        "mutation_allowed": False,
        "deploy_allowed": False,
        "expected_foundation_0009_endpoint_presence": endpoint_presence,
        "professional_knowledge_repository": {
            "path": "assistant_repository/knowledge/professional_knowledge.json",
            "exists": knowledge_path.exists(),
            "documents_count": len(knowledge_documents),
            "readable": knowledge_path.exists() and isinstance(knowledge_documents, list),
        },
        "business_domain_knowledge_repository": {
            "path": "assistant_repository/business_domains/bonboason/business_knowledge.json",
            "exists": business_knowledge_path.exists(),
            "documents_count": len(business_knowledge_documents),
            "readable": business_knowledge_path.exists() and isinstance(business_knowledge_documents, list),
        },
        "updated_at": _now(),
    }


def get_repository_manifest() -> Dict[str, Any]:
    files = _iter_files()
    by_extension: Dict[str, int] = {}
    total_bytes = 0
    file_records: List[Dict[str, Any]] = []
    for path in files:
        ext = path.suffix.lower() or "[no_extension]"
        by_extension[ext] = by_extension.get(ext, 0) + 1
        total_bytes += path.stat().st_size
        file_records.append({"path": _rel(path), "size_bytes": path.stat().st_size, "sha256": _sha256(path)})
    manifest_hash = hashlib.sha256(json.dumps(file_records, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    return {
        "status": "ok",
        "render_mode": "vectra_laboratory_repository_manifest",
        "release": INSPECTION_RELEASE,
        "read_only": True,
        "manifest_id": f"repository-manifest-{manifest_hash[:12]}",
        "created_at": _now(),
        "project_root": str(PROJECT_ROOT),
        "files_count": len(files),
        "total_bytes": total_bytes,
        "extensions": dict(sorted(by_extension.items())),
        "key_files": [_file_description(PROJECT_ROOT / rel) for rels in KEY_COMPONENTS.values() for rel in rels if (PROJECT_ROOT / rel).exists()],
        "changed_files": {
            "status": "not_confirmed",
            "reason": "Deploy package does not include previous-package baseline or git metadata. Laboratory can inspect current files but cannot prove file-level diff without baseline.",
        },
        "manifest_hash": manifest_hash,
    }


def get_repository_tree(max_items: int = MAX_TREE_ITEMS) -> Dict[str, Any]:
    safe_limit = min(max(int(max_items or MAX_TREE_ITEMS), 1), MAX_TREE_ITEMS)
    files = _iter_files()
    entries: List[Dict[str, Any]] = []
    seen_dirs = set()
    for path in files:
        rel_parts = Path(_rel(path)).parts
        accum = Path("")
        for part in rel_parts[:-1]:
            accum = accum / part
            directory = str(accum).replace(os.sep, "/")
            if directory not in seen_dirs:
                seen_dirs.add(directory)
                entries.append({"type": "directory", "path": directory})
        entries.append({"type": "file", "path": _rel(path), "size_bytes": path.stat().st_size})
        if len(entries) >= safe_limit:
            break
    return {
        "status": "ok",
        "render_mode": "vectra_laboratory_repository_tree",
        "release": INSPECTION_RELEASE,
        "read_only": True,
        "max_items": safe_limit,
        "truncated": len(entries) >= safe_limit,
        "tree": entries,
    }


def get_repository_components() -> Dict[str, Any]:
    components = []
    for component_id, rel_paths in KEY_COMPONENTS.items():
        file_info = []
        present_count = 0
        for rel in rel_paths:
            path = PROJECT_ROOT / rel
            if path.exists():
                present_count += 1
                file_info.append(_file_description(path))
            else:
                file_info.append({"path": rel, "present": False})
        components.append({
            "component_id": component_id,
            "status": "present" if present_count == len(rel_paths) else "partial" if present_count else "missing",
            "read_only": True,
            "files": file_info,
        })
    return {
        "status": "ok",
        "render_mode": "vectra_laboratory_repository_components",
        "release": INSPECTION_RELEASE,
        "read_only": True,
        "components": components,
    }


def verify_repository_against_release_brief(release_brief_text: Optional[str] = None) -> Dict[str, Any]:
    status = get_repository_inspection_status()
    component_payload = get_repository_components()
    components = component_payload["components"]
    routes_path = PROJECT_ROOT / "app" / "api" / "routes.py"
    routes_text = routes_path.read_text(encoding="utf-8", errors="replace") if routes_path.exists() else ""
    endpoint_presence = {endpoint: (endpoint.replace("{domain}", "") in routes_text or endpoint in routes_text) for endpoint in EXPECTED_FOUNDATION_0009_ENDPOINTS}
    confirmed = {
        "repository_inspection_module_present": (PROJECT_ROOT / "app" / "assistant_runtime" / "repository_inspection.py").exists(),
        "knowledge_capitalization_module_present": (PROJECT_ROOT / "app" / "assistant_runtime" / "knowledge_capitalization.py").exists(),
        "business_domain_knowledge_repository_present": (PROJECT_ROOT / "assistant_repository" / "business_domains" / "bonboason" / "business_knowledge.json").exists(),
        "laboratory_openapi_function_present": "_laboratory_public_openapi_schema" in routes_text,
        "split_openapi_functions_present": all(name in routes_text for name in ["_laboratory_core_openapi_schema", "_laboratory_business_data_openapi_schema", "_laboratory_knowledge_openapi_schema"]),
        "all_expected_endpoints_registered_in_routes": all(endpoint_presence.values()),
        "read_only_repository_inspection": True,
    }
    risks = []
    if not all(endpoint_presence.values()):
        risks.append("Some FOUNDATION-0009 endpoints are not registered in routes.py.")
    if not release_brief_text:
        risks.append("Release Brief text was not provided to /verify, so Laboratory can verify implementation presence but cannot semantically compare against a specific Release Brief.")
    report = {
        "status": "PASS" if all(confirmed.values()) else "FAIL",
        "render_mode": "vectra_laboratory_repository_verify",
        "release": INSPECTION_RELEASE,
        "read_only": True,
        "what_is_confirmed": confirmed,
        "endpoint_presence": endpoint_presence,
        "components_summary": [{"component_id": c["component_id"], "status": c["status"]} for c in components],
        "what_is_not_confirmed": ["file-level diff vs previous deploy package"] if not release_brief_text else [],
        "risks": risks,
        "professional_boundary": {
            "vectra_may_inspect_repository": True,
            "vectra_may_change_code": False,
            "vectra_may_deploy": False,
            "vectra_may_replace_engineering_decision": False,
        },
        "generated_at": _now(),
    }
    return report
