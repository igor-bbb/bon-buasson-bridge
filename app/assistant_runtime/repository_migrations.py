"""Controlled Runtime Repository data reconciliations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from app.assistant_runtime.repository import _read_json, _write_json, ensure_repository


MIGRATION_ID = "RESTORE-PK-002-AFTER-EPHEMERAL-DEPLOY-001"
PK_002_CANDIDATE_ID = "KC-PK-002-OPERATIONAL-CAPABILITY-READINESS-001"
PK_002_KNOWLEDGE_ID = "PK-002"
PK_002_CONTENT = (
    "Профессиональная способность цифрового коллеги считается эксплуатационно "
    "доступной только тогда, когда согласованы исполняемый Runtime, API, "
    "Capability Registry, Action Manifest и пользовательская маршрутизация. "
    "Наличие функции в коде или внутренней операции без опубликованного и "
    "фактически исполнимого профессионального контракта не делает эту способность "
    "доступной цифровому коллеге. Доступность должна подтверждаться фактическим "
    "вызовом через пользовательский фасад, корректной передачей обязательных "
    "параметров, успешным результатом и обратной проверкой там, где она применима. "
    "Правило относится только к способностям, заявляемым цифровому коллеге как "
    "профессионально доступные, и не требует публикации внутренних технических функций."
)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def reconcile_lost_pk002_candidate() -> Dict[str, Any]:
    """Restore the confirmed candidate lost by ephemeral deployment, once.

    This is a repository reconciliation, not a new candidate creation and not
    knowledge capitalization. Existing records always win and are never reset.
    """
    base = ensure_repository()
    candidate_path = base / "runtime" / "knowledge_capitalization" / "candidates.json"
    migration_path = base / "runtime" / "repository_migrations.json"
    candidates = _read_json(candidate_path, [])
    if not isinstance(candidates, list):
        candidates = []

    existing = next(
        (
            item
            for item in candidates
            if isinstance(item, dict)
            and (
                item.get("candidate_id") == PK_002_CANDIDATE_ID
                or item.get("knowledge_id") == PK_002_KNOWLEDGE_ID
            )
        ),
        None,
    )
    restored = False
    if existing is None:
        now = _now()
        existing = {
            "candidate_id": PK_002_CANDIDATE_ID,
            "knowledge_id": PK_002_KNOWLEDGE_ID,
            "knowledge_type": "professional",
            "title": "Критерий эксплуатационной доступности профессиональной способности цифрового коллеги",
            "content": PK_002_CONTENT,
            "domain": None,
            "status": "CONFIRMED_BY_PRODUCT_OWNER",
            "product_owner_approval": True,
            "source": (
                "Product Verification релизов KNOWLEDGE-FACADE-READ-ROUTING-001 "
                "и KNOWLEDGE-FACADE-ID-PARAMETER-ROUTING-001"
            ),
            "created_at": now,
            "updated_at": now,
            "original_created_at": None,
            "professional_model_auto_update": False,
            "target_repository": "knowledge/professional_knowledge.json",
            "content_checksum": None,
            "revision": 1,
            "capitalization_change_type": "new",
            "evidence": {
                "restored_from": "confirmed Product Verification transcript",
                "reason": "candidate lost after ephemeral Render deployment",
                "migration_id": MIGRATION_ID,
            },
            "recommended_memory_type": None,
            "knowledge_subtype": None,
            "prepared_item_status": None,
        }
        candidates.append(existing)
        _write_json(candidate_path, candidates)
        restored = True

    migrations = _read_json(migration_path, [])
    if not isinstance(migrations, list):
        migrations = []
    marker = next(
        (item for item in migrations if isinstance(item, dict) and item.get("migration_id") == MIGRATION_ID),
        None,
    )
    if marker is None:
        marker = {
            "migration_id": MIGRATION_ID,
            "status": "APPLIED",
            "candidate_id": PK_002_CANDIDATE_ID,
            "knowledge_id": PK_002_KNOWLEDGE_ID,
            "restored": restored,
            "duplicate_created": False,
            "package_created": False,
            "knowledge_written": False,
            "professional_model_auto_update": False,
            "applied_at": _now(),
        }
        migrations.append(marker)
        _write_json(migration_path, migrations)

    readback = _read_json(candidate_path, [])
    matches = [
        item
        for item in readback
        if isinstance(item, dict) and item.get("candidate_id") == PK_002_CANDIDATE_ID
    ] if isinstance(readback, list) else []
    return {
        "status": "PASS" if len(matches) == 1 else "FAIL",
        "migration_id": MIGRATION_ID,
        "restored": restored,
        "candidate_id": PK_002_CANDIDATE_ID,
        "knowledge_id": PK_002_KNOWLEDGE_ID,
        "candidate_instances": len(matches),
        "duplicate_created": len(matches) > 1,
        "package_created": False,
        "knowledge_written": False,
        "professional_model_auto_update": False,
        "candidate_status": matches[0].get("status") if len(matches) == 1 else None,
    }
