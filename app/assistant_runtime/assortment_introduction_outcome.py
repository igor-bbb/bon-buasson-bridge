"""Canonical Assortment Introduction Outcome with append-only lineage.

DEV-0036 closes the operational loop after an assortment-matrix candidate is
identified.  The materialized record is convenient for reads, while every
mutation is also persisted as a hash-linked event so forecast, actuals,
evaluation and learning can never be silently rewritten.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional

from app.assistant_runtime.repository import ensure_repository, _read_json, _write_json
from app.domain.business_abc import build_business_abc
from app.domain.category_abc import build_category_network_sku_package
from app.domain.filters import get_normalized_rows
from app.domain.normalization import clean_text, round_money


RELEASE_ID = "VECTRA-ASSORTMENT-INTRODUCTION-OUTCOME-001"
SCHEMA_VERSION = "1.0"
OUTCOME_STATUSES = (
    "NOT_EVALUATED",
    "IN_PROGRESS",
    "CONFIRMED_POSITIVE",
    "PARTIALLY_CONFIRMED",
    "NO_EFFECT",
    "NEGATIVE_OUTCOME",
    "INCONCLUSIVE",
)
FINAL_OUTCOME_STATUSES = OUTCOME_STATUSES[2:]
DECISION_STATUSES = ("introduced", "test", "deferred")
CHECKPOINTS = ("M1", "M2", "M3")
EXPECTED_IMPACT_STATUSES = ("NOT_ASSESSED", "SCENARIO_DEFINED")
MAX_LIST_LIMIT = 100
_LOCK = Lock()


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def outcome_repository_path() -> Path:
    path = ensure_repository() / "runtime" / "business" / "assortment_introduction_outcomes.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _empty_repository() -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "release": RELEASE_ID,
        "records": {},
        "events": [],
    }


def _read_repository() -> Dict[str, Any]:
    value = _read_json(outcome_repository_path(), _empty_repository())
    if not isinstance(value, dict):
        return _empty_repository()
    value.setdefault("schema_version", SCHEMA_VERSION)
    value.setdefault("release", RELEASE_ID)
    value.setdefault("records", {})
    value.setdefault("events", [])
    if not isinstance(value["records"], dict):
        value["records"] = {}
    if not isinstance(value["events"], list):
        value["events"] = []
    return value


def _record_hash(record: Dict[str, Any]) -> str:
    payload = deepcopy(record)
    payload.pop("record_hash", None)
    traceability = payload.get("traceability")
    if isinstance(traceability, dict):
        traceability.pop("last_event_hash", None)
    return _hash(payload)


def _append_event(
    repository: Dict[str, Any],
    record: Dict[str, Any],
    event_type: str,
    changes: Dict[str, Any],
    *,
    actor: str,
    request_id: str = "",
) -> Dict[str, Any]:
    events = repository["events"]
    outcome_events = [event for event in events if event.get("outcome_id") == record["outcome_id"]]
    event_id = f"AIOE-{uuid.uuid4().hex[:12].upper()}"
    traceability = record.setdefault("traceability", {})
    traceability.setdefault("event_ids", []).append(event_id)
    traceability["revision"] = len(traceability["event_ids"])
    materialized_hash = _record_hash(record)
    event = {
        "event_id": event_id,
        "outcome_id": record["outcome_id"],
        "sequence": len(outcome_events) + 1,
        "event_type": event_type,
        "recorded_at": _now(),
        "actor": clean_text(actor) or "VECTRA Runtime",
        "request_id": clean_text(request_id) or None,
        "changes": deepcopy(changes),
        "changes_hash": _hash(changes),
        "previous_event_hash": outcome_events[-1].get("event_hash") if outcome_events else None,
        "record_hash": materialized_hash,
    }
    event["event_hash"] = _hash(event)
    events.append(event)
    record["record_hash"] = materialized_hash
    traceability["last_event_hash"] = event["event_hash"]
    return event


def _error(operation_type: str, failure_reason: str, **extra: Any) -> Dict[str, Any]:
    return {
        "status": "error",
        "operation_type": operation_type,
        "failure_reason": failure_reason,
        "release": RELEASE_ID,
        **extra,
    }


def _valid_date(value: Any) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", clean_text(value)))


def _number_or_none(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None


def _number(value: Any) -> float:
    parsed = _number_or_none(value)
    return float(parsed) if parsed is not None else 0.0
    try:
        return round_money(float(value))
    except (TypeError, ValueError):
        return None


def _normalized_expected_impact(value: Any) -> Dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    status = clean_text(source.get("status") or "NOT_ASSESSED").upper()
    if status == "ASSESSED":
        status = "SCENARIO_DEFINED"
    if status not in EXPECTED_IMPACT_STATUSES:
        return {}
    result = {
        "status": status,
        "scenario": deepcopy(source.get("scenario")),
        "range": deepcopy(source.get("range")),
        "assumptions": deepcopy(source.get("assumptions") or []),
        "horizon": deepcopy(source.get("horizon")),
        "confidence": clean_text(source.get("confidence")) or None,
        "professional_rule": "Historical business revenue is evidence, not Expected Impact for a selected Network.",
    }
    if status == "SCENARIO_DEFINED" and not all(
        [result["scenario"], result["range"], result["assumptions"], result["horizon"], result["confidence"]]
    ):
        return {}
    result["forecast_hash"] = _hash({key: val for key, val in result.items() if key != "forecast_hash"})
    return result


def _candidate_evidence(period: str, network: str, category: str, sku: str) -> Dict[str, Any]:
    rows = list(get_normalized_rows())
    business = build_business_abc(period, limit=100, rows=rows)
    category_package = build_category_network_sku_package(period, category, network, limit=100, rows=rows)
    if business.get("status") != "PASS":
        return _error("create_assortment_outcome", "business_abc_evidence_unavailable", source_failure=business.get("failure_reason"))
    if category_package.get("status") != "PASS":
        return _error("create_assortment_outcome", "category_candidate_evidence_unavailable", source_failure=category_package.get("failure_reason"))
    candidate = next(
        (item for item in category_package.get("candidates") or [] if clean_text(item.get("sku")).casefold() == sku.casefold()),
        None,
    )
    if not candidate or candidate.get("category_matrix_candidate") is not True:
        return _error("create_assortment_outcome", "sku_is_not_current_category_matrix_candidate")
    business_item = next(
        (item for item in business.get("items") or [] if clean_text(item.get("sku")).casefold() == sku.casefold()),
        None,
    )
    business_projection = {
        "release": business.get("release"),
        "horizon": business.get("horizon"),
        "methodology": business.get("methodology"),
        "summary": business.get("summary"),
        "sku_evidence": business_item,
        "bounded_source": business.get("bounded_result"),
    }
    category_projection = {
        "release": category_package.get("release"),
        "category": category_package.get("category"),
        "horizon": category_package.get("horizon"),
        "category_classification_hash": category_package.get("category_classification_hash"),
        "candidate_rule": category_package.get("category_candidate_rule"),
    }
    candidate_projection = {
        "network": network,
        "period": period,
        "presence_basis": category_package.get("presence_basis"),
        "candidate": candidate,
    }
    category_rows = [
        row for row in rows
        if clean_text(row.get("period")) == period
        and clean_text(row.get("network")).casefold() == network.casefold()
        and clean_text(row.get("category")).casefold() == category.casefold()
    ]
    baseline = {
        "source_period": period,
        "sku_before_introduction": {
            "presence_in_network": candidate.get("presence_in_network"),
            **deepcopy(candidate.get("current_network_context") or {}),
        },
        "category_before_introduction": {
            "revenue": round_money(sum(_number(row.get("revenue")) for row in category_rows)),
            "finrez_pre": round_money(sum(_number(row.get("finrez_pre")) for row in category_rows)),
            "existing_sku_count": len({clean_text(row.get("sku")) for row in category_rows if clean_text(row.get("sku"))}),
        },
        "available_network_metrics": ["revenue", "finrez_pre"],
        "data_source": "normalized_business_data",
    }
    return {
        "status": "PASS",
        "business_abc": {**business_projection, "evidence_hash": _hash(business_projection)},
        "category_abc": {**category_projection, "evidence_hash": _hash(category_projection)},
        "network_candidate": {**candidate_projection, "evidence_hash": _hash(candidate_projection)},
        "baseline": baseline,
    }


def create_assortment_outcome(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    operation_type = "create_assortment_outcome"
    payload = payload if isinstance(payload, dict) else {}
    network = clean_text(payload.get("network"))
    category = clean_text(payload.get("category"))
    sku = clean_text(payload.get("sku"))
    period = clean_text(payload.get("period"))
    decision_date = clean_text(payload.get("decision_date"))
    owner_role = clean_text(payload.get("owner_role"))
    decision_status = clean_text(payload.get("decision") or payload.get("decision_status")).lower()
    if not all([network, category, sku, period, decision_date, owner_role]):
        return _error(operation_type, "identity_fields_required", required=["network", "category", "sku", "period", "decision_date", "owner_role"])
    if not _valid_date(decision_date):
        return _error(operation_type, "decision_date_must_be_yyyy_mm_dd")
    if decision_status not in DECISION_STATUSES:
        return _error(operation_type, "invalid_decision", supported=list(DECISION_STATUSES))
    expected_impact = _normalized_expected_impact(payload.get("expected_impact"))
    if not expected_impact:
        return _error(operation_type, "invalid_expected_impact", supported_statuses=list(EXPECTED_IMPACT_STATUSES))

    evidence = _candidate_evidence(period, network, category, sku)
    if evidence.get("status") != "PASS":
        return evidence
    outcome_id = clean_text(payload.get("outcome_id")) or f"AIO-{decision_date.replace('-', '')}-{_hash([network, category, sku, decision_date])[:10].upper()}"
    now = _now()
    identity = {
        "network": network,
        "category": category,
        "sku": sku,
        "decision_date": decision_date,
        "owner_role": owner_role,
    }
    baseline = evidence.pop("baseline")
    supplied_baseline = payload.get("baseline")
    if isinstance(supplied_baseline, dict):
        baseline["owner_supplied_context"] = deepcopy(supplied_baseline)
    record = {
        "outcome_id": outcome_id,
        "schema_version": SCHEMA_VERSION,
        "release": RELEASE_ID,
        "identity": identity,
        "evidence_origin": evidence,
        "decision": {"status": decision_status, "recorded_at": now, "owner_role": owner_role},
        "baseline": baseline,
        "expected_impact": expected_impact,
        "checkpoints": {checkpoint: None for checkpoint in CHECKPOINTS},
        "observed_outcome": {"status": "NOT_EVALUATED", "checkpoints_recorded": []},
        "evaluation": {"status": "NOT_EVALUATED", "evaluated_at": None},
        "learning": {
            "status": "NOT_RECORDED",
            "items": [],
            "automatic_capitalization": False,
            "capitalization_status": "NOT_CAPITALIZED",
            "knowledge_candidate_status": "NOT_CREATED",
            "knowledge_candidate_requires_product_owner_approval": True,
        },
        "traceability": {
            "source_chain": "Business ABC -> Category ABC -> Network Candidate -> Decision -> Introduction -> M1/M2/M3 -> Outcome -> Evaluation -> Learning",
            "event_ids": [],
            "revision": 0,
        },
        "created_at": now,
        "updated_at": now,
    }
    with _LOCK:
        repository = _read_repository()
        if outcome_id in repository["records"]:
            return _error(operation_type, "outcome_id_already_exists", outcome_id=outcome_id)
        if any(existing.get("identity") == identity for existing in repository["records"].values()):
            return _error(operation_type, "outcome_identity_already_exists")
        event = _append_event(repository, record, "OUTCOME_CREATED", {"record": deepcopy(record)}, actor=owner_role, request_id=clean_text(payload.get("request_id")))
        repository["records"][outcome_id] = record
        _write_json(outcome_repository_path(), repository)
    return {
        "status": "PASS",
        "operation_type": operation_type,
        "release": RELEASE_ID,
        "outcome_id": outcome_id,
        "readback_status": "PASS",
        "event_id": event["event_id"],
        "record": deepcopy(record),
    }


def _mutate_record(
    operation_type: str,
    outcome_id: str,
    mutator: Any,
    *,
    actor: str,
    request_id: str = "",
) -> Dict[str, Any]:
    with _LOCK:
        repository = _read_repository()
        current = repository["records"].get(outcome_id)
        if not isinstance(current, dict):
            return _error(operation_type, "outcome_not_found", outcome_id=outcome_id)
        record = deepcopy(current)
        mutation = mutator(record)
        if isinstance(mutation, dict) and mutation.get("status") == "error":
            return mutation
        event_type, changes = mutation
        record["updated_at"] = _now()
        event = _append_event(repository, record, event_type, changes, actor=actor, request_id=request_id)
        repository["records"][outcome_id] = record
        _write_json(outcome_repository_path(), repository)
    return {
        "status": "PASS",
        "operation_type": operation_type,
        "release": RELEASE_ID,
        "outcome_id": outcome_id,
        "readback_status": "PASS",
        "event_id": event["event_id"],
        "record": deepcopy(record),
    }


def record_expected_impact(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    operation_type = "record_assortment_expected_impact"
    payload = payload if isinstance(payload, dict) else {}
    outcome_id = clean_text(payload.get("outcome_id"))
    expected = _normalized_expected_impact(payload.get("expected_impact"))
    if not outcome_id or not expected:
        return _error(operation_type, "outcome_id_and_valid_expected_impact_required")

    def mutate(record: Dict[str, Any]) -> Any:
        if any(record.get("checkpoints", {}).get(checkpoint) is not None for checkpoint in CHECKPOINTS):
            return _error(operation_type, "forecast_locked_after_actual")
        if (record.get("expected_impact") or {}).get("status") != "NOT_ASSESSED":
            return _error(operation_type, "forecast_already_defined_and_immutable")
        record["expected_impact"] = expected
        return "EXPECTED_IMPACT_RECORDED", {"expected_impact": deepcopy(expected)}

    return _mutate_record(operation_type, outcome_id, mutate, actor=clean_text(payload.get("actor")) or "Product Owner", request_id=clean_text(payload.get("request_id")))


def record_outcome_checkpoint(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    operation_type = "record_assortment_checkpoint"
    payload = payload if isinstance(payload, dict) else {}
    outcome_id = clean_text(payload.get("outcome_id"))
    checkpoint = clean_text(payload.get("checkpoint")).upper()
    if not outcome_id or checkpoint not in CHECKPOINTS:
        return _error(operation_type, "outcome_id_and_checkpoint_required", supported_checkpoints=list(CHECKPOINTS))
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
    checkpoint_value = {
        "checkpoint": checkpoint,
        "observation_date": clean_text(payload.get("observation_date")) or _now()[:10],
        "revenue": _number_or_none(metrics.get("revenue")),
        "finrez_pre": _number_or_none(metrics.get("finrez_pre")),
        "category_result": deepcopy(metrics.get("category_result")),
        "category_delta": deepcopy(metrics.get("category_delta")),
        "existing_sku_change": deepcopy(metrics.get("existing_sku_change")),
        "cannibalization": deepcopy(metrics.get("cannibalization")),
        "limitations": deepcopy(payload.get("limitations") or metrics.get("limitations") or []),
        "data_completeness": deepcopy(payload.get("data_completeness") or metrics.get("data_completeness") or {"status": "UNKNOWN"}),
        "recorded_at": _now(),
    }

    def mutate(record: Dict[str, Any]) -> Any:
        checkpoints = record.get("checkpoints") or {}
        index = CHECKPOINTS.index(checkpoint)
        if checkpoints.get(checkpoint) is not None:
            return _error(operation_type, "checkpoint_already_recorded_and_immutable", checkpoint=checkpoint)
        if index > 0 and checkpoints.get(CHECKPOINTS[index - 1]) is None:
            return _error(operation_type, "previous_checkpoint_required", required_checkpoint=CHECKPOINTS[index - 1])
        checkpoints[checkpoint] = checkpoint_value
        record["checkpoints"] = checkpoints
        recorded = [name for name in CHECKPOINTS if checkpoints.get(name) is not None]
        record["observed_outcome"] = {
            "status": "IN_PROGRESS",
            "checkpoints_recorded": recorded,
            "latest_checkpoint": checkpoint,
            "m1_is_final_conclusion": False,
            "m2_purpose": "repeatability_check",
            "m3_purpose": "sustained_result_evaluation",
        }
        record["evaluation"] = {"status": "IN_PROGRESS", "evaluated_at": None}
        return "CHECKPOINT_RECORDED", {"checkpoint": deepcopy(checkpoint_value), "forecast_hash_preserved": (record.get("expected_impact") or {}).get("forecast_hash")}

    return _mutate_record(operation_type, outcome_id, mutate, actor=clean_text(payload.get("actor")) or "Business Owner", request_id=clean_text(payload.get("request_id")))


def record_outcome_evaluation(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    operation_type = "record_assortment_evaluation"
    payload = payload if isinstance(payload, dict) else {}
    outcome_id = clean_text(payload.get("outcome_id"))
    final_status = clean_text(payload.get("outcome_status")).upper()
    if not outcome_id or final_status not in FINAL_OUTCOME_STATUSES:
        return _error(operation_type, "outcome_id_and_final_status_required", supported_statuses=list(FINAL_OUTCOME_STATUSES))
    evaluation = {
        "status": final_status,
        "rationale": clean_text(payload.get("rationale")),
        "category_net_effect": deepcopy(payload.get("category_net_effect")),
        "forecast_vs_actual": deepcopy(payload.get("forecast_vs_actual")),
        "limitations": deepcopy(payload.get("limitations") or []),
        "evaluated_by": clean_text(payload.get("actor")) or "Business Owner",
        "evaluated_at": _now(),
    }
    if not evaluation["rationale"]:
        return _error(operation_type, "evaluation_rationale_required")

    def mutate(record: Dict[str, Any]) -> Any:
        if (record.get("checkpoints") or {}).get("M3") is None:
            return _error(operation_type, "m3_required_for_final_evaluation")
        if (record.get("evaluation") or {}).get("status") in FINAL_OUTCOME_STATUSES:
            return _error(operation_type, "final_evaluation_already_recorded_and_immutable")
        forecast_hash = (record.get("expected_impact") or {}).get("forecast_hash")
        record["evaluation"] = evaluation
        record["observed_outcome"] = {
            **deepcopy(record.get("observed_outcome") or {}),
            "status": final_status,
            "finalized_at": evaluation["evaluated_at"],
            "category_effect_assessed": evaluation["category_net_effect"] is not None,
        }
        return "FINAL_EVALUATION_RECORDED", {"evaluation": deepcopy(evaluation), "forecast_hash_preserved": forecast_hash}

    return _mutate_record(operation_type, outcome_id, mutate, actor=evaluation["evaluated_by"], request_id=clean_text(payload.get("request_id")))


def record_outcome_learning(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    operation_type = "record_assortment_learning"
    payload = payload if isinstance(payload, dict) else {}
    outcome_id = clean_text(payload.get("outcome_id"))
    items = payload.get("learning") if isinstance(payload.get("learning"), list) else []
    items = [clean_text(item) for item in items if clean_text(item)]
    if not outcome_id or not items:
        return _error(operation_type, "outcome_id_and_learning_items_required")

    def mutate(record: Dict[str, Any]) -> Any:
        if (record.get("evaluation") or {}).get("status") not in FINAL_OUTCOME_STATUSES:
            return _error(operation_type, "final_evaluation_required_before_learning")
        learning = record.get("learning") or {}
        learning["status"] = "OBSERVED_NOT_CAPITALIZED"
        learning["items"] = list(learning.get("items") or []) + items
        learning["recorded_at"] = _now()
        learning["automatic_capitalization"] = False
        learning["capitalization_status"] = "NOT_CAPITALIZED"
        learning["knowledge_candidate_status"] = "NOT_CREATED"
        learning["knowledge_candidate_requires_product_owner_approval"] = True
        record["learning"] = learning
        return "LEARNING_RECORDED", {"learning_items": items, "automatic_capitalization": False}

    return _mutate_record(operation_type, outcome_id, mutate, actor=clean_text(payload.get("actor")) or "Business Owner", request_id=clean_text(payload.get("request_id")))


def get_assortment_outcome(outcome_id: str) -> Dict[str, Any]:
    repository = _read_repository()
    record = repository["records"].get(clean_text(outcome_id))
    if not isinstance(record, dict):
        return _error("get_assortment_outcome", "outcome_not_found", outcome_id=clean_text(outcome_id), read_only=True)
    events = [deepcopy(event) for event in repository["events"] if event.get("outcome_id") == record["outcome_id"]]
    return {
        "status": "PASS",
        "operation_type": "get_assortment_outcome",
        "release": RELEASE_ID,
        "read_only": True,
        "outcome": deepcopy(record),
        "lineage": events,
        "lineage_event_count": len(events),
    }


def list_assortment_outcomes(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    repository = _read_repository()
    records = list(repository["records"].values())
    network = clean_text(payload.get("network")).casefold()
    category = clean_text(payload.get("category")).casefold()
    status_filter = clean_text(payload.get("outcome_status")).upper()
    if network:
        records = [record for record in records if clean_text((record.get("identity") or {}).get("network")).casefold() == network]
    if category:
        records = [record for record in records if clean_text((record.get("identity") or {}).get("category")).casefold() == category]
    if status_filter:
        records = [record for record in records if clean_text((record.get("observed_outcome") or {}).get("status")).upper() == status_filter]
    records.sort(key=lambda record: (str(record.get("created_at") or ""), str(record.get("outcome_id") or "")), reverse=True)
    try:
        limit = min(max(int(payload.get("limit") or 50), 1), MAX_LIST_LIMIT)
    except (TypeError, ValueError):
        limit = 50
    items = [{
        "outcome_id": record.get("outcome_id"),
        "identity": deepcopy(record.get("identity")),
        "decision": deepcopy(record.get("decision")),
        "expected_impact_status": (record.get("expected_impact") or {}).get("status"),
        "observed_outcome_status": (record.get("observed_outcome") or {}).get("status"),
        "checkpoints_recorded": (record.get("observed_outcome") or {}).get("checkpoints_recorded") or [],
        "learning_status": (record.get("learning") or {}).get("status"),
        "revision": (record.get("traceability") or {}).get("revision"),
    } for record in records[:limit]]
    return {
        "status": "PASS",
        "operation_type": "list_assortment_outcomes",
        "release": RELEASE_ID,
        "read_only": True,
        "total_count": len(records),
        "returned_count": len(items),
        "bounded": True,
        "items": items,
    }


def verify_assortment_outcome_repository(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    requested_id = clean_text(payload.get("outcome_id"))
    repository = _read_repository()
    records = repository["records"]
    selected = {requested_id: records.get(requested_id)} if requested_id else records
    failures: List[Dict[str, Any]] = []
    checked = 0
    for outcome_id, record in selected.items():
        if not isinstance(record, dict):
            failures.append({"outcome_id": outcome_id, "reason": "outcome_not_found"})
            continue
        checked += 1
        events = [event for event in repository["events"] if event.get("outcome_id") == outcome_id]
        previous = None
        for sequence, event in enumerate(events, start=1):
            event_copy = deepcopy(event)
            claimed_hash = event_copy.pop("event_hash", None)
            if event.get("sequence") != sequence or event.get("previous_event_hash") != previous or _hash(event_copy) != claimed_hash:
                failures.append({"outcome_id": outcome_id, "event_id": event.get("event_id"), "reason": "lineage_hash_chain_invalid"})
                break
            previous = claimed_hash
        if not events or previous != (record.get("traceability") or {}).get("last_event_hash"):
            failures.append({"outcome_id": outcome_id, "reason": "materialized_record_lineage_mismatch"})
        if _record_hash(record) != record.get("record_hash"):
            failures.append({"outcome_id": outcome_id, "reason": "materialized_record_hash_invalid"})
        learning = record.get("learning") or {}
        if learning.get("automatic_capitalization") is not False or learning.get("capitalization_status") != "NOT_CAPITALIZED":
            failures.append({"outcome_id": outcome_id, "reason": "learning_capitalization_boundary_violated"})
    verification = "PASS" if not failures else "FAIL"
    return {
        "status": verification,
        "operation_type": "verify_assortment_outcome_repository",
        "release": RELEASE_ID,
        "verification_status": verification,
        "readback_status": verification,
        "read_only": True,
        "records_checked": checked,
        "events_checked": len([event for event in repository["events"] if not requested_id or event.get("outcome_id") == requested_id]),
        "failure_reason": None if not failures else "assortment_outcome_repository_verification_failed",
        "failures": failures,
        "canonical_statuses": list(OUTCOME_STATUSES),
        "automatic_learning_capitalization": False,
    }


def execute_assortment_outcome_operation(operation_type: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    operations = {
        "create_assortment_outcome": lambda data: create_assortment_outcome(data),
        "record_assortment_expected_impact": lambda data: record_expected_impact(data),
        "record_assortment_checkpoint": lambda data: record_outcome_checkpoint(data),
        "record_assortment_evaluation": lambda data: record_outcome_evaluation(data),
        "record_assortment_learning": lambda data: record_outcome_learning(data),
        "get_assortment_outcome": lambda data: get_assortment_outcome(clean_text(data.get("outcome_id"))),
        "list_assortment_outcomes": lambda data: list_assortment_outcomes(data),
        "verify_assortment_outcome_repository": lambda data: verify_assortment_outcome_repository(data),
    }
    handler = operations.get(clean_text(operation_type).lower())
    if handler is None:
        return _error(operation_type, "unsupported_assortment_outcome_operation", supported_operations=sorted(operations))
    return handler(payload if isinstance(payload, dict) else {})
