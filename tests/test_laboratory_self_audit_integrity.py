from __future__ import annotations

import hashlib
import json
from pathlib import Path

from fastapi.testclient import TestClient

from app.api import routes
from app.main import app
from app.assistant_runtime.business_framework_services import execute_framework_service
from app.assistant_runtime.laboratory_self_audit_integrity import run_laboratory_self_audit


client = TestClient(app)


def _runtime_fingerprints() -> dict[str, str]:
    return {
        path.as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(Path("runtime").rglob("*"))
        if path.is_file()
    }


def test_self_audit_returns_full_separated_evidence_and_is_read_only():
    before = _runtime_fingerprints()
    result = run_laboratory_self_audit()
    after = _runtime_fingerprints()

    assert result["operation_type"] == "self_audit"
    assert result["status"] == "PASS"
    assert result["failure_reason"] is None
    assert result["response_mode"] == "full"
    assert {
        "confirmed_runtime_state",
        "normative_documents",
        "code_implementation",
        "product_verification",
        "unconfirmed_gaps",
        "integrity",
    } <= set(result)
    assert result["integrity"]["runtime_files_unchanged"] is True
    assert result["integrity"]["business_domain_required"] is False
    assert result["integrity"]["business_domain_activated"] is False
    assert result["integrity"]["knowledge_capitalized"] is False
    assert result["integrity"]["work_context_changed"] is False
    assert result["integrity"]["release_opened"] is False
    assert result["integrity"]["architectural_priority_selected"] is False
    for section in (
        "confirmed_runtime_state",
        "normative_documents",
        "code_implementation",
        "product_verification",
        "unconfirmed_gaps",
        "integrity",
    ):
        assert f'"{section}"' in result["assistant_response"]
    assert result["response_contract"]["summarization_forbidden"] is True
    assert before == after


def test_dedicated_action_returns_full_audit_without_runtime_mutation():
    before = _runtime_fingerprints()
    response = client.get("/vectra/laboratory/self-audit")
    after = _runtime_fingerprints()

    assert response.status_code == 200
    body = response.json()
    assert body["operation_type"] == "self_audit"
    assert body["status"] == "PASS"
    assert body["response_mode"] == "full"
    assert body["read_only"] is True
    assert before == after


def test_self_audit_is_not_available_through_business_framework_facade():
    result = execute_framework_service({"operation_type": "self_audit"})

    assert result["status"] == "VALIDATION_ERROR"
    assert result["reason"] == "unsupported_operation_type"
    assert "self_audit" not in result["supported_operations"]


def test_openapi_publishes_one_unambiguous_self_audit_route_and_no_auto_domain_activation(monkeypatch):
    monkeypatch.setenv("VECTRA_PUBLIC_RUNTIME_URL", "https://bon-buasson-api.onrender.com")
    schema = routes._laboratory_facade_openapi_schema()
    operations = [operation for methods in schema["paths"].values() for operation in methods.values()]
    operation_ids = [operation["operationId"] for operation in operations]

    assert len(operations) == 29
    assert operation_ids.count("runVectraSelfAudit") == 1
    self_audit = schema["paths"]["/vectra/laboratory/self-audit"]["get"]
    assert self_audit["x-openai-isConsequential"] is False
    assert "mandatory and exclusive" in self_audit["description"].lower()
    assert "read-only" in self_audit["description"]

    framework = schema["paths"]["/vectra/laboratory/framework-services"]["post"]
    framework_schema = framework["requestBody"]["content"]["application/json"]["schema"]
    assert "self_audit" not in framework_schema["properties"]["operation_type"]["enum"]

    domain = schema["paths"]["/vectra/laboratory/facade/business-domain"]["post"]
    domain_schema = domain["requestBody"]["content"]["application/json"]["schema"]
    domain_description = domain_schema["properties"]["operation_type"]["description"]
    assert "Never call activate_domain automatically" in domain_description
    assert domain_schema["examples"] == [{"operation_type": "list_domains"}]
    assert schema["servers"] == [{"url": "https://bon-buasson-api.onrender.com"}]
    assert schema["info"]["version"] == "VECTRA-LABORATORY-SELF-AUDIT-INTEGRITY-001"
    assert schema["x-vectra-release"] == "VECTRA-LABORATORY-SELF-AUDIT-INTEGRITY-001"
    assert schema["x-vectra-gpt-actions-operation-limit"]["operation_count"] == 29


def test_self_audit_response_is_json_serializable():
    json.dumps(run_laboratory_self_audit(), ensure_ascii=False)
