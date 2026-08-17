from __future__ import annotations

import json

from app.api import routes
from app.assistant_runtime.canonical_workspace_contract import attach_canonical_workspace_contract
from app.workspace_runtime import apply_runtime_contract


def _workspace(*, large: bool = False):
    lines = [
        "# Варус | 2026-04",
        "",
        "## Показатели",
        "Показатель | Значение",
        "Оборот | 100",
    ]
    if large:
        lines.extend(f"Строка {index} | {index}" for index in range(5000))
    lines.extend(["", "## Что делаем дальше?", "1. Открыть категорию."])
    return apply_runtime_contract({
        "status": "ok",
        "render_mode": "contract_workspace",
        "context": {"level": "network", "object_name": "Варус", "period": "2026-04"},
        "path": ["Бизнес", "Топ", "Менеджер", "Варус"],
        "filter": {"period": "2026-04", "network": "Варус"},
        "workspace_markdown": "\n".join(lines),
    })


def _request(response_mode: str):
    return {
        "operation_type": "get_canonical_workspace",
        "payload": {
            "business_domain": "bonboason",
            "period": "2026-04",
            "workspace_type": "contract",
            "object_id": "Варус",
            "response_mode": response_mode,
        },
    }


def test_transport_readback_executes_full_workspace_but_returns_bounded_proof(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    source = _workspace(large=True)

    def fake_query(_request):
        return routes.json_response(source)

    monkeypatch.setattr(routes, "vectra_query", fake_query)
    response = routes.vectra_laboratory_facade_business_data(_request("transport_readback"))
    body = json.loads(response.body)
    result = body["result"]

    assert body["status"] == "ok"
    assert body["runtime_service_called"] == "canonical_workspace.get_transport_readback"
    assert result["status"] == "PASS"
    assert result["verification_status"] == "PASS"
    assert result["readback_status"] == "PASS"
    assert result["full_workspace_executed"] is True
    assert result["full_workspace_returned"] is False
    assert result["workspace_markdown_present"] is True
    assert result["workspace_markdown_chars"] > 90000
    assert len(result["workspace_markdown_sha256"]) == 64
    assert result["canonical_identity"] == {
        "business_domain": "bonboason",
        "workspace_type": "network",
        "object_id": "Варус",
        "period": "2026-04",
    }
    assert len(result["canonical_hashes"]["semantic_hash"]) == 64
    assert len(result["canonical_hashes"]["presentation_hash"]) == 64
    assert result["transport_projection"]["bounded"] is True
    assert len(response.body) < 90000
    assert "workspace_markdown" not in result
    assert body["professional_pipeline"]["professional_context"]["operation_access"]["classification"] == "READ_ONLY_RESEARCH"


def test_full_mode_remains_backward_compatible(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    source = _workspace()

    def fake_query(_request):
        return routes.json_response(source)

    monkeypatch.setattr(routes, "vectra_query", fake_query)
    response = routes.vectra_laboratory_facade_business_data(_request("full"))
    body = json.loads(response.body)

    assert body["status"] == "ok"
    assert body["runtime_service_called"] == "canonical_workspace.get"
    assert body["result"]["workspace_markdown"] == attach_canonical_workspace_contract(source)["workspace_markdown"]
    assert body["result"]["canonical_workspace"]["presentation_hash"]


def test_openapi_and_manifest_publish_transport_readback_without_new_action():
    schema = routes._laboratory_facade_openapi_schema()
    operation = schema["paths"]["/vectra/laboratory/facade/business-data"]["post"]
    request_schema = operation["requestBody"]["content"]["application/json"]["schema"]
    response_mode = request_schema["properties"]["payload"]["properties"]["response_mode"]
    assert response_mode["default"] == "full"
    assert set(response_mode["enum"]) == {"full", "transport_readback"}
    assert routes._count_openapi_operations(schema) == 29

    manifest = routes.get_vectra_business_data_manifest()
    canonical = next(item for item in manifest["operations"] if item["operation_type"] == "get_canonical_workspace")
    assert canonical["supported_response_modes"] == ["full", "transport_readback"]
    assert canonical["read_only"] is True

