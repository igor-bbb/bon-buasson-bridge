from app.assistant_runtime.business_action_transport import (
    RELEASE_ID,
    canonical_workspace_request_properties,
    project_workspace_action_response,
    route_explicit_business_data_fields,
)


def _payload(markdown="# Бизнес\n\n## Показатели\n\n| Показатель | Факт |\n| --- | --- |\n| Оборот | 100 |"):
    return {
        "status": "ok",
        "context": {"level": "business", "period": "2026-02"},
        "path": ["business"],
        "render_mode": "business_workspace",
        "workspace_markdown": markdown,
        "workspace_primary_block": markdown.splitlines(),
        "metrics": [{"name": "Оборот", "value": 100}] * 500,
        "active_workspace_state": {"current": "business"},
        "workspace_action_map": [{"action": "open", "object_id": "manager"}],
        "workspace_runtime_contract": {"source": "runtime"},
        "canonical_workspace": {
            "release_id": "display-release",
            "contract_version": "2.2",
            "workspace_type": "business",
            "business_domain": "bonboason",
            "period": "2026-02",
            "object_id": "Бон Буассон",
            "semantic_model": {"duplicate": "x" * 20000},
            "presentation": {
                "format": "markdown",
                "renderer": "canonical_markdown_renderer",
                "headings_count": 2,
                "tables_count": 1,
                "sections": [{"markdown": markdown}] * 100,
                "required_rendering_rule": "render workspace_markdown verbatim as Markdown",
            },
            "semantic_hash": "semantic-hash",
            "presentation_hash": "presentation-hash",
            "read_only": True,
        },
    }


def test_transport_projection_preserves_complete_workspace_and_removes_duplicates():
    source = _payload()
    result = project_workspace_action_response(source, budget_chars=60000)

    assert result["workspace_markdown"] == source["workspace_markdown"]
    assert "workspace_primary_block" not in result
    assert "metrics" not in result
    assert "semantic_model" not in result["canonical_workspace"]
    assert "sections" not in result["canonical_workspace"]["presentation"]
    assert result["screen_order"] == ["workspace_markdown"]
    assert result["response_budget_guard"]["release_id"] == RELEASE_ID
    assert result["response_budget_guard"]["within_budget"] is True


def test_transport_projection_keeps_markdown_even_when_it_alone_exceeds_budget():
    markdown = "# Бизнес\n\n" + ("Полная строка рабочего стола.\n" * 100)
    result = project_workspace_action_response(_payload(markdown), budget_chars=1000)

    assert result["workspace_markdown"] == markdown
    assert result["response_budget_guard"]["workspace_markdown_preserved"] is True
    assert result["response_budget_guard"]["continuation_state_omitted"] is True
    assert "active_workspace_state" not in result


def test_canonical_workspace_fields_are_explicit_for_gpt_actions():
    properties = canonical_workspace_request_properties(
        ("business", "top_manager", "manager", "network", "contract")
    )

    assert set(properties) == {"business_domain", "workspace_type", "object_id"}
    assert properties["workspace_type"]["enum"] == [
        "business", "top_manager", "manager", "network", "contract"
    ]


def test_explicit_action_fields_route_to_existing_facade_payload():
    payload = route_explicit_business_data_fields(
        {
            "operation_type": "get_canonical_workspace",
            "business_domain": "bonboason",
            "period": "2026-02",
            "workspace_type": "business",
            "object_id": "bonboason",
        },
        {"period": "legacy-period"},
    )

    assert payload == {
        "business_domain": "bonboason",
        "period": "legacy-period",
        "workspace_type": "business",
        "object_id": "bonboason",
    }


def test_business_openapi_uses_dedicated_query_edge_and_laboratory_facade_keeps_shared_runtime():
    import inspect

    from app.api.routes import (
        _business_gpt_openapi_schema,
        vectra_laboratory_facade_business_data,
    )

    business_schema = _business_gpt_openapi_schema()
    assert business_schema["paths"]["/vectra/business/query"]["post"]["operationId"] == "executeVectraQuery"
    assert "/vectra/query" not in business_schema["paths"]

    laboratory_facade_source = inspect.getsource(vectra_laboratory_facade_business_data)
    assert "response = vectra_query(" in laboratory_facade_source
    assert "response = vectra_business_query(" not in laboratory_facade_source


def test_business_query_edge_always_projects_workspace_without_changing_markdown(monkeypatch):
    from fastapi.responses import JSONResponse

    from app.api import routes
    from app.models.request_models import VectraQueryRequest

    source = _payload("# Бизнес\n\nПолный канонический рабочий стол")
    calls = []

    def fake_vectra_query(request):
        calls.append(request.message)
        return JSONResponse(content=source)

    monkeypatch.setattr(routes, "vectra_query", fake_vectra_query)
    response = routes.vectra_business_query(
        VectraQueryRequest(message="Бизнес 2026-02", session_id="business-edge-test")
    )
    import json

    result = json.loads(response.body.decode("utf-8"))

    assert calls == ["Бизнес 2026-02"]
    assert result["workspace_markdown"] == source["workspace_markdown"]
    assert "workspace_primary_block" not in result
    assert "metrics" not in result
    assert result["response_budget_guard"]["release_id"] == RELEASE_ID
