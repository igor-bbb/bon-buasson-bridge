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
