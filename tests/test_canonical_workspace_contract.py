from app.assistant_runtime.canonical_workspace_contract import attach_canonical_workspace_contract


def _payload():
    return {
        "status": "ok",
        "context": {"level": "business", "object_name": "Бон Буассон", "period": "2026-02"},
        "path": ["Бон Буассон"],
        "render_mode": "workspace",
        "workspace_markdown": "# Бизнес\n\n## Показатели\nПоказатель | Факт | Изменение\nОборот | 100 | +10%\n\n## Что делаем дальше?\n1. Открыть руководителя",
        "active_workspace_state": {"workspace_level": "business"},
        "workspace_action_map": [{"number": 1, "label": "Открыть руководителя"}],
    }


def test_canonical_renderer_promotes_plain_pipe_rows_to_markdown_table():
    result = attach_canonical_workspace_contract(_payload())
    assert "| --- | --- | --- |" in result["workspace_markdown"]
    contract = result["canonical_workspace"]
    assert contract["presentation"]["tables_count"] == 1
    assert contract["presentation"]["headings_count"] == 3
    assert contract["semantic_model"]["context"]["period"] == "2026-02"


def test_canonical_contract_is_deterministic_for_same_workspace():
    left = attach_canonical_workspace_contract(_payload())["canonical_workspace"]
    right = attach_canonical_workspace_contract(_payload())["canonical_workspace"]
    assert left["semantic_hash"] == right["semantic_hash"]
    assert left["presentation_hash"] == right["presentation_hash"]

