import pytest

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


@pytest.mark.parametrize("level,title", [
    ("business", "📊 Бизнес — Бон Буассон"),
    ("manager_top", "👤 Топ-менеджер — Ситников Микола"),
    ("manager", "👤 Менеджер — Иваненко"),
    ("network", "🏢 Контракт — Варус"),
])
def test_all_management_levels_share_bounded_visual_structure(level, title):
    payload = _payload()
    payload["context"]["level"] = level
    payload["workspace_markdown"] = "\n".join([
        title,
        "Период: 2026-02",
        "",
        "📊 Ключевые показатели",
        "Показатель | Факт",
        "Оборот | 100",
        "",
        "Комментарий ассистента: данные сформированы Runtime.",
        "",
        "🎯 Приоритеты",
        "Приоритет | Основание",
        "Первый | Факт Runtime",
        "",
        "➡️ Что делаем дальше?",
        "1. Открыть следующий уровень",
    ])
    result = attach_canonical_workspace_contract(payload)
    markdown = result["workspace_markdown"]

    assert markdown.startswith(f"# {title}")
    assert "## 📊 Ключевые показатели" in markdown
    assert "## 🎯 Приоритеты" in markdown
    assert "## ➡️ Что делаем дальше?" in markdown
    assert markdown.count("| --- | --- |") == 2
    assert "\n\nКомментарий ассистента:" in markdown
    assert result["canonical_workspace"]["presentation"]["tables_count"] == 2


def test_orphan_separators_are_removed_and_do_not_create_mega_table():
    payload = _payload()
    payload["workspace_markdown"] = "\n".join([
        "👤 Топ-менеджер — Ситников Микола",
        "Показатель | Значение",
        "| --- | --- |",
        "Оборот | 100",
        "| --- | --- |",
        "🎯 Самостоятельный раздел",
        "Риск | Значение",
        "Маржа | 5%",
    ])
    markdown = attach_canonical_workspace_contract(payload)["workspace_markdown"]

    assert markdown.count("| --- | --- |") == 2
    assert "## 🎯 Самостоятельный раздел" in markdown
    assert "Оборот | 100\n\n## 🎯" in markdown
