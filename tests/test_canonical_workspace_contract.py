import pytest

from app.assistant_runtime.canonical_workspace_contract import (
    attach_canonical_workspace_contract,
    normalize_workspace_type,
)


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
    assert "| Показатель | Факт | Изменение |" in result["workspace_markdown"]
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


@pytest.mark.parametrize("value,expected", [
    ("business", "business"),
    ("Business", "business"),
    ("top_manager", "top_manager"),
    ("Top Manager", "top_manager"),
    ("network / contract", "network"),
    ("contract", "contract"),
])
def test_workspace_type_contract_accepts_canonical_and_legacy_values(value, expected):
    assert normalize_workspace_type(value) == expected


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
    assert result["workspace_primary_block"] == markdown.splitlines()
    assert result["screen_order"] == ["workspace_markdown"]


def test_management_title_with_period_pipe_is_heading_not_table():
    payload = _payload()
    payload["context"]["level"] = "manager_top"
    payload["workspace_markdown"] = "\n".join([
        "🧭 Рабочий стол управления — Ситников Микола | 2026-02",
        "👤 Рабочий стол: Руководитель направления",
        "📊 Ключевые показатели",
        "Показатель | Факт",
        "Оборот | 100",
    ])

    markdown = attach_canonical_workspace_contract(payload)["workspace_markdown"]

    assert markdown.startswith("# 🧭 Рабочий стол управления — Ситников Микола | 2026-02")
    assert "# 🧭 Рабочий стол управления — Ситников Микола | 2026-02\n| --- | --- |" not in markdown
    assert markdown.count("| --- | --- |") == 1


def test_business_context_has_three_independent_subsections_and_tables():
    payload = _payload()
    payload["workspace_markdown"] = "\n".join([
        "📍 Рабочий стол бизнеса — 2026-02",
        "🌐 Business Context: что отличается внутри бизнеса",
        "Категории | Оборот | Доля бизнеса",
        "Газированные напитки | 100 | 50%",
        "Форматы бизнеса | Оборот | SKU",
        "2 л | 80 | 10",
        "SKU-лидеры бизнеса | Оборот | Финрез ДО",
        "Лимонад | 40 | 8",
        "Комментарий ассистента: карта возможностей бизнеса.",
    ])

    result = attach_canonical_workspace_contract(payload)
    markdown = result["workspace_markdown"]

    assert "## 🌐 Business Context: что отличается внутри бизнеса" in markdown
    assert "### Категории" in markdown
    assert "### Форматы бизнеса" in markdown
    assert "### SKU-лидеры бизнеса" in markdown
    assert result["canonical_workspace"]["presentation"]["tables_count"] == 3
    assert "| Лимонад | 40 | 8 |\n\nКомментарий ассистента:" in markdown

    repeated = attach_canonical_workspace_contract(result)
    assert repeated["workspace_markdown"] == markdown
    assert repeated["canonical_workspace"]["presentation_hash"] == result["canonical_workspace"]["presentation_hash"]


def test_normalization_preserves_runtime_content_and_numbers():
    source_lines = [
        "🧭 Рабочий стол управления — Ситников Микола | 2026-02",
        "📊 Ключевые показатели",
        "Показатель | Текущий период | Прошлый год | Изменение",
        "Оборот | 12 345 678 | 13 000 000 | −654 322",
        "Маржа | 14,2% | 11,8% | +2,4 п.п.",
        "Комментарий ассистента: значения сформированы Runtime.",
        "➡️ Что делаем дальше?",
        "1. Открыть менеджера",
    ]
    payload = _payload()
    payload["workspace_markdown"] = "\n".join(source_lines)

    normalized = attach_canonical_workspace_contract(payload)["workspace_markdown"]
    for source in source_lines:
        if " | " in source and not source.startswith("🧭"):
            assert f"| {source} |" in normalized
        else:
            assert source in normalized


def test_actual_business_shape_keeps_headers_and_structural_category_row():
    payload = _payload()
    payload["workspace_markdown"] = "\n".join([
        "📍 Рабочий стол бизнеса — 2026-02",
        "📊 Ключевые показатели бизнеса",
        "Показатель | Текущий период | Прошлый год | Изменение | Что означает",
        "Оборот | 33 740 715 | 43 892 201 | −10 151 486 | масштаб бизнеса",
        "🏗 Структурный анализ бизнеса",
        "Структура | Сейчас | Прошлый год | Δ | Что означает",
        "Топ-менеджеры | 8 | 8 | +0 | без изменений",
        "Категории | 3 | 4 | −1 | сокращение: состав бизнеса уменьшился",
        "Группы ТМС | 31 | 25 | +6 | расширение",
        "🌐 Business Context: что отличается внутри бизнеса",
        "Категории | Оборот | Доля бизнеса | Финрез ДО | Δ прибыли | Что означает",
        "Напитки | 21 239 143 | 62.95% | +6 413 690 | +2 696 315 | ключевой контур",
        "Форматы бизнеса | Оборот | Доля бизнеса | SKU | Финрез | Управленческий смысл",
        "2 л | 18 352 637 | 54.39% | 32 | +5 724 342 | формат масштаба",
        "SKU-лидеры бизнеса | Оборот | Финрез ДО | Сетей | Зачем смотреть",
        "Лимонад 2л | 3 155 825 | +1 144 215 | 77 | доказательная база",
    ])

    markdown = attach_canonical_workspace_contract(payload)["workspace_markdown"]

    assert "| Показатель | Текущий период | Прошлый год | Изменение | Что означает |" in markdown
    assert "| Структура | Сейчас | Прошлый год | Δ | Что означает |" in markdown
    assert "| Категории | 3 | 4 | −1 | сокращение: состав бизнеса уменьшился |" in markdown
    assert "### Категории\n\n| Категории | Оборот | Доля бизнеса" in markdown
    assert markdown.count("### Категории") == 1
    assert markdown.count("### Форматы бизнеса") == 1
    assert markdown.count("### SKU-лидеры бизнеса") == 1


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
    assert "| Оборот | 100 |\n\n## 🎯" in markdown


def test_contract_nested_blocks_are_headings_and_actions_stay_a_numbered_list():
    payload = _payload()
    payload["context"] = {
        "level": "network",
        "object_name": "АТБ",
        "period": "2026-02",
    }
    payload["workspace_markdown"] = "\n".join([
        "🏢 Контракт — АТБ | 2026-02",
        "📦 Категории в контракте",
        "Категория | Оборот | Финрез",
        "Напитки | 100 | 20",
        "📐 Форматы контракта",
        "Формат | Оборот | Доля контракта | Финрез | SKU | Что делать",
        "2 л | 80 | 80% | 16 | 10 | защитить/масштабировать",
        "🚀 План развития контракта",
        "1. Разобрать категорию с максимальным вкладом.",
        "2. Собрать пакет отсутствующих SKU-лидеров.",
        "3. Проверить экономику условий.",
        "🤝 Переговорный пакет КАМ",
        "Цель: перейти от общего разговора к пакету развития.",
        "✅ Что делаем дальше?",
        "1. Подготовить переговоры по контракту.",
        "2. Собрать пакет SKU для ввода.",
        "3. Разобрать категорию с наибольшим эффектом.",
    ])

    result = attach_canonical_workspace_contract(payload)
    markdown = result["workspace_markdown"]

    assert "## 📦 Категории в контракте" in markdown
    assert "### 📐 Форматы контракта" in markdown
    assert "## 🚀 План развития контракта" in markdown
    assert "### 🤝 Переговорный пакет КАМ" in markdown
    assert "### ✅ Что делаем дальше?" in markdown
    assert "### ✅ Что делаем дальше?\n\n1. Подготовить переговоры по контракту." in markdown
    assert result["canonical_workspace"]["presentation"]["tables_count"] == 2

    repeated = attach_canonical_workspace_contract(result)
    assert repeated["workspace_markdown"] == markdown
    assert repeated["canonical_workspace"]["presentation_hash"] == result["canonical_workspace"]["presentation_hash"]
