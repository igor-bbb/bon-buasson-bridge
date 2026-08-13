import uuid

from app.api import routes
from app.assistant_runtime.business_object_discovery import OBJECT_TYPES
from app.query import orchestration
from app.workspace_runtime import apply_runtime_contract, classify_action_label


def _workspace(level, object_name, *, path, filters, children, menu):
    return apply_runtime_contract({
        "status": "ok",
        "render_mode": f"{level}_workspace",
        "context": {"level": level, "object_name": object_name, "period": "2026-02"},
        "path": path,
        "filter": filters,
        "all_block": children,
        "workspace_markdown": "\n".join([
            f"# {object_name} | 2026-02",
            "",
            "## Что делаем дальше?",
            *menu,
        ]),
    })


def test_product_navigation_actions_have_dedicated_runtime_types():
    assert classify_action_label("Выбрать другой контракт / сеть из своего портфеля.") == "select_network"
    assert classify_action_label("Открыть продуктовую ветку и выбрать SKU.") == "product_navigation"
    assert classify_action_label("Отфильтровать SKU по формату.") == "format_filter"
    assert "format" not in {item["object_type"] for item in OBJECT_TYPES}


def test_rendered_workspaces_publish_the_new_visible_actions(monkeypatch):
    monkeypatch.setattr(routes, "_pr_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(routes, "_pr_structural_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(routes, "_pr_trend_lines", lambda *args, **kwargs: [])
    monkeypatch.setattr(routes, "_w3_factor_evidence_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(routes, "_w3_contract_factor_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(routes, "_pr_group_table", lambda *args, **kwargs: [])
    monkeypatch.setattr(routes, "_pr_business_sku_leaders", lambda *args, **kwargs: [])

    manager = routes._pr_management_workspace_block({
        "context": {"level": "manager", "object_name": "Труш Максим", "period": "2026-02"},
        "metrics": [],
    })
    contract = routes._pr_contract_workspace_block({
        "context": {"level": "network", "object_name": "Аврора", "period": "2026-02"},
        "metrics": [],
    })
    category = routes._w3_category_workspace_block({
        "context": {"level": "category", "object_name": "Напитки", "period": "2026-02"},
        "metrics": [],
        "path": ["Бизнес", "Труш Максим", "Аврора", "Напитки"],
    })

    assert "7. Выбрать другой контракт / сеть из своего портфеля." in manager
    assert "7. Открыть продуктовую ветку и выбрать SKU." in contract
    assert "6. Отфильтровать SKU по формату." in category


def test_manager_can_select_any_visible_network_without_losing_context(monkeypatch):
    session_id = f"product-network-selector-{uuid.uuid4().hex}"
    screen = _workspace(
        "manager",
        "Труш Максим",
        path=["Бизнес", "Ситников Микола", "Труш Максим"],
        filters={
            "period": "2026-02",
            "manager_top": "Ситников Микола",
            "manager": "Труш Максим",
        },
        children=[
            {"object_name": "Варус", "level": "network"},
            {"object_name": "Аврора", "level": "network"},
        ],
        menu=[
            "1. Показать причины.",
            "7. Выбрать другой контракт / сеть из своего портфеля.",
        ],
    )
    orchestration.update_session(session_id, {
        "current_screen": screen,
        "last_payload": screen,
        "view_mode": "drain",
    })

    selector = orchestration._execute_numeric_workspace_action(
        "7", orchestration.get_session(session_id), session_id
    )
    assert selector["status"] == "ok"
    assert selector["render_mode"] == "list_only"
    assert orchestration.get_session_state(orchestration.get_session(session_id))["view_mode"] == "all"

    parsed = orchestration._build_query_from_numeric_selection(
        "2", orchestration.get_session(session_id)
    )
    query = parsed["query"]
    assert query["level"] == "network"
    assert query["object_name"] == "Аврора"
    assert query["period_current"] == "2026-02"
    assert query["filter_payload"]["manager_top"] == "Ситников Микола"
    assert query["filter_payload"]["manager"] == "Труш Максим"
    assert query["filter_payload"]["network"] == "Аврора"


def test_contract_product_branch_opens_category_selector_in_current_scope():
    session_id = f"product-contract-branch-{uuid.uuid4().hex}"
    screen = _workspace(
        "network",
        "Аврора",
        path=["Бизнес", "Ситников Микола", "Труш Максим", "Аврора"],
        filters={
            "period": "2026-02",
            "manager_top": "Ситников Микола",
            "manager": "Труш Максим",
            "network": "Аврора",
        },
        children=[
            {"object_name": "Напитки", "level": "category"},
            {"object_name": "Вода", "level": "category"},
        ],
        menu=[
            "1. Подготовить переговоры по контракту.",
            "7. Открыть продуктовую ветку и выбрать SKU.",
        ],
    )
    orchestration.update_session(session_id, {
        "current_screen": screen,
        "last_payload": screen,
        "view_mode": "drain",
    })

    selector = orchestration._execute_numeric_workspace_action(
        "7", orchestration.get_session(session_id), session_id
    )
    assert selector["render_mode"] == "list_only"
    parsed = orchestration._build_query_from_numeric_selection(
        "1", orchestration.get_session(session_id)
    )
    query = parsed["query"]
    assert query["level"] == "category"
    assert query["object_name"] == "Напитки"
    assert query["period_current"] == "2026-02"
    assert query["filter_payload"]["network"] == "Аврора"
    assert query["filter_payload"]["category"] == "Напитки"


def test_visible_category_text_and_number_use_identical_canonical_filter(monkeypatch):
    session_id = f"product-category-text-{uuid.uuid4().hex}"
    screen = _workspace(
        "network",
        "Аврора",
        path=["Бизнес", "Ситников Микола", "Кіряхно Дмитро", "Аврора"],
        filters={
            "period": "2026-02",
            "manager_top": "Ситников Микола",
            "manager": "Кіряхно Дмитро",
            "network": "Аврора",
        },
        children=[
            {"object_name": "Напитки", "level": "category"},
            {"object_name": "Вода", "level": "category"},
        ],
        menu=["7. Открыть продуктовую ветку и выбрать SKU."],
    )
    orchestration.update_session(session_id, {
        "current_screen": screen,
        "last_payload": screen,
        "view_mode": "drain",
    })
    orchestration._execute_numeric_workspace_action(
        "7", orchestration.get_session(session_id), session_id
    )

    numeric = orchestration._build_query_from_numeric_selection(
        "1", orchestration.get_session(session_id)
    )["query"]
    captured = {}

    def route(query, _session_id):
        captured.update(query)
        return _workspace(
            "category",
            query["object_name"],
            path=["Бизнес", "Ситников Микола", "Кіряхно Дмитро", "Аврора", "Напитки"],
            filters=query["filter_payload"],
            children=[],
            menu=["6. Отфильтровать SKU по формату."],
        )

    monkeypatch.setattr(orchestration, "_route_base_query", route)
    monkeypatch.setattr(
        orchestration,
        "_maybe_execute_direct_object_opening",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("global resolver must not run")),
    )
    result = orchestration.orchestrate_vectra_query("Напитки", session_id=session_id)

    assert result["status"] == "ok"
    assert captured["level"] == numeric["level"] == "category"
    assert captured["object_name"] == numeric["object_name"] == "Напитки"
    assert captured["period_current"] == numeric["period_current"] == "2026-02"
    assert captured["filter_payload"] == numeric["filter_payload"]
    assert captured["filter_payload"]["manager"] == "Кіряхно Дмитро"
    assert captured["filter_payload"]["network"] == "Аврора"


def test_format_is_a_derived_filter_and_filtered_sku_keeps_full_scope(monkeypatch):
    session_id = f"product-format-filter-{uuid.uuid4().hex}"
    screen = _workspace(
        "category",
        "Напитки",
        path=["Бизнес", "Ситников Микола", "Труш Максим", "Аврора", "Напитки"],
        filters={
            "period": "2026-02",
            "manager_top": "Ситников Микола",
            "manager": "Труш Максим",
            "network": "Аврора",
            "category": "Напитки",
        },
        children=[],
        menu=[
            "1. Подготовить пакет развития категории.",
            "6. Отфильтровать SKU по формату.",
        ],
    )
    rows = [
        {"period": "2026-02", "manager_top": "Ситников Микола", "manager": "Труш Максим", "network": "Аврора", "category": "Напитки", "tmc_group": "Газированные напитки 2 л", "sku": "Лимонад 2 л", "revenue": 300, "finrez_pre": 60},
        {"period": "2026-02", "manager_top": "Ситников Микола", "manager": "Труш Максим", "network": "Аврора", "category": "Напитки", "tmc_group": "Газированные напитки 2 л", "sku": "Тархун 2 л", "revenue": 200, "finrez_pre": 40},
        {"period": "2026-02", "manager_top": "Ситников Микола", "manager": "Труш Максим", "network": "Аврора", "category": "Напитки", "tmc_group": "Энергетики 0,5 л", "sku": "Black 0,5 л", "revenue": 100, "finrez_pre": 10},
    ]
    monkeypatch.setattr(orchestration, "_filter_rows_safe", lambda _flt: rows)
    orchestration.update_session(session_id, {
        "current_screen": screen,
        "last_payload": screen,
        "view_mode": "drain",
    })

    selector = orchestration._execute_numeric_workspace_action(
        "6", orchestration.get_session(session_id), session_id
    )
    assert selector["status"] == "ok"
    assert selector["format_filter"] == {
        "mode": "derived_filter",
        "object_type_created": False,
        "formats_count": 2,
    }
    assert selector["context"]["level"] == "category"

    parsed_format = orchestration._build_query_from_numeric_selection(
        "1", orchestration.get_session(session_id)
    )
    assert parsed_format["query"]["query_type"] == "format_filter"
    sku_list = orchestration._route_format_filter_query(parsed_format["query"], session_id)
    assert sku_list["status"] == "ok"
    assert sku_list["format_filter"]["object_type_created"] is False
    assert sku_list["context"]["level"] == "category"

    parsed_sku = orchestration._build_query_from_numeric_selection(
        "1", orchestration.get_session(session_id)
    )
    sku_query = parsed_sku["query"]
    assert sku_query["level"] == "sku"
    assert sku_query["period_current"] == "2026-02"
    assert sku_query["filter_payload"]["manager_top"] == "Ситников Микола"
    assert sku_query["filter_payload"]["manager"] == "Труш Максим"
    assert sku_query["filter_payload"]["network"] == "Аврора"
    assert sku_query["filter_payload"]["category"] == "Напитки"
    assert sku_query["filter_payload"]["sku"] in {"Лимонад 2 л", "Тархун 2 л"}


def test_back_from_sku_restores_executable_category_workspace(monkeypatch):
    session_id = f"product-category-back-{uuid.uuid4().hex}"
    category_filter = {
        "period": "2026-02",
        "manager_top": "Ситников Микола",
        "manager": "Кіряхно Дмитро",
        "network": "Аврора",
        "category": "Напитки",
    }
    category = _workspace(
        "category",
        "Напитки",
        path=["Бизнес", "Ситников Микола", "Кіряхно Дмитро", "Аврора", "Напитки"],
        filters=category_filter,
        children=[],
        menu=["6. Отфильтровать SKU по формату."],
    )
    filtered_list = {
        "status": "ok",
        "render_mode": "list_only",
        "context": category["context"],
        "path": category["path"],
        "workspace_markdown": "Витрина SKU",
    }
    orchestration.update_session(session_id, {
        "scope_level": "category",
        "scope_object_name": "Напитки",
        "period_current": "2026-02",
        "filter": category_filter,
        "current_screen": category,
        "last_payload": filtered_list,
        "view_mode": "all",
        "show_all": True,
        "full_view": True,
        "last_list_items": [{"object_name": "Orange fresh 0,5л", "level": "sku"}],
    })
    orchestration.push_state(session_id)
    sku = _workspace(
        "sku",
        "Orange fresh 0,5л",
        path=category["path"] + ["Orange fresh 0,5л"],
        filters={**category_filter, "sku": "Orange fresh 0,5л"},
        children=[],
        menu=["3. Назад."],
    )
    orchestration.update_session(session_id, {
        "scope_level": "sku",
        "scope_object_name": "Orange fresh 0,5л",
        "filter": {**category_filter, "sku": "Orange fresh 0,5л"},
        "current_screen": sku,
        "last_payload": sku,
        "view_mode": "drain",
        "show_all": False,
    })

    restored = orchestration._handle_back(session_id)
    state = orchestration.get_session_state(orchestration.get_session(session_id))
    assert restored["status"] == "ok"
    assert restored["context"]["level"] == "category"
    assert state["level"] == "category"
    assert state["object_name"] == "Напитки"
    assert state["view_mode"] == "drain"
    assert state["show_all"] is False
    assert state["last_list_items"] == []
    assert state["last_payload"]["context"]["level"] == "category"

    rows = [
        {**category_filter, "tmc_group": "Энергетики 0,5 л", "sku": "Orange fresh 0,5л", "revenue": 100, "finrez_pre": 20},
    ]
    monkeypatch.setattr(orchestration, "_filter_rows_safe", lambda _flt: rows)
    selector = orchestration._execute_numeric_workspace_action(
        "6", orchestration.get_session(session_id), session_id
    )
    assert selector["status"] == "ok"
    assert selector["context"]["level"] == "category"
    assert selector["format_filter"]["object_type_created"] is False
