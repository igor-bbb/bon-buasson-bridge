import uuid

from app.api import routes
from app.models.request_models import VectraQueryRequest
from app.query import orchestration
from app.workspace_runtime import apply_runtime_contract


def _screen(level="network", object_name="Аврора", period="2026-02"):
    return apply_runtime_contract({
        "status": "ok",
        "render_mode": "contract_workspace",
        "context": {"level": level, "object_name": object_name, "period": period},
        "path": ["Бизнес", "Топ", "Менеджер", object_name],
        "filter": {"period": period, "network": object_name},
        "workspace_markdown": "\n".join([
            f"# {object_name} | {period}",
            "",
            "## Что делаем дальше?",
            "1. Подготовить переговоры по контракту.",
            "2. Создать задачи по контракту.",
            "3. Назад — вернуться к менеджеру.",
        ]),
    })


def test_new_session_does_not_inherit_another_objects_workspace():
    first = f"context-first-{uuid.uuid4().hex}"
    second = f"context-second-{uuid.uuid4().hex}"
    orchestration.update_session(first, {"current_screen": _screen(), "last_payload": _screen()})

    fresh = orchestration.get_session(second)

    assert fresh["current_screen"] is None
    assert fresh["last_payload"] is None
    assert fresh["period_current"] is None


def test_explicit_workspace_state_hydrates_current_screen_for_business_roles():
    session_id = f"context-hydrate-{uuid.uuid4().hex}"
    source = _screen()
    state = source["active_workspace_state"]

    routes._hydrate_runtime_context_from_request(
        session_id,
        VectraQueryRequest(
            message="подготовить переговоры",
            session_id=session_id,
            active_workspace_state=state,
            workspace_action_map=state["action_map"],
        ),
    )

    current = orchestration.get_session(session_id)["current_screen"]
    assert current["context"] == {
        "level": "network",
        "object_name": "Аврора",
        "period": "2026-02",
        "parent_object": None,
    }
    assert current["active_workspace_state"]["action_map"] == state["action_map"]


def test_visible_text_command_and_digit_resolve_to_same_action_map_entry():
    source = _screen()
    session_ctx = {
        "current_screen": source,
        "last_payload": source,
        "view_mode": "drain",
    }

    assert orchestration._visible_workspace_action_number(
        "Подготовить переговоры по контракту",
        session_ctx,
    ) == 1
    assert orchestration.get_action_from_state(source, 1)["action_type"] == "negotiation"


def test_canonical_open_command_preserves_requested_role():
    assert routes._canonical_workspace_open_command("top_manager", "Труш Максим", "2026-02") == \
        "Покажи Труш Максим как топ-менеджера 2026-02"
    assert routes._canonical_workspace_open_command("manager", "Труш Максим", "2026-02") == \
        "Покажи Труш Максим как менеджера 2026-02"
    assert routes._canonical_workspace_open_command("network", "Аврора", "2026-02") == \
        "Покажи контракт Аврора 2026-02"


def test_semantic_priority_action_does_not_confuse_menu_number_with_child_index(monkeypatch):
    source = apply_runtime_contract({
        "status": "ok",
        "render_mode": "contract_workspace",
        "context": {"level": "network", "object_name": "Варус", "period": "2026-02"},
        "path": ["Бизнес", "Топ", "Менеджер", "Варус"],
        "filter": {"period": "2026-02", "network": "Варус"},
        "all_block": [
            {"object_name": "A", "level": "category", "effect_money": 10},
            {"object_name": "B", "level": "category", "effect_money": 90},
            {"object_name": "C", "level": "category", "effect_money": 20},
        ],
        "workspace_markdown": "\n".join([
            "# Варус",
            "## Что делаем дальше?",
            "1. Подготовить переговоры.",
            "2. Создать задачи.",
            "3. Разобрать категорию с наибольшим эффектом.",
        ]),
    })
    session_id = f"priority-action-{uuid.uuid4().hex}"
    orchestration.update_session(session_id, {
        "current_screen": source,
        "last_payload": source,
        "view_mode": "drain",
    })
    opened = {}

    def fake_route(query, _session_id):
        opened.update(query)
        return {"status": "ok", "context": {"level": query["level"], "object_name": query["object_name"], "period": query["period_current"]}}

    monkeypatch.setattr(orchestration, "_route_base_query", fake_route)
    result = orchestration._execute_numeric_workspace_action("3", orchestration.get_session(session_id), session_id)

    assert result["status"] == "ok"
    assert opened["object_name"] == "B"
    assert opened["period_current"] == "2026-02"


def test_free_dialogue_keeps_canonical_workspace_for_next_local_command():
    session_id = f"free-dialogue-continuity-{uuid.uuid4().hex}"
    source = _screen(level="business", object_name="Бизнес", period="2026-02")
    source["reasons_block"] = [{"name": "Логистика", "effect_money": -100}]
    source["reasons_block_render"] = ["Логистика | -100 грн"]
    # The visible Business menu routes the user's question through the same
    # action dispatcher as its numeric free-dialogue item.
    source["workspace_markdown"] = "\n".join([
        "# Бизнес | 2026-02",
        "## Что делаем дальше?",
        "1. Показать причины изменения результата.",
        "2. Спросить ассистента: «что бы ты сделал первым и почему?»",
    ])
    source = apply_runtime_contract(source)
    orchestration.update_session(session_id, {
        "current_screen": source,
        "last_payload": source,
        "view_mode": "drain",
    })

    free_response = routes.vectra_business_query(VectraQueryRequest(
        message="Что бы ты сделал первым и почему?",
        session_id=session_id,
    ))
    import json
    free_payload = json.loads(free_response.body.decode("utf-8"))

    assert free_payload["status"] == "ok"
    assert free_payload["render_mode"] == "voice_diagnostic"
    assert free_payload["active_workspace_state"]["workspace_level"] == "business"
    assert free_payload["active_workspace_state"]["period"] == "2026-02"
    assert len(free_response.body) < 20_000
    preserved = orchestration.get_session(session_id)["current_screen"]
    assert preserved["context"]["level"] == "business"
    assert preserved["context"]["object_name"] == "Бизнес"
    assert preserved["context"]["period"] == "2026-02"

    reasons_response = routes.vectra_business_query(VectraQueryRequest(
        message="Показать причины изменения результата",
        session_id=session_id,
    ))
    reasons_payload = json.loads(reasons_response.body.decode("utf-8"))

    assert reasons_payload["status"] == "ok"
    assert reasons_payload["render_mode"] == "reasons"
    assert reasons_payload["context"]["level"] == "business"
    assert reasons_payload["context"]["object_name"] == "Бизнес"
    assert reasons_payload["context"]["period"] == "2026-02"
