"""Dependency Manager for Professional Activity Engine.

DEV-0011C moves Product Team Assistant from value-ranked planning to
relationship-aware planning.  The Assistant must understand which professional
work blocks depend on each other, which blocks are ready, which are blocked,
which can be done in parallel and which should be consolidated into one
logical work cycle.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

from app.self_evolution.repository import now_iso
from app.self_evolution.state_manager import load_assistant_state_model, save_assistant_state_model

DEPENDENCY_MANAGER_VERSION = "PAE-0011C.1"

BLOCKING_STATUSES = {
    "blocked",
    "pending",
    "pending_after_deploy",
    "integration_pending_product_acceptance",
    "in_progress",
    "queued",
    "active",
}

READY_STATUSES = {
    "completed",
    "completed_locally_pending_acceptance",
    "accepted",
    "confirmed",
    "done",
}

DEPENDENCY_TYPE_LABELS = {
    "explicit": "explicit dependency",
    "release_sequence": "release sequence dependency",
    "acceptance_gate": "Product Acceptance gate",
    "knowledge_gate": "knowledge integration gate",
    "shared_stage": "shared professional stage",
}


def _as_list(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower().replace("ё", "е")


def _node_id(item: Dict[str, Any]) -> str:
    return str(item.get("id") or item.get("release_id") or item.get("stage") or item.get("title") or "unknown")


def _stage(item: Dict[str, Any]) -> str:
    return str(item.get("stage") or item.get("related_release") or item.get("release_id") or item.get("target_stage") or "general")


def _status(item: Dict[str, Any]) -> str:
    return str(item.get("status") or "active")


def _release_number(value: Any) -> Optional[Tuple[int, str]]:
    text = _normalize_text(value)
    import re

    match = re.search(r"dev[-_ ]?(\d{4})([a-z]?)", text)
    if not match:
        return None
    return int(match.group(1)), match.group(2).upper()


def _release_order_key(value: Any) -> Tuple[int, str]:
    parsed = _release_number(value)
    if parsed:
        return parsed
    return (9999, "")


def _depends_on_ids(item: Dict[str, Any]) -> List[str]:
    deps = item.get("depends_on")
    if isinstance(deps, list):
        return [str(x) for x in deps if x]
    if isinstance(deps, str) and deps.strip():
        return [deps.strip()]
    raw = item.get("raw")
    if isinstance(raw, dict):
        raw_deps = raw.get("depends_on")
        if isinstance(raw_deps, list):
            return [str(x) for x in raw_deps if x]
        if isinstance(raw_deps, str) and raw_deps.strip():
            return [raw_deps.strip()]
    return []


def _item_text(item: Dict[str, Any]) -> str:
    parts = [
        item.get("id"),
        item.get("title"),
        item.get("stage"),
        item.get("status"),
        item.get("next_action"),
        item.get("source"),
        item.get("type"),
    ]
    raw = item.get("raw")
    if isinstance(raw, dict):
        parts.extend(raw.values())
    return " ".join(_normalize_text(p) for p in parts if isinstance(p, (str, int, float)))


def _is_acceptance_item(item: Dict[str, Any]) -> bool:
    text = _item_text(item)
    return item.get("type") == "pending_product_acceptance" or "acceptance" in text or "прием" in text or "приём" in text


def _is_knowledge_item(item: Dict[str, Any]) -> bool:
    text = _item_text(item)
    return item.get("type") == "knowledge_integration" or "knowledge" in text or "знани" in text


def _is_engineering_item(item: Dict[str, Any]) -> bool:
    text = _item_text(item)
    return item.get("type") == "engineering_review" or "engineering" in text or "engineer" in text


def _build_nodes(plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    nodes: Dict[str, Dict[str, Any]] = {}
    for block in _as_list(plan.get("work_blocks")):
        bid = str(block.get("id") or f"WORK-BLOCK-{block.get('stage') or 'general'}")
        nodes[bid] = {
            "id": bid,
            "kind": "work_block",
            "title": str(block.get("title") or bid),
            "stage": str(block.get("stage") or "general"),
            "status": str(block.get("status") or "active"),
            "combined_priority_score": block.get("combined_priority_score"),
            "value_score": block.get("value_score"),
            "depends_on": block.get("depends_on") if isinstance(block.get("depends_on"), list) else [],
            "source": "professional_activity_plan",
            "raw": block,
        }
        for item in _as_list(block.get("items")):
            iid = _node_id(item)
            nodes[iid] = {
                "id": iid,
                "kind": "activity_item",
                "title": str(item.get("title") or iid),
                "stage": _stage(item),
                "status": _status(item),
                "type": str(item.get("type") or item.get("source") or "activity_item"),
                "priority_score": item.get("priority_score"),
                "combined_priority_score": item.get("combined_priority_score"),
                "value_score": item.get("value_score"),
                "depends_on": _depends_on_ids(item),
                "source": str(item.get("source") or "activity_item"),
                "raw": item,
            }
    for item in _as_list(plan.get("activity_items")):
        iid = _node_id(item)
        nodes.setdefault(iid, {
            "id": iid,
            "kind": "activity_item",
            "title": str(item.get("title") or iid),
            "stage": _stage(item),
            "status": _status(item),
            "type": str(item.get("type") or item.get("source") or "activity_item"),
            "priority_score": item.get("priority_score"),
            "combined_priority_score": item.get("combined_priority_score"),
            "value_score": item.get("value_score"),
            "depends_on": _depends_on_ids(item),
            "source": str(item.get("source") or "activity_item"),
            "raw": item,
        })
    return list(nodes.values())


def _edge_key(source: str, target: str, dependency_type: str) -> Tuple[str, str, str]:
    return (str(source), str(target), dependency_type)


def _build_edges(nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_id = {node["id"]: node for node in nodes}
    edges: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    def add_edge(source: str, target: str, dependency_type: str, reason: str, strength: int = 50) -> None:
        if not source or not target or source == target:
            return
        key = _edge_key(source, target, dependency_type)
        edges[key] = {
            "source": source,
            "target": target,
            "dependency_type": dependency_type,
            "label": DEPENDENCY_TYPE_LABELS.get(dependency_type, dependency_type),
            "reason": reason,
            "strength": int(strength),
        }

    # Explicit dependencies from the work item contract.
    for node in nodes:
        for dep in node.get("depends_on") or []:
            dep_text = str(dep)
            target = None
            if dep_text in by_id:
                target = dep_text
            else:
                dep_norm = _normalize_text(dep_text)
                for candidate in nodes:
                    if dep_norm and (dep_norm in _normalize_text(candidate.get("id")) or dep_norm in _normalize_text(candidate.get("title")) or dep_norm in _normalize_text(candidate.get("stage"))):
                        target = candidate["id"]
                        break
            if target:
                add_edge(node["id"], target, "explicit", f"{node['title']} explicitly depends on {dep_text}.", 90)

    # Product Acceptance gates release-specific knowledge and engineering work.
    acceptance_nodes = [n for n in nodes if _is_acceptance_item(n)]
    for node in nodes:
        if _is_acceptance_item(node):
            continue
        for acc in acceptance_nodes:
            if _stage(node) == _stage(acc) or _release_order_key(_stage(node)) == _release_order_key(_stage(acc)):
                add_edge(node["id"], acc["id"], "acceptance_gate", f"{node['title']} should wait for Product Acceptance of {acc.get('stage') or acc.get('title')}.", 85)

    # Knowledge integration should follow accepted/reviewed release work.
    for node in nodes:
        if not _is_knowledge_item(node):
            continue
        for candidate in nodes:
            if candidate["id"] == node["id"]:
                continue
            if _stage(candidate) == _stage(node) and (_is_acceptance_item(candidate) or _is_engineering_item(candidate)):
                add_edge(node["id"], candidate["id"], "knowledge_gate", f"Knowledge integration for {node.get('stage')} depends on acceptance/review completion.", 75)

    # Release sequence: later DEV stages should not complete before earlier active DEV stages.
    release_nodes = [n for n in nodes if _release_number(n.get("stage") or n.get("id") or n.get("title"))]
    release_nodes = sorted(release_nodes, key=lambda n: _release_order_key(n.get("stage") or n.get("id") or n.get("title")))
    for idx, node in enumerate(release_nodes):
        node_key = _release_order_key(node.get("stage") or node.get("id") or node.get("title"))
        for previous in release_nodes[:idx]:
            prev_key = _release_order_key(previous.get("stage") or previous.get("id") or previous.get("title"))
            if prev_key < node_key and _status(previous) in BLOCKING_STATUSES:
                add_edge(node["id"], previous["id"], "release_sequence", f"{node.get('stage')} follows unfinished {previous.get('stage')}.", 65)
                break

    # Shared stage: related items should be consolidated into the same professional work block.
    by_stage: Dict[str, List[Dict[str, Any]]] = {}
    for node in nodes:
        by_stage.setdefault(_stage(node), []).append(node)
    for stage, stage_nodes in by_stage.items():
        if stage == "general" or len(stage_nodes) < 2:
            continue
        anchor = sorted(stage_nodes, key=lambda n: float(n.get("combined_priority_score") or n.get("priority_score") or 0), reverse=True)[0]
        for node in stage_nodes:
            if node["id"] != anchor["id"]:
                add_edge(node["id"], anchor["id"], "shared_stage", f"{node['title']} belongs to the same professional stage as {anchor['title']}.", 35)

    return sorted(edges.values(), key=lambda e: e.get("strength", 0), reverse=True)


def _incoming_targets(edges: List[Dict[str, Any]]) -> Set[str]:
    return {str(edge.get("target")) for edge in edges if edge.get("target")}


def _outgoing_sources(edges: List[Dict[str, Any]]) -> Set[str]:
    return {str(edge.get("source")) for edge in edges if edge.get("source")}


def _node_readiness(node: Dict[str, Any], edges: List[Dict[str, Any]], node_by_id: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    blockers = [edge for edge in edges if edge.get("source") == node.get("id") and edge.get("dependency_type") in {"explicit", "acceptance_gate", "knowledge_gate", "release_sequence"}]
    unresolved = []
    for edge in blockers:
        target = node_by_id.get(str(edge.get("target")))
        target_status = _status(target or {})
        if target_status not in READY_STATUSES:
            unresolved.append({
                "blocker_id": edge.get("target"),
                "blocker_title": (target or {}).get("title") or edge.get("target"),
                "blocker_status": target_status,
                "reason": edge.get("reason"),
            })
    if unresolved:
        return {"readiness": "blocked", "unresolved_blockers": unresolved}
    if _status(node) in READY_STATUSES:
        return {"readiness": "completed_or_ready_for_acceptance", "unresolved_blockers": []}
    return {"readiness": "ready", "unresolved_blockers": []}


def _parallel_candidates(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]], node_by_id: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    candidates = []
    for node in nodes:
        readiness = _node_readiness(node, edges, node_by_id)
        if readiness["readiness"] == "ready":
            candidates.append({
                "id": node.get("id"),
                "title": node.get("title"),
                "stage": node.get("stage"),
                "combined_priority_score": node.get("combined_priority_score") or node.get("priority_score") or 0,
            })
    return sorted(candidates, key=lambda x: float(x.get("combined_priority_score") or 0), reverse=True)


def _consolidation_groups(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_stage: Dict[str, List[Dict[str, Any]]] = {}
    for node in nodes:
        stage = _stage(node)
        if stage != "general":
            by_stage.setdefault(stage, []).append(node)
    groups = []
    for stage, stage_nodes in by_stage.items():
        if len(stage_nodes) < 2:
            continue
        groups.append({
            "id": f"CONSOLIDATE-{stage}",
            "stage": stage,
            "title": f"Consolidated professional work block: {stage}",
            "items": [{"id": n.get("id"), "title": n.get("title"), "status": n.get("status")} for n in stage_nodes],
            "reason": "Related work items share the same release/stage and should be managed as one logical professional work block.",
            "recommended_action": "Complete the consolidated block before opening a new architectural direction.",
        })
    return sorted(groups, key=lambda g: len(g.get("items") or []), reverse=True)


def evaluate_dependency_map(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Build the dependency graph and persist it into Assistant State."""
    plan = dict(plan or {})
    nodes = _build_nodes(plan)
    edges = _build_edges(nodes)
    node_by_id = {node["id"]: node for node in nodes}

    readiness = []
    for node in nodes:
        r = _node_readiness(node, edges, node_by_id)
        readiness.append({
            "id": node.get("id"),
            "title": node.get("title"),
            "stage": node.get("stage"),
            "status": node.get("status"),
            **r,
        })

    blocked = [item for item in readiness if item.get("readiness") == "blocked"]
    ready = [item for item in readiness if item.get("readiness") == "ready"]
    consolidation_groups = _consolidation_groups(nodes, edges)
    parallel_candidates = _parallel_candidates(nodes, edges, node_by_id)

    next_block = plan.get("next_work_block") or plan.get("next_value_block") or {}
    next_node_id = str(next_block.get("id") or "")
    next_readiness = next((x for x in readiness if x.get("id") == next_node_id), None)
    if next_readiness and next_readiness.get("readiness") == "blocked":
        first_blocker = (next_readiness.get("unresolved_blockers") or [{}])[0]
        next_action = f"Resolve blocker {first_blocker.get('blocker_title') or first_blocker.get('blocker_id')} before starting {next_block.get('title') or next_node_id}."
    elif consolidation_groups:
        next_action = f"Complete consolidated work block {consolidation_groups[0].get('stage')} before opening a new architectural direction."
    elif parallel_candidates:
        next_action = f"Proceed with ready work block {parallel_candidates[0].get('title')} while monitoring dependencies."
    else:
        next_action = "No dependency-ready professional work block is currently available."

    evaluation = {
        "status": "ok",
        "engine": "Dependency Manager",
        "release_stage": "DEV-0011C",
        "dependency_manager_version": DEPENDENCY_MANAGER_VERSION,
        "principle": "Product Team Assistant manages professional activity as a dependency-aware system, not as isolated tasks or value scores.",
        "nodes": nodes,
        "edges": edges,
        "readiness": sorted(readiness, key=lambda x: (x.get("readiness") != "blocked", str(x.get("stage")))) ,
        "blocked_items": blocked,
        "ready_items": ready,
        "parallel_candidates": parallel_candidates,
        "consolidation_groups": consolidation_groups,
        "next_dependency_action": next_action,
        "dependency_summary": {
            "nodes_count": len(nodes),
            "edges_count": len(edges),
            "blocked_count": len(blocked),
            "ready_count": len(ready),
            "consolidation_groups_count": len(consolidation_groups),
        },
        "updated_at": now_iso(),
    }
    persist_dependency_evaluation(evaluation)
    return evaluation


def persist_dependency_evaluation(evaluation: Dict[str, Any]) -> Dict[str, Any]:
    state = load_assistant_state_model()
    manager = state.setdefault("state_manager", {})
    manager["dependency_manager"] = {
        "dependency_manager_version": DEPENDENCY_MANAGER_VERSION,
        "last_dependency_evaluation_at": evaluation.get("updated_at") or now_iso(),
        "dependency_summary": evaluation.get("dependency_summary") or {},
        "next_dependency_action": evaluation.get("next_dependency_action"),
        "consolidation_groups": evaluation.get("consolidation_groups") or [],
        "blocked_items": evaluation.get("blocked_items") or [],
        "ready_items": evaluation.get("ready_items") or [],
    }
    manager["professional_dependency_model"] = {
        "principle": evaluation.get("principle"),
        "selection_rule": "Before starting a work block, resolve blockers, consolidate related work and replan after each completed block.",
        "dependency_types": list(DEPENDENCY_TYPE_LABELS.keys()),
    }
    save_assistant_state_model(state)
    return state


def build_dependency_response(evaluation: Dict[str, Any]) -> Dict[str, Any]:
    summary = evaluation.get("dependency_summary") or {}
    groups = evaluation.get("consolidation_groups") or []
    blocked = evaluation.get("blocked_items") or []
    lines = [
        "# Dependency Manager",
        "",
        "Статус: карта зависимостей профессиональной работы сформирована.",
        "",
        "Что теперь умеет Assistant:",
        "- видеть связи между рабочими блоками;",
        "- отличать готовую работу от заблокированной;",
        "- объединять связанные изменения в один логический цикл;",
        "- не начинать работу, которая зависит от незавершённого решения;",
        "- восстанавливать карту зависимостей вместе с профессиональным состоянием.",
        "",
        f"Узлов: {summary.get('nodes_count', 0)}",
        f"Связей: {summary.get('edges_count', 0)}",
        f"Заблокировано: {summary.get('blocked_count', 0)}",
        f"Групп для объединения: {summary.get('consolidation_groups_count', 0)}",
        "",
        f"Следующее действие: {evaluation.get('next_dependency_action') or '—'}",
    ]
    if groups:
        lines.extend(["", "Главный объединённый блок:", f"- {groups[0].get('title')} — {groups[0].get('recommended_action')}"])
    if blocked:
        lines.extend(["", "Главный блокер:", f"- {blocked[0].get('title')} — {(blocked[0].get('unresolved_blockers') or [{}])[0].get('reason', 'requires previous work completion')}"])
    return {
        "status": evaluation.get("status", "ok"),
        "render_mode": "self_evolution",
        "workspace_markdown": "\n".join(lines),
        "dependency_evaluation": evaluation,
        "documentation_sync": {
            "vectra_instruction": "not_required",
            "product_team_assistant_architecture": "required",
            "engineering_documentation": "required",
        },
    }
