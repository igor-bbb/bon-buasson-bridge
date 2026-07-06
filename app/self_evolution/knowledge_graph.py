"""Knowledge Graph for SEE DEV-0009B with classification links."""

from __future__ import annotations

from typing import Any, Dict, List

from app.self_evolution.repository import load_graph, save_graph, now_iso


def upsert_decision_graph(entry: Dict[str, Any]) -> Dict[str, Any]:
    graph = load_graph()
    nodes = graph.setdefault("nodes", [])
    edges = graph.setdefault("edges", [])

    decision_id = f"decision:{entry.get('id')}"
    _upsert_node(nodes, {"id": decision_id, "type": "product_decision", "label": entry.get("decision"), "knowledge_type": entry.get("knowledge_type"), "knowledge_status": entry.get("knowledge_status"), "updated_at": now_iso()})

    knowledge_type = entry.get("knowledge_type")
    if knowledge_type:
        type_id = f"knowledge_type:{knowledge_type}"
        _upsert_node(nodes, {"id": type_id, "type": "knowledge_type", "label": entry.get("knowledge_type_label") or knowledge_type, "updated_at": now_iso()})
        _upsert_edge(edges, {"from": decision_id, "to": type_id, "type": "classified_as"})

    status = entry.get("knowledge_status")
    if status:
        status_id = f"knowledge_status:{status}"
        _upsert_node(nodes, {"id": status_id, "type": "knowledge_status", "label": status, "updated_at": now_iso()})
        _upsert_edge(edges, {"from": decision_id, "to": status_id, "type": "has_status"})

    for doc in entry.get("related_documents") or []:
        doc_id = f"document:{doc}"
        _upsert_node(nodes, {"id": doc_id, "type": "document", "label": doc, "updated_at": now_iso()})
        _upsert_edge(edges, {"from": decision_id, "to": doc_id, "type": "affects"})

    model_version = entry.get("new_model_version")
    if model_version:
        version_id = f"model_version:{model_version}"
        _upsert_node(nodes, {"id": version_id, "type": "model_version", "label": model_version, "updated_at": now_iso()})
        _upsert_edge(edges, {"from": decision_id, "to": version_id, "type": "creates"})

    graph["updated_at"] = now_iso()
    save_graph(graph)
    return graph


def _upsert_node(nodes: List[Dict[str, Any]], node: Dict[str, Any]) -> None:
    for idx, current in enumerate(nodes):
        if isinstance(current, dict) and current.get("id") == node.get("id"):
            current.update(node)
            nodes[idx] = current
            return
    nodes.append(node)


def _upsert_edge(edges: List[Dict[str, Any]], edge: Dict[str, Any]) -> None:
    key = (edge.get("from"), edge.get("to"), edge.get("type"))
    for current in edges:
        if isinstance(current, dict) and (current.get("from"), current.get("to"), current.get("type")) == key:
            return
    edges.append(edge)
