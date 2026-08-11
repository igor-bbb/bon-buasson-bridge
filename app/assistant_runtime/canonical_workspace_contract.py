"""Canonical Professional Workspace contract shared by Business and Laboratory.

The module deliberately does not calculate business metrics.  It takes the
already rendered Runtime payload and publishes one deterministic semantic and
presentation projection for every consumer.
"""
from __future__ import annotations

import hashlib
import re
from copy import deepcopy
from typing import Any, Dict, List


RELEASE_ID = "VECTRA-PROFESSIONAL-WORKSPACE-RUNTIME-STABILITY-001"
CONTRACT_VERSION = "1.0"
SUPPORTED_WORKSPACE_TYPES = ("Business", "Top Manager", "Manager", "Network / Contract")


def _table_separator(line: str) -> str:
    cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
    return "| " + " | ".join("---" for _ in cells) + " |"


def normalize_markdown_tables(markdown: str) -> str:
    """Promote pipe-delimited rows to valid Markdown tables without changing values."""
    lines = str(markdown or "").splitlines()
    out: List[str] = []
    for index, line in enumerate(lines):
        out.append(line)
        if "|" not in line:
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 2 or all(re.fullmatch(r":?-{3,}:?", cell or "") for cell in cells):
            continue
        next_line = lines[index + 1] if index + 1 < len(lines) else ""
        next_cells = [cell.strip() for cell in next_line.strip().strip("|").split("|")]
        has_separator = (
            "|" in next_line
            and len(next_cells) == len(cells)
            and all(re.fullmatch(r":?-{3,}:?", cell or "") for cell in next_cells)
        )
        previous_is_table_row = index > 0 and "|" in lines[index - 1]
        if not has_separator and not previous_is_table_row:
            out.append(_table_separator(line))
    return "\n".join(out).strip()


def _sections(markdown: str) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    current = {"heading": "Workspace", "level": 1, "lines": []}
    for line in markdown.splitlines():
        match = re.match(r"^(#{1,6})\s+(.+?)\s*$", line)
        if match:
            if current["lines"]:
                result.append(current)
            current = {"heading": match.group(2), "level": len(match.group(1)), "lines": [line]}
        else:
            current["lines"].append(line)
    if current["lines"]:
        result.append(current)
    return [
        {
            "section_id": f"section-{index}",
            "heading": item["heading"],
            "heading_level": item["level"],
            "markdown": "\n".join(item["lines"]).strip(),
        }
        for index, item in enumerate(result, start=1)
        if "\n".join(item["lines"]).strip()
    ]


def _presentation_structure(markdown: str) -> Dict[str, Any]:
    sections = _sections(markdown)
    headings = [line for line in markdown.splitlines() if re.match(r"^#{1,6}\s+", line)]
    table_separators = [
        line for line in markdown.splitlines()
        if line.count("|") >= 2 and all(
            re.fullmatch(r":?-{3,}:?", cell.strip() or "")
            for cell in line.strip().strip("|").split("|")
        )
    ]
    return {
        "format": "markdown",
        "renderer": "canonical_markdown_renderer",
        "headings_count": len(headings),
        "tables_count": len(table_separators),
        "sections": sections,
        "required_rendering_rule": "render workspace_markdown verbatim as Markdown",
    }


def attach_canonical_workspace_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return payload
    markdown = payload.get("workspace_markdown")
    if not isinstance(markdown, str) or not markdown.strip():
        return payload
    result = deepcopy(payload)
    normalized = normalize_markdown_tables(markdown)
    result["workspace_markdown"] = normalized
    context = result.get("context") if isinstance(result.get("context"), dict) else {}
    semantic_model = {
        "context": deepcopy(context),
        "path": deepcopy(result.get("path") or []),
        "active_workspace_state": deepcopy(result.get("active_workspace_state") or {}),
        "workspace_action_map": deepcopy(result.get("workspace_action_map") or []),
        "render_mode": result.get("render_mode"),
    }
    semantic_hash = hashlib.sha256(
        repr(sorted(semantic_model.items(), key=lambda item: item[0])).encode("utf-8")
    ).hexdigest()
    presentation_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    result["canonical_workspace"] = {
        "release_id": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "workspace_type": context.get("level") or result.get("render_mode"),
        "business_domain": context.get("business_domain") or "bonboason",
        "period": context.get("period"),
        "object_id": context.get("object_name") or "Бон Буассон",
        "semantic_model": semantic_model,
        "presentation": _presentation_structure(normalized),
        "semantic_hash": semantic_hash,
        "presentation_hash": presentation_hash,
        "read_only": True,
    }
    result["workspace_render_instruction"] = (
        "Показать workspace_markdown полностью как Markdown: заголовки — заголовками, "
        "таблицы — таблицами, разделы — в порядке canonical_workspace.presentation.sections."
    )
    return result
