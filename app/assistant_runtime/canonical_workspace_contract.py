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


RELEASE_ID = "VECTRA-PROFESSIONAL-PRODUCT-NAVIGATION-001"
CONTRACT_VERSION = "2.4.0"
SUPPORTED_WORKSPACE_TYPES = ("business", "top_manager", "manager", "network", "contract")
_WORKSPACE_TYPE_ALIASES = {
    "business": "business",
    "top_manager": "top_manager",
    "top manager": "top_manager",
    "manager": "manager",
    "network": "network",
    "contract": "contract",
    "network / contract": "network",
    "network_contract": "network",
}

_SECTION_ICONS = "📊📈📉🧭🎯💡⚠️🚨🏢👤👥🌐📦🧾🧬⭐➕🗣🚀➡️🔎💰🧩🧠🏗💵🧲📍🤖"
_FINAL_SECTION_LABELS = {"Что делаем дальше?", "Что делаем дальше", "Что я бы сделал первым"}
_NESTED_TABLE_SECTIONS = {
    "Категории": "Категории",
    "Форматы бизнеса": "Форматы бизнеса",
    "SKU-лидеры бизнеса": "SKU-лидеры бизнеса",
}
_CONTRACT_NESTED_SECTIONS = {
    "📐 Форматы контракта": "📐 Форматы контракта",
    "🤝 Переговорный пакет КАМ": "🤝 Переговорный пакет КАМ",
    "✅ Что делаем дальше?": "✅ Что делаем дальше?",
}


def normalize_workspace_type(value: str) -> str:
    """Return the public workspace type while accepting legacy aliases."""
    return _WORKSPACE_TYPE_ALIASES.get(str(value or "").strip().lower(), "")


def _table_separator(line: str) -> str:
    cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
    return "| " + " | ".join("---" for _ in cells) + " |"


def _is_separator(line: str) -> bool:
    cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
    return len(cells) >= 2 and all(re.fullmatch(r":?-{3,}:?", cell or "") for cell in cells)


def _is_table_row(line: str) -> bool:
    if "|" not in line or line.lstrip().startswith(("#", "- ", "* ")):
        return False
    cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
    if len(cells) < 2:
        return False
    # Runtime uses this as object context, not as a two-column table row.
    if cells[0].lower().startswith("период:"):
        return False
    return True


def _canonical_table_row(line: str) -> str:
    """Return one unambiguous, fully bounded Markdown table row."""
    cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
    return "| " + " | ".join(cells) + " |"


def normalize_markdown_tables(markdown: str) -> str:
    """Build bounded Markdown tables and remove orphan separators.

    Only presentation punctuation is added or removed. Cell values and their
    order remain untouched.
    """
    lines = str(markdown or "").splitlines()
    out: List[str] = []
    index = 0
    while index < len(lines):
        if not _is_table_row(lines[index]):
            # A separator outside a table is invalid presentation noise.
            if not _is_separator(lines[index]):
                out.append(lines[index])
            index += 1
            continue

        table: List[str] = []
        while index < len(lines) and _is_table_row(lines[index]):
            table.append(lines[index])
            index += 1
        table = [line for pos, line in enumerate(table) if not _is_separator(line) or pos == 1]
        if not table or _is_separator(table[0]):
            continue
        if len(table) == 1 or not _is_separator(table[1]):
            table.insert(1, _table_separator(table[0]))
        if out and out[-1].strip():
            out.append("")
        # Fully bound every row.  Mixed rows (header without edge pipes plus a
        # separator with edge pipes) are interpreted inconsistently by Custom
        # GPT rendering and can collapse all header labels into the first cell.
        out.extend(_canonical_table_row(line) for line in table)
        if index < len(lines) and lines[index].strip():
            out.append("")
    return "\n".join(out).strip()


def normalize_markdown_headings(markdown: str) -> str:
    """Promote Runtime labels before table detection.

    The order matters: some workspace titles contain ``| period``.  If table
    parsing runs first, that title is incorrectly converted into a two-column
    table.  Business Context table headers also carry the subsection name in
    their first cell, so an H3 boundary is inserted without changing the row.
    """
    lines = markdown.splitlines()
    first_content = next((i for i, line in enumerate(lines) if line.strip()), None)
    out: List[str] = []
    current_section = ""
    for index, line in enumerate(lines):
        stripped = line.strip()
        promoted = line
        if stripped and not stripped.startswith("#"):
            if index == first_content and not stripped.startswith(("Период:", "Комментарий")):
                promoted = f"# {stripped}"
            elif stripped in _CONTRACT_NESTED_SECTIONS:
                promoted = f"### {_CONTRACT_NESTED_SECTIONS[stripped]}"
            elif stripped.startswith(tuple(_SECTION_ICONS)) or stripped.rstrip(":") in _FINAL_SECTION_LABELS:
                promoted = f"## {stripped.rstrip(':')}"

        heading_match = re.match(r"^##\s+(.+?)\s*$", promoted.strip())
        if heading_match:
            current_section = heading_match.group(1)

        # A subsection label is currently embedded in the first header cell.
        # Add a real visual boundary but preserve the complete original header
        # row and every value exactly as Runtime produced them.
        if _is_table_row(stripped) and "business context" in current_section.lower():
            first_cell = stripped.strip("|").split("|", 1)[0].strip()
            subsection = _NESTED_TABLE_SECTIONS.get(first_cell)
            if subsection:
                subsection_heading = f"### {subsection}"
                last_content = next((item.strip() for item in reversed(out) if item.strip()), "")
                if last_content != subsection_heading:
                    if out and out[-1].strip():
                        out.append("")
                    out.extend([subsection_heading, ""])
        if promoted.lstrip().startswith("#") and out and out[-1].strip():
            out.append("")
        out.append(promoted)
        if promoted.lstrip().startswith("#"):
            out.append("")
    # Collapse presentation-only repeated blank lines.
    compact: List[str] = []
    for line in out:
        if not line.strip() and compact and not compact[-1].strip():
            continue
        compact.append(line)
    return "\n".join(compact).strip()


def normalize_workspace_markdown(markdown: str) -> str:
    # Headings must be established first so that titles containing a pipe and
    # nested Business Context sections become hard table boundaries.
    return normalize_markdown_tables(normalize_markdown_headings(markdown))


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
    normalized = normalize_workspace_markdown(markdown)
    result["workspace_markdown"] = normalized
    # Publish the same normalized surface through both legacy and canonical
    # presentation fields.  Older Business clients read workspace_primary_block
    # while Laboratory and newer clients read workspace_markdown.
    result["workspace_primary_block"] = normalized.splitlines()
    result["screen_order"] = ["workspace_markdown"]
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
        "Вывести только workspace_markdown полностью и без пересборки: заголовки — заголовками, "
        "каждую таблицу — отдельно, комментарии — вне таблиц, действия — нумерованным списком."
    )
    return result
