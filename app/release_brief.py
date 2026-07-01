"""Release Brief support for VECTRA Autonomous Development Bridge.

Release Brief replaces separate CHANGE LOG, TEST PLAN and Implementation Report
as the single working document for Release Manager. It is not intended for
Product Owner analysis; Product Owner receives only a human Product Owner Report
after Release Manager completes acceptance.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
from typing import Any, Dict, List, Optional


DEFAULT_RELEASE_BRIEF_ID = "RELEASE_BRIEF_MANUAL"


@dataclass
class ReleaseBrief:
    release_id: str = "manual-release"
    build: str = "manual-build"
    sprint: str = "manual"
    build_date: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    previous_release: str = "unknown"
    implemented: List[str] = field(default_factory=list)
    changed: List[str] = field(default_factory=list)
    mandatory_scenarios: List[str] = field(default_factory=list)
    regression_scope: List[str] = field(default_factory=list)
    fixed_engineering_tasks: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "release_id": self.release_id,
            "build": self.build,
            "sprint": self.sprint,
            "build_date": self.build_date,
            "previous_release": self.previous_release,
            "implemented": list(self.implemented),
            "changed": list(self.changed),
            "mandatory_scenarios": list(self.mandatory_scenarios),
            "regression_scope": list(self.regression_scope),
            "fixed_engineering_tasks": list(self.fixed_engineering_tasks),
            "notes": list(self.notes),
        }


def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, tuple):
        return [str(x).strip() for x in value if str(x).strip()]
    text = str(value).strip()
    if not text:
        return []
    # Markdown bullet friendly parser for simple Release Brief paste payloads.
    lines = [line.strip(" -\t") for line in text.splitlines()]
    return [line for line in lines if line]



def normalize_task_ids(items: Any) -> List[str]:
    out: List[str] = []
    for item in _as_list(items):
        for match in re.finditer(r'DEV-?\d{1,6}', item, flags=re.IGNORECASE):
            raw = match.group(0)
            num_match = re.search(r'\d+', raw)
            if num_match:
                out.append(f'DEV-{int(num_match.group(0)):04d}')
    return sorted(set(out))

def parse_release_brief(raw: Any, fallback_release_id: str = "manual-release") -> ReleaseBrief:
    """Normalize Release Brief from dict, markdown-like string or absent value."""
    if isinstance(raw, ReleaseBrief):
        return raw
    if isinstance(raw, dict):
        info = raw.get("release_information") if isinstance(raw.get("release_information"), dict) else raw
        return ReleaseBrief(
            release_id=str(info.get("release_id") or raw.get("release_id") or fallback_release_id),
            build=str(info.get("build") or raw.get("build") or "manual-build"),
            sprint=str(info.get("sprint") or raw.get("sprint") or "manual"),
            build_date=str(info.get("build_date") or raw.get("build_date") or datetime.now(timezone.utc).isoformat()),
            previous_release=str(info.get("previous_release") or raw.get("previous_release") or "unknown"),
            implemented=_as_list(raw.get("implemented") or raw.get("what_implemented") or raw.get("engineering_summary")),
            changed=_as_list(raw.get("changed") or raw.get("affected_components") or raw.get("what_changed")),
            mandatory_scenarios=_as_list(raw.get("mandatory_scenarios") or raw.get("mandatory_acceptance") or raw.get("test_plan")),
            regression_scope=_as_list(raw.get("regression_scope")),
            fixed_engineering_tasks=normalize_task_ids(raw.get("fixed_engineering_tasks") or raw.get("fixed_tasks") or raw.get("resolved_engineering_tasks") or raw.get("исправленные_инженерные_задачи")),
            notes=_as_list(raw.get("notes") or raw.get("release_notes")),
        )
    text = str(raw or "").strip()
    if not text:
        return ReleaseBrief(
            release_id=fallback_release_id,
            build="manual-build",
            sprint="manual",
            implemented=["Release Manager acceptance requested without explicit Release Brief payload."],
            changed=[],
            mandatory_scenarios=[],
            regression_scope=[],
            notes=["Release Manager will use full active TEST PLAN and Regression Suite."],
        )
    # Minimal text support: keep text as notes while using active TEST PLAN.
    return ReleaseBrief(
        release_id=fallback_release_id,
        build="brief-text-payload",
        sprint="manual",
        implemented=["Release Brief text payload supplied."],
        fixed_engineering_tasks=normalize_task_ids(text),
        notes=[text[:4000]],
    )


def scenario_ids_from_release_brief(brief: ReleaseBrief, available_ids: List[str]) -> Optional[List[str]]:
    """Select scenario ids from brief. If none are specified, caller should run full TEST PLAN."""
    requested = []
    lookup = {sid.lower(): sid for sid in available_ids}
    text_items = brief.mandatory_scenarios + brief.regression_scope + brief.changed
    for item in text_items:
        norm = str(item or "").strip().lower()
        if norm in lookup:
            requested.append(lookup[norm])
        # Component shortcuts used by Release Brief authors.
        if "journal" in norm or "журнал" in norm:
            requested.append("S1-JOURNAL-COMMANDS")
        if "start" in norm or "старт" in norm:
            requested.append("S1-START-SCREEN")
        if "runtime" in norm or "navigation" in norm or "навига" in norm or "contract" in norm:
            requested.append("S1-CONTRACT-FLOW")
        if "command" in norm or "routing" in norm or "команд" in norm:
            requested.append("S1-LOCAL-COMMANDS")
    selected = [sid for sid in sorted(set(requested)) if sid in available_ids]
    return selected or None


def build_release_brief_markdown(brief: ReleaseBrief) -> str:
    def bullets(items: List[str]) -> List[str]:
        return [f"- {item}" for item in items] if items else ["- Не указано. Release Manager использует полный TEST PLAN."]
    lines = [
        f"# Release Brief — {brief.release_id}",
        "",
        "## 1. Release Information",
        f"- Release ID: {brief.release_id}",
        f"- Build: {brief.build}",
        f"- Sprint: {brief.sprint}",
        f"- Дата сборки: {brief.build_date}",
        f"- Предыдущий релиз: {brief.previous_release}",
        "",
        "## 2. Что реализовано",
        *bullets(brief.implemented),
        "",
        "## 3. Что изменилось",
        *bullets(brief.changed),
        "",
        "## 4. Обязательная программа проверки",
        *bullets(brief.mandatory_scenarios),
        "",
        "## 5. Regression Scope",
        *bullets(brief.regression_scope),
        "",
        "## 6. Исправленные инженерные задачи",
        *( [f"- {item} — исправлено в коде, ожидает подтверждения Release Manager." for item in brief.fixed_engineering_tasks] if brief.fixed_engineering_tasks else ["- Нет задач, заявленных как исправленные в этом релизе."] ),
        "",
        "## 7. Особые замечания",
        *bullets(brief.notes),
    ]
    return "\n".join(lines)
