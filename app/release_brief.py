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
    instruction_update_required: bool = False
    instruction_update_note: str = "Изменение инструкции VECTRA не требуется."

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
            "instruction_update_required": bool(self.instruction_update_required),
            "instruction_update_note": self.instruction_update_note,
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



def _parse_instruction_update_required(raw: Any) -> bool:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return False
    text = str(raw).strip().lower()
    if not text:
        return False
    required_markers = (
        'required',
        'requires',
        'yes',
        'true',
        'требуется',
        'потрібно',
        'нужно',
        'необходимо',
    )
    not_required_markers = (
        'not required',
        'no',
        'false',
        'не требуется',
        'не потрібно',
        'не нужно',
        'не требуется обновление',
    )
    if any(marker in text for marker in not_required_markers):
        return False
    if any(marker in text for marker in required_markers):
        return True
    return False


def _instruction_update_from_payload(raw: Dict[str, Any]) -> tuple[bool, str]:
    sync = raw.get('instruction_synchronization')
    if isinstance(sync, dict):
        required = _parse_instruction_update_required(
            sync.get('required')
            if 'required' in sync
            else sync.get('instruction_update_required')
            if 'instruction_update_required' in sync
            else sync.get('status')
        )
        note = str(sync.get('note') or sync.get('status') or '').strip()
    else:
        required = _parse_instruction_update_required(
            raw.get('instruction_update_required')
            if 'instruction_update_required' in raw
            else raw.get('requires_instruction_update')
            if 'requires_instruction_update' in raw
            else sync
        )
        note = str(
            raw.get('instruction_update_note')
            or raw.get('instruction_synchronization_note')
            or sync
            or ''
        ).strip()
    default_note = (
        'Требуется обновление инструкции VECTRA.'
        if required
        else 'Изменение инструкции VECTRA не требуется.'
    )
    return required, note or default_note


def normalize_task_ids(items: Any) -> List[str]:
    out: List[str] = []
    for item in _as_list(items):
        for match in re.finditer(r'DEV-?\d{1,6}', item, flags=re.IGNORECASE):
            raw = match.group(0)
            num_match = re.search(r'\d+', raw)
            if num_match:
                out.append(f'DEV-{int(num_match.group(0)):04d}')
    return sorted(set(out))



def _journal_fixed_task_rows(release_id: str) -> List[Dict[str, Any]]:
    """Load fixed engineering tasks for Release Brief from Development Journal.

    Development Journal is the only source for the section
    "Исправленные инженерные задачи". Release Brief text or payload may not
    manually inject task ids into this section.
    """
    release = str(release_id or '').strip()
    if not release:
        return []
    rows: List[Dict[str, Any]] = []
    try:
        from app.development_journal import list_fixed_engineering_tasks_for_release
        records = list_fixed_engineering_tasks_for_release(release, include_awaiting=True)
        for rec in records:
            task_id = str(rec.get('id') or '').strip()
            if not task_id:
                continue
            rows.append({
                'id': task_id,
                'status': rec.get('status') or rec.get('current_status') or 'Fixed',
                'release': rec.get('fixed_release') or rec.get('verification_release') or release or '—',
                'fixed_at': rec.get('fixed_at') or rec.get('updated_at') or '—',
            })
    except Exception:
        return []
    seen = set()
    uniq: List[Dict[str, Any]] = []
    for row in rows:
        if row['id'] in seen:
            continue
        seen.add(row['id'])
        uniq.append(row)
    return uniq

def parse_release_brief(raw: Any, fallback_release_id: str = "manual-release") -> ReleaseBrief:
    """Normalize Release Brief from dict, markdown-like string or absent value."""
    if isinstance(raw, ReleaseBrief):
        return raw
    if isinstance(raw, dict):
        info = raw.get("release_information") if isinstance(raw.get("release_information"), dict) else raw
        instruction_required, instruction_note = _instruction_update_from_payload(raw)
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
            fixed_engineering_tasks=[],
            notes=_as_list(raw.get("notes") or raw.get("release_notes")),
            instruction_update_required=instruction_required,
            instruction_update_note=instruction_note,
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
            instruction_update_required=False,
            instruction_update_note="Изменение инструкции VECTRA не требуется.",
        )
    # Minimal text support: keep text as notes while using active TEST PLAN.
    return ReleaseBrief(
        release_id=fallback_release_id,
        build="brief-text-payload",
        sprint="manual",
        implemented=["Release Brief text payload supplied."],
        fixed_engineering_tasks=[],
        notes=[text[:4000]],
        instruction_update_required=_parse_instruction_update_required(text),
        instruction_update_note=(
            "Требуется обновление инструкции VECTRA."
            if _parse_instruction_update_required(text)
            else "Изменение инструкции VECTRA не требуется."
        ),
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
    fixed_rows = _journal_fixed_task_rows(brief.release_id)
    brief.fixed_engineering_tasks = [row['id'] for row in fixed_rows]
    fixed_lines = [
        f"- {row['id']} — статус: {row['status']}; релиз: {row['release']}; дата исправления: {row['fixed_at']}"
        for row in fixed_rows
    ] or ["- Нет задач, получивших статус Fixed в этом релизе."]
    instruction_line = (
        "- Требуется обновление инструкции VECTRA."
        if brief.instruction_update_required
        else "- Изменение инструкции VECTRA не требуется."
    )
    instruction_note_line = f"- Комментарий: {brief.instruction_update_note}" if brief.instruction_update_note else "- Комментарий: не указан."
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
        *fixed_lines,
        "",
        "## 7. Instruction Synchronization",
        instruction_line,
        instruction_note_line,
        "",
        "## 8. Особые замечания",
        *bullets(brief.notes),
    ]
    return "\n".join(lines)
