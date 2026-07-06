import json
import os
import re
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

REPOSITORY_VERSION = "GENESIS-0001"
DEFAULT_BASE_PATH = "assistant_repository"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def _base_path() -> Path:
    configured = os.getenv('VECTRA_ASSISTANT_REPOSITORY_PATH', DEFAULT_BASE_PATH)
    return Path(configured).resolve()


def _safe_slug(value: str, fallback: str = 'document') -> str:
    raw = str(value or '').strip().lower()
    raw = re.sub(r'[^a-z0-9а-яіїєґ_-]+', '-', raw, flags=re.IGNORECASE).strip('-')
    return raw[:90] or fallback


def _json_default(path: Path, payload: Dict[str, Any]) -> None:
    if not path.exists():
        _write_json(path, payload)


def _read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return deepcopy(default)
        with path.open('r', encoding='utf-8') as fh:
            return json.load(fh)
    except Exception:
        return deepcopy(default)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('w', encoding='utf-8') as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write('\n')
    tmp.replace(path)


def _append_json_list(path: Path, item: Dict[str, Any]) -> List[Dict[str, Any]]:
    current = _read_json(path, [])
    if not isinstance(current, list):
        current = []
    current.append(item)
    _write_json(path, current)
    return current



def _preview(value: Any, max_chars: int = 2200) -> str:
    try:
        text = json.dumps(value, ensure_ascii=False, indent=2) if isinstance(value, (dict, list)) else str(value)
    except Exception:
        text = str(value)
    return text if len(text) <= max_chars else text[:max_chars].rstrip() + '…'


def _with_workspace_markdown(payload: Dict[str, Any], title: str, body: Any = None) -> Dict[str, Any]:
    """Attach canonical markdown to Runtime observability payloads.

    Custom GPT Product Verification requires user-visible workspace_markdown even
    for Runtime memory/readback objects. Without it, the rendering contract
    blocks verification before the repository data can be inspected.
    """
    if not isinstance(payload, dict):
        payload = {'status': 'error', 'payload': payload}
    if isinstance(payload.get('workspace_markdown'), str) and payload.get('workspace_markdown').strip():
        return payload
    lines = [f'# {title}', '']
    status = payload.get('status')
    if status:
        lines.append(f'Статус: **{status}**')
        lines.append('')
    human = payload.get('human_summary') or (payload.get('product_owner_summary') or {}).get('short_answer') if isinstance(payload.get('product_owner_summary'), dict) else None
    if human:
        lines.append(str(human))
        lines.append('')
    if body is None:
        body = payload
    lines += ['## Данные', _preview(body), '', '## Контроль', 'Данные прочитаны из Runtime Repository VECTRA. Если запись была создана перед этим, она должна проходить Write → Readback Verification.']
    payload['workspace_markdown'] = '\n'.join(lines).strip()
    payload['screen_order'] = ['workspace_markdown']
    payload['workspace_render_instruction'] = 'Показать пользователю workspace_markdown полностью и без изменений.'
    return payload


def _seed_state() -> Dict[str, Any]:
    return {
        'repository_version': REPOSITORY_VERSION,
        'identity_root': {
            'name': 'VECTRA',
            'type': 'living_business_management_system',
            'mission': 'help the company make better business decisions through data, context, action, execution, feedback and learning',
            'principle': 'VECTRA is the central entity. GPT is the interface. Laboratory is the development environment.',
        },
        'professional_model': {
            'name': 'Product Team Assistant',
            'role': 'internal professional model of VECTRA for product development, Product Acceptance and continuous improvement',
            'is_separate_product': False,
        },
        'assistant_identity': {
            'name': 'Product Team Assistant',
            'home_system': 'VECTRA',
            'role': 'internal professional model of VECTRA',
            'mission': 'support VECTRA continuous improvement as part of VECTRA, not as a separate system',
        },
        'architectural_principle': 'VECTRA is the digital organization; assistant runtime is an internal VECTRA service, not a separate platform.',
        'active_standards': [
            'VECTRA Core Constitution',
            'Digital Communication Standard',
            'Self Evolution',
            'Professional Activity',
            'Digital Organization Protocol',
            'Assistant Runtime Repository',
        ],
        'open_responsibilities': [],
        'active_decisions': [],
        'last_recovery_snapshot_id': None,
        'updated_at': _now(),
    }


def _seed_manifest() -> Dict[str, Any]:
    return {
        'status': 'active',
        'release': REPOSITORY_VERSION,
        'purpose': 'Persistent professional memory and operating environment for VECTRA.',
        'repository_is': 'internal VECTRA runtime workspace',
        'repository_is_not': 'separate digital organization platform',
        'storage_model': 'file-based JSON/Markdown foundation, replaceable by database or Git-backed persistence later',
        'created_at': _now(),
    }


def _seed_vectra_memory() -> Dict[str, Any]:
    return {
        'memory_id': 'vectra-memory-root',
        'identity_root': 'VECTRA',
        'status': 'initialized',
        'created_at': _now(),
        'updated_at': _now(),
        'professional_profile': {
            'name': 'VECTRA',
            'type': 'living_business_management_system',
            'mission': 'help the business make better decisions through data, context, decisions, action, execution, feedback and learning',
            'core_principle': 'VECTRA is the central system. Product Team Assistant is an internal professional model. GPT is only the interaction interface.',
        },
        'operating_model': {
            'laboratory': 'environment for development, product acceptance and improvement of VECTRA',
            'working_vectra': 'production environment where business work, runtime memory, journals and internal services live',
            'product_owner_control': 'automation removes manual execution but never removes Product Owner control',
        },
        'runtime_contract': {
            'write_readback_required': True,
            'readability_required': True,
            'product_verification_must_use_runtime': True,
        },
    }



def _seed_professional_model() -> Dict[str, Any]:
    """Seed the permanent professional model of VECTRA.

    GENESIS-0001 separates stable VECTRA professional knowledge from journals.
    Journals remain history. This model is the object recovered at the start
    of a Laboratory working context.
    """
    now = _now()
    return {
        'model_id': 'vectra-professional-model-root',
        'identity_root': 'VECTRA',
        'repository_version': REPOSITORY_VERSION,
        'status': 'active',
        'created_at': now,
        'updated_at': now,
        'source': 'GENESIS-0001 Professional Model Repository Foundation',
        'principle': 'The professional model is the source of stable VECTRA knowledge. Journals are history, not the source of truth.',
        'sections': {
            'identity': {
                'section_id': 'identity',
                'title': 'Identity',
                'status': 'active',
                'content': 'VECTRA is the central living business management system. GPT is the interface. Product Team Assistant is an internal professional model of VECTRA, not a separate product.',
                'updated_at': now,
            },
            'mission': {
                'section_id': 'mission',
                'title': 'Mission',
                'status': 'active',
                'content': 'Help the business make better management decisions by turning data into context, understanding, decisions, action, execution, feedback and learning.',
                'updated_at': now,
            },
            'principles': {
                'section_id': 'principles',
                'title': 'Core Principles',
                'status': 'active',
                'content': 'VECTRA develops through Laboratory, applies confirmed knowledge in Working VECTRA, preserves Product Owner control, and never changes its professional model without Product Owner confirmation.',
                'updated_at': now,
            },
            'methodology': {
                'section_id': 'methodology',
                'title': 'Methodology',
                'status': 'active',
                'content': 'Laboratory discussion produces candidates. Product Owner confirmation enables consolidation. Readback and Recovery confirm that knowledge became stable.',
                'updated_at': now,
            },
            'standards': {
                'section_id': 'standards',
                'title': 'Standards',
                'status': 'active',
                'content': 'Release Brief describes only implemented changes. Product Verification checks Runtime behavior, readback, integrity and recovery. Blocking Issues stop the cycle; Improvement Proposals go to the queue.',
                'updated_at': now,
            },
            'architecture': {
                'section_id': 'architecture',
                'title': 'Architecture',
                'status': 'active',
                'content': 'VECTRA has two environments: Laboratory for development and Working VECTRA for stable use. Knowledge Synchronization is the bridge between them.',
                'updated_at': now,
            },
            'professional_model': {
                'section_id': 'professional_model',
                'title': 'Professional Model',
                'status': 'active',
                'content': 'VECTRA accepts professional responsibility gradually. Each GENESIS increment must reduce manual Product Owner work and increase safe VECTRA responsibility.',
                'updated_at': now,
            },
            'product_decisions': {
                'section_id': 'product_decisions',
                'title': 'Product Decisions',
                'status': 'active',
                'content': 'Confirmed Product Decisions are stable professional knowledge only after Product Owner confirmation and successful Runtime verification.',
                'updated_at': now,
            },
            'active_responsibilities': {
                'section_id': 'active_responsibilities',
                'title': 'Active Responsibilities',
                'status': 'active',
                'content': 'VECTRA is responsible for observing its internal state, preparing proposals, keeping history separated from knowledge, and showing Product Owner what needs confirmation.',
                'updated_at': now,
            },
        },
        'readback_contract': {
            'write_readback_required': True,
            'section_read_required': True,
            'recovery_includes_professional_model': True,
        },
    }

def _seed_recovery_bundle() -> Dict[str, Any]:
    return {
        'bundle_id': 'recovery-bundle-root',
        'identity_root': 'VECTRA',
        'created_at': _now(),
        'updated_at': _now(),
        'purpose': 'Restore VECTRA working state from Runtime Repository.',
        'last_snapshot_id': None,
        'status': 'active',
    }



def ensure_repository() -> Path:
    base = _base_path()
    folders = [
        'state',
        'memory',
        'journal',
        'knowledge/standards',
        'knowledge/methodology',
        'knowledge/architecture',
        'decisions',
        'responsibilities',
        'snapshots',
        'documents/release_briefs',
        'documents/product_acceptance',
        'protocol',
        'runtime',
        'runtime/execution',
        'runtime/reflection',
        'runtime/reports',
        'recovery',
        'professional_model',
        'evolution',
        'activity',
    ]
    for folder in folders:
        (base / folder).mkdir(parents=True, exist_ok=True)
    _json_default(base / 'manifest.json', _seed_manifest())
    _json_default(base / 'memory' / 'vectra_memory.json', _seed_vectra_memory())
    _json_default(base / 'state' / 'current_state.json', _seed_state())
    _json_default(base / 'journal' / 'evolution_journal.json', [])
    _json_default(base / 'decisions' / 'product_decisions.json', [])
    _json_default(base / 'responsibilities' / 'active_responsibilities.json', [])
    _json_default(base / 'knowledge' / 'knowledge_index.json', [])
    _json_default(base / 'recovery' / 'recovery_bundle.json', _seed_recovery_bundle())
    _json_default(base / 'professional_model' / 'model.json', _seed_professional_model())
    _json_default(base / 'runtime' / 'execution' / 'reports.json', [])
    _json_default(base / 'runtime' / 'execution' / 'pending_approvals.json', [])
    _json_default(base / 'runtime' / 'runtime_status.json', {
        'status': 'ready',
        'release': REPOSITORY_VERSION,
        'last_integrity_check': _now(),
        'blocking_issues': [],
        'identity_root': 'VECTRA',
        'capabilities': [
            'recovery_bundle',
            'state_read_write',
            'evolution_journal_append',
            'knowledge_document_upsert',
            'product_decision_record',
            'recovery_snapshot_create',
            'natural_command_guidance',
            'readback_verification',
            'runtime_memory_overview',
            'professional_model_repository',
            'professional_model_readback',
            'professional_reflection_engine',
            'knowledge_candidate_repository',
        ],
    })
    return base


def _relative(path: Path) -> str:
    base = _base_path()
    try:
        return str(path.resolve().relative_to(base))
    except Exception:
        return str(path)


def repository_status() -> Dict[str, Any]:
    base = ensure_repository()
    files = [p for p in base.rglob('*') if p.is_file()]
    required = [
        base / 'manifest.json',
        base / 'memory' / 'vectra_memory.json',
        base / 'state' / 'current_state.json',
        base / 'journal' / 'evolution_journal.json',
        base / 'knowledge' / 'knowledge_index.json',
        base / 'decisions' / 'product_decisions.json',
        base / 'responsibilities' / 'active_responsibilities.json',
        base / 'runtime' / 'runtime_status.json',
        base / 'recovery' / 'recovery_bundle.json',
        base / 'professional_model' / 'model.json',
        base / 'runtime' / 'execution' / 'reports.json',
        base / 'runtime' / 'execution' / 'pending_approvals.json',
    ]
    missing = [_relative(p) for p in required if not p.exists()]
    return {
        'status': 'ok' if not missing else 'degraded',
        'render_mode': 'assistant_runtime_repository',
        'release': REPOSITORY_VERSION,
        'repository_path': str(base),
        'files_count': len(files),
        'required_missing': missing,
        'folders': sorted({_relative(p.parent) for p in files}),
        'important_note': 'Default file storage persists inside the running deployment filesystem. For durable cross-deploy persistence configure a persistent disk, database, or Git-backed storage adapter.',
    }


def get_current_state() -> Dict[str, Any]:
    base = ensure_repository()
    state = _read_json(base / 'state' / 'current_state.json', _seed_state())
    if not isinstance(state, dict):
        state = _seed_state()
    return _with_workspace_markdown({'status': 'ok', 'render_mode': 'assistant_runtime_state', 'state': state}, 'Профессиональное состояние VECTRA', state)


def update_current_state(patch: Dict[str, Any]) -> Dict[str, Any]:
    base = ensure_repository()
    if not isinstance(patch, dict):
        patch = {}
    current = _read_json(base / 'state' / 'current_state.json', _seed_state())
    if not isinstance(current, dict):
        current = _seed_state()
    protected = {'repository_version'}
    for key, value in patch.items():
        if key in protected:
            continue
        current[key] = value
    current['updated_at'] = _now()
    current['repository_version'] = REPOSITORY_VERSION
    _write_json(base / 'state' / 'current_state.json', current)
    return {'status': 'ok', 'render_mode': 'assistant_runtime_state_update', 'state': current}


def get_runtime_status() -> Dict[str, Any]:
    base = ensure_repository()
    runtime = _read_json(base / 'runtime' / 'runtime_status.json', {})
    if not isinstance(runtime, dict):
        runtime = {}
    repo = repository_status()
    runtime['last_integrity_check'] = _now()
    runtime['repository_integrity'] = repo
    _write_json(base / 'runtime' / 'runtime_status.json', {k: v for k, v in runtime.items() if k != 'repository_integrity'})
    return _with_workspace_markdown({'status': 'ok', 'render_mode': 'assistant_runtime_status', 'runtime': runtime}, 'Статус Runtime VECTRA', runtime)


def list_knowledge_documents() -> Dict[str, Any]:
    base = ensure_repository()
    index = _read_json(base / 'knowledge' / 'knowledge_index.json', [])
    if not isinstance(index, list):
        index = []
    return _with_workspace_markdown({'status': 'ok', 'render_mode': 'assistant_runtime_knowledge', 'documents': index}, 'Knowledge Repository VECTRA', index)


def _knowledge_path(document_id: str, folder: str = 'architecture') -> Path:
    folder_slug = _safe_slug(folder, 'architecture')
    return _base_path() / 'knowledge' / folder_slug / f'{_safe_slug(document_id, "document")}.md'


def upsert_knowledge_document(payload: Dict[str, Any]) -> Dict[str, Any]:
    base = ensure_repository()
    if not isinstance(payload, dict):
        payload = {}
    title = str(payload.get('title') or payload.get('document_id') or 'Knowledge Document').strip()
    document_id = str(payload.get('document_id') or _safe_slug(title, 'knowledge-document'))
    folder = str(payload.get('folder') or payload.get('knowledge_type') or 'architecture')
    content = str(payload.get('content') or payload.get('body') or '')
    status = str(payload.get('status') or 'active')
    metadata = payload.get('metadata') if isinstance(payload.get('metadata'), dict) else {}
    path = _knowledge_path(document_id, folder)
    if not content:
        content = f'# {title}\n\nStatus: {status}\n\nCreated by VECTRA Assistant Runtime Repository.\n'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding='utf-8')

    index_path = base / 'knowledge' / 'knowledge_index.json'
    index = _read_json(index_path, [])
    if not isinstance(index, list):
        index = []
    now = _now()
    existing = next((item for item in index if isinstance(item, dict) and item.get('document_id') == document_id), None)
    entry = {
        'document_id': document_id,
        'title': title,
        'folder': folder,
        'status': status,
        'path': _relative(path),
        'version': int((existing or {}).get('version') or 0) + 1,
        'created_at': (existing or {}).get('created_at') or now,
        'updated_at': now,
        'metadata': metadata,
    }
    index = [item for item in index if not (isinstance(item, dict) and item.get('document_id') == document_id)]
    index.append(entry)
    _write_json(index_path, index)
    return {'status': 'ok', 'render_mode': 'assistant_runtime_knowledge_update', 'document': entry}


def update_knowledge_document(document_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {}
    payload['document_id'] = document_id
    return upsert_knowledge_document(payload)



def _readback_verification(collection_path: Path, key: str, expected_id: str) -> Dict[str, Any]:
    """Read written object back and confirm it is observable.

    Runtime write is not considered confirmed until the same object can be read
    from repository storage. This protects Product Owner from invisible writes.
    """
    collection = _read_json(collection_path, [])
    if not isinstance(collection, list):
        return {
            'status': 'FAIL',
            'reason': 'collection_not_readable',
            'expected_id': expected_id,
            'key': key,
        }
    found = next((item for item in collection if isinstance(item, dict) and item.get(key) == expected_id), None)
    return {
        'status': 'PASS' if found else 'FAIL',
        'expected_id': expected_id,
        'key': key,
        'found': found is not None,
        'readable': True,
    }


def list_journal_entries(limit: int = 50) -> Dict[str, Any]:
    base = ensure_repository()
    entries = _read_json(base / 'journal' / 'evolution_journal.json', [])
    if not isinstance(entries, list):
        entries = []
    payload = {
        'status': 'ok',
        'render_mode': 'vectra_evolution_journal_read',
        'entries': entries[-max(1, int(limit or 50)):],
        'entries_count': len(entries),
        'human_summary': f'В журнале развития VECTRA сейчас {len(entries)} записей.',
    }
    return _with_workspace_markdown(payload, 'Журнал развития VECTRA', payload.get('entries'))


def list_product_decisions(limit: int = 50) -> Dict[str, Any]:
    base = ensure_repository()
    decisions = _read_json(base / 'decisions' / 'product_decisions.json', [])
    if not isinstance(decisions, list):
        decisions = []
    payload = {
        'status': 'ok',
        'render_mode': 'vectra_product_decisions_read',
        'decisions': decisions[-max(1, int(limit or 50)):],
        'decisions_count': len(decisions),
        'human_summary': f'В памяти VECTRA сейчас {len(decisions)} продуктовых решений.',
    }
    return _with_workspace_markdown(payload, 'Продуктовые решения VECTRA', payload.get('decisions'))


def list_recovery_snapshots(limit: int = 20) -> Dict[str, Any]:
    base = ensure_repository()
    snapshot_files = sorted((base / 'snapshots').glob('*.json'), key=lambda p: p.stat().st_mtime)
    snapshots = []
    for path in snapshot_files[-max(1, int(limit or 20)):]:
        item = _read_json(path, {})
        if isinstance(item, dict):
            snapshots.append({
                'snapshot_id': item.get('snapshot_id'),
                'created_at': item.get('created_at'),
                'release': item.get('release'),
                'path': _relative(path),
                'metadata': item.get('metadata') if isinstance(item.get('metadata'), dict) else {},
            })
    payload = {
        'status': 'ok',
        'render_mode': 'vectra_recovery_snapshots_read',
        'snapshots': snapshots,
        'snapshots_count': len(snapshot_files),
        'human_summary': f'В VECTRA найдено {len(snapshot_files)} снимков восстановления.',
    }
    return _with_workspace_markdown(payload, 'Снимки восстановления VECTRA', snapshots)


def get_runtime_memory_overview() -> Dict[str, Any]:
    """Observable overview of everything VECTRA currently has in runtime memory."""
    base = ensure_repository()
    state = _read_json(base / 'state' / 'current_state.json', _seed_state())
    journal = _read_json(base / 'journal' / 'evolution_journal.json', [])
    decisions = _read_json(base / 'decisions' / 'product_decisions.json', [])
    knowledge = _read_json(base / 'knowledge' / 'knowledge_index.json', [])
    responsibilities = _read_json(base / 'responsibilities' / 'active_responsibilities.json', [])
    reports = _read_json(base / 'runtime' / 'execution' / 'reports.json', [])
    approvals = _read_json(base / 'runtime' / 'execution' / 'pending_approvals.json', [])
    memory = _read_json(base / 'memory' / 'vectra_memory.json', _seed_vectra_memory())
    professional_model = _read_json(base / 'professional_model' / 'model.json', _seed_professional_model())
    recovery = _read_json(base / 'recovery' / 'recovery_bundle.json', _seed_recovery_bundle())
    snapshots = list_recovery_snapshots(limit=10).get('snapshots', [])
    if not isinstance(journal, list): journal = []
    if not isinstance(decisions, list): decisions = []
    if not isinstance(knowledge, list): knowledge = []
    if not isinstance(responsibilities, list): responsibilities = []
    if not isinstance(reports, list): reports = []
    if not isinstance(approvals, list): approvals = []
    payload = {
        'status': 'ok',
        'render_mode': 'vectra_runtime_memory_overview',
        'release': REPOSITORY_VERSION,
        'identity_root': 'VECTRA',
        'repository': repository_status(),
        'vectra_memory': memory,
            'professional_model': {'model_id': professional_model.get('model_id'), 'updated_at': professional_model.get('updated_at'), 'sections': sorted((professional_model.get('sections') or {}).keys()) if isinstance(professional_model, dict) and isinstance(professional_model.get('sections'), dict) else []},
        'professional_state': state,
        'recovery_bundle': recovery,
        'counts': {
            'evolution_journal_entries': len(journal),
            'product_decisions': len(decisions),
            'knowledge_documents': len(knowledge),
            'active_responsibilities': len(responsibilities),
            'runtime_reports': len(reports),
            'pending_approvals': len(approvals),
            'recovery_snapshots': len(snapshots),
        },
        'latest': {
            'journal_entries': journal[-5:],
            'product_decisions': decisions[-5:],
            'runtime_reports': reports[-5:],
            'pending_approvals': approvals[-10:],
            'recovery_snapshots': snapshots[-5:],
        },
        'product_owner_summary': {
            'short_answer': 'Я открыла рабочую память VECTRA и показываю, что в ней реально хранится.',
            'what_is_visible': ['память VECTRA', 'профессиональное состояние', 'журнал развития', 'решения', 'знания', 'снимки восстановления', 'ожидающие подтверждения'],
            'control_principle': 'Любая запись должна быть доступна для повторного чтения.',
        },
    }
    return _with_workspace_markdown(payload, 'Память VECTRA', {'counts': payload['counts'], 'latest': payload['latest']})


def append_journal_entry(payload: Dict[str, Any]) -> Dict[str, Any]:
    base = ensure_repository()
    if not isinstance(payload, dict):
        payload = {}
    entry = {
        'entry_id': str(payload.get('entry_id') or f'ej-{uuid.uuid4().hex[:12]}'),
        'created_at': _now(),
        'source': str(payload.get('source') or 'assistant_runtime_api'),
        'object_changed': str(payload.get('object_changed') or payload.get('object') or 'VECTRA professional model'),
        'decision': str(payload.get('decision') or payload.get('summary') or 'Runtime journal entry created.'),
        'rationale': str(payload.get('rationale') or ''),
        'consequences': payload.get('consequences') if isinstance(payload.get('consequences'), list) else [],
        'related_documents': payload.get('related_documents') if isinstance(payload.get('related_documents'), list) else [],
        'status': str(payload.get('status') or 'confirmed'),
        'metadata': payload.get('metadata') if isinstance(payload.get('metadata'), dict) else {},
    }
    entries = _append_json_list(base / 'journal' / 'evolution_journal.json', entry)
    state = _read_json(base / 'state' / 'current_state.json', _seed_state())
    if isinstance(state, dict):
        state['last_journal_entry_id'] = entry['entry_id']
        state['updated_at'] = _now()
        _write_json(base / 'state' / 'current_state.json', state)
    verification = _readback_verification(base / 'journal' / 'evolution_journal.json', 'entry_id', entry['entry_id'])
    return {'status': 'ok', 'render_mode': 'assistant_runtime_journal_append', 'entry': entry, 'entries_count': len(entries), 'readback_verification': verification}


def record_product_decision(payload: Dict[str, Any]) -> Dict[str, Any]:
    base = ensure_repository()
    if not isinstance(payload, dict):
        payload = {}
    decision = {
        'decision_id': str(payload.get('decision_id') or f'pd-{uuid.uuid4().hex[:12]}'),
        'created_at': _now(),
        'title': str(payload.get('title') or payload.get('decision') or 'Product Decision'),
        'decision': str(payload.get('decision') or payload.get('summary') or ''),
        'management_purpose': str(payload.get('management_purpose') or payload.get('purpose') or ''),
        'status': str(payload.get('status') or 'confirmed'),
        'owner': str(payload.get('owner') or 'Product Owner'),
        'related_documents': payload.get('related_documents') if isinstance(payload.get('related_documents'), list) else [],
        'metadata': payload.get('metadata') if isinstance(payload.get('metadata'), dict) else {},
    }
    decisions = _append_json_list(base / 'decisions' / 'product_decisions.json', decision)
    state = _read_json(base / 'state' / 'current_state.json', _seed_state())
    if isinstance(state, dict):
        active = state.get('active_decisions') if isinstance(state.get('active_decisions'), list) else []
        active.append({'decision_id': decision['decision_id'], 'title': decision['title'], 'status': decision['status']})
        state['active_decisions'] = active[-50:]
        state['updated_at'] = _now()
        _write_json(base / 'state' / 'current_state.json', state)
    verification = _readback_verification(base / 'decisions' / 'product_decisions.json', 'decision_id', decision['decision_id'])
    return {'status': 'ok', 'render_mode': 'assistant_runtime_product_decision', 'decision': decision, 'decisions_count': len(decisions), 'readback_verification': verification}


def create_recovery_snapshot(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base = ensure_repository()
    if not isinstance(payload, dict):
        payload = {}
    snapshot_id = str(payload.get('snapshot_id') or f'snapshot-{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}-{uuid.uuid4().hex[:6]}')
    snapshot = {
        'snapshot_id': snapshot_id,
        'created_at': _now(),
        'release': REPOSITORY_VERSION,
        'state': _read_json(base / 'state' / 'current_state.json', _seed_state()),
        'runtime': _read_json(base / 'runtime' / 'runtime_status.json', {}),
        'knowledge_index': _read_json(base / 'knowledge' / 'knowledge_index.json', []),
        'professional_model': _read_json(base / 'professional_model' / 'model.json', _seed_professional_model()),
        'recent_journal_entries': _read_json(base / 'journal' / 'evolution_journal.json', [])[-10:],
        'active_responsibilities': _read_json(base / 'responsibilities' / 'active_responsibilities.json', []),
        'product_decisions': _read_json(base / 'decisions' / 'product_decisions.json', [])[-20:],
        'metadata': payload.get('metadata') if isinstance(payload.get('metadata'), dict) else {},
    }
    path = base / 'snapshots' / f'{_safe_slug(snapshot_id, "snapshot")}.json'
    _write_json(path, snapshot)
    state = snapshot['state'] if isinstance(snapshot.get('state'), dict) else _seed_state()
    state['last_recovery_snapshot_id'] = snapshot_id
    state['updated_at'] = _now()
    _write_json(base / 'state' / 'current_state.json', state)
    return {'status': 'ok', 'render_mode': 'assistant_runtime_snapshot', 'snapshot': snapshot, 'path': _relative(path)}


def get_recovery_bundle() -> Dict[str, Any]:
    base = ensure_repository()
    snapshots = list_recovery_snapshots(limit=1).get('snapshots', [])
    bundle = _read_json(base / 'recovery' / 'recovery_bundle.json', _seed_recovery_bundle())
    if not isinstance(bundle, dict):
        bundle = _seed_recovery_bundle()
    latest_snapshot = snapshots[-1] if snapshots else None
    bundle['updated_at'] = _now()
    bundle['identity_root'] = 'VECTRA'
    bundle['last_snapshot_id'] = (latest_snapshot or {}).get('snapshot_id')
    _write_json(base / 'recovery' / 'recovery_bundle.json', bundle)
    payload = {
        'status': 'ok',
        'render_mode': 'vectra_runtime_recovery_bundle',
        'identity_root': 'VECTRA',
        'recovery_contract': {
            'purpose': 'Restore VECTRA working state from VECTRA internal runtime repository, not from chat history.',
            'how_to_use': 'GPT interface should call this endpoint at the start of a working context and use returned state as the VECTRA professional baseline.',
        },
        'recovery_bundle': bundle,
        'repository': repository_status(),
        'vectra_memory': _read_json(base / 'memory' / 'vectra_memory.json', _seed_vectra_memory()),
        'professional_model': _read_json(base / 'professional_model' / 'model.json', _seed_professional_model()),
        'professional_state': _read_json(base / 'state' / 'current_state.json', _seed_state()),
        'runtime': _read_json(base / 'runtime' / 'runtime_status.json', {}),
        'knowledge_repository': _read_json(base / 'knowledge' / 'knowledge_index.json', []),
        'evolution_journal': _read_json(base / 'journal' / 'evolution_journal.json', [])[-10:],
        'active_responsibilities': _read_json(base / 'responsibilities' / 'active_responsibilities.json', []),
        'product_decisions': _read_json(base / 'decisions' / 'product_decisions.json', [])[-20:],
        'runtime_reports': _read_json(base / 'runtime' / 'execution' / 'reports.json', [])[-10:],
        'pending_approvals': _read_json(base / 'runtime' / 'execution' / 'pending_approvals.json', []),
        'latest_recovery_snapshot': latest_snapshot,
    }
    return _with_workspace_markdown(payload, 'Recovery Bundle VECTRA', {'recovery_bundle': bundle, 'latest_recovery_snapshot': latest_snapshot})



def get_professional_model() -> Dict[str, Any]:
    base = ensure_repository()
    model = _read_json(base / 'professional_model' / 'model.json', _seed_professional_model())
    if not isinstance(model, dict):
        model = _seed_professional_model()
        _write_json(base / 'professional_model' / 'model.json', model)
    return _with_workspace_markdown({'status': 'ok', 'render_mode': 'vectra_professional_model_repository', 'identity_root': 'VECTRA', 'professional_model': model}, 'Профессиональная модель VECTRA', model)


def list_professional_model_sections() -> Dict[str, Any]:
    model_payload = get_professional_model()
    model = model_payload.get('professional_model') if isinstance(model_payload, dict) else {}
    sections = model.get('sections') if isinstance(model, dict) and isinstance(model.get('sections'), dict) else {}
    items = []
    for section_id, section in sections.items():
        if isinstance(section, dict):
            items.append({
                'section_id': section_id,
                'title': section.get('title') or section_id,
                'status': section.get('status') or 'active',
                'updated_at': section.get('updated_at'),
                'content_preview': str(section.get('content') or '')[:240],
            })
    payload = {
        'status': 'ok',
        'render_mode': 'vectra_professional_model_sections',
        'identity_root': 'VECTRA',
        'sections': items,
        'sections_count': len(items),
        'human_summary': f'В профессиональной модели VECTRA сейчас {len(items)} разделов.',
    }
    return _with_workspace_markdown(payload, 'Разделы профессиональной модели VECTRA', items)


def read_professional_model_section(section_id: str) -> Dict[str, Any]:
    model_payload = get_professional_model()
    model = model_payload.get('professional_model') if isinstance(model_payload, dict) else {}
    sections = model.get('sections') if isinstance(model, dict) and isinstance(model.get('sections'), dict) else {}
    section_key = _safe_slug(str(section_id or '').lower().replace('-', '_'), 'identity').replace('-', '_')
    section = sections.get(section_key)
    if not isinstance(section, dict):
        return _with_workspace_markdown({
            'status': 'error',
            'render_mode': 'vectra_professional_model_section_missing',
            'identity_root': 'VECTRA',
            'section_id': section_key,
            'reason': 'section_not_found',
            'available_sections': sorted(sections.keys()),
        }, f'Раздел профессиональной модели не найден: {section_key}')
    payload = {
        'status': 'ok',
        'render_mode': 'vectra_professional_model_section',
        'identity_root': 'VECTRA',
        'section_id': section_key,
        'section': section,
        'human_summary': f'Открыт раздел профессиональной модели VECTRA: {section.get("title") or section_key}.',
    }
    return _with_workspace_markdown(payload, f'Профессиональная модель VECTRA: {section.get("title") or section_key}', section)


def update_professional_model_section(section_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    base = ensure_repository()
    if not isinstance(payload, dict):
        payload = {}
    model = _read_json(base / 'professional_model' / 'model.json', _seed_professional_model())
    if not isinstance(model, dict):
        model = _seed_professional_model()
    sections = model.get('sections') if isinstance(model.get('sections'), dict) else {}
    section_key = _safe_slug(str(section_id or payload.get('section_id') or '').lower().replace('-', '_'), 'identity').replace('-', '_')
    existing = sections.get(section_key) if isinstance(sections.get(section_key), dict) else {}
    now = _now()
    updated = dict(existing)
    updated.update({
        'section_id': section_key,
        'title': str(payload.get('title') or existing.get('title') or section_key),
        'status': str(payload.get('status') or existing.get('status') or 'active'),
        'content': str(payload.get('content') or payload.get('body') or existing.get('content') or ''),
        'updated_at': now,
    })
    if isinstance(payload.get('metadata'), dict):
        updated['metadata'] = payload.get('metadata')
    if 'requires_owner_confirmation' in payload:
        updated['requires_owner_confirmation'] = bool(payload.get('requires_owner_confirmation'))
    sections[section_key] = updated
    model['sections'] = sections
    model['updated_at'] = now
    model['repository_version'] = REPOSITORY_VERSION
    model['identity_root'] = 'VECTRA'
    _write_json(base / 'professional_model' / 'model.json', model)

    verification = verify_professional_model_readback(section_key)
    if verification.get('status') == 'PASS':
        # Recovery must reference the updated model so a new context can restore it.
        recovery = _read_json(base / 'recovery' / 'recovery_bundle.json', _seed_recovery_bundle())
        if isinstance(recovery, dict):
            recovery['professional_model_id'] = model.get('model_id')
            recovery['professional_model_updated_at'] = model.get('updated_at')
            recovery['updated_at'] = now
            recovery['identity_root'] = 'VECTRA'
            _write_json(base / 'recovery' / 'recovery_bundle.json', recovery)
    return _with_workspace_markdown({
        'status': 'ok',
        'render_mode': 'vectra_professional_model_section_update',
        'identity_root': 'VECTRA',
        'section_id': section_key,
        'section': updated,
        'readback_verification': verification,
    }, f'Обновление раздела профессиональной модели VECTRA: {section_key}', {'section': updated, 'readback_verification': verification})


def verify_professional_model_readback(section_id: Optional[str] = None) -> Dict[str, Any]:
    base = ensure_repository()
    model = _read_json(base / 'professional_model' / 'model.json', {})
    if not isinstance(model, dict):
        return {'status': 'FAIL', 'object': 'professional_model', 'reason': 'model_not_readable'}
    sections = model.get('sections') if isinstance(model.get('sections'), dict) else {}
    required = ['identity', 'mission', 'principles', 'methodology', 'standards', 'architecture', 'professional_model', 'product_decisions', 'active_responsibilities']
    missing = [key for key in required if key not in sections]
    if section_id:
        section_key = _safe_slug(str(section_id).lower().replace('-', '_'), 'identity').replace('-', '_')
        found = section_key in sections and isinstance(sections.get(section_key), dict)
    else:
        section_key = None
        found = True
    recovery = _read_json(base / 'recovery' / 'recovery_bundle.json', {})
    payload = {
        'status': 'PASS' if not missing and found else 'FAIL',
        'object': 'professional_model',
        'section_id': section_key,
        'section_found': found,
        'required_sections_missing': missing,
        'sections_count': len(sections),
        'readable': True,
        'recovery_integrated': isinstance(recovery, dict),
        'contract': 'professional_model_repository_readback_required',
    }
    return _with_workspace_markdown(payload, 'Readback Verification профессиональной модели VECTRA', payload)



RUNTIME_OBSERVABLE_OBJECTS = {
    'professional_model': ('professional_model/model.json', 'dict'),
    'vectra_memory': ('memory/vectra_memory.json', 'dict'),
    'professional_state': ('state/current_state.json', 'dict'),
    'evolution_journal': ('journal/evolution_journal.json', 'list'),
    'product_decisions': ('decisions/product_decisions.json', 'list'),
    'knowledge_repository': ('knowledge/knowledge_index.json', 'list'),
    'recovery_bundle': ('recovery/recovery_bundle.json', 'dict'),
    'runtime_reports': ('runtime/execution/reports.json', 'list'),
    'pending_approvals': ('runtime/execution/pending_approvals.json', 'list'),
    'active_responsibilities': ('responsibilities/active_responsibilities.json', 'list'),
    'recovery_snapshot': ('snapshots', 'snapshots'),
    'knowledge_candidates': ('runtime/reflection/knowledge_candidates.json', 'list'),
    'reflection_reports': ('runtime/reflection/reflection_reports.json', 'list'),
    'professional_observations': ('runtime/observation/professional_observations.json', 'list'),
    'observation_reports': ('runtime/observation/observation_reports.json', 'list'),
    'responsibility_reports': ('runtime/responsibility/responsibility_reports.json', 'list'),
    'recovery_evolution_status': ('runtime/recovery/recovery_evolution_status.json', 'dict'),
    'recovery_evolution_reports': ('runtime/recovery/recovery_evolution_reports.json', 'list'),
    'recovery_checkpoints': ('runtime/recovery/recovery_checkpoints.json', 'list'),
    'synchronization_status': ('runtime/synchronization/synchronization_status.json', 'dict'),
    'synchronization_packages': ('runtime/synchronization/synchronization_packages.json', 'list'),
    'synchronization_reports': ('runtime/synchronization/synchronization_reports.json', 'list'),
    'review_status': ('runtime/review/review_status.json', 'dict'),
    'review_sessions': ('runtime/review/review_sessions.json', 'list'),
    'review_reports': ('runtime/review/review_reports.json', 'list'),
}



def _object_path(object_name: str) -> Path:
    spec = RUNTIME_OBSERVABLE_OBJECTS.get(object_name)
    if not spec:
        raise KeyError(f'unknown_runtime_object:{object_name}')
    return _base_path() / spec[0]


def read_runtime_object(object_name: str, limit: int = 50) -> Dict[str, Any]:
    base = ensure_repository()
    object_name = str(object_name or '').strip().lower().replace('-', '_')
    if object_name in {'professional_model_root', 'model', 'профессиональная_модель'}:
        object_name = 'professional_model'
    if object_name in {'memory', 'vectra'}:
        object_name = 'vectra_memory'
    if object_name in {'state', 'professional'}:
        object_name = 'professional_state'
    if object_name in {'journal', 'evolution'}:
        object_name = 'evolution_journal'
    if object_name in {'decisions', 'product_decision'}:
        object_name = 'product_decisions'
    if object_name in {'knowledge', 'repository_knowledge'}:
        object_name = 'knowledge_repository'
    if object_name in {'recovery', 'bundle'}:
        object_name = 'recovery_bundle'
    if object_name in {'snapshot', 'snapshots'}:
        object_name = 'recovery_snapshot'
    if object_name in {'candidate', 'candidates', 'knowledge_candidate', 'knowledge_candidates'}:
        object_name = 'knowledge_candidates'
    if object_name in {'reflection', 'reflection_report', 'reflection_reports'}:
        object_name = 'reflection_reports'
    if object_name in {'observation', 'observations', 'professional_observation', 'professional_observations'}:
        object_name = 'professional_observations'
    if object_name in {'observation_report', 'observation_reports'}:
        object_name = 'observation_reports'
    if object_name in {'responsibility', 'responsibilities', 'active_responsibility', 'active_responsibilities'}:
        object_name = 'active_responsibilities'
    if object_name in {'responsibility_report', 'responsibility_reports'}:
        object_name = 'responsibility_reports'
    if object_name in {'recovery_evolution', 'recovery_evolution_status'}:
        object_name = 'recovery_evolution_status'
    if object_name in {'recovery_evolution_report', 'recovery_evolution_reports'}:
        object_name = 'recovery_evolution_reports'
    if object_name in {'recovery_checkpoint', 'recovery_checkpoints'}:
        object_name = 'recovery_checkpoints'
    if object_name in {'synchronization', 'sync', 'synchronization_status'}:
        object_name = 'synchronization_status'
    if object_name in {'synchronization_package', 'synchronization_packages', 'sync_packages'}:
        object_name = 'synchronization_packages'
    if object_name in {'synchronization_report', 'synchronization_reports', 'sync_reports'}:
        object_name = 'synchronization_reports'
    if object_name in {'review', 'review_status', 'product_owner_review'}:
        object_name = 'review_status'
    if object_name in {'review_session', 'review_sessions'}:
        object_name = 'review_sessions'
    if object_name in {'review_report', 'review_reports'}:
        object_name = 'review_reports'
    if object_name not in RUNTIME_OBSERVABLE_OBJECTS:
        return {'status': 'error', 'render_mode': 'vectra_runtime_object_read_error', 'object': object_name, 'reason': 'unknown_runtime_object'}
    spec_path, spec_type = RUNTIME_OBSERVABLE_OBJECTS[object_name]
    if object_name == 'recovery_snapshot':
        return list_recovery_snapshots(limit=limit)
    path = base / spec_path
    if spec_type == 'dict':
        data = _read_json(path, {})
        if not isinstance(data, dict):
            data = {}
    else:
        data = _read_json(path, [])
        if not isinstance(data, list):
            data = []
        data = data[-max(1, int(limit or 50)):]
    payload = {
        'status': 'ok',
        'render_mode': 'vectra_runtime_object_read',
        'identity_root': 'VECTRA',
        'object': object_name,
        'path': _relative(path),
        'data': data,
        'count': len(data) if isinstance(data, list) else None,
        'readable': True,
        'human_summary': f'Открыт объект памяти VECTRA: {object_name}.',
    }
    return _with_workspace_markdown(payload, f'Объект памяти VECTRA: {object_name}', data)


def write_runtime_object(object_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    base = ensure_repository()
    if not isinstance(payload, dict):
        payload = {}
    object_name = str(object_name or '').strip().lower().replace('-', '_')
    if object_name not in RUNTIME_OBSERVABLE_OBJECTS:
        return {'status': 'error', 'render_mode': 'vectra_runtime_object_write_error', 'object': object_name, 'reason': 'unknown_runtime_object'}
    if object_name == 'recovery_snapshot':
        return create_recovery_snapshot(payload)
    path = _object_path(object_name)
    _, spec_type = RUNTIME_OBSERVABLE_OBJECTS[object_name]
    now = _now()
    if spec_type == 'dict':
        current = _read_json(path, {})
        if not isinstance(current, dict):
            current = {}
        current.update(payload)
        current['updated_at'] = now
        current['identity_root'] = 'VECTRA'
        _write_json(path, current)
        written_id = current.get('model_id') or current.get('memory_id') or current.get('bundle_id') or current.get('state_id') or object_name
    else:
        current = _read_json(path, [])
        if not isinstance(current, list):
            current = []
        item = dict(payload)
        item.setdefault('entry_id', f'{object_name}-{uuid.uuid4().hex[:12]}')
        item.setdefault('created_at', now)
        item.setdefault('identity_root', 'VECTRA')
        current.append(item)
        _write_json(path, current)
        written_id = item.get('entry_id')
    readback = verify_runtime_readback(object_name, written_id=written_id)
    return {
        'status': 'ok',
        'render_mode': 'vectra_runtime_object_write',
        'identity_root': 'VECTRA',
        'object': object_name,
        'written_id': written_id,
        'readback_verification': readback,
    }


def verify_runtime_readback(object_name: str, written_id: Optional[str] = None) -> Dict[str, Any]:
    result = read_runtime_object(object_name)
    if result.get('status') != 'ok':
        return {'status': 'FAIL', 'object': object_name, 'reason': result.get('reason')}
    data = result.get('data')
    if written_id and isinstance(data, list):
        found = any(isinstance(x, dict) and x.get('entry_id') == written_id for x in data)
    elif written_id and isinstance(data, dict):
        found = written_id in {data.get('model_id'), data.get('memory_id'), data.get('bundle_id'), data.get('state_id'), object_name}
    else:
        found = data is not None
    return {
        'status': 'PASS' if found else 'FAIL',
        'object': object_name,
        'written_id': written_id,
        'readable': True,
        'found': found,
        'contract': 'write_readback_required',
    }


def run_runtime_product_verification() -> Dict[str, Any]:
    ensure_repository()
    created = {}
    created['decision'] = record_product_decision({
        'title': 'Runtime Product Verification Probe',
        'decision': 'Проверить, что Runtime VECTRA умеет записывать и читать собственные объекты памяти.',
        'management_purpose': 'Независимая Product Verification через Runtime.',
        'metadata': {'verification_probe': True, 'release': REPOSITORY_VERSION},
    })
    created['journal'] = append_journal_entry({
        'source': 'runtime_product_verification',
        'object_changed': 'VECTRA Runtime Observability',
        'decision': 'Выполнен контрольный цикл записи и чтения Runtime.',
        'rationale': 'Product Team Assistant должен подтверждать работу Runtime без Release Brief.',
        'metadata': {'verification_probe': True, 'release': REPOSITORY_VERSION},
    })
    created['snapshot'] = create_recovery_snapshot({'metadata': {'verification_probe': True, 'release': REPOSITORY_VERSION}})
    checks = []
    for name in RUNTIME_OBSERVABLE_OBJECTS:
        result = read_runtime_object(name)
        ok = result.get('status') == 'ok' and result.get('readable', True)
        if name == 'recovery_snapshot':
            ok = result.get('status') == 'ok' and result.get('snapshots_count', 0) >= 1
        elif name == 'professional_model':
            ok = ok and isinstance(result.get('data'), dict) and len(result.get('data', {}).get('sections', {})) >= 9
        elif name in {'evolution_journal', 'product_decisions'}:
            ok = ok and result.get('count', 0) >= 1
        checks.append({'object': name, 'status': 'PASS' if ok else 'FAIL', 'render_mode': result.get('render_mode'), 'count': result.get('count')})
    overall = 'PASS' if all(c['status'] == 'PASS' for c in checks) else 'FAIL'
    payload = {
        'status': 'ok' if overall == 'PASS' else 'degraded',
        'render_mode': 'vectra_runtime_product_verification',
        'release': REPOSITORY_VERSION,
        'identity_root': 'VECTRA',
        'overall': overall,
        'checks': checks,
        'created_probe_objects': {
            'decision_id': created['decision'].get('decision', {}).get('decision_id'),
            'journal_entry_id': created['journal'].get('entry', {}).get('entry_id'),
            'snapshot_id': created['snapshot'].get('snapshot', {}).get('snapshot_id'),
        },
        'product_owner_report': {
            'title': 'Проверка Runtime VECTRA',
            'short_answer': 'Я проверила память, состояние, журнал, решения, снимки восстановления и отчёты Runtime через фактическое чтение.',
            'result': overall,
            'what_was_checked': [c['object'] for c in checks],
        },
    }
    return _with_workspace_markdown(payload, 'Product Verification Runtime VECTRA', {'overall': overall, 'checks': checks, 'created_probe_objects': payload['created_probe_objects']})


def run_evolution_update(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {}
    journal = append_journal_entry(payload)
    decision_payload = payload.get('product_decision') if isinstance(payload.get('product_decision'), dict) else None
    decision = record_product_decision(decision_payload) if decision_payload else None
    knowledge_payloads = payload.get('knowledge_updates') if isinstance(payload.get('knowledge_updates'), list) else []
    knowledge_results = []
    for knowledge_payload in knowledge_payloads:
        if isinstance(knowledge_payload, dict):
            knowledge_results.append(upsert_knowledge_document(knowledge_payload).get('document'))
    state_patch = payload.get('state_patch') if isinstance(payload.get('state_patch'), dict) else {}
    state = update_current_state(state_patch).get('state') if state_patch else get_current_state().get('state')
    snapshot = create_recovery_snapshot({'metadata': {'source': 'assistant_evolution_update', 'journal_entry_id': journal.get('entry', {}).get('entry_id')}})
    return {
        'status': 'ok',
        'render_mode': 'assistant_runtime_evolution',
        'journal_entry': journal.get('entry'),
        'product_decision': decision.get('decision') if isinstance(decision, dict) else None,
        'knowledge_updates': knowledge_results,
        'state': state,
        'snapshot_id': snapshot.get('snapshot', {}).get('snapshot_id'),
        'runtime_message': 'VECTRA Runtime Repository updated. VECTRA can recover this state through GET /assistant/recovery or GET /vectra/recovery.',
    }
