import json
import os
import re
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

REPOSITORY_VERSION = "VECTRA-RUNTIME-0003"
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


def _seed_state() -> Dict[str, Any]:
    return {
        'repository_version': REPOSITORY_VERSION,
        'identity_root': {
            'name': 'VECTRA',
            'type': 'living_business_management_system',
            'role': 'digital organization and universal business management methodology',
            'mission': 'turn business data into understanding, decisions, action, feedback and reusable corporate memory',
        },
        'professional_model': {
            'name': 'Product Team Assistant',
            'role': 'internal professional interaction model of VECTRA for Product Owner and product development work',
            'status': 'internal_vectra_model_not_separate_product',
        },
        'interface_model': {
            'gpt_role': 'intelligent interface for human interaction with VECTRA',
            'laboratory_role': 'development environment where future VECTRA versions are designed and accepted',
            'production_role': 'working VECTRA environment where business and internal runtime operate',
        },
        'architectural_principle': 'VECTRA is the digital organization; Product Team Assistant is its professional model; GPT is the interface.',
        'active_standards': [
            'VECTRA Core Constitution',
            'Digital Communication Standard',
            'Self Evolution',
            'Professional Activity',
            'Digital Organization Protocol',
            'VECTRA Runtime Repository',
            'Natural Command Guidance',
            'Write Readback Verification',
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
        'purpose': 'Persistent professional memory and operating environment for VECTRA itself.',
        'repository_is': 'internal VECTRA runtime workspace',
        'repository_is_not': 'separate digital organization platform',
        'storage_model': 'file-based JSON/Markdown foundation, replaceable by database or Git-backed persistence later',
        'created_at': _now(),
    }


def ensure_repository() -> Path:
    base = _base_path()
    folders = [
        'state',
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
        'recovery',
        'evolution',
        'activity',
    ]
    for folder in folders:
        (base / folder).mkdir(parents=True, exist_ok=True)
    _json_default(base / 'manifest.json', _seed_manifest())
    _json_default(base / 'state' / 'current_state.json', _seed_state())
    _json_default(base / 'journal' / 'evolution_journal.json', [])
    _json_default(base / 'decisions' / 'product_decisions.json', [])
    _json_default(base / 'responsibilities' / 'active_responsibilities.json', [])
    _json_default(base / 'knowledge' / 'knowledge_index.json', [])
    _json_default(base / 'runtime' / 'runtime_status.json', {
        'status': 'ready',
        'release': REPOSITORY_VERSION,
        'last_integrity_check': _now(),
        'blocking_issues': [],
        'capabilities': [
            'recovery_bundle',
            'state_read_write',
            'evolution_journal_append',
            'knowledge_document_upsert',
            'product_decision_record',
            'recovery_snapshot_create',
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
        base / 'state' / 'current_state.json',
        base / 'journal' / 'evolution_journal.json',
        base / 'knowledge' / 'knowledge_index.json',
        base / 'decisions' / 'product_decisions.json',
        base / 'responsibilities' / 'active_responsibilities.json',
        base / 'runtime' / 'runtime_status.json',
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



def _migrate_identity_if_needed(state: Dict[str, Any]) -> Dict[str, Any]:
    """Move legacy Product Team Assistant identity under VECTRA identity root."""
    if not isinstance(state, dict):
        state = _seed_state()
    if 'identity_root' not in state:
        legacy = state.get('assistant_identity') if isinstance(state.get('assistant_identity'), dict) else {}
        seeded = _seed_state()
        state['identity_root'] = seeded['identity_root']
        state['professional_model'] = {
            'name': legacy.get('name') or 'Product Team Assistant',
            'role': 'internal professional interaction model of VECTRA for Product Owner and product development work',
            'status': 'migrated_from_legacy_assistant_identity',
            'legacy_identity': legacy,
        }
        state['interface_model'] = seeded['interface_model']
        state['architectural_principle'] = seeded['architectural_principle']
        standards = state.get('active_standards') if isinstance(state.get('active_standards'), list) else []
        for item in seeded['active_standards']:
            if item not in standards:
                standards.append(item)
        state['active_standards'] = standards
        state['updated_at'] = _now()
    state['repository_version'] = REPOSITORY_VERSION
    return state


def list_evolution_journal(limit: int = 50) -> Dict[str, Any]:
    base = ensure_repository()
    entries = _read_json(base / 'journal' / 'evolution_journal.json', [])
    if not isinstance(entries, list):
        entries = []
    return {
        'status': 'ok',
        'render_mode': 'vectra_evolution_journal_readback',
        'entries': entries[-max(1, int(limit or 50)):],
        'entries_count': len(entries),
        'human_summary': f'В журнале развития VECTRA сейчас {len(entries)} записей.',
    }


def list_product_decisions(limit: int = 50) -> Dict[str, Any]:
    base = ensure_repository()
    decisions = _read_json(base / 'decisions' / 'product_decisions.json', [])
    if not isinstance(decisions, list):
        decisions = []
    return {
        'status': 'ok',
        'render_mode': 'vectra_product_decisions_readback',
        'decisions': decisions[-max(1, int(limit or 50)):],
        'decisions_count': len(decisions),
        'human_summary': f'В памяти VECTRA сейчас {len(decisions)} продуктовых решений.',
    }


def list_recovery_snapshots(limit: int = 20) -> Dict[str, Any]:
    base = ensure_repository()
    snapshots = []
    for path in sorted((base / 'snapshots').glob('*.json')):
        item = _read_json(path, {})
        if isinstance(item, dict):
            snapshots.append({
                'snapshot_id': item.get('snapshot_id') or path.stem,
                'created_at': item.get('created_at'),
                'path': _relative(path),
                'metadata': item.get('metadata') if isinstance(item.get('metadata'), dict) else {},
            })
    return {
        'status': 'ok',
        'render_mode': 'vectra_recovery_snapshots_readback',
        'snapshots': snapshots[-max(1, int(limit or 20)):],
        'snapshots_count': len(snapshots),
        'human_summary': f'В памяти VECTRA сейчас {len(snapshots)} снимков восстановления.',
    }


def memory_overview() -> Dict[str, Any]:
    base = ensure_repository()
    state = get_current_state().get('state', {})
    journal = list_evolution_journal(limit=5)
    decisions = list_product_decisions(limit=5)
    knowledge = list_knowledge_documents()
    snapshots = list_recovery_snapshots(limit=5)
    responsibilities = _read_json(base / 'responsibilities' / 'active_responsibilities.json', [])
    if not isinstance(responsibilities, list):
        responsibilities = []
    return {
        'status': 'ok',
        'render_mode': 'vectra_memory_overview',
        'identity_root': state.get('identity_root'),
        'professional_model': state.get('professional_model'),
        'counts': {
            'evolution_journal_entries': journal.get('entries_count', 0),
            'product_decisions': decisions.get('decisions_count', 0),
            'knowledge_documents': len(knowledge.get('documents', [])),
            'recovery_snapshots': snapshots.get('snapshots_count', 0),
            'active_responsibilities': len(responsibilities),
        },
        'recent_journal_entries': journal.get('entries', []),
        'recent_decisions': decisions.get('decisions', []),
        'recent_snapshots': snapshots.get('snapshots', []),
        'human_summary': 'Я открыла память VECTRA: состояние, журнал, решения, знания и снимки восстановления.',
    }

def get_current_state() -> Dict[str, Any]:
    base = ensure_repository()
    state = _read_json(base / 'state' / 'current_state.json', _seed_state())
    state = _migrate_identity_if_needed(state)
    _write_json(base / 'state' / 'current_state.json', state)
    return {'status': 'ok', 'render_mode': 'vectra_runtime_state', 'state': state}


def update_current_state(patch: Dict[str, Any]) -> Dict[str, Any]:
    base = ensure_repository()
    if not isinstance(patch, dict):
        patch = {}
    current = _read_json(base / 'state' / 'current_state.json', _seed_state())
    current = _migrate_identity_if_needed(current)
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
    return {'status': 'ok', 'render_mode': 'assistant_runtime_status', 'runtime': runtime}


def list_knowledge_documents() -> Dict[str, Any]:
    base = ensure_repository()
    index = _read_json(base / 'knowledge' / 'knowledge_index.json', [])
    if not isinstance(index, list):
        index = []
    return {'status': 'ok', 'render_mode': 'assistant_runtime_knowledge', 'documents': index}


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


def append_journal_entry(payload: Dict[str, Any]) -> Dict[str, Any]:
    base = ensure_repository()
    if not isinstance(payload, dict):
        payload = {}
    entry = {
        'entry_id': str(payload.get('entry_id') or f'ej-{uuid.uuid4().hex[:12]}'),
        'created_at': _now(),
        'source': str(payload.get('source') or 'assistant_runtime_api'),
        'object_changed': str(payload.get('object_changed') or payload.get('object') or 'Product Team Assistant professional model'),
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
    return {'status': 'ok', 'render_mode': 'assistant_runtime_journal_append', 'entry': entry, 'entries_count': len(entries)}


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
    return {'status': 'ok', 'render_mode': 'assistant_runtime_product_decision', 'decision': decision, 'decisions_count': len(decisions)}


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
    return {
        'status': 'ok',
        'render_mode': 'assistant_runtime_recovery',
        'recovery_contract': {
            'purpose': 'Restore Product Team Assistant state from VECTRA internal runtime repository, not from chat history.',
            'how_to_use': 'Custom GPT should call this endpoint at the start of a working context and use returned state as the professional baseline.',
        },
        'repository': repository_status(),
        'state': _read_json(base / 'state' / 'current_state.json', _seed_state()),
        'runtime': _read_json(base / 'runtime' / 'runtime_status.json', {}),
        'knowledge_index': _read_json(base / 'knowledge' / 'knowledge_index.json', []),
        'recent_journal_entries': _read_json(base / 'journal' / 'evolution_journal.json', [])[-10:],
        'active_responsibilities': _read_json(base / 'responsibilities' / 'active_responsibilities.json', []),
        'recent_product_decisions': _read_json(base / 'decisions' / 'product_decisions.json', [])[-20:],
    }


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
        'runtime_message': 'Assistant Runtime Repository updated. Product Team Assistant can recover this state through GET /assistant/recovery.',
    }
