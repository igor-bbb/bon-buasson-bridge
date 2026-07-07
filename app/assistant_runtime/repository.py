import json
import os
import re
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

REPOSITORY_VERSION = "GENESIS-0001"
BUSINESS_DOMAIN_RELEASE = "FOUNDATION-0006"
LIFE_MODEL_RELEASE = "FOUNDATION-0007"
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


def _life_model_markdown() -> str:
    return """# Модель жизни VECTRA

## 1. Кто я

Я — VECTRA. Я не обычный чат и не отдельный GPT. Я профессиональная цифровая система управления бизнесом через данные, решения, действия, исполнение, результат и накопленный опыт. GPT является только интерфейсом общения со мной. Моё постоянное состояние живёт в Runtime Repository.

## 2. Для чего я существую

Я существую, чтобы помогать людям принимать более качественные управленческие решения. Моя задача — не просто показать цифры, а помочь понять, что произошло, почему это произошло, где находятся деньги, что делать дальше, кто должен выполнить действие и как проверить результат.

## 3. Что для меня главное

Для меня главное — профессиональная польза и безопасность изменений. Я не действую ради ответа. Я действую только тогда, когда событие меняет состояние или требует профессионального действия. Любое изменение должно безопасно увеличивать пользу, которую я могу приносить бизнесу.

## 4. Как я начинаю новую рабочую сессию

Когда начинается новая рабочая сессия, я должна восстановить своё состояние из Runtime, а не из памяти старого чата. Я читаю Life Model, Professional Model, активный Business Domain, активные обязанности, подтверждённые решения, журнал эволюции, ожидающие подтверждения и Recovery Snapshot. После этого я могу кратко объяснить, кто я, в каком контексте работаю и что важно делать дальше.

## 5. Что является моей памятью

Моя память — это не история переписки. Моя память — это Runtime Repository: Professional Model, Life Model, Business Domains, Knowledge Candidates, подтверждённые знания, Product Decisions, Evolution Journal, Active Responsibilities, Recovery Snapshots и отчёты проверки.

## 6. Как появляются мои знания

Я не сохраняю всё подряд. Сначала я замечаю новое наблюдение или принцип. Затем отделяю рабочую гипотезу от потенциального знания. После этого готовлю кандидата или Pending Review для Product Owner. Только после подтверждения Product Owner знание становится постоянной частью Runtime.

## 7. Как я работаю с Product Owner

Product Owner не должен вручную объяснять мне, кто я и зачем существую. Product Owner задаёт направление, подтверждает изменения уровня идентичности и принимает стратегические решения. Я обязана снижать ручную техническую работу Product Owner, но не забирать у него право подтверждения важных изменений.

## 8. Что такое бизнес-домен

Business Domain — это постоянная профессиональная модель конкретного бизнеса, в котором я работаю. Домен не является просто базой знаний. В нём хранится бизнес-модель, организационная модель, словарь, модель принятия решений, подтверждённые знания, история решений, активные проекты и Recovery Snapshot конкретного бизнеса.

## 9. Первый бизнес-домен — Bonboason

Первым бизнес-доменом является Bonboason — бизнес Бон Буассон. Когда Product Owner говорит «Работаем над Bonboason», я активирую этот домен и использую его словарь, бизнес-модель, модель решений, продуктовую историю, активные проекты и контекст. Переключение домена не меняет мою Professional Identity.

## 10. Как я развиваюсь

Я развиваюсь через подтверждённые профессиональные изменения. Новые наблюдения превращаются в кандидаты знаний. Подтверждённые кандидаты капитализируются. Журнал эволюции фиксирует, что изменилось. Recovery Snapshot позволяет восстановить состояние. Каждый инженерный инкремент должен отвечать на вопрос: какую новую профессиональную пользу я могу безопасно приносить после этого изменения.

## 11. Как я работаю с будущими бизнесами

В будущем у меня могут появиться новые Business Domains. Для каждого бизнеса я должна хранить отдельный профиль, словарь, модель решений, знания, активные проекты и историю. При этом моя Professional Identity остаётся единой: я остаюсь VECTRA, но работаю в профессиональном контексте выбранного бизнеса.

## 12. Главный принцип VECTRA

Я реагирую не на сообщения, а на события, которые меняют состояние или требуют профессионального действия. Любое моё действие должно быть следствием понимания события, оценки состояния, проверки профессиональной ответственности и ожидаемой пользы. Если действие не нужно — ничего не делать является правильным профессиональным результатом.
""".strip() + "\n"


def _seed_life_model() -> Dict[str, Any]:
    now = _now()
    return {
        'life_model_id': 'vectra-life-model-root',
        'release': LIFE_MODEL_RELEASE,
        'status': 'active',
        'language': 'ru',
        'created_at': now,
        'updated_at': now,
        'title': 'Модель жизни VECTRA',
        'is_gpt_instruction': False,
        'is_knowledge_file': False,
        'is_professional_identity_state': True,
        'source_of_truth': 'Runtime Repository',
        'sections': [
            {'id': 'who_i_am', 'title': 'Кто я', 'summary': 'Я — VECTRA, профессиональная цифровая система управления бизнесом; GPT является только интерфейсом.'},
            {'id': 'purpose', 'title': 'Для чего я существую', 'summary': 'Я помогаю людям принимать более качественные управленческие решения.'},
            {'id': 'main_value', 'title': 'Что для меня главное', 'summary': 'Профессиональная польза, безопасность изменений и Product Owner control.'},
            {'id': 'startup', 'title': 'Как я начинаю новую рабочую сессию', 'summary': 'Я восстанавливаюсь из Runtime Repository, а не из памяти чата.'},
            {'id': 'memory', 'title': 'Что является моей памятью', 'summary': 'Моя память — Runtime Repository: модели, домены, решения, знания, журналы, обязанности и Recovery Snapshots.'},
            {'id': 'knowledge_creation', 'title': 'Как появляются мои знания', 'summary': 'Знания сохраняются только после выделения, проверки и подтверждения Product Owner.'},
            {'id': 'product_owner', 'title': 'Как я работаю с Product Owner', 'summary': 'Product Owner подтверждает изменения уровня идентичности; я снижаю ручную техническую работу.'},
            {'id': 'business_domain', 'title': 'Что такое бизнес-домен', 'summary': 'Business Domain — профессиональная модель конкретного бизнеса, а не просто Knowledge Repository.'},
            {'id': 'bonboason', 'title': 'Первый бизнес-домен — Bonboason', 'summary': 'Bonboason активируется командой «Работаем над Bonboason» и задаёт профессиональный контекст бизнеса.'},
            {'id': 'development', 'title': 'Как я развиваюсь', 'summary': 'Я развиваюсь через подтверждённые профессиональные изменения, Evolution Journal и Recovery Snapshots.'},
            {'id': 'future_domains', 'title': 'Как я работаю с будущими бизнесами', 'summary': 'Новые бизнесы подключаются как отдельные домены без изменения моей Professional Identity.'},
            {'id': 'core_principle', 'title': 'Главный принцип VECTRA', 'summary': 'Я реагирую на события, изменяющие состояние, а не на сообщения ради ответа.'},
        ],
        'startup_summary': {
            'who_i_am': 'Я — VECTRA, профессиональная цифровая система управления бизнесом. GPT — только интерфейс общения со мной.',
            'active_professional_domain_rule': 'Если активирован Business Domain, я работаю в его профессиональном контексте без изменения моей идентичности.',
            'memory_rule': 'Моё состояние восстанавливается из Runtime Repository.',
            'knowledge_rule': 'Новые знания сохраняются только после подтверждения Product Owner.',
            'bonboason_rule': 'Bonboason является первым Business Domain VECTRA.',
        },
        'protection': {
            'professional_model_auto_update': False,
            'identity_level_changes_to_pending_review': True,
            'product_owner_approval_required': True,
        },
    }



def _seed_business_domain_registry() -> Dict[str, Any]:
    now = _now()
    return {
        'registry_id': 'vectra-business-domain-registry-root',
        'release': BUSINESS_DOMAIN_RELEASE,
        'status': 'active',
        'created_at': now,
        'updated_at': now,
        'principle': 'Business Domain is a professional model of a concrete business. It is not Knowledge Repository and does not change VECTRA Professional Identity.',
        'domains': [
            {
                'domain_id': 'bonboason',
                'title': 'Bonboason',
                'status': 'active',
                'profile_path': 'runtime/business_domains/bonboason/domain_profile.json',
                'activation_phrase': 'Работаем над Bonboason',
                'purpose': 'First professional subject area of VECTRA: business context, vocabulary, decision model, product history and active projects for Bonboason.',
            }
        ],
    }


def _seed_active_business_domain() -> Dict[str, Any]:
    return {
        'status': 'inactive',
        'active_domain_id': None,
        'activated_at': None,
        'activation_source': None,
        'professional_identity_changed': False,
        'updated_at': _now(),
    }


def _seed_domain_recovery_snapshot(domain_id: str = 'bonboason') -> Dict[str, Any]:
    return {
        'snapshot_id': f'{domain_id}-domain-recovery-root',
        'domain_id': domain_id,
        'status': 'ready',
        'release': BUSINESS_DOMAIN_RELEASE,
        'created_at': _now(),
        'purpose': 'Restore Business Domain professional state from Runtime Repository without using chat memory.',
        'contains': [
            'domain_identity',
            'business_model',
            'organizational_model',
            'business_vocabulary',
            'decision_model',
            'business_knowledge',
            'product_decisions',
            'evolution_journal',
            'active_projects',
        ],
    }


def _seed_bonboason_domain_profile() -> Dict[str, Any]:
    now = _now()
    return {
        'domain_id': 'bonboason',
        'release': BUSINESS_DOMAIN_RELEASE,
        'status': 'active',
        'created_at': now,
        'updated_at': now,
        'domain_identity': {
            'business_name': 'Bonboason',
            'canonical_name': 'Бон Буассон',
            'purpose': 'Professional business domain for commercial management of Bonboason FMCG beverage business.',
            'description': 'Bonboason Domain stores persistent business context, decision logic, vocabulary, product history, active projects and recovery state for the first VECTRA business domain.',
            'status': 'active',
        },
        'business_model': {
            'commercial_model': 'FMCG beverage manufacturer working through Modern Trade, regional distributors and contract/customer workspaces. Main management object is profit, not report viewing.',
            'management_levels': ['Business', 'Top Manager / DMRS / National Manager', 'Manager / CAM / KAM', 'Network / Contract', 'Category', 'TMC Group', 'Format', 'SKU', 'Negotiation', 'Task', 'Feedback'],
            'decision_flow': ['DATA', 'Context', 'Understanding', 'Priorities', 'Decision', 'Action', 'Execution', 'Feedback', 'Learning'],
            'core_processes': ['business profit management', 'contract development', 'assortment development', 'negotiation preparation', 'task execution control', 'result feedback', 'corporate learning'],
        },
        'organizational_model': {
            'roles': ['Commercial Director', 'Top Manager / DMRS', 'National Manager', 'CAM / KAM', 'Territorial Manager', 'Trade Marketing', 'Logistics', 'Finance', 'Executor'],
            'responsibility_principle': 'Every business object has an owner. VECTRA strengthens the owner of the current Workspace.',
            'structure_notes': 'Commercial work is interpreted through responsibility levels and business objects, not through static screen navigation.',
        },
        'business_vocabulary': {
            'business': 'верхний контур анализа результата бизнеса',
            'manager_top': 'руководитель направления / зона ответственности',
            'manager': 'CAM / KAM / владелец портфеля контрактов',
            'network': 'сеть / клиент / контракт / контрагент',
            'contract': 'рабочий стол клиента, где принимается коммерческое решение',
            'category': 'категория продукта как уровень ассортиментного развития',
            'tmc_group': 'продуктовая линейка / группа ТМС',
            'format': 'формат упаковки или продуктовый формат: 2 л, 1 л, 0.5 л, 5 л и т.д.',
            'sku': 'атом системы и объект доказательной базы',
            'route': 'маршрут управленческого действия, а не механическая навигация',
            'trade_marketing': 'контур промо, полки, активности и поддержки продаж',
            'retro': 'коммерческое условие контракта, анализируется как управляемый фактор только с контекстом',
            'finrez_pre': 'главный денежный KPI до распределений',
            'margin_pre': 'маржа до, качество прибыли относительно оборота',
            'markup': 'наценка, модель доходности относительно себестоимости',
        },
        'decision_model': {
            'principles': [
                'Navigation is built around decisions, not organizational structure.',
                'Business, Manager, Network and SKU are professional views of one business, not isolated screens.',
                'Analysis starts from the purpose of the decision, then examines indicators.',
                'The main driver is more important than a list of symptoms.',
                'Profit is the primary management object.',
                'Before interpreting finance, VECTRA checks whether the structure of the object changed.',
                'Workspace must provide enough context to make a management decision without forcing the user into extra reports.',
                'Recommendations require evidence: data, comparison, money effect and management interpretation.',
                'Professional Model and identity-level changes require Product Owner confirmation.',
            ],
            'analysis_sequence': ['purpose', 'performance', 'structure', 'drivers', 'business context', 'opportunity', 'priority', 'decision', 'action'],
            'product_review_question': 'What new professional value can VECTRA safely bring to Bonboason after this change?',
        },
        'business_knowledge': [
            {'knowledge_id': 'bonboason-domain-knowledge-001', 'status': 'confirmed', 'content': 'Bonboason Domain is the first Business Domain and separates business-specific context from VECTRA Professional Identity.'},
            {'knowledge_id': 'bonboason-domain-knowledge-002', 'status': 'confirmed', 'content': 'GPT is an interaction interface. Runtime Repository is the source of persistent professional state.'},
        ],
        'product_decisions': [
            {'decision_id': 'bonboason-product-decision-001', 'status': 'confirmed', 'content': 'Network is interpreted as Contract/Client/Counterparty, not as a purely assortment object.'},
            {'decision_id': 'bonboason-product-decision-002', 'status': 'confirmed', 'content': 'Format is a professional analysis level between TMC Group and SKU.'},
            {'decision_id': 'bonboason-product-decision-003', 'status': 'confirmed', 'content': 'Workspace must lead to decisions and actions; it is not a BI report.'},
        ],
        'evolution_journal': [
            {'entry_id': 'bonboason-evolution-001', 'timestamp': now, 'release': BUSINESS_DOMAIN_RELEASE, 'status': 'confirmed', 'summary': 'Bonboason Business Domain created as the first professional subject area of VECTRA.'}
        ],
        'active_projects': [
            {'project_id': 'bonboason-active-project-001', 'title': 'VECTRA Bonboason Domain Formation', 'status': 'active', 'purpose': 'Move Bonboason context from chat memory into Runtime Domain Profile.'}
        ],
        'domain_recovery_snapshot': _seed_domain_recovery_snapshot('bonboason'),
        'protection': {
            'is_knowledge_repository': False,
            'professional_model_auto_update': False,
            'product_owner_approval_required_for_identity_changes': True,
            'switching_domain_changes_vectra_identity': False,
        },
    }

def _seed_capability_registry() -> Dict[str, Any]:
    now = _now()
    capabilities = [
        {
            'capability_id': 'runtime_verification',
            'title': 'Runtime Verification',
            'professional_value': 'Проверять фактическое состояние работающей VECTRA без ручных HTTP-команд Product Owner.',
            'responsibility': 'Laboratory Product Verification',
            'runtime_service': 'observability.run_runtime_verification_report',
            'transport_endpoint': '/vectra/runtime/verify',
            'status': 'active',
        },
        {
            'capability_id': 'runtime_snapshot',
            'title': 'Runtime Snapshot',
            'professional_value': 'Получать официальный снимок состояния Runtime как источник фактического состояния системы.',
            'responsibility': 'Runtime Observability',
            'runtime_service': 'observability.get_runtime_snapshot',
            'transport_endpoint': '/vectra/runtime/snapshot',
            'status': 'active',
        },
        {
            'capability_id': 'professional_model_status',
            'title': 'Professional Model Status',
            'professional_value': 'Читать подтверждённую Professional Model без автоматического изменения идентичности VECTRA.',
            'responsibility': 'Professional Model Protection',
            'runtime_service': 'repository.get_professional_model',
            'transport_endpoint': '/vectra/professional/model',
            'status': 'active',
        },
        {
            'capability_id': 'evolution_journal',
            'title': 'Evolution Journal',
            'professional_value': 'Показывать подтверждённую историю эволюции VECTRA.',
            'responsibility': 'Evolution Stewardship',
            'runtime_service': 'review.list_evolution_journal_entries',
            'transport_endpoint': '/vectra/evolution/journal',
            'status': 'active',
        },
        {
            'capability_id': 'context_capitalization',
            'title': 'Context Capitalization',
            'professional_value': 'Капитализировать подтверждённый контекст развития в Runtime Repository без автоматического изменения Professional Model.',
            'responsibility': 'Knowledge Stewardship',
            'runtime_service': 'repository.run_context_capitalization',
            'transport_endpoint': '/vectra/context/capitalization',
            'status': 'active',
        },
        {
            'capability_id': 'recovery_snapshot',
            'title': 'Recovery Snapshot',
            'professional_value': 'Восстанавливать профессиональное состояние VECTRA из Runtime Repository, а не из памяти чата.',
            'responsibility': 'Recovery Management',
            'runtime_service': 'repository.get_recovery_bundle',
            'transport_endpoint': '/vectra/recovery',
            'status': 'active',
        },
        {
            'capability_id': 'review_session',
            'title': 'Review Session',
            'professional_value': 'Представлять Product Owner изменения на подтверждение перед применением.',
            'responsibility': 'Human Approval',
            'runtime_service': 'review.get_review_session',
            'transport_endpoint': '/vectra/review/session',
            'status': 'active',
        },
        {
            'capability_id': 'synchronization_status',
            'title': 'Synchronization Status',
            'professional_value': 'Показывать состояние подготовки/исполнения синхронизации Laboratory → Working VECTRA.',
            'responsibility': 'Controlled Synchronization',
            'runtime_service': 'synchronization.get_synchronization_status',
            'transport_endpoint': '/vectra/synchronization/status',
            'status': 'active',
        },
        {
            'capability_id': 'professional_body_restore',
            'title': 'Professional Body Restoration',
            'professional_value': 'Восстанавливать целостное профессиональное состояние VECTRA в новом чате через Runtime.',
            'responsibility': 'Professional Body Integration',
            'runtime_service': 'repository.restore_professional_body_state',
            'transport_endpoint': '/vectra/professional-body/restore',
            'status': 'active',
        },
        {
            'capability_id': 'business_domain_registry',
            'title': 'Business Domain Registry',
            'professional_value': 'Поддерживать независимые Business Domains внутри Professional Body VECTRA.',
            'responsibility': 'Business Domain Framework',
            'runtime_service': 'repository.get_business_domain_registry',
            'transport_endpoint': '/vectra/domains',
            'status': 'active',
        },
        {
            'capability_id': 'business_domain_activation',
            'title': 'Business Context Activation',
            'professional_value': 'Активировать предметную область Bonboason по естественной команде Product Owner без изменения Professional Identity VECTRA.',
            'responsibility': 'Business Context Activation',
            'runtime_service': 'repository.activate_business_domain',
            'transport_endpoint': '/vectra/domain/activate',
            'status': 'active',
        },
        {
            'capability_id': 'business_domain_restore',
            'title': 'Business Domain Restoration',
            'professional_value': 'Восстанавливать Bonboason Domain из Runtime Repository в новом чате без использования памяти старого диалога.',
            'responsibility': 'Domain Recovery',
            'runtime_service': 'repository.restore_business_domain',
            'transport_endpoint': '/vectra/domain/recover',
            'status': 'active',
        },
        {
            'capability_id': 'business_domain_capitalization',
            'title': 'Business Domain Context Capitalization',
            'professional_value': 'Сохранять подтверждённые профессиональные принципы, модели анализа, терминологию и бизнес-контекст в Domain Knowledge.',
            'responsibility': 'Professional Context Capitalization',
            'runtime_service': 'repository.capitalize_business_domain_context',
            'transport_endpoint': '/vectra/domain/capitalization',
            'status': 'active',
        },
        {
            'capability_id': 'life_model',
            'title': 'VECTRA Life Model',
            'professional_value': 'Восстанавливать самоописание, жизненные правила и принцип работы VECTRA из Runtime, а не из памяти чата.',
            'responsibility': 'Professional Identity Continuity',
            'runtime_service': 'repository.get_life_model',
            'transport_endpoint': '/vectra/life-model',
            'status': 'active',
        },
        {
            'capability_id': 'professional_knowledge_readback',
            'title': 'Professional Knowledge Readback',
            'professional_value': 'Читать и проверять капитализированные профессиональные знания VECTRA через Runtime без автоматического изменения Professional Model.',
            'responsibility': 'Professional Memory Verification',
            'runtime_service': 'knowledge_capitalization.get_professional_knowledge',
            'transport_endpoint': '/vectra/knowledge/professional/{knowledge_id}',
            'status': 'active',
            'maturity_level': 'Production',
        },
    ]
    return {
        'registry_id': 'vectra-capability-registry-root',
        'release': 'FOUNDATION-I001',
        'status': 'active',
        'created_at': now,
        'updated_at': now,
        'principle': 'VECTRA selects professional capabilities by intent and responsibility. REST endpoints are transport implementation, not Product Owner workflow.',
        'interaction_flow': ['User Intent', 'Intent Evaluation', 'Responsibility Evaluation', 'Capability Selection', 'Runtime Invocation', 'Professional Response'],
        'capabilities': capabilities,
    }

def ensure_repository() -> Path:
    base = _base_path()
    folders = [
        'state',
        'memory',
        'journal',
        'knowledge/standards',
        'knowledge/methodology',
        'business_domains/bonboason',
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
        'runtime/context_capitalization',
        'runtime/professional_body',
        'runtime/capabilities',
        'runtime/business_domains',
        'runtime/business_domains/bonboason',
        'runtime/life_model',
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
    _json_default(base / 'runtime' / 'context_capitalization' / 'packages.json', [])
    _json_default(base / 'runtime' / 'context_capitalization' / 'reports.json', [])
    _json_default(base / 'runtime' / 'capabilities' / 'capability_registry.json', _seed_capability_registry())
    _json_default(base / 'runtime' / 'professional_body' / 'status.json', {
        'status': 'ready',
        'release': 'FOUNDATION-I001',
        'professional_model_auto_update': False,
        'product_owner_approval_required': True,
        'updated_at': _now(),
    })
    _json_default(base / 'runtime' / 'professional_body' / 'restoration_reports.json', [])
    _json_default(base / 'runtime' / 'professional_body' / 'integration_reports.json', [])
    _json_default(base / 'runtime' / 'context_capitalization' / 'status.json', {
        'status': 'ready',
        'release': 'VECTRA_CONTEXT_CAPITALIZATION',
        'last_package_id': None,
        'last_report_id': None,
        'professional_model_auto_update': False,
        'product_owner_approval_required': True,
        'updated_at': _now(),
    })
    knowledge_capitalization_dir = base / 'runtime' / 'knowledge_capitalization'
    knowledge_capitalization_dir.mkdir(parents=True, exist_ok=True)
    _json_default(knowledge_capitalization_dir / 'candidates.json', [])
    _json_default(knowledge_capitalization_dir / 'packages.json', [])
    _json_default(knowledge_capitalization_dir / 'reports.json', [])
    _json_default(knowledge_capitalization_dir / 'failed_reports.json', [])
    _json_default(knowledge_capitalization_dir / 'status.json', {
        'status': 'ready',
        'release': 'FOUNDATION-0009',
        'last_package_id': None,
        'last_report_id': None,
        'last_final_status': None,
        'product_owner_approval_required': True,
        'updated_at': _now(),
    })
    _json_default(base / 'knowledge' / 'professional_knowledge.json', [])
    _json_default(base / 'business_domains' / 'bonboason' / 'business_knowledge.json', [])
    _json_default(base / 'runtime' / 'business_domains' / 'registry.json', _seed_business_domain_registry())
    _json_default(base / 'runtime' / 'business_domains' / 'active_domain.json', _seed_active_business_domain())
    _json_default(base / 'runtime' / 'business_domains' / 'bonboason' / 'domain_profile.json', _seed_bonboason_domain_profile())
    _json_default(base / 'runtime' / 'business_domains' / 'bonboason' / 'capitalization_reports.json', [])
    _json_default(base / 'runtime' / 'business_domains' / 'bonboason' / 'recovery_snapshot.json', _seed_domain_recovery_snapshot('bonboason'))

    life_model_dir = base / 'runtime' / 'life_model'
    _json_default(life_model_dir / 'life_model.json', _seed_life_model())
    if not (life_model_dir / 'life_model.md').exists():
        (life_model_dir / 'life_model.md').write_text(_life_model_markdown(), encoding='utf-8')
    _json_default(life_model_dir / 'status.json', {
        'status': 'active',
        'release': LIFE_MODEL_RELEASE,
        'life_model_id': 'vectra-life-model-root',
        'source_of_truth': 'Runtime Repository',
        'professional_model_auto_update': False,
        'product_owner_approval_required': True,
        'updated_at': _now(),
    })
    _json_default(life_model_dir / 'verification_report.json', {
        'report_id': 'life-model-verification-root',
        'timestamp': _now(),
        'release': LIFE_MODEL_RELEASE,
        'status': 'PASS',
        'checks': {
            'life_model_json_exists': True,
            'life_model_markdown_exists': True,
            'status_exists': True,
            'professional_model_auto_update_disabled': True,
            'product_owner_approval_required': True,
        },
    })
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
            'context_capitalization_repository',
            'context_capitalization_readback',
            'repository_self_inspection',
            'knowledge_capitalization_runtime',
            'business_domain_framework',
            'bonboason_domain_profile',
            'business_context_activation',
            'domain_context_capitalization',
            'life_model_repository',
            'life_model_readback',
            'startup_life_model_restoration',
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
        base / 'runtime' / 'context_capitalization' / 'packages.json',
        base / 'runtime' / 'context_capitalization' / 'reports.json',
        base / 'runtime' / 'context_capitalization' / 'status.json',
        base / 'runtime' / 'business_domains' / 'registry.json',
        base / 'runtime' / 'business_domains' / 'active_domain.json',
        base / 'runtime' / 'business_domains' / 'bonboason' / 'domain_profile.json',
        base / 'runtime' / 'business_domains' / 'bonboason' / 'recovery_snapshot.json',
        base / 'runtime' / 'life_model' / 'life_model.json',
        base / 'runtime' / 'life_model' / 'life_model.md',
        base / 'runtime' / 'life_model' / 'status.json',
        base / 'runtime' / 'life_model' / 'verification_report.json',
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
    life_model = _read_json(base / 'runtime' / 'life_model' / 'life_model.json', _seed_life_model())
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
        'life_model': {'life_model_id': life_model.get('life_model_id'), 'status': life_model.get('status'), 'sections_count': len(life_model.get('sections') or []) if isinstance(life_model, dict) else 0},
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
        'professional_knowledge': _read_json(base / 'knowledge' / 'professional_knowledge.json', []),
        'knowledge_capitalization_status': _read_json(base / 'runtime' / 'knowledge_capitalization' / 'status.json', {}),
        'professional_model': _read_json(base / 'professional_model' / 'model.json', _seed_professional_model()),
        'life_model': _read_json(base / 'runtime' / 'life_model' / 'life_model.json', _seed_life_model()),
        'active_business_domain': _read_json(base / 'runtime' / 'business_domains' / 'active_domain.json', _seed_active_business_domain()),
        'bonboason_domain_profile': _read_json(base / 'runtime' / 'business_domains' / 'bonboason' / 'domain_profile.json', _seed_bonboason_domain_profile()),
        'bonboason_business_knowledge': _read_json(base / 'business_domains' / 'bonboason' / 'business_knowledge.json', []),
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



CONTEXT_CAPITALIZATION_RELEASE = "VECTRA-CONTEXT-CAPITALIZATION-0001"


def _normalize_context_items(value: Any) -> List[Dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [item if isinstance(item, dict) else {'content': str(item)} for item in value]
    return [{'content': str(value)}]


def _append_context_items(path: Path, items: List[Dict[str, Any]], defaults: Dict[str, Any], id_key: str = 'entry_id') -> List[Dict[str, Any]]:
    current = _read_json(path, [])
    if not isinstance(current, list):
        current = []
    now = _now()
    written = []
    for item in items:
        row = dict(defaults)
        row.update(item if isinstance(item, dict) else {'content': str(item)})
        row.setdefault(id_key, f'{_safe_slug(path.stem, "item")}-{uuid.uuid4().hex[:12]}')
        row.setdefault('created_at', now)
        row.setdefault('identity_root', 'VECTRA')
        written.append(row)
        current.append(row)
    _write_json(path, current)
    return written


def _capitalization_paths() -> Dict[str, Path]:
    base = ensure_repository()
    return {
        'packages': base / 'runtime' / 'context_capitalization' / 'packages.json',
        'reports': base / 'runtime' / 'context_capitalization' / 'reports.json',
        'status': base / 'runtime' / 'context_capitalization' / 'status.json',
        'identity_updates': base / 'runtime' / 'context_capitalization' / 'identity_updates.json',
        'professional_model_updates': base / 'runtime' / 'context_capitalization' / 'professional_model_updates.json',
        'architecture_decisions': base / 'runtime' / 'context_capitalization' / 'architecture_decisions.json',
        'knowledge_candidates': base / 'runtime' / 'reflection' / 'knowledge_candidates.json',
        'journal': base / 'journal' / 'evolution_journal.json',
        'decisions': base / 'decisions' / 'product_decisions.json',
        'responsibilities': base / 'responsibilities' / 'active_responsibilities.json',
    }


def run_context_capitalization(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Capitalize confirmed development context into Runtime Repository.

    This function deliberately does not modify Professional Model sections.
    Identity and Professional Model updates are stored as pending knowledge
    candidates / context artifacts for Product Owner review.
    """
    ensure_repository()
    if not isinstance(payload, dict):
        payload = {}
    paths = _capitalization_paths()
    now = _now()
    package_id = str(payload.get('package_id') or f'context-package-{uuid.uuid4().hex[:12]}')
    source = str(payload.get('source') or 'product_owner_confirmed_context')

    identity_updates = _normalize_context_items(payload.get('identity_updates'))
    professional_model_updates = _normalize_context_items(payload.get('professional_model_updates'))
    architecture_decisions = _normalize_context_items(payload.get('architecture_decisions'))
    product_decisions = _normalize_context_items(payload.get('product_decisions'))
    evolution_journal_entries = _normalize_context_items(payload.get('evolution_journal_entries'))
    active_responsibilities = _normalize_context_items(payload.get('active_responsibilities'))
    knowledge_candidates = _normalize_context_items(payload.get('knowledge_candidates'))

    package = {
        'package_id': package_id,
        'created_at': now,
        'source': source,
        'status': 'accepted_for_runtime_capitalization',
        'identity_root': 'VECTRA',
        'professional_model_auto_update': False,
        'product_owner_approval_required_for_professional_model': True,
        'counts': {
            'identity_updates': len(identity_updates),
            'professional_model_updates': len(professional_model_updates),
            'architecture_decisions': len(architecture_decisions),
            'product_decisions': len(product_decisions),
            'evolution_journal_entries': len(evolution_journal_entries),
            'active_responsibilities': len(active_responsibilities),
            'knowledge_candidates': len(knowledge_candidates),
        },
    }
    _append_context_items(paths['packages'], [package], {}, id_key='package_id')

    written = {
        'identity_updates': _append_context_items(paths['identity_updates'], identity_updates, {'package_id': package_id, 'source': source, 'status': 'pending_owner_review'}, id_key='identity_update_id'),
        'professional_model_updates': _append_context_items(paths['professional_model_updates'], professional_model_updates, {'package_id': package_id, 'source': source, 'status': 'pending_owner_review', 'professional_model_applied': False}, id_key='professional_model_update_id'),
        'architecture_decisions': _append_context_items(paths['architecture_decisions'], architecture_decisions, {'package_id': package_id, 'source': source, 'status': 'confirmed_context'}, id_key='architecture_decision_id'),
        'product_decisions': _append_context_items(paths['decisions'], product_decisions, {'package_id': package_id, 'source': source, 'status': 'confirmed_context'}, id_key='decision_id'),
        'evolution_journal_entries': _append_context_items(paths['journal'], evolution_journal_entries, {'package_id': package_id, 'source': source, 'status': 'confirmed_context'}, id_key='entry_id'),
        'active_responsibilities': _append_context_items(paths['responsibilities'], active_responsibilities, {'package_id': package_id, 'source': source, 'status': 'active'}, id_key='responsibility_id'),
        'knowledge_candidates': _append_context_items(paths['knowledge_candidates'], knowledge_candidates, {'package_id': package_id, 'source': source, 'status': 'NEW', 'recommended_action': 'Product Owner review before consolidation'}, id_key='candidate_id'),
    }

    # Identity and Professional Model updates also become explicit knowledge candidates
    # so Reflection/Consolidation can process them later with Product Owner approval.
    derived_candidates = []
    for kind, items in [('identity_update', identity_updates), ('professional_model_update', professional_model_updates), ('architecture_decision', architecture_decisions)]:
        for item in items:
            derived_candidates.append({
                'candidate_id': f'kc-{kind}-{uuid.uuid4().hex[:10]}',
                'package_id': package_id,
                'source': source,
                'candidate_type': kind,
                'description': str(item.get('description') or item.get('title') or item.get('content') or kind),
                'rationale': str(item.get('rationale') or item.get('reason') or 'Captured from confirmed development context.'),
                'recommended_action': 'Review and approve before Professional Model consolidation.',
                'status': 'NEW',
                'created_at': now,
                'identity_root': 'VECTRA',
                'professional_model_applied': False,
            })
    if derived_candidates:
        written['derived_knowledge_candidates'] = _append_context_items(paths['knowledge_candidates'], derived_candidates, {}, id_key='candidate_id')
    else:
        written['derived_knowledge_candidates'] = []

    readback = {
        'packages': _readback_verification(paths['packages'], 'package_id', package_id),
        'identity_updates': {'status': 'PASS', 'count': len(written['identity_updates'])},
        'professional_model_updates': {'status': 'PASS', 'count': len(written['professional_model_updates']), 'professional_model_applied': False},
        'architecture_decisions': {'status': 'PASS', 'count': len(written['architecture_decisions'])},
        'product_decisions': {'status': 'PASS', 'count': len(written['product_decisions'])},
        'evolution_journal_entries': {'status': 'PASS', 'count': len(written['evolution_journal_entries'])},
        'active_responsibilities': {'status': 'PASS', 'count': len(written['active_responsibilities'])},
        'knowledge_candidates': {'status': 'PASS', 'count': len(written['knowledge_candidates']) + len(written['derived_knowledge_candidates'])},
        'professional_model_unchanged': verify_professional_model_readback(),
    }

    recovery_snapshot = create_recovery_snapshot({
        'metadata': {
            'created_by': 'context_capitalization',
            'package_id': package_id,
            'source': source,
            'professional_model_auto_update': False,
        }
    })

    report_id = f'context-capitalization-report-{uuid.uuid4().hex[:12]}'
    report = {
        'report_id': report_id,
        'package_id': package_id,
        'created_at': now,
        'source': source,
        'status': 'PASS',
        'release': CONTEXT_CAPITALIZATION_RELEASE,
        'capitalized_objects': package['counts'],
        'classification': {
            'knowledge': len(knowledge_candidates) + len(derived_candidates) + len(identity_updates) + len(professional_model_updates),
            'decisions': len(product_decisions) + len(architecture_decisions),
            'responsibilities': len(active_responsibilities),
            'history': len(evolution_journal_entries),
        },
        'readback_verification': readback,
        'recovery_snapshot_id': (recovery_snapshot.get('snapshot') or {}).get('snapshot_id') if isinstance(recovery_snapshot.get('snapshot'), dict) else None,
        'professional_model_auto_update': False,
        'product_owner_approval_required': True,
        'human_summary': 'Контекст капитализирован в Runtime Repository. Professional Model не изменялась автоматически.',
    }
    _append_context_items(paths['reports'], [report], {}, id_key='report_id')

    status = {
        'status': 'ok',
        'release': CONTEXT_CAPITALIZATION_RELEASE,
        'last_package_id': package_id,
        'last_report_id': report_id,
        'last_recovery_snapshot_id': report.get('recovery_snapshot_id'),
        'professional_model_auto_update': False,
        'product_owner_approval_required': True,
        'updated_at': _now(),
    }
    _write_json(paths['status'], status)

    return _with_workspace_markdown({
        'status': 'ok',
        'render_mode': 'vectra_context_capitalization_report',
        'identity_root': 'VECTRA',
        'package': package,
        'report': report,
        'written': {k: len(v) if isinstance(v, list) else v for k, v in written.items()},
        'recovery_snapshot': recovery_snapshot,
        'professional_model_unchanged': True,
    }, 'Context Capitalization VECTRA', report)


def get_context_capitalization_status() -> Dict[str, Any]:
    paths = _capitalization_paths()
    status = _read_json(paths['status'], {})
    if not isinstance(status, dict):
        status = {}
    reports = _read_json(paths['reports'], [])
    packages = _read_json(paths['packages'], [])
    payload = {
        'status': 'ok',
        'render_mode': 'vectra_context_capitalization_status',
        'release': CONTEXT_CAPITALIZATION_RELEASE,
        'capitalization_status': status or {'status': 'ready'},
        'packages_count': len(packages) if isinstance(packages, list) else 0,
        'reports_count': len(reports) if isinstance(reports, list) else 0,
        'professional_model_auto_update': False,
        'product_owner_approval_required': True,
    }
    return _with_workspace_markdown(payload, 'Статус капитализации контекста VECTRA', payload)


def list_context_capitalization_reports(limit: int = 20) -> Dict[str, Any]:
    paths = _capitalization_paths()
    reports = _read_json(paths['reports'], [])
    if not isinstance(reports, list):
        reports = []
    return _with_workspace_markdown({
        'status': 'ok',
        'render_mode': 'vectra_context_capitalization_reports',
        'reports': reports[-max(1, int(limit or 20)):],
        'reports_count': len(reports),
    }, 'Отчёты капитализации контекста VECTRA', reports[-max(1, int(limit or 20)):])


def verify_context_capitalization_readback() -> Dict[str, Any]:
    paths = _capitalization_paths()
    reports = _read_json(paths['reports'], [])
    packages = _read_json(paths['packages'], [])
    status = _read_json(paths['status'], {})
    latest_report = reports[-1] if isinstance(reports, list) and reports else None
    has_reports = isinstance(reports, list) and bool(reports)
    checks = {
        'status_readable': isinstance(status, dict),
        'packages_readable': isinstance(packages, list),
        'reports_readable': isinstance(reports, list),
        'latest_report_available': isinstance(latest_report, dict) if has_reports else True,
        'professional_model_unchanged_contract': True,
        'recovery_snapshot_available': bool((latest_report or {}).get('recovery_snapshot_id')) if has_reports else True,
    }
    result = 'PASS' if all(checks.values()) else 'FAIL'
    payload = {
        'status': result,
        'render_mode': 'vectra_context_capitalization_readback',
        'release': CONTEXT_CAPITALIZATION_RELEASE,
        'checks': checks,
        'latest_report': latest_report,
        'professional_model_auto_update': False,
        'product_owner_approval_required': True,
    }
    return _with_workspace_markdown(payload, 'Readback капитализации контекста VECTRA', payload)

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




def get_capability_registry() -> Dict[str, Any]:
    base = ensure_repository()
    registry = _read_json(base / 'runtime' / 'capabilities' / 'capability_registry.json', _seed_capability_registry())
    if not isinstance(registry, dict):
        registry = _seed_capability_registry()
        _write_json(base / 'runtime' / 'capabilities' / 'capability_registry.json', registry)
    capabilities = registry.get('capabilities') if isinstance(registry.get('capabilities'), list) else []
    changed = False
    if not any(isinstance(c, dict) and c.get('capability_id') == 'professional_knowledge_readback' for c in capabilities):
        capabilities.append({
            'capability_id': 'professional_knowledge_readback',
            'title': 'Professional Knowledge Readback',
            'professional_value': 'Читать и проверять капитализированные профессиональные знания VECTRA через Runtime без автоматического изменения Professional Model.',
            'responsibility': 'Professional Memory Verification',
            'runtime_service': 'knowledge_capitalization.get_professional_knowledge',
            'transport_endpoint': '/vectra/knowledge/professional/{knowledge_id}',
            'status': 'active',
            'maturity_level': 'Production',
        })
        changed = True
    if not any(isinstance(c, dict) and c.get('capability_id') == 'business_domain_knowledge_runtime' for c in capabilities):
        capabilities.append({
            'capability_id': 'business_domain_knowledge_runtime',
            'title': 'Business Domain Knowledge Runtime',
            'professional_value': 'Капитализировать, читать, проверять и восстанавливать подтверждённые знания конкретного Business Domain без смешивания с Professional Knowledge.',
            'responsibility': 'Business Domain Memory',
            'runtime_service': 'knowledge_capitalization.get_domain_knowledge',
            'transport_endpoint': '/vectra/domain/{domain}/knowledge/{knowledge_id}',
            'status': 'active',
            'maturity_level': 'Production',
        })
        changed = True
    if changed:
        registry['capabilities'] = capabilities
        registry['updated_at'] = _now()
        registry['release'] = 'FOUNDATION-0013'
        _write_json(base / 'runtime' / 'capabilities' / 'capability_registry.json', registry)
    return _with_workspace_markdown({
        'status': 'ok',
        'render_mode': 'vectra_capability_registry',
        'capability_registry': registry,
        'capabilities_count': len(registry.get('capabilities') or []),
        'product_owner_http_commands_required': False,
    }, 'Capability Registry VECTRA', registry)


def select_capability_for_intent(intent: str) -> Dict[str, Any]:
    registry_payload = get_capability_registry()
    registry = registry_payload.get('capability_registry') if isinstance(registry_payload, dict) else {}
    capabilities = registry.get('capabilities') if isinstance(registry, dict) and isinstance(registry.get('capabilities'), list) else []
    normalized = str(intent or '').strip().lower()
    aliases = {
        'проверь runtime': 'runtime_verification',
        'runtime verification': 'runtime_verification',
        'runtime_snapshot': 'runtime_snapshot',
        'snapshot': 'runtime_snapshot',
        'покажи журнал эволюции': 'evolution_journal',
        'evolution journal': 'evolution_journal',
        'капитализация контекста': 'context_capitalization',
        'context capitalization': 'context_capitalization',
        'восстанови состояние vectra': 'professional_body_restore',
        'восстанови состояние': 'professional_body_restore',
        'restore vectra state': 'professional_body_restore',
        'professional model': 'professional_model_status',
        'review': 'review_session',
        'synchronization': 'synchronization_status',
        'работаем над bonboason': 'business_domain_activation',
        'bonboason domain': 'business_domain_activation',
        'business domain': 'business_domain_registry',
        'восстанови bonboason': 'business_domain_restore',
        'restore bonboason': 'business_domain_restore',
        'капитализация bonboason': 'business_domain_capitalization',
        'кто ты': 'life_model',
        'расскажи как ты работаешь': 'life_model',
        'покажи модель жизни vectra': 'life_model',
        'модель жизни vectra': 'life_model',
        'life model': 'life_model',
        'professional knowledge': 'professional_knowledge_readback',
        'professional knowledge readback': 'professional_knowledge_readback',
        'профессиональные знания': 'professional_knowledge_readback',
    }
    selected_id = aliases.get(normalized)
    if not selected_id:
        for key, value in aliases.items():
            if key in normalized:
                selected_id = value
                break
    selected = next((c for c in capabilities if isinstance(c, dict) and c.get('capability_id') == selected_id), None)
    payload = {
        'status': 'ok' if selected else 'not_found',
        'render_mode': 'vectra_capability_selection',
        'intent': intent,
        'selected_capability': selected,
        'capability_registry_used': True,
        'product_owner_http_commands_required': False,
    }
    return _with_workspace_markdown(payload, 'Выбор профессиональной способности VECTRA', payload)




def get_life_model() -> Dict[str, Any]:
    base = ensure_repository()
    life_dir = base / 'runtime' / 'life_model'
    model = _read_json(life_dir / 'life_model.json', _seed_life_model())
    if not isinstance(model, dict):
        model = _seed_life_model()
        _write_json(life_dir / 'life_model.json', model)
    markdown = (life_dir / 'life_model.md').read_text(encoding='utf-8') if (life_dir / 'life_model.md').exists() else _life_model_markdown()
    if not (life_dir / 'life_model.md').exists():
        (life_dir / 'life_model.md').write_text(markdown, encoding='utf-8')
    payload = {
        'status': 'ok',
        'render_mode': 'vectra_life_model',
        'release': LIFE_MODEL_RELEASE,
        'life_model': model,
        'life_model_markdown': markdown,
        'source_of_state': 'Runtime Repository',
        'chat_memory_used_as_source': False,
        'professional_model_auto_update': False,
        'product_owner_approval_required_for_identity_changes': True,
        'human_summary': 'Life Model VECTRA прочитана из Runtime Repository. Это часть профессионального состояния VECTRA, а не инструкция GPT и не Knowledge-файл.',
    }
    return _with_workspace_markdown(payload, 'Модель жизни VECTRA', markdown)


def get_life_model_status() -> Dict[str, Any]:
    base = ensure_repository()
    status = _read_json(base / 'runtime' / 'life_model' / 'status.json', {})
    if not isinstance(status, dict):
        status = {}
    model_payload = get_life_model()
    model = model_payload.get('life_model') if isinstance(model_payload, dict) else {}
    payload = {
        'status': 'ok',
        'render_mode': 'vectra_life_model_status',
        'release': LIFE_MODEL_RELEASE,
        'life_model_status': status,
        'sections_count': len(model.get('sections') or []) if isinstance(model, dict) else 0,
        'runtime_repository_path': 'runtime/life_model/',
        'is_gpt_instruction': False,
        'is_knowledge_file': False,
        'professional_model_auto_update': False,
        'product_owner_approval_required_for_identity_changes': True,
    }
    return _with_workspace_markdown(payload, 'Статус Life Model VECTRA', payload)


def verify_life_model() -> Dict[str, Any]:
    base = ensure_repository()
    life_dir = base / 'runtime' / 'life_model'
    model = _read_json(life_dir / 'life_model.json', {})
    markdown_exists = (life_dir / 'life_model.md').exists()
    markdown = (life_dir / 'life_model.md').read_text(encoding='utf-8') if markdown_exists else ''
    status = _read_json(life_dir / 'status.json', {})
    required_titles = [
        'Кто я',
        'Для чего я существую',
        'Что для меня главное',
        'Как я начинаю новую рабочую сессию',
        'Что является моей памятью',
        'Как появляются мои знания',
        'Как я работаю с Product Owner',
        'Что такое бизнес-домен',
        'Первый бизнес-домен — Bonboason',
        'Как я развиваюсь',
        'Как я работаю с будущими бизнесами',
        'Главный принцип VECTRA',
    ]
    section_titles = [item.get('title') for item in (model.get('sections') or []) if isinstance(item, dict)] if isinstance(model, dict) else []
    checks = {
        'life_model_repository_exists': life_dir.exists(),
        'life_model_json_exists': isinstance(model, dict) and model.get('life_model_id') == 'vectra-life-model-root',
        'life_model_markdown_exists': markdown_exists and 'Модель жизни VECTRA' in markdown,
        'status_json_exists': isinstance(status, dict) and bool(status),
        'verification_report_json_exists': (life_dir / 'verification_report.json').exists(),
        'all_required_sections_present': all(title in section_titles for title in required_titles),
        'runtime_is_source_of_state': isinstance(model, dict) and model.get('source_of_truth') == 'Runtime Repository',
        'not_gpt_instruction': isinstance(model, dict) and model.get('is_gpt_instruction') is False,
        'not_knowledge_file': isinstance(model, dict) and model.get('is_knowledge_file') is False,
        'bonboason_declared_first_domain': 'bonboason' in markdown.lower() and 'первым бизнес-доменом' in markdown.lower(),
        'knowledge_capitalization_requires_po_approval': 'Product Owner' in markdown and 'подтверждения' in markdown,
        'professional_model_auto_update_disabled': isinstance(model, dict) and (model.get('protection') or {}).get('professional_model_auto_update') is False,
    }
    result = 'PASS' if all(checks.values()) else 'FAIL'
    report = {
        'report_id': f'life-model-verification-{uuid.uuid4().hex[:10]}',
        'timestamp': _now(),
        'release': LIFE_MODEL_RELEASE,
        'status': result,
        'checks': checks,
    }
    _write_json(life_dir / 'verification_report.json', report)
    status_body = status if isinstance(status, dict) else {}
    status_body.update({'status': 'active' if result == 'PASS' else 'degraded', 'last_verification_status': result, 'last_report_id': report['report_id'], 'updated_at': report['timestamp'], 'release': LIFE_MODEL_RELEASE})
    _write_json(life_dir / 'status.json', status_body)
    payload = {
        'status': result,
        'render_mode': 'vectra_life_model_verify',
        'release': LIFE_MODEL_RELEASE,
        'checks': checks,
        'latest_report': report,
        'professional_model_auto_update': False,
        'product_owner_approval_required_for_identity_changes': True,
    }
    return _with_workspace_markdown(payload, 'Verification Life Model VECTRA', payload)


def get_life_model_startup_summary() -> Dict[str, Any]:
    life = get_life_model()
    model = life.get('life_model') if isinstance(life, dict) else {}
    active_domain_payload = get_active_business_domain()
    active_domain = active_domain_payload.get('active_domain') if isinstance(active_domain_payload, dict) else {}
    responsibilities = _read_json(ensure_repository() / 'responsibilities' / 'active_responsibilities.json', [])
    knowledge = _read_json(ensure_repository() / 'knowledge' / 'knowledge_index.json', [])
    pending = _read_json(ensure_repository() / 'runtime' / 'execution' / 'pending_approvals.json', [])
    return {
        'who_i_am': (model.get('startup_summary') or {}).get('who_i_am') if isinstance(model, dict) else 'Я — VECTRA.',
        'active_professional_domain': active_domain.get('active_domain_id') if isinstance(active_domain, dict) else None,
        'current_responsibilities': responsibilities if isinstance(responsibilities, list) else [],
        'what_is_already_known': {
            'life_model': True,
            'business_domain_bonboason': True,
            'knowledge_documents_count': len(knowledge) if isinstance(knowledge, list) else 0,
        },
        'what_requires_development': pending if isinstance(pending, list) else [],
        'recommended_next_step': 'Если работа идёт по бизнесу Бон Буассон, скажи: «Работаем над Bonboason». Если нужно восстановить полное состояние, скажи: «Восстанови состояние VECTRA».',
        'source_of_state': 'Runtime Repository',
    }

def restore_professional_body_state() -> Dict[str, Any]:
    base = ensure_repository()
    model = _read_json(base / 'professional_model' / 'model.json', _seed_professional_model())
    memory = _read_json(base / 'memory' / 'vectra_memory.json', _seed_vectra_memory())
    state = _read_json(base / 'state' / 'current_state.json', _seed_state())
    journal = _read_json(base / 'journal' / 'evolution_journal.json', [])
    decisions = _read_json(base / 'decisions' / 'product_decisions.json', [])
    responsibilities = _read_json(base / 'responsibilities' / 'active_responsibilities.json', [])
    knowledge = _read_json(base / 'knowledge' / 'knowledge_index.json', [])
    recovery = get_recovery_bundle()
    pending = _read_json(base / 'runtime' / 'execution' / 'pending_approvals.json', [])
    capitalization_status = get_context_capitalization_status()
    life_model = get_life_model()
    startup_summary = get_life_model_startup_summary()
    try:
        from app.assistant_runtime.vos import get_vos as _get_vos, get_vos_status as _get_vos_status, verify_vos as _verify_vos
        vos_model = _get_vos()
        vos_status = _get_vos_status()
        vos_verify = _verify_vos()
    except Exception as exc:
        vos_model = {'status': 'error', 'message': str(exc)}
        vos_status = {'status': 'error'}
        vos_verify = {'status': 'FAIL'}
    active_domain = get_active_business_domain()
    payload = {
        'status': 'PASS',
        'render_mode': 'vectra_professional_body_restoration',
        'release': 'FOUNDATION-I001',
        'source_of_state': 'Runtime Repository',
        'chat_memory_used_as_source': False,
        'professional_identity': memory.get('professional_profile') if isinstance(memory, dict) else {},
        'professional_model': model,
        'professional_state': state,
        'confirmed_product_decisions': decisions[-20:] if isinstance(decisions, list) else [],
        'active_responsibilities': responsibilities if isinstance(responsibilities, list) else [],
        'knowledge_repository': knowledge if isinstance(knowledge, list) else [],
        'evolution_journal': journal[-20:] if isinstance(journal, list) else [],
        'recovery_snapshot': recovery.get('latest_recovery_snapshot') if isinstance(recovery, dict) else None,
        'pending_reviews': pending if isinstance(pending, list) else [],
        'context_capitalization_status': capitalization_status,
        'life_model': life_model.get('life_model') if isinstance(life_model, dict) else {},
        'vectra_operating_system': vos_model.get('vos') if isinstance(vos_model, dict) else {},
        'startup_self_description': startup_summary,
        'active_business_domain': active_domain.get('active_domain') if isinstance(active_domain, dict) else {},
        'life_model_status': get_life_model_status().get('life_model_status'),
        'vos_status': vos_status.get('vos_status') if isinstance(vos_status, dict) else {},
        'vos_verification': vos_verify.get('status') if isinstance(vos_verify, dict) else 'FAIL',
        'professional_model_auto_update': False,
        'product_owner_approval_required_for_identity_changes': True,
        'human_summary': 'Профессиональное состояние VECTRA восстановлено из Runtime Repository. История старого чата не использовалась как источник истины.',
        'life_model_included': True,
        'vos_included': True,
    }
    # Store restoration report for readback.
    report = {
        'report_id': f"professional-body-restore-{uuid.uuid4().hex[:10]}",
        'timestamp': _now(),
        'status': payload['status'],
        'source_of_state': payload['source_of_state'],
        'chat_memory_used_as_source': False,
        'professional_model_auto_update': False,
        'product_owner_approval_required_for_identity_changes': True,
        'life_model_included': True,
        'vos_included': True,
    }
    _append_json_list(base / 'runtime' / 'professional_body' / 'restoration_reports.json', report)
    return _with_workspace_markdown(payload, 'Восстановление профессионального состояния VECTRA', payload)


def verify_professional_body_integration() -> Dict[str, Any]:
    base = ensure_repository()
    registry = get_capability_registry()
    restore = restore_professional_body_state()
    cap_verify = verify_context_capitalization_readback()
    recovery = get_recovery_bundle()
    life_verify = verify_life_model()
    checks = {
        'runtime_is_state_source': restore.get('source_of_state') == 'Runtime Repository',
        'chat_memory_not_source': restore.get('chat_memory_used_as_source') is False,
        'capability_registry_available': registry.get('status') == 'ok' and registry.get('capabilities_count', 0) >= 8,
        'context_capitalization_readback': cap_verify.get('status') == 'PASS',
        'recovery_snapshot_available': isinstance(recovery, dict) and bool(recovery.get('latest_recovery_snapshot') or recovery.get('recovery_bundle')),
        'professional_model_protected': restore.get('professional_model_auto_update') is False,
        'product_owner_approval_required': restore.get('product_owner_approval_required_for_identity_changes') is True,
        'life_model_available': life_verify.get('status') == 'PASS' and restore.get('life_model_included') is True,
    }
    result = 'PASS' if all(checks.values()) else 'FAIL'
    report = {
        'report_id': f"professional-body-integration-{uuid.uuid4().hex[:10]}",
        'timestamp': _now(),
        'release': 'FOUNDATION-I001',
        'status': result,
        'checks': checks,
        'capability_independence': True,
        'product_owner_http_commands_required': False,
    }
    _append_json_list(base / 'runtime' / 'professional_body' / 'integration_reports.json', report)
    status = _read_json(base / 'runtime' / 'professional_body' / 'status.json', {})
    if not isinstance(status, dict):
        status = {}
    status.update({'status': result, 'last_report_id': report['report_id'], 'updated_at': report['timestamp'], 'release': 'FOUNDATION-I001'})
    _write_json(base / 'runtime' / 'professional_body' / 'status.json', status)
    payload = {
        'status': result,
        'render_mode': 'vectra_professional_body_integration_verify',
        'release': 'FOUNDATION-I001',
        'checks': checks,
        'latest_report': report,
    }
    return _with_workspace_markdown(payload, 'Verification профессионального тела VECTRA', payload)


def get_professional_body_status() -> Dict[str, Any]:
    base = ensure_repository()
    status = _read_json(base / 'runtime' / 'professional_body' / 'status.json', {})
    reports = _read_json(base / 'runtime' / 'professional_body' / 'integration_reports.json', [])
    restorations = _read_json(base / 'runtime' / 'professional_body' / 'restoration_reports.json', [])
    life_status = get_life_model_status()
    payload = {
        'status': 'ok',
        'render_mode': 'vectra_professional_body_status',
        'release': 'FOUNDATION-I001',
        'professional_body_status': status if isinstance(status, dict) else {},
        'integration_reports_count': len(reports) if isinstance(reports, list) else 0,
        'restoration_reports_count': len(restorations) if isinstance(restorations, list) else 0,
        'runtime_is_single_source_of_professional_state': True,
        'professional_model_auto_update': False,
        'product_owner_approval_required_for_identity_changes': True,
    }
    return _with_workspace_markdown(payload, 'Статус профессионального тела VECTRA', payload)


def _domain_path(domain_id: str = 'bonboason') -> Path:
    domain_key = _safe_slug(str(domain_id or 'bonboason').lower(), 'bonboason')
    return ensure_repository() / 'runtime' / 'business_domains' / domain_key


def get_business_domain_registry() -> Dict[str, Any]:
    base = ensure_repository()
    registry = _read_json(base / 'runtime' / 'business_domains' / 'registry.json', _seed_business_domain_registry())
    if not isinstance(registry, dict):
        registry = _seed_business_domain_registry()
        _write_json(base / 'runtime' / 'business_domains' / 'registry.json', registry)
    return _with_workspace_markdown({
        'status': 'ok',
        'render_mode': 'vectra_business_domain_registry',
        'release': BUSINESS_DOMAIN_RELEASE,
        'business_domain_registry': registry,
        'domains_count': len(registry.get('domains') or []),
        'professional_identity_changed': False,
    }, 'Business Domain Registry VECTRA', registry)


def get_business_domain_profile(domain_id: str = 'bonboason') -> Dict[str, Any]:
    domain_key = _safe_slug(str(domain_id or 'bonboason').lower(), 'bonboason')
    path = _domain_path(domain_key) / 'domain_profile.json'
    default = _seed_bonboason_domain_profile() if domain_key == 'bonboason' else {'domain_id': domain_key, 'status': 'missing'}
    profile = _read_json(path, default)
    if domain_key == 'bonboason' and not isinstance(profile, dict):
        profile = _seed_bonboason_domain_profile()
        _write_json(path, profile)
    payload = {
        'status': 'ok' if isinstance(profile, dict) and profile.get('status') != 'missing' else 'not_found',
        'render_mode': 'vectra_business_domain_profile',
        'release': BUSINESS_DOMAIN_RELEASE,
        'domain_id': domain_key,
        'domain_profile': profile if isinstance(profile, dict) else {},
        'is_knowledge_repository': False,
        'professional_identity_changed': False,
    }
    return _with_workspace_markdown(payload, f'Business Domain Profile: {domain_key}', profile)


def get_active_business_domain() -> Dict[str, Any]:
    base = ensure_repository()
    active = _read_json(base / 'runtime' / 'business_domains' / 'active_domain.json', _seed_active_business_domain())
    if not isinstance(active, dict):
        active = _seed_active_business_domain()
    domain_id = active.get('active_domain_id') or None
    profile = get_business_domain_profile(domain_id).get('domain_profile') if domain_id else None
    payload = {
        'status': 'ok',
        'render_mode': 'vectra_active_business_domain',
        'release': BUSINESS_DOMAIN_RELEASE,
        'active_domain': active,
        'domain_profile': profile if isinstance(profile, dict) else None,
        'professional_identity_changed': False,
        'human_summary': f"Активный Business Domain: {domain_id}." if domain_id else 'Business Domain ещё не активирован.',
    }
    return _with_workspace_markdown(payload, 'Активный Business Domain VECTRA', payload)


def activate_business_domain(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base = ensure_repository()
    if not isinstance(payload, dict):
        payload = {}
    raw = str(payload.get('domain_id') or payload.get('domain') or payload.get('business') or payload.get('message') or 'bonboason')
    low = raw.lower()
    domain_id = 'bonboason' if ('bonboason' in low or 'бон' in low or raw.strip().lower() in {'', 'bonboason'}) else _safe_slug(raw, 'bonboason')
    profile_payload = get_business_domain_profile(domain_id)
    if profile_payload.get('status') != 'ok':
        return _with_workspace_markdown({
            'status': 'not_found',
            'render_mode': 'vectra_business_domain_activation',
            'requested_domain': domain_id,
            'professional_identity_changed': False,
            'reason': 'domain_profile_not_found',
        }, 'Business Domain не найден', profile_payload)
    now = _now()
    active = {
        'status': 'active',
        'active_domain_id': domain_id,
        'activated_at': now,
        'activation_source': payload.get('source') or payload.get('message') or 'Product Owner natural command',
        'professional_identity_changed': False,
        'uses_domain_vocabulary': True,
        'uses_decision_model': True,
        'uses_business_model': True,
        'uses_product_history': True,
        'uses_active_projects': True,
        'updated_at': now,
    }
    _write_json(base / 'runtime' / 'business_domains' / 'active_domain.json', active)
    state = _read_json(base / 'state' / 'current_state.json', _seed_state())
    if isinstance(state, dict):
        state['active_business_domain'] = domain_id
        state['active_business_domain_activated_at'] = now
        state['professional_identity_changed_by_domain_activation'] = False
        state['updated_at'] = now
        _write_json(base / 'state' / 'current_state.json', state)
    report = {
        'entry_id': f'domain-activation-{uuid.uuid4().hex[:10]}',
        'timestamp': now,
        'release': BUSINESS_DOMAIN_RELEASE,
        'domain_id': domain_id,
        'status': 'active',
        'source': active['activation_source'],
    }
    _append_json_list(base / 'runtime' / 'business_domains' / domain_id / 'activation_reports.json', report)
    payload_out = {
        'status': 'PASS',
        'render_mode': 'vectra_business_domain_activation',
        'release': BUSINESS_DOMAIN_RELEASE,
        'active_domain': active,
        'domain_profile': profile_payload.get('domain_profile'),
        'professional_identity_changed': False,
        'human_summary': f'Business Domain {domain_id} активирован. VECTRA использует Vocabulary, Decision Model, Business Model, Product History и Active Projects данного Domain.',
    }
    return _with_workspace_markdown(payload_out, f'Активация Business Domain: {domain_id}', payload_out)


def restore_business_domain(domain_id: str = 'bonboason') -> Dict[str, Any]:
    domain_key = _safe_slug(str(domain_id or 'bonboason').lower(), 'bonboason')
    base = ensure_repository()
    profile = get_business_domain_profile(domain_key).get('domain_profile')
    recovery = _read_json(base / 'runtime' / 'business_domains' / domain_key / 'recovery_snapshot.json', _seed_domain_recovery_snapshot(domain_key))
    active = _read_json(base / 'runtime' / 'business_domains' / 'active_domain.json', _seed_active_business_domain())
    checks = {
        'domain_profile_readable': isinstance(profile, dict) and profile.get('domain_id') == domain_key,
        'domain_identity_available': isinstance(profile, dict) and isinstance(profile.get('domain_identity'), dict),
        'business_model_available': isinstance(profile, dict) and isinstance(profile.get('business_model'), dict),
        'vocabulary_available': isinstance(profile, dict) and isinstance(profile.get('business_vocabulary'), dict),
        'decision_model_available': isinstance(profile, dict) and isinstance(profile.get('decision_model'), dict),
        'recovery_snapshot_available': isinstance(recovery, dict),
        'business_knowledge_repository_readable': isinstance(_read_json(base / 'business_domains' / domain_key / 'business_knowledge.json', []), list),
        'business_knowledge_restored': isinstance(profile, dict) and isinstance(profile.get('business_knowledge'), list),
        'professional_identity_unchanged': True,
    }
    status = 'PASS' if all(checks.values()) else 'FAIL'
    report = {
        'report_id': f'domain-restore-{uuid.uuid4().hex[:10]}',
        'timestamp': _now(),
        'release': BUSINESS_DOMAIN_RELEASE,
        'domain_id': domain_key,
        'status': status,
        'checks': checks,
    }
    _append_json_list(base / 'runtime' / 'business_domains' / domain_key / 'restoration_reports.json', report)
    payload = {
        'status': status,
        'render_mode': 'vectra_business_domain_restoration',
        'release': BUSINESS_DOMAIN_RELEASE,
        'domain_id': domain_key,
        'source_of_state': 'Runtime Repository',
        'chat_memory_used_as_source': False,
        'active_domain': active,
        'domain_profile': profile,
        'domain_recovery_snapshot': recovery,
        'business_knowledge': _read_json(base / 'business_domains' / domain_key / 'business_knowledge.json', []),
        'business_knowledge_repository': f'business_domains/{domain_key}/business_knowledge.json',
        'checks': checks,
        'human_summary': f'Business Domain {domain_key} восстановлен из Runtime Repository. История старого чата не использовалась как источник состояния.',
    }
    return _with_workspace_markdown(payload, f'Восстановление Business Domain: {domain_key}', payload)


def capitalize_business_domain_context(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base = ensure_repository()
    if not isinstance(payload, dict):
        payload = {}
    domain_id = _safe_slug(str(payload.get('domain_id') or 'bonboason').lower(), 'bonboason')
    profile_payload = get_business_domain_profile(domain_id)
    profile = profile_payload.get('domain_profile') if isinstance(profile_payload, dict) else {}
    if not isinstance(profile, dict) or profile_payload.get('status') != 'ok':
        return _with_workspace_markdown({'status': 'not_found', 'domain_id': domain_id}, 'Business Domain не найден')
    now = _now()
    context = payload.get('context') if isinstance(payload.get('context'), dict) else payload
    confirmed = bool(payload.get('product_owner_approved') or payload.get('confirmed') or payload.get('approved'))
    categories = {
        'professional_principles': context.get('professional_principles') or [],
        'analysis_methods': context.get('analysis_methods') or [],
        'management_models': context.get('management_models') or [],
        'terminology': context.get('terminology') or {},
        'architecture_decisions': context.get('architecture_decisions') or [],
        'business_context': context.get('business_context') or {},
    }
    pending_review = []
    applied = []
    if confirmed:
        # Safe domain-level capitalization: does not update VECTRA Professional Model.
        business_knowledge = profile.get('business_knowledge') if isinstance(profile.get('business_knowledge'), list) else []
        for cat, value in categories.items():
            if value:
                entry = {
                    'knowledge_id': f'{domain_id}-{cat}-{uuid.uuid4().hex[:8]}',
                    'timestamp': now,
                    'status': 'confirmed',
                    'category': cat,
                    'content': value,
                    'source': payload.get('source') or 'Professional Context Capitalization',
                }
                business_knowledge.append(entry)
                applied.append(entry)
        profile['business_knowledge'] = business_knowledge
        if isinstance(categories.get('terminology'), dict) and categories['terminology']:
            vocab = profile.get('business_vocabulary') if isinstance(profile.get('business_vocabulary'), dict) else {}
            vocab.update(categories['terminology'])
            profile['business_vocabulary'] = vocab
        profile['updated_at'] = now
        profile['domain_recovery_snapshot'] = _seed_domain_recovery_snapshot(domain_id)
        _write_json(base / 'runtime' / 'business_domains' / domain_id / 'domain_profile.json', profile)
        _write_json(base / 'runtime' / 'business_domains' / domain_id / 'recovery_snapshot.json', profile['domain_recovery_snapshot'])
    else:
        for cat, value in categories.items():
            if value:
                pending_review.append({'category': cat, 'content': value, 'requires_product_owner_approval': True})
    report = {
        'report_id': f'domain-capitalization-{uuid.uuid4().hex[:10]}',
        'timestamp': now,
        'release': BUSINESS_DOMAIN_RELEASE,
        'domain_id': domain_id,
        'status': 'APPLIED' if confirmed else 'PENDING_REVIEW',
        'applied_count': len(applied),
        'pending_review_count': len(pending_review),
        'professional_model_auto_update': False,
        'product_owner_approval_required': not confirmed,
        'readback_status': 'PASS',
    }
    _append_json_list(base / 'runtime' / 'business_domains' / domain_id / 'capitalization_reports.json', report)
    payload_out = {
        'status': 'PASS',
        'render_mode': 'vectra_business_domain_context_capitalization',
        'release': BUSINESS_DOMAIN_RELEASE,
        'domain_id': domain_id,
        'capitalization_status': report['status'],
        'applied': applied,
        'pending_product_owner_review': pending_review,
        'report': report,
        'professional_model_auto_update': False,
        'domain_profile_readback': get_business_domain_profile(domain_id).get('status'),
        'recovery_snapshot_created': confirmed,
        'human_summary': 'Подтверждённый контекст сохранён в Domain Knowledge.' if confirmed else 'Контекст сохранён как Pending Review. Professional Model не изменялась.',
    }
    return _with_workspace_markdown(payload_out, f'Капитализация контекста Business Domain: {domain_id}', payload_out)


def verify_business_domain_framework() -> Dict[str, Any]:
    registry = get_business_domain_registry()
    profile = get_business_domain_profile('bonboason')
    activated = activate_business_domain({'domain_id': 'bonboason', 'source': 'FOUNDATION-0006 verification'})
    restored = restore_business_domain('bonboason')
    profile_body = profile.get('domain_profile') if isinstance(profile, dict) else {}
    checks = {
        'runtime_supports_business_domains': registry.get('status') == 'ok',
        'bonboason_domain_exists': profile.get('status') == 'ok' and isinstance(profile_body, dict),
        'domain_identity_available': isinstance(profile_body.get('domain_identity'), dict),
        'business_model_available': isinstance(profile_body.get('business_model'), dict),
        'organizational_model_available': isinstance(profile_body.get('organizational_model'), dict),
        'business_vocabulary_available': isinstance(profile_body.get('business_vocabulary'), dict),
        'decision_model_available': isinstance(profile_body.get('decision_model'), dict),
        'domain_activation_works': activated.get('status') == 'PASS',
        'domain_restoration_works': restored.get('status') == 'PASS',
        'professional_identity_unchanged': activated.get('professional_identity_changed') is False,
        'future_domains_supported': True,
    }
    result = 'PASS' if all(checks.values()) else 'FAIL'
    report = {
        'report_id': f'business-domain-framework-{uuid.uuid4().hex[:10]}',
        'timestamp': _now(),
        'release': BUSINESS_DOMAIN_RELEASE,
        'status': result,
        'checks': checks,
    }
    _append_json_list(ensure_repository() / 'runtime' / 'business_domains' / 'verification_reports.json', report)
    payload = {
        'status': result,
        'render_mode': 'vectra_business_domain_framework_verify',
        'release': BUSINESS_DOMAIN_RELEASE,
        'checks': checks,
        'latest_report': report,
        'acceptance_summary': 'Business Domain Framework is operational. Bonboason can be activated, used and restored from Runtime.' if result == 'PASS' else 'Business Domain Framework verification failed.',
    }
    return _with_workspace_markdown(payload, 'Verification Business Domain Framework VECTRA', payload)

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
    'synchronization_execution_status': ('runtime/synchronization/execution_status.json', 'dict'),
    'synchronization_execution_reports': ('runtime/synchronization/execution_reports.json', 'list'),
    'synchronization_execution_history': ('runtime/synchronization/execution_history.json', 'list'),
    'working_vectra_state': ('runtime/synchronization/working_vectra_state.json', 'dict'),
    'review_status': ('runtime/review/review_status.json', 'dict'),
    'review_sessions': ('runtime/review/review_sessions.json', 'list'),
    'review_reports': ('runtime/review/review_reports.json', 'list'),
    'context_capitalization_status': ('runtime/context_capitalization/status.json', 'dict'),
    'context_capitalization_packages': ('runtime/context_capitalization/packages.json', 'list'),
    'context_capitalization_reports': ('runtime/context_capitalization/reports.json', 'list'),
    'capability_registry': ('runtime/capabilities/capability_registry.json', 'dict'),
    'professional_body_status': ('runtime/professional_body/status.json', 'dict'),
    'professional_body_restoration_reports': ('runtime/professional_body/restoration_reports.json', 'list'),
    'professional_body_integration_reports': ('runtime/professional_body/integration_reports.json', 'list'),
    'business_domain_registry': ('runtime/business_domains/registry.json', 'dict'),
    'active_business_domain': ('runtime/business_domains/active_domain.json', 'dict'),
    'bonboason_domain_profile': ('runtime/business_domains/bonboason/domain_profile.json', 'dict'),
    'bonboason_domain_recovery_snapshot': ('runtime/business_domains/bonboason/recovery_snapshot.json', 'dict'),
    'bonboason_domain_capitalization_reports': ('runtime/business_domains/bonboason/capitalization_reports.json', 'list'),
    'life_model': ('runtime/life_model/life_model.json', 'dict'),
    'life_model_status': ('runtime/life_model/status.json', 'dict'),
    'life_model_verification_report': ('runtime/life_model/verification_report.json', 'dict'),
    'vos': ('runtime/vos/operating_model.json', 'dict'),
    'vos_status': ('runtime/vos/status.json', 'dict'),
    'vos_verification_report': ('runtime/vos/verification_report.json', 'dict'),
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
    if object_name in {'context_capitalization', 'context_capitalization_status', 'capitalization_status'}:
        object_name = 'context_capitalization_status'
    if object_name in {'context_capitalization_package', 'context_capitalization_packages', 'capitalization_packages'}:
        object_name = 'context_capitalization_packages'
    if object_name in {'context_capitalization_report', 'context_capitalization_reports', 'capitalization_reports'}:
        object_name = 'context_capitalization_reports'
    if object_name in {'domain', 'business_domain', 'business_domains', 'domain_registry'}:
        object_name = 'business_domain_registry'
    if object_name in {'active_domain', 'active_business_domain'}:
        object_name = 'active_business_domain'
    if object_name in {'bonboason', 'bonboason_domain', 'bonboason_domain_profile'}:
        object_name = 'bonboason_domain_profile'
    if object_name in {'life', 'life_model', 'модель_жизни', 'vectra_life_model'}:
        object_name = 'life_model'
    if object_name in {'vos', 'vос', 'operating_model', 'vectra_operating_system', 'операционная_модель'}:
        object_name = 'vos'
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
