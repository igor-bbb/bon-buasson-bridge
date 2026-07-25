"""VECTRA Assistant Runtime Repository.

This package turns the previously designed assistant architecture into an
operational VECTRA service: ChatGPT remains the intelligence/interface, while
VECTRA stores the professional state, journals, decisions, knowledge and
recovery snapshots.
"""

from app.assistant_runtime.repository import (
    repository_status,
    get_recovery_bundle,
    get_current_state,
    update_current_state,
    get_runtime_status,
    append_journal_entry,
    run_evolution_update,
    list_knowledge_documents,
    upsert_knowledge_document,
    update_knowledge_document,
    record_product_decision,
    create_recovery_snapshot,
)

__all__ = [
    'repository_status',
    'get_recovery_bundle',
    'get_current_state',
    'update_current_state',
    'get_runtime_status',
    'append_journal_entry',
    'run_evolution_update',
    'list_knowledge_documents',
    'upsert_knowledge_document',
    'update_knowledge_document',
    'record_product_decision',
    'create_recovery_snapshot',
]

# VECTRA-RUNTIME-0002: Runtime Execution & Transparent Control
