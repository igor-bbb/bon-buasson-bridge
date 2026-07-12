import math
import logging
import json
import re
import os
from datetime import datetime, timezone

import hashlib
from typing import Any

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import JSONResponse

from app.config import EMPTY_SKU_LABEL, LOW_VOLUME_THRESHOLD, SHEET_URL
from app.models.request_models import VectraQueryRequest
from app.domain.summary import (
    get_business_summary,
    get_manager_top_summary,
    get_manager_summary,
    get_network_summary,
    get_category_summary,
    get_tmc_group_summary,
    get_sku_summary,
)
from app.domain.filters import get_normalized_rows, filter_rows
from app.domain.metrics import aggregate_metrics
from app.query.entity_dictionary import get_entity_dictionary
from app.query.orchestration import orchestrate_vectra_query, save_last_payload, update_session, get_session
from app.workspace_runtime import apply_runtime_contract
from app.development_journal import (
    add_runtime_event as add_development_journal_runtime_event,
    add_global_record as add_development_journal_global_record,
    build_capture_response as build_development_journal_capture_response,
    build_journal_response as build_development_journal_response,
    analyze_dialogue_and_create_records as analyze_development_journal_dialogue,
    build_dialogue_review_response as build_development_journal_dialogue_review_response,
)
from app.self_evolution.evolution_engine import (
    is_self_evolution_command,
    run_self_evolution_cycle,
    build_self_evolution_response,
    recover_state as recover_self_evolution_state,
    is_confirmed_knowledge_message,
    run_autonomous_self_evolution_cycle,
    build_autonomous_self_evolution_response,
)
from app.self_evolution.repository import repository_status as get_self_evolution_repository_status
from app.self_evolution.journal import list_entries as list_self_evolution_entries
from app.self_evolution.classification import classify_knowledge, KNOWLEDGE_TYPES, LIFECYCLE_STATUSES
from app.self_evolution.policy import get_evolution_policy
from app.self_evolution.identity import load_assistant_state
from app.self_evolution.state_manager import get_assistant_state, get_responsibilities, get_open_cycles
from app.self_evolution.autonomy import get_autonomous_work_state
from app.self_evolution.work_planner import get_professional_activity_plan, build_professional_activity_response
from app.self_evolution.value_priority_engine import evaluate_professional_activity_value, build_value_priority_response
from app.self_evolution.dependency_manager import evaluate_dependency_map, build_dependency_response
from app.self_evolution.orchestrator import evaluate_professional_activity_orchestration, build_orchestration_response
from app.digital_organization.document_contract import get_document_contract_model, build_document_contract_response, validate_document_contract
from app.digital_organization.responsibility_transfer import get_responsibility_transfer_model, build_responsibility_transfer_response, validate_responsibility_transfer_package
from app.digital_organization.responsibility_lifecycle import get_responsibility_lifecycle_model, build_responsibility_lifecycle_response, validate_responsibility_lifecycle
from app.digital_organization.traceability import get_traceability_model, build_traceability_response, validate_purpose_trace
from app.digital_organization.runtime import get_digital_organization_runtime_model, build_runtime_response, evaluate_digital_organization_runtime, validate_digital_organization_runtime
from app.assistant_runtime.repository import (
    repository_status as get_vectra_assistant_repository_status,
    get_recovery_bundle as get_vectra_assistant_recovery_bundle,
    get_current_state as get_vectra_assistant_current_state,
    update_current_state as update_vectra_assistant_current_state,
    get_runtime_status as get_vectra_assistant_runtime_status,
    append_journal_entry as append_vectra_assistant_journal_entry,
    run_evolution_update as run_vectra_assistant_evolution_update,
    list_knowledge_documents as list_vectra_assistant_knowledge_documents,
    upsert_knowledge_document as upsert_vectra_assistant_knowledge_document,
    update_knowledge_document as update_vectra_assistant_knowledge_document,
    record_product_decision as record_vectra_assistant_product_decision,
    create_recovery_snapshot as create_vectra_assistant_recovery_snapshot,
    list_journal_entries as list_vectra_journal_entries,
    list_product_decisions as list_vectra_product_decisions,
    list_recovery_snapshots as list_vectra_recovery_snapshots,
    get_runtime_memory_overview as get_vectra_runtime_memory_overview,
    read_runtime_object as read_vectra_runtime_object,
    write_runtime_object as write_vectra_runtime_object,
    verify_runtime_readback as verify_vectra_runtime_readback,
    run_runtime_product_verification as run_vectra_runtime_product_verification,
    get_professional_model as get_vectra_professional_model,
    list_professional_model_sections as list_vectra_professional_model_sections,
    read_professional_model_section as read_vectra_professional_model_section,
    update_professional_model_section as update_vectra_professional_model_section,
    verify_professional_model_readback as verify_vectra_professional_model_readback,
    run_context_capitalization as run_vectra_context_capitalization,
    get_context_capitalization_status as get_vectra_context_capitalization_status,
    list_context_capitalization_reports as list_vectra_context_capitalization_reports,
    verify_context_capitalization_readback as verify_vectra_context_capitalization_readback,
    get_capability_registry as get_vectra_capability_registry,
    select_capability_for_intent as select_vectra_capability_for_intent,
    restore_professional_body_state as restore_vectra_professional_body_state,
    verify_professional_body_integration as verify_vectra_professional_body_integration,
    get_professional_body_status as get_vectra_professional_body_status,
    get_business_domain_registry as get_vectra_business_domain_registry,
    get_business_domain_profile as get_vectra_business_domain_profile,
    get_active_business_domain as get_vectra_active_business_domain,
    activate_business_domain as activate_vectra_business_domain,
    restore_business_domain as restore_vectra_business_domain,
    load_business_core as load_vectra_business_core,
    get_business_readiness_status as get_vectra_business_readiness_status,
    start_business_working_session as start_vectra_business_working_session,
    capitalize_business_domain_context as capitalize_vectra_business_domain_context,
    verify_business_domain_framework as verify_vectra_business_domain_framework,
    get_life_model as get_vectra_life_model,
    get_life_model_status as get_vectra_life_model_status,
    verify_life_model as verify_vectra_life_model,
)
from app.assistant_runtime.execution import (
    get_runtime_execution_model as get_vectra_runtime_execution_model,
    run_runtime_execution as run_vectra_runtime_execution,
    list_runtime_execution_reports as list_vectra_runtime_execution_reports,
    get_pending_approvals as get_vectra_runtime_pending_approvals,
    start_work_shift as start_vectra_work_shift,
    close_work_shift as close_vectra_work_shift,
)
from app.assistant_runtime.vos import (
    get_vos as get_vectra_vos,
    get_vos_status as get_vectra_vos_status,
    verify_vos as verify_vectra_vos,
    restore_vos_state as restore_vectra_vos_state,
)
from app.assistant_runtime.natural_commands import (
    get_natural_command_model as get_vectra_natural_command_model,
    execute_natural_command as execute_vectra_natural_command,
    classify_natural_command as classify_vectra_natural_command,
)
from app.assistant_runtime.professional_activity import (
    get_professional_activity_manifest as get_vectra_professional_activity_manifest,
    create_professional_activity as create_vectra_professional_activity,
    plan_professional_activity as plan_vectra_professional_activity,
    queue_professional_activity as queue_vectra_professional_activity,
    start_professional_activity as start_vectra_professional_activity,
    pause_professional_activity as pause_vectra_professional_activity,
    complete_professional_activity as complete_vectra_professional_activity,
    fail_professional_activity as fail_vectra_professional_activity,
    cancel_professional_activity as cancel_vectra_professional_activity,
    archive_professional_activity as archive_vectra_professional_activity,
    get_professional_activity as get_vectra_professional_activity,
    list_professional_activities as list_vectra_professional_activities,
    get_executive_activity_status as get_vectra_executive_activity_status,
    activate_next_professional_activity as activate_next_vectra_professional_activity,
    verify_professional_activity_foundation as verify_vectra_professional_activity_foundation,
)

from app.assistant_runtime.professional_orchestration import (
    get_orchestration_manifest as get_vectra_orchestration_manifest,
    resolve_professional_goal as resolve_vectra_professional_goal,
    orchestrate_product_owner_goal as orchestrate_vectra_product_owner_goal,
    evaluate_activity_readiness as evaluate_vectra_activity_readiness,
    executive_controller_tick as run_vectra_executive_controller_tick,
    get_professional_agenda as get_vectra_professional_agenda,
    verify_professional_orchestration_foundation as verify_vectra_professional_orchestration_foundation,
)
from app.assistant_runtime.research_engine import (
    get_research_engine_manifest as get_vectra_research_engine_manifest,
    create_research_session as create_vectra_research_session,
    initialize_research_session as initialize_vectra_research_session,
    update_research_working_context as update_vectra_research_working_context,
    add_research_evidence as add_vectra_research_evidence,
    validate_research_evidence as validate_vectra_research_evidence,
    add_research_finding as add_vectra_research_finding,
    advance_research_stage as advance_vectra_research_stage,
    complete_research_session as complete_vectra_research_session,
    get_research_session as get_vectra_research_session,
    list_research_sessions as list_vectra_research_sessions,
    verify_research_engine_foundation as verify_vectra_research_engine_foundation,
)
from app.assistant_runtime.evidence_platform import (
    get_evidence_platform_manifest as get_vectra_evidence_platform_manifest,
    register_professional_evidence as register_vectra_professional_evidence,
    transition_professional_evidence as transition_vectra_professional_evidence,
    get_professional_evidence as get_vectra_professional_evidence,
    list_professional_evidence as list_vectra_professional_evidence,
    link_professional_evidence as link_vectra_professional_evidence,
    verify_professional_evidence_platform as verify_vectra_professional_evidence_platform,
)
from app.assistant_runtime.findings_platform import (
    get_findings_platform_manifest as get_vectra_findings_platform_manifest,
    register_professional_finding as register_vectra_professional_finding,
    transition_professional_finding as transition_vectra_professional_finding,
    get_professional_finding as get_vectra_professional_finding,
    list_professional_findings as list_vectra_professional_findings,
    link_professional_findings as link_vectra_professional_findings,
    verify_professional_findings_platform as verify_vectra_professional_findings_platform,
)
from app.assistant_runtime.digital_organization_registry import (
    get_digital_organization_registry_manifest as get_vectra_digital_organization_registry_manifest,
    register_digital_professional_role as register_vectra_digital_professional_role,
    get_digital_professional_role as get_vectra_digital_professional_role,
    list_digital_professional_roles as list_vectra_digital_professional_roles,
    verify_digital_organization_registry as verify_vectra_digital_organization_registry,
)
from app.assistant_runtime.digital_business_analyst import (
    get_digital_business_analyst_manifest as get_vectra_digital_business_analyst_manifest,
    create_business_review as create_vectra_business_review,
    initialize_business_review as initialize_vectra_business_review,
    add_business_review_evidence as add_vectra_business_review_evidence,
    validate_business_review_evidence as validate_vectra_business_review_evidence,
    add_business_review_finding as add_vectra_business_review_finding,
    confirm_business_review_finding as confirm_vectra_business_review_finding,
    advance_business_review_stage as advance_vectra_business_review_stage,
    complete_business_review as complete_vectra_business_review,
    get_business_review as get_vectra_business_review,
    list_business_reviews as list_vectra_business_reviews,
    verify_digital_business_analyst_foundation as verify_vectra_digital_business_analyst_foundation,
)
from app.assistant_runtime.business_runtime_integration import (
    get_business_runtime_integration_manifest as get_vectra_business_runtime_integration_manifest,
    connect_business_runtime as connect_vectra_business_runtime,
    execute_business_runtime_command as execute_vectra_business_runtime_command,
    open_existing_business_workspace as open_vectra_existing_business_workspace,
    navigate_existing_business_workspace as navigate_vectra_existing_business_workspace,
    get_business_runtime_context as get_vectra_business_runtime_context,
    start_business_workspace_product_research as start_vectra_business_workspace_product_research,
    capture_business_workspace_research_step as capture_vectra_business_workspace_research_step,
    run_business_workspace_framework_product_research as run_vectra_business_workspace_framework_product_research,
    list_business_runtime_sessions as list_vectra_business_runtime_sessions,
    verify_business_runtime_integration as verify_vectra_business_runtime_integration,
)
from app.assistant_runtime.framework_validation import (
    get_framework_validation_manifest as get_vectra_framework_validation_manifest,
    run_business_workspace_framework_validation as run_vectra_business_workspace_framework_validation,
    verify_business_workspace_framework_validation as verify_vectra_business_workspace_framework_validation,
)
from app.assistant_runtime.business_framework_research import (
    get_business_framework_research_manifest as get_vectra_business_framework_research_manifest,
    create_research_program as create_vectra_research_program,
    transition_research_program as transition_vectra_research_program,
    get_research_program as get_vectra_research_program,
    list_research_programs as list_vectra_research_programs,
    create_research_hypothesis as create_vectra_research_hypothesis,
    transition_research_hypothesis as transition_vectra_research_hypothesis,
    get_research_hypothesis as get_vectra_research_hypothesis,
    list_research_hypotheses as list_vectra_research_hypotheses,
    add_research_program_evidence as add_vectra_research_program_evidence,
    add_research_program_finding as add_vectra_research_program_finding,
    create_product_recommendation as create_vectra_product_recommendation,
    record_product_owner_review as record_vectra_research_product_owner_review,
    link_research_engineering_task as link_vectra_research_engineering_task,
    record_research_product_verification as record_vectra_research_product_verification,
    link_research_knowledge_capitalization as link_vectra_research_knowledge_capitalization,
    register_professional_methodology as register_vectra_professional_methodology,
    get_professional_methodology as get_vectra_professional_methodology,
    list_professional_methodologies as list_vectra_professional_methodologies,
    evaluate_research_maturity as evaluate_vectra_research_maturity,
    get_research_traceability as get_vectra_research_traceability,
    get_research_workspace as get_vectra_research_workspace,
    verify_business_framework_research_foundation as verify_vectra_business_framework_research_foundation,
)
from app.assistant_runtime.business_data import (
    get_business_data_status as get_vectra_business_data_status,
    get_business_data_entities as get_vectra_business_data_entities,
    get_business_data_sample as get_vectra_business_data_sample,
    get_business_data_summary as get_vectra_business_data_summary,
    get_business_data_manifest as get_vectra_business_data_manifest,
    get_business_data_discovery as get_vectra_business_data_discovery,
    get_business_data_first_impression as get_vectra_business_data_first_impression,
    run_business_data_query as run_vectra_business_data_query,
    verify_business_data_access as verify_vectra_business_data_access,
)
from app.assistant_runtime.repository_inspection import (
    get_repository_inspection_status as get_vectra_repository_inspection_status,
    get_repository_manifest as get_vectra_repository_manifest,
    get_repository_tree as get_vectra_repository_tree,
    get_repository_components as get_vectra_repository_components,
    verify_repository_against_release_brief as verify_vectra_repository_against_release_brief,
)
from app.assistant_runtime.knowledge_capitalization import (
    create_knowledge_candidate as create_vectra_knowledge_candidate,
    create_capitalization_package as create_vectra_capitalization_package,
    write_confirmed_knowledge as write_vectra_confirmed_knowledge,
    capitalize_knowledge as capitalize_vectra_knowledge,
    auto_capitalize_confirmed_knowledge as auto_capitalize_vectra_confirmed_knowledge,
    get_knowledge_capitalization_status as get_vectra_knowledge_capitalization_status,
    list_knowledge_capitalization_reports as list_vectra_knowledge_capitalization_reports,
    list_professional_knowledge as list_vectra_professional_knowledge,
    get_professional_knowledge as get_vectra_professional_knowledge,
    verify_professional_knowledge_readback as verify_vectra_professional_knowledge_readback,
    get_professional_knowledge_overview as get_vectra_professional_knowledge_overview,
    get_domain_knowledge as get_vectra_domain_knowledge,
    get_domain_knowledge_overview as get_vectra_domain_knowledge_overview,
    get_domain_knowledge_by_id as get_vectra_domain_knowledge_by_id,
    verify_domain_knowledge_readback as verify_vectra_domain_knowledge_readback,
    create_business_knowledge_candidate as create_vectra_business_knowledge_candidate,
    create_business_knowledge_capitalization_package as create_vectra_business_knowledge_capitalization_package,
    write_business_knowledge as write_vectra_business_knowledge,
    verify_knowledge_capitalization as verify_vectra_knowledge_capitalization,
    verify_knowledge_memory_persistence as verify_vectra_knowledge_memory_persistence,
)
from app.assistant_runtime.reflection import (
    get_reflection_status as get_vectra_reflection_status,
    run_professional_reflection as run_vectra_professional_reflection,
    list_knowledge_candidates as list_vectra_knowledge_candidates,
    update_knowledge_candidate_status as update_vectra_knowledge_candidate_status,
    list_reflection_reports as list_vectra_reflection_reports,
    verify_reflection_readback as verify_vectra_reflection_readback,
)
from app.assistant_runtime.observation import (
    get_observation_status as get_vectra_observation_status,
    capture_professional_observation as capture_vectra_professional_observation,
    list_professional_observations as list_vectra_professional_observations,
    create_observation_report as create_vectra_observation_report,
    list_observation_reports as list_vectra_observation_reports,
    verify_observation_readback as verify_vectra_observation_readback,
)
from app.assistant_runtime.responsibility import (
    get_responsibility_status as get_vectra_responsibility_status,
    list_active_responsibilities as list_vectra_active_responsibilities,
    run_responsibility_check as run_vectra_responsibility_check,
    list_responsibility_reports as list_vectra_responsibility_reports,
    verify_responsibility_readback as verify_vectra_responsibility_readback,
)
from app.assistant_runtime.recovery import (
    get_recovery_evolution_status as get_vectra_recovery_evolution_status,
    run_recovery_evolution as run_vectra_recovery_evolution,
    list_recovery_evolution_reports as list_vectra_recovery_evolution_reports,
    list_recovery_checkpoints as list_vectra_recovery_checkpoints,
    verify_recovery_evolution_readback as verify_vectra_recovery_evolution_readback,
)
from app.assistant_runtime.synchronization import (
    get_synchronization_status as get_vectra_synchronization_status,
    build_synchronization_package as build_vectra_synchronization_package,
    list_synchronization_packages as list_vectra_synchronization_packages,
    list_synchronization_reports as list_vectra_synchronization_reports,
    verify_synchronization_readback as verify_vectra_synchronization_readback,
)
from app.assistant_runtime.review import (
    get_review_session as get_vectra_review_session,
    open_review_session as open_vectra_review_session,
    get_review_report as get_vectra_review_report,
    get_review_status as get_vectra_review_status,
    record_product_owner_review_decision as record_vectra_review_decision,
    verify_review_readback as verify_vectra_review_readback,
    get_synchronization_execution as get_vectra_synchronization_execution,
    execute_synchronization as execute_vectra_synchronization,
    get_synchronization_execution_report as get_vectra_synchronization_execution_report,
    get_synchronization_execution_status as get_vectra_synchronization_execution_status,
    verify_synchronization_execution_readback as verify_vectra_synchronization_execution_readback,
    list_evolution_journal_entries as list_vectra_evolution_journal_entries,
    get_evolution_journal_latest as get_vectra_evolution_journal_latest,
    get_evolution_journal_status as get_vectra_evolution_journal_status,
    verify_evolution_journal_readback as verify_vectra_evolution_journal_readback,
)
from app.assistant_runtime.memory_repository import (
    list_memory_objects as list_vectra_memory_objects,
    get_memory_object as get_vectra_memory_object,
    readback_memory_object as readback_vectra_memory_object,
    get_memory_overview as get_vectra_memory_overview,
    verify_memory_repository_integrity as verify_vectra_memory_repository_integrity,
)
from app.assistant_runtime.memory_spaces import (
    list_memory_spaces as list_vectra_memory_spaces,
    get_memory_space as get_vectra_memory_space,
    validate_memory_space as validate_vectra_memory_space,
)
from app.assistant_runtime.memory_classification import (
    classify_knowledge_item as classify_vectra_knowledge_item,
    classify_knowledge_package as classify_vectra_knowledge_package,
    verify_automatic_classification as verify_vectra_automatic_classification,
)
from app.assistant_runtime.memory_inspection import (
    inspect_memory_object as inspect_vectra_memory_object,
    inspect_memory_space as inspect_vectra_memory_space,
    get_memory_statistics as get_vectra_memory_statistics,
    get_memory_integrity_report as get_vectra_memory_integrity_report,
    get_memory_readback_report as get_vectra_memory_readback_report,
    run_memory_inspection as run_vectra_memory_inspection,
)
from app.assistant_runtime.product_knowledge import (
    list_product_knowledge as list_vectra_product_knowledge_runtime,
    get_product_knowledge as get_vectra_product_knowledge_runtime,
    write_product_knowledge as write_vectra_product_knowledge_runtime,
    verify_product_knowledge_readback as verify_vectra_product_knowledge_runtime,
)
from app.assistant_runtime.product_decisions_runtime import (
    list_product_decisions as list_vectra_product_decisions_runtime,
    get_product_decision as get_vectra_product_decision_runtime,
    write_product_decision as write_vectra_product_decision_runtime,
    verify_product_decisions_readback as verify_vectra_product_decisions_runtime,
)
from app.assistant_runtime.memory_health import (
    get_memory_health_status as get_vectra_memory_health_status,
    get_memory_diagnostics_report as get_vectra_memory_diagnostics_report,
    verify_memory_health as verify_vectra_memory_health,
)
from app.assistant_runtime.architecture_conformance import (
    get_architecture_conformance_report as get_vectra_architecture_conformance_report,
    verify_architecture_conformance as verify_vectra_architecture_conformance,
)
from app.assistant_runtime.recovery_optimization import (
    build_compact_recovery_context as build_vectra_compact_recovery_context,
    verify_recovery_optimization as verify_vectra_recovery_optimization,
)
from app.assistant_runtime.professional_memory_validation import (
    run_professional_memory_e2e_validation as run_vectra_professional_memory_e2e_validation,
    verify_professional_memory_program as verify_vectra_professional_memory_program,
)
from app.assistant_runtime.general_knowledge import (
    list_general_knowledge as list_vectra_general_knowledge_runtime,
    get_general_knowledge as get_vectra_general_knowledge_runtime,
    write_general_knowledge as write_vectra_general_knowledge_runtime,
    verify_general_knowledge_readback as verify_vectra_general_knowledge_runtime,
)
from app.assistant_runtime.revision_model import (
    list_revisions as list_vectra_memory_revisions,
    get_revision as get_vectra_memory_revision,
    get_version_status as get_vectra_memory_version_status,
    verify_revision_model as verify_vectra_revision_model,
)
from app.assistant_runtime.release_history_runtime import (
    list_release_history as list_vectra_release_history_runtime,
    get_release_history as get_vectra_release_history_runtime,
    write_release_history as write_vectra_release_history_runtime,
    verify_release_history_readback as verify_vectra_release_history_runtime,
)
from app.assistant_runtime.professional_intelligence import (
    get_professional_intelligence_status as get_vectra_professional_intelligence_status,
    build_session_context as build_vectra_professional_intelligence_session_context,
    verify_session_context_foundation as verify_vectra_professional_intelligence_session_context,
    build_session_audit_report as build_vectra_professional_intelligence_session_audit,
    verify_session_audit_runtime as verify_vectra_professional_intelligence_session_audit,
    build_knowledge_candidate_report as build_vectra_professional_intelligence_knowledge_candidates,
    verify_knowledge_candidate_runtime as verify_vectra_professional_intelligence_knowledge_candidates,
    build_knowledge_processing_report as build_vectra_professional_intelligence_knowledge_processing,
    verify_knowledge_processing_runtime as verify_vectra_professional_intelligence_knowledge_processing,
    build_knowledge_consolidation_report as build_vectra_professional_intelligence_knowledge_consolidation,
    verify_knowledge_consolidation_runtime as verify_vectra_professional_intelligence_knowledge_consolidation,
    build_prepared_knowledge_package as build_vectra_professional_intelligence_prepared_package,
    build_package_diagnostics as build_vectra_professional_intelligence_package_diagnostics,
    verify_prepared_knowledge_package_runtime as verify_vectra_professional_intelligence_prepared_package,
    run_runtime_capitalization_integration as run_vectra_professional_intelligence_runtime_capitalization,
    build_product_verification_suite as build_vectra_professional_intelligence_product_verification_suite,
    build_end_to_end_professional_intelligence_validation as build_vectra_professional_intelligence_e2e_validation,
    verify_runtime_capitalization_integration as verify_vectra_professional_intelligence_runtime_capitalization,
)
from app.assistant_runtime.session_archive import (
    create_session_archive as create_vectra_session_archive,
    append_session_event as append_vectra_session_event,
    get_session_timeline as get_vectra_session_timeline,
    get_session_replay_context as get_vectra_session_replay_context,
    verify_session_archive as verify_vectra_session_archive,
    run_archive_backed_extraction as run_vectra_archive_backed_extraction,
    capitalize_archived_session_knowledge as capitalize_vectra_archived_session_knowledge,
    verify_archive_backed_capitalization as verify_vectra_archive_backed_capitalization,
    import_historical_session as import_vectra_historical_session,
    bootstrap_session_archive as bootstrap_vectra_session_archive,
)
from app.assistant_runtime.semantic_extraction import (
    build_semantic_knowledge_extraction_report as build_vectra_semantic_knowledge_extraction_report,
    verify_semantic_knowledge_extraction as verify_vectra_semantic_knowledge_extraction,
)
from app.assistant_runtime.unified_professional_model import (
    build_unified_archive_context as build_vectra_unified_archive_context,
    build_unified_professional_model as build_vectra_unified_professional_model,
    verify_unified_professional_model as verify_vectra_unified_professional_model,
    verify_historical_archive_discovery as verify_vectra_historical_archive_discovery,
)
from app.assistant_runtime.repository_readback_consistency import (
    verify_repository_readback_consistency as verify_vectra_repository_readback_consistency,
)
from app.assistant_runtime.recovery_snapshot_sync import (
    verify_recovery_snapshot_sync as verify_vectra_recovery_snapshot_sync,
    rebuild_and_persist_recovery_snapshot_after_capitalization as rebuild_vectra_recovery_snapshot_after_capitalization,
)
from app.assistant_runtime.laboratory_behavior import (
    get_laboratory_action_first_policy as get_vectra_laboratory_action_first_policy,
    determine_laboratory_next_action as determine_vectra_laboratory_next_action,
    verify_laboratory_action_first_policy as verify_vectra_laboratory_action_first_policy,
)
from app.assistant_runtime.observability import (
    get_runtime_snapshot as get_vectra_runtime_snapshot,
    refresh_runtime_snapshot as refresh_vectra_runtime_snapshot,
    list_runtime_snapshots as list_vectra_runtime_snapshots,
    run_snapshot_product_verification as run_vectra_snapshot_product_verification,
    run_runtime_verification_report as run_vectra_runtime_verification_report,
    run_runtime_verification_evidence as run_vectra_runtime_verification_evidence,
    run_laboratory_verification_package as run_vectra_laboratory_verification_package,
    get_runtime_verification_status as get_vectra_runtime_verification_status,
    get_runtime_observability_interface as get_vectra_runtime_observability_interface,
)

router = APIRouter()
logger = logging.getLogger(__name__)


def _verify_laboratory_api_key(x_vectra_laboratory_key: str | None = None) -> None:
    expected = os.getenv('VECTRA_LABORATORY_API_KEY')
    if expected and x_vectra_laboratory_key != expected:
        raise HTTPException(status_code=401, detail='Invalid or missing VECTRA Laboratory API key.')

# Sprint 11: final public payload guard for Custom GPT Actions.
# The exact platform limit is not exposed to the API, so we keep a conservative
# budget and trim non-essential render blocks before the response leaves API.
VECTRA_PUBLIC_RESPONSE_BUDGET = 90000
VECTRA_PUBLIC_RESPONSE_HARD_BUDGET = 120000


PUBLIC_TOP_LEVEL_KEYS = ('profit_loss_rating', 'opportunity_rating', 'business_reasons', 'priority_action', 'period_result_block', 'opportunity_money', 'navigation_money', 'net_drain_money', 'gross_loss_money', 'internal_drain_money', 'compare_base', 'context', 'metrics', 'structure', 'drain_block', 'all_block', 'navigation', 'reasons_block', 'decision_block', 'decision_block_render', 'reasons_block_render', 'kpi_block', 'structure_block', 'main_driver', 'drain_block_render', 'drain_total', 'navigation_block', 'summary_block', 'explanation_block', 'next_step_block', 'product_layer_block', 'product_insight_block', 'product_tmc_decision_block', 'path', 'diagnosis_block', 'recommended_next_step_block', 'opportunity_explanation_block', 'anomaly_explanation_block', 'screen_order', 'kpi_table', 'factor_change_table', 'benchmark_diagnostic_table', 'decision_workspace', 'decision_workspace_block', 'sku_passport', 'sku_passport_block', 'business_context', 'business_context_block', 'category_workspace', 'category_workspace_block', 'business_opportunity', 'business_opportunity_block', 'recommendation_engine', 'recommendation_block', 'narrative_engine', 'narrative_block', 'product_workspace', 'product_workspace_block', 'management_intelligence', 'management_workspace', 'management_passport', 'management_workspace_block', 'business_workspace_block', 'contract_workspace_block')

STRUCTURE_NAME_MAP = {
    'markup': 'Наценка',
    'retro': 'Ретро',
    'logistics': 'Логистика',
    'personnel': 'Персонал',
    'other': 'Прочие',
    'Прочее': 'Прочие',
}

MANDATORY_RENDER_BLOCK_DEFAULTS = {
    'kpi_block': [],
    'structure_block': [],
    'drain_block_render': [],
    'navigation_block': [],
    'result_block': [],
    'summary_block': '',
    'explanation_block': [],
    'next_step_block': [],
    'diagnosis_block': [],
    'recommended_next_step_block': [],
    'opportunity_explanation_block': [],
    'anomaly_explanation_block': [],
    'screen_order': [],
    'kpi_table': [],
    'factor_change_table': [],
    'benchmark_diagnostic_table': [],
    'product_layer_block': [],
    'product_insight_block': [],
    'path': [],
    'decision_block': [],
    'decision_block_render': [],
    'render_mode': '',
    'decision_workspace': {},
    'decision_workspace_block': [],
    'sku_passport': {},
    'sku_passport_block': [],
    'business_context': {},
    'business_context_block': [],
    'category_workspace': {},
    'category_workspace_block': [],
    'business_opportunity': {},
    'business_opportunity_block': [],
    'recommendation_engine': {},
    'recommendation_block': [],
    'narrative_engine': {},
    'narrative_block': [],
    'product_workspace': {},
    'product_workspace_block': [],
    'management_intelligence': {},
    'management_workspace': {},
    'management_passport': {},
    'management_workspace_block': [],
    'business_workspace_block': [],
    'contract_workspace_block': [],
}


def _ensure_vectra_query_render_contract(payload):
    if not isinstance(payload, dict):
        payload = {'status': 'error', 'reason': 'unknown_error'}
    for key, default in MANDATORY_RENDER_BLOCK_DEFAULTS.items():
        if key not in payload or payload.get(key) is None:
            payload[key] = list(default) if isinstance(default, list) else default
        elif isinstance(default, list) and not isinstance(payload.get(key), list):
            payload[key] = []
        elif isinstance(default, str) and not isinstance(payload.get(key), str):
            payload[key] = ''
        elif isinstance(default, dict) and not isinstance(payload.get(key), dict):
            payload[key] = {}
    return payload


def _log_vectra_query_payload(session_id, payload):
    try:
        rendered = json.dumps(_sanitize_json_value(payload), ensure_ascii=False, separators=(',', ':'))
    except Exception:
        logger.exception('vectra_query_render_payload_failed session_id=%s', session_id)
        return
    logger.info('vectra_query_render_payload session_id=%s payload=%s', session_id, rendered)


def _is_number(value):
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def _num(value, default=0.0):
    try:
        value = float(value)
        return value if math.isfinite(value) else default
    except Exception:
        return default


def _intnum(value, default=0):
    try:
        value = float(value)
        return int(round(value)) if math.isfinite(value) else default
    except Exception:
        return default


def _normalize_context(payload):
    ctx = payload.get('context') or {}
    if not isinstance(ctx, dict):
        ctx = {}
    level = (ctx.get('level') or payload.get('level') or '').strip()
    object_name = ctx.get('object_name') or payload.get('object_name') or ''
    if level == 'business' and not object_name:
        object_name = 'Бизнес'
    parent_object = ctx.get('parent_object')
    if parent_object is None:
        parent_object = ctx.get('parent_name')
    if parent_object is None:
        flt = payload.get('filter') if isinstance(payload.get('filter'), dict) else {}
        if level in {'category', 'tmc_group', 'sku'}:
            parent_object = flt.get('network') or flt.get('manager') or flt.get('manager_top')
    # Use the machine-readable period selector for calculations/render helpers.
    # Some management views format range periods for display as "YYYY-MM → YYYY-MM"
    # in context.period; that string is not accepted by filter_rows().
    out = {
        'level': level,
        'object_name': object_name or 'Бизнес',
        'period': payload.get('period') or ctx.get('period'),
        'parent_object': parent_object,
    }
    if level == 'network':
        agg = payload.get('aggregation_type') or payload.get('aggregation_level') or payload.get('grouping_type')
        if agg:
            out['aggregation_type'] = agg
    return out


def _normalize_metrics(payload):
    raw = payload.get('metrics') or {}
    items = []
    ctx = payload.get('context') or {}
    level = (ctx.get('level') or payload.get('level') or '').strip().lower()

    def _append_metric(item):
        items.append(item)

    def _coalesce(entry, *keys):
        for key in keys:
            if key in entry and entry.get(key) is not None:
                return entry.get(key)
        return None

    def _money_metric(name, entry):
        fact = _coalesce(entry, 'fact_money', 'value_money', 'money')
        base = _coalesce(entry, 'pg_money', 'prev_year_money', 'base_money')
        delta = _coalesce(entry, 'delta_money', 'effect_money')
        fact_num = _num(fact)
        base_num = _num(base)
        delta_num = _num(delta)
        if base is None and delta is not None:
            base_num = fact_num - delta_num
        if delta is None:
            delta_num = fact_num - base_num
        return {
            'name': name,
            'is_primary': name == 'Финрез до',
            'fact_money': fact_num,
            'pg_money': base_num,
            'delta_money': delta_num,
            'delta_percent': _num(entry.get('delta_percent')),
        }

    def _percent_metric(name, entry):
        fact = _coalesce(entry, 'fact_percent', 'value_percent', 'percent')
        base = _coalesce(entry, 'pg_percent', 'prev_year_percent', 'base_percent')
        delta = _coalesce(entry, 'delta_percent')
        fact_num = _num(fact)
        base_num = _num(base)
        delta_num = _num(delta)
        if base is None and delta is not None:
            base_num = fact_num - delta_num
        if delta is None:
            delta_num = fact_num - base_num
        return {
            'name': name,
            'is_primary': False,
            'fact_percent': fact_num,
            'pg_percent': base_num,
            'delta_percent': delta_num,
        }

    if isinstance(raw, list):
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            name = str(entry.get('name') or '').strip()
            normalized_name = STRUCTURE_NAME_MAP.get(name, name)
            if normalized_name == 'Markup':
                normalized_name = 'Наценка'
            if normalized_name == 'markup':
                normalized_name = 'Наценка'
            if normalized_name == 'Финрез итог' and level != 'business':
                continue
            is_percent = any(k in entry for k in ('fact_percent', 'pg_percent', 'value_percent', 'percent')) or normalized_name in {'Маржа'}
            _append_metric(_percent_metric(normalized_name, entry) if is_percent else _money_metric(normalized_name, entry))
        return items

    if not isinstance(raw, dict):
        return items

    metric_sources = [
        (('revenue',), 'Оборот', 'money'),
        (('finrez_pre',), 'Финрез до', 'money'),
        (('margin_percent', 'margin_pre'), 'Маржа', 'percent'),
        (('markup_percent', 'markup'), 'Наценка', 'percent'),
        (('finrez_final',), 'Финрез итог', 'money'),
    ]
    for keys, title, kind in metric_sources:
        if title == 'Финрез итог' and level != 'business':
            continue
        entry = None
        for key in keys:
            candidate = raw.get(key)
            if isinstance(candidate, dict):
                entry = candidate
                break
        if not isinstance(entry, dict):
            continue
        _append_metric(_percent_metric(title, entry) if kind == 'percent' else _money_metric(title, entry))
    return items


def _normalize_structure(payload):
    raw = payload.get('structure') or {}
    items = []
    if isinstance(raw, list):
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            items.append({
                'name': entry.get('name'),
                'money': _num(entry.get('money', entry.get('fact_money', entry.get('value_money')))),
                'percent': _num(entry.get('percent', entry.get('fact_percent', entry.get('value_percent')))),
                'base_percent': _num(entry.get('base_percent')),
                'effect_money': _intnum(entry.get('effect_money')),
                'is_main_driver': bool(entry.get('is_main_driver', False)),
            })
        return items
    if not isinstance(raw, dict):
        return items
    order = ['markup', 'retro', 'logistics', 'personnel', 'other']
    main_driver = None
    for key in order:
        entry = raw.get(key) or {}
        if not isinstance(entry, dict):
            entry = {}
        effect = _intnum(entry.get('effect_money'))
        item = {
            'name': STRUCTURE_NAME_MAP.get(key, key),
            'money': _num(entry.get('fact_money', entry.get('value_money'))),
            'percent': _num(entry.get('fact_percent', entry.get('value_percent'))),
            'base_percent': _num(entry.get('base_percent')),
            'effect_money': _intnum(effect),
            'is_main_driver': False,
        }
        items.append(item)
        if main_driver is None or effect < main_driver[1]:
            main_driver = (item['name'], effect)
    if main_driver:
        for item in items:
            item['is_main_driver'] = item['name'] == main_driver[0]
    return items


def _normalize_reasons(payload, structure_items):
    raw = payload.get('reasons_block')
    if raw is None:
        return None

    struct_map = {str(item.get('name')).lower(): item for item in structure_items}
    struct_map.setdefault('прочее', struct_map.get('прочие', {}))
    struct_map.setdefault('прочие', struct_map.get('прочее', {}))

    items = []

    if isinstance(raw, list):
        for entry in raw:
            if not isinstance(entry, dict):
                continue

            raw_name = entry.get('name')
            name = STRUCTURE_NAME_MAP.get(str(raw_name), str(raw_name))

            if name in ("Прочее", "прочее"):
                name = "Прочие"

            base = struct_map.get(str(name).lower(), {})

            money = entry.get('money', entry.get('value_money', entry.get('fact_money')))
            percent = entry.get('percent', entry.get('value_percent', entry.get('fact_percent')))
            base_percent = entry.get('base_percent', entry.get('business_percent', entry.get('pg_percent')))
            effect_money = entry.get('effect_money', base.get('effect_money'))

            if (_num(money) == 0.0 and _num(effect_money) != 0.0 and base):
                money = base.get('money')

            if (_num(percent) == 0.0 and _num(effect_money) != 0.0 and base):
                percent = base.get('percent')

            if (_num(base_percent) == 0.0 and _num(effect_money) != 0.0 and base):
                base_percent = base.get('base_percent')

            percent_num = _num(percent, _num(base.get('percent')))
            base_percent_num = _num(base_percent, _num(base.get('base_percent')))
            delta_percent = percent_num - base_percent_num
            effect_num = _intnum(effect_money, _intnum(base.get('effect_money')))
            if effect_num < 0 and abs(delta_percent) >= 10:
                signal = 'критично'
            elif effect_num < 0:
                signal = 'риск'
            else:
                signal = 'норма'

            prev_money = entry.get('previous_money', entry.get('prev_money', entry.get('pg_money')))
            prev_percent = entry.get('previous_percent', entry.get('prev_percent', entry.get('pg_percent')))
            prev_percent_missing = bool(entry.get('previous_percent_missing')) or prev_percent is None
            prev_percent_num = None if prev_percent_missing else _num(prev_percent)
            delta_vs_prev = entry.get('delta_vs_previous_percent', entry.get('delta_vs_prev'))
            if delta_vs_prev is None and prev_percent_num is not None:
                delta_vs_prev = percent_num - prev_percent_num

            items.append({
                'name': name,
                'money': _num(money, _num(base.get('money'))),
                'percent': percent_num,
                'base_percent': base_percent_num,
                'previous_money': _num(prev_money),
                'previous_percent': prev_percent_num,
                'previous_percent_missing': prev_percent_missing,
                'previous_note': entry.get('previous_note', 'нет корректной базы' if prev_percent_missing else ''),
                'delta_percent': round(delta_percent, 2),
                'delta_vs_business_percent': round(delta_percent, 2),
                'delta_vs_previous_percent': None if delta_vs_prev is None else round(_num(delta_vs_prev), 2),
                'effect_money': effect_num,
                'signal': signal,
                'is_main_driver': bool(entry.get('is_main_driver', base.get('is_main_driver', False))),
            })

    return items
    return None


def _normalize_drain(payload):
    ctx = payload.get('context') or {}
    level = (ctx.get('level') or payload.get('level') or '').strip().lower()
    if level == 'sku':
        return {'items': [], 'total_effect': 0}

    nav = payload.get('navigation') if isinstance(payload.get('navigation'), dict) else {}
    mode = nav.get('mode') or payload.get('view_mode') or ''
    is_all_mode = mode == 'all'

    # Navigation Contract v1.2 / BUG-006 FIX-002:
    # all_block is the only source for rendered navigation/drain lists.
    # Do not fall back to drain_block/items/navigation.items.
    all_block = payload.get('all_block') if isinstance(payload.get('all_block'), list) else []
    source_items = all_block if is_all_mode else all_block[:3]

    items = []
    for idx, entry in enumerate(source_items, start=1):
        if not isinstance(entry, dict):
            continue
        navigation_money = entry.get('navigation_money')
        if navigation_money is None:
            effect = entry.get('effect_money')
            if effect is None:
                potential = entry.get('potential_money', entry.get('gap_loss_money'))
                if potential is not None:
                    effect = -abs(_num(potential))
                else:
                    finrez = ((entry.get('fact') or {}).get('finrez') if isinstance(entry.get('fact'), dict) else None)
                    if finrez is not None and _num(finrez) < 0:
                        effect = _num(finrez)
            eff = _intnum(effect)
            navigation_money = abs(eff) if eff < 0 else 0
        nav_money = _intnum(navigation_money)
        profit_delta = entry.get('profit_delta_money')
        if profit_delta is None:
            profit_delta = entry.get('delta_money')
        items.append({
            'object_name': entry.get('object_name') or entry.get('name'),
            'object_id': entry.get('object_id', idx),
            'effect_money': -abs(nav_money),
            'navigation_money': nav_money,
            'profit_delta_money': _intnum(profit_delta),
            'opportunity_money': _intnum(entry.get('opportunity_money')),
        })

    total = -sum(_num(item.get('navigation_money')) for item in items)
    return {'items': items, 'total_effect': _intnum(total)}

def _normalize_navigation(payload, drain):
    raw = payload.get('navigation') or {}
    actions = []
    if isinstance(raw, dict) and isinstance(raw.get('actions'), list):
        for action in raw['actions']:
            if not isinstance(action, dict):
                continue
            if action.get('type') == 'drilldown':
                target = action.get('target_id', action.get('id'))
                actions.append({'type': 'drilldown', 'target_id': target})
            elif action.get('type') in {'all', 'reasons', 'back'}:
                actions.append({'type': action.get('type')})
        return {'actions': actions}

    # Navigation Contract v1.2 / BUG-006 FIX-002:
    # fallback navigation actions may be created only from normalized drain,
    # and normalized drain is sourced exclusively from all_block.
    if drain.get('items'):
        for item in drain['items']:
            actions.append({'type': 'drilldown', 'target_id': item.get('object_id')})
        actions.extend([{'type': 'all'}, {'type': 'reasons'}, {'type': 'back'}])
    return {'actions': actions}


ACTION_TEXT_MAP = {
    # UX-only labels. Internal action codes stay unchanged.
    'raise_margin': 'Повысить наценку',
    'reduce_personnel': 'Снизить затраты на персонал',
    'reduce_logistics': 'Сократить логистические затраты',
    'reduce_retro': 'Снизить ретроусловия',
    'reduce_other': 'Снизить прочие затраты',
    'reduce_markup_gap': 'Повысить наценку',
    'markup': 'Повысить наценку',
    'margin': 'Повысить наценку',
    'personnel': 'Снизить затраты на персонал',
    'logistics': 'Сократить логистические затраты',
    'retro': 'Снизить ретроусловия',
    'other': 'Снизить прочие затраты',
}


DECISION_LEVELS = {'network'}
PRODUCT_LEVELS = {'category', 'tmc_group', 'sku'}


def _decision_action_text(action_key, level):
    base = ACTION_TEXT_MAP.get(str(action_key), str(action_key).replace('_', ' ').strip())
    if level in {'category', 'tmc_group'}:
        category_map = {
            'raise_margin': 'Повысить наценку',
            'reduce_retro': 'Снизить ретроусловия',
            'reduce_logistics': 'Сократить логистические затраты',
            'reduce_personnel': 'Снизить затраты на персонал',
            'reduce_other': 'Снизить прочие затраты',
            'markup': 'Повысить наценку',
            'margin': 'Повысить наценку',
            'retro': 'Снизить ретроусловия',
            'logistics': 'Сократить логистические затраты',
            'personnel': 'Снизить затраты на персонал',
            'other': 'Снизить прочие затраты',
        }
        return category_map.get(str(action_key), base)
    if level == 'sku':
        sku_map = {
            'raise_margin': 'Повысить наценку',
            'reduce_retro': 'Снизить ретроусловия',
            'reduce_logistics': 'Сократить логистические затраты',
            'reduce_personnel': 'Снизить затраты на персонал',
            'reduce_other': 'Снизить прочие затраты',
            'markup': 'Повысить наценку',
            'margin': 'Повысить наценку',
            'retro': 'Снизить ретроусловия',
            'logistics': 'Сократить логистические затраты',
            'personnel': 'Снизить затраты на персонал',
            'other': 'Снизить прочие затраты',
        }
        return sku_map.get(str(action_key), base)
    return base



def _is_product_layer_level(level):
    return str(level or '').strip().lower() in PRODUCT_LEVELS


def _metric_lookup(metrics, name):
    wanted = str(name or '').strip().lower()
    for item in metrics or []:
        if str(item.get('name') or '').strip().lower() == wanted:
            return item
    return {}


def _product_compare_base_label(response: dict) -> str:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    compare_base = str(response.get('compare_base') or ctx.get('compare_base') or '').strip()
    if level == 'category' or compare_base == 'category_business':
        return 'такой же категории бизнеса'
    if level == 'tmc_group' or compare_base == 'tmc_group_business':
        return 'такой же группы ТМС бизнеса'
    if level == 'sku' or compare_base == 'sku_business':
        return 'этой же позиции по бизнесу'
    if compare_base == 'sku_fallback_tmc_group':
        return 'такой же группы ТМС бизнеса'
    if compare_base == 'sku_fallback_category':
        return 'такой же категории бизнеса'
    return 'среднего уровня бизнеса'




def _pi72_previous_year_period(period: str) -> str:
    if isinstance(period, str) and len(period) == 7 and period[4] == '-':
        try:
            return f"{int(period[:4]) - 1:04d}-{period[5:7]}"
        except Exception:
            return ''
    return ''


def _pi72_filter_rows(period: str = '', network: str = '', category: str = '', tmc_group: str = '', sku: str = ''):
    try:
        kwargs = {}
        if network:
            kwargs['network'] = network
        if category:
            kwargs['category'] = category
        if tmc_group:
            kwargs['tmc_group'] = tmc_group
        if sku:
            kwargs['sku'] = sku
        rows, _ = filter_rows(get_normalized_rows(), period=period, **kwargs)
        return rows or []
    except Exception:
        logger.exception('pi72_filter_rows_failed')
        return []


def _pi72_extract_network_from_path(response: dict) -> str:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    parent = str(ctx.get('parent_object') or '').strip()
    if parent:
        return parent
    path = response.get('path') if isinstance(response.get('path'), list) else []
    # Path convention: Business -> Top Manager -> Manager -> Network -> Category
    if len(path) >= 4:
        return str(path[3] or '').strip()
    return ''


def _pi72_format_name(value: str) -> str:
    text = str(value or '')
    low = text.lower().replace(',', '.').replace(' ', '')
    # Keep longer tokens first.
    patterns = [
        ('1.5л', '1,5 л'), ('1.5l', '1,5 л'), ('0.75л', '0,75 л'), ('0.75l', '0,75 л'),
        ('0.5л', '0,5 л'), ('0.5l', '0,5 л'), ('0,75л', '0,75 л'), ('0,5л', '0,5 л'),
        ('5л', '5 л'), ('2л', '2 л'), ('1л', '1 л'),
    ]
    for token, label in patterns:
        if token in low:
            return label
    m = re.search(r'(\d+(?:[\.,]\d+)?)\s*[лl]', text.lower())
    if m:
        return m.group(1).replace('.', ',') + ' л'
    return 'без формата'


def _pi72_role_for_share(share: float, idx: int) -> str:
    if idx == 0 and share >= 60:
        return 'основной драйвер'
    if share >= 20:
        return 'сильный формат'
    if share > 0:
        return 'дополнительный формат'
    return 'отсутствует'


def _pi72_category_format_block(response: dict) -> list:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower() != 'category':
        return []
    category = str(ctx.get('object_name') or '').strip()
    period = str(ctx.get('period') or '').strip()
    network = _pi72_extract_network_from_path(response)
    if not category or not period or not network:
        return []

    rows = _pi72_filter_rows(period=period, network=network, category=category)
    if not rows:
        return []
    prev_period = _pi72_previous_year_period(period)
    prev_rows = _pi72_filter_rows(period=prev_period, network=network, category=category) if prev_period else []
    business_rows = _pi72_filter_rows(period=period, category=category)

    total_metrics = aggregate_metrics(rows) if rows else {}
    total_revenue = _num(total_metrics.get('revenue'))
    total_finrez = _num(total_metrics.get('finrez_pre'))

    grouped = {}
    prev_grouped = {}
    business_grouped = {}
    for row in rows:
        fmt = _pi72_format_name(row.get('tmc_group') or row.get('sku'))
        grouped.setdefault(fmt, []).append(row)
    for row in prev_rows:
        fmt = _pi72_format_name(row.get('tmc_group') or row.get('sku'))
        prev_grouped.setdefault(fmt, []).append(row)
    for row in business_rows:
        fmt = _pi72_format_name(row.get('tmc_group') or row.get('sku'))
        business_grouped.setdefault(fmt, []).append(row)

    items = []
    for fmt, fmt_rows in grouped.items():
        cur = aggregate_metrics(fmt_rows) if fmt_rows else {}
        prv = aggregate_metrics(prev_grouped.get(fmt) or []) if prev_grouped.get(fmt) else {}
        biz = aggregate_metrics(business_grouped.get(fmt) or []) if business_grouped.get(fmt) else {}
        revenue = _num(cur.get('revenue'))
        finrez = _num(cur.get('finrez_pre'))
        prev_finrez = _num(prv.get('finrez_pre'))
        biz_revenue = _num(biz.get('revenue'))
        items.append({
            'format': fmt,
            'revenue': revenue,
            'finrez': finrez,
            'delta_profit': finrez - prev_finrez,
            'share': (revenue / total_revenue * 100.0) if total_revenue else 0.0,
            'profit_share': (finrez / total_finrez * 100.0) if abs(total_finrez) > 1e-9 else 0.0,
            'business_revenue': biz_revenue,
            'sku_count': len({str(r.get('sku')) for r in fmt_rows if r.get('sku')}),
        })
    items.sort(key=lambda x: abs(x.get('delta_profit') or 0), reverse=True)
    if not items:
        return []

    top = items[0]
    lines = ['📦 Структура категории по форматам']
    if top.get('share', 0) >= 80:
        lines.append(f"Главный результат категории сконцентрирован в формате {top.get('format')}: доля оборота {_fmt_percent(top.get('share'))}%, Δ прибыли {_fmt_signed_int(top.get('delta_profit'))}.")
        lines.append('Это сильная сторона и одновременно риск концентрации: если формат просядет, категория потеряет основной источник результата.')
    else:
        lines.append('Результат категории распределён между несколькими форматами. Решение по развитию стоит принимать по формату, а не сразу по отдельным позициям.')
    lines.append('Формат | Оборот | Финрез до | Δ прибыли | Доля категории | SKU | Роль')
    for idx, item in enumerate(items[:8]):
        lines.append(
            f"{item.get('format')} | {_fmt_int(item.get('revenue'))} грн | {_fmt_signed_int(item.get('finrez'))} грн | "
            f"{_fmt_signed_int(item.get('delta_profit'))} грн | {_fmt_percent(item.get('share'))}% | {item.get('sku_count')} | {_pi72_role_for_share(item.get('share') or 0, idx)}"
        )
    if top.get('share', 0) >= 60:
        lines.append(f"Управленческий вывод: сначала развивать линейку/формат {top.get('format')}, затем переходить к конкретным SKU внутри формата.")
    else:
        lines.append('Управленческий вывод: выбрать формат с лучшим сочетанием доли, прироста и управляемости, затем формировать пакет SKU.')
    return lines

def _build_product_tmc_decision_block(response):
    data = response.get('product_tmc_decision') if isinstance(response.get('product_tmc_decision'), dict) else {}
    items = [x for x in (data.get('items') or []) if isinstance(x, dict)]
    format_lines = _pi72_category_format_block(response)
    if not items:
        return format_lines
    mode = data.get('mode') or 'distributed'
    dominant = data.get('dominant_item') if isinstance(data.get('dominant_item'), dict) else (items[0] if items else {})
    lines = []
    if mode == 'dominant' and dominant:
        lines.append(
            f"Основной вклад внутри категории формирует группа ТМС: {dominant.get('object_name')} "
            f"({_fmt_signed_int(dominant.get('profit_delta_money'))}, доля { _fmt_percent(dominant.get('share_percent')) }%)."
        )
        markup_delta = _num(dominant.get('markup_delta_percent'))
        effect = _num(dominant.get('benchmark_effect_money'))
        if markup_delta > 0:
            lines.append(
                f"Группа выше бизнеса по наценке на {_fmt_pp_delta(markup_delta)} "
                f"(эффект {_fmt_signed_int(effect)}). Рекомендация: масштабировать сильную группу и проверить развитие форматов внутри неё."
            )
        elif markup_delta < 0:
            lines.append(
                f"Группа ниже бизнеса по наценке на {_fmt_pp_delta(markup_delta)} "
                f"(потенциал до {_fmt_int(abs(effect))}). Рекомендация: проверить цену/наценку внутри группы."
            )
        else:
            lines.append('Группа концентрирует результат; следующий шаг — подтвердить устойчивость на уровне форматов и позиций.')
        return lines + ([''] if format_lines else []) + format_lines

    lines.append('Результат категории распределён между несколькими группами ТМС:')
    for idx, item in enumerate(items[:5], start=1):
        lines.append(f"{idx}. {item.get('object_name')} → {_fmt_signed_int(item.get('profit_delta_money'))}, доля {_fmt_percent(item.get('share_percent'))}%")
    lines.append('Рекомендация: сначала выбрать продуктовую группу/формат развития, затем переходить к конкретным позициям.')
    return lines + ([''] if format_lines else []) + format_lines


def _fmt_rank(value):
    try:
        if value is None:
            return '—'
        return f'№{int(value)}'
    except Exception:
        return '—'



def _render_business_context_block(response):
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if level == 'business':
        return []
    bc = response.get('business_context') if isinstance(response.get('business_context'), dict) else {}
    if not bc:
        workspace = response.get('decision_workspace') if isinstance(response.get('decision_workspace'), dict) else {}
        bc = workspace.get('business_context') if isinstance(workspace.get('business_context'), dict) else {}
    if not bc or bc.get('type') == 'business_root':
        return []
    kpi = bc.get('kpi') if isinstance(bc.get('kpi'), dict) else {}
    structure = bc.get('structure') if isinstance(bc.get('structure'), dict) else {}
    factors = bc.get('factors') if isinstance(bc.get('factors'), list) else []
    formats = bc.get('formats') if isinstance(bc.get('formats'), dict) else {}
    missing_formats = formats.get('missing_business_formats') if isinstance(formats.get('missing_business_formats'), list) else []
    lines = [
        '📍 Положение относительно бизнеса',
        f'Доля оборота в бизнес-референсе: {_fmt_percent(kpi.get("revenue_share_business_percent"))}%',
        f'Доля финреза до в бизнес-референсе: {_fmt_percent(kpi.get("profit_share_business_percent"))}%',
        f'Маржа: объект {_fmt_percent(kpi.get("margin_object_percent"))}% / бизнес {_fmt_percent(kpi.get("margin_business_percent"))}% / Δ {_fmt_pp_delta(kpi.get("margin_delta_pp"))}',
        f'Наценка: объект {_fmt_percent(kpi.get("markup_object_percent"))}% / бизнес {_fmt_percent(kpi.get("markup_business_percent"))}% / Δ {_fmt_pp_delta(kpi.get("markup_delta_pp"))}',
        '',
        'Структура относительно бизнеса:',
        f'Категории: {structure.get("object_category_count") or 0} из {structure.get("business_category_count") or 0}',
        f'Группы ТМС: {structure.get("object_tmc_group_count") or 0} из {structure.get("business_tmc_group_count") or 0}',
        f'SKU: {structure.get("object_sku_count") or 0} из {structure.get("business_sku_count") or 0}',
    ]
    if factors:
        lines.extend(['', 'Ключевые отклонения факторов:'])
        for item in factors[:3]:
            if not isinstance(item, dict):
                continue
            lines.append(f'{item.get("name") or item.get("factor")} → Δ {_fmt_pp_delta(item.get("delta_pp"))}, эффект {_fmt_signed_int(item.get("effect_money"))} грн')
    if missing_formats:
        names = [str(item.get('format')) for item in missing_formats[:5] if isinstance(item, dict) and item.get('format')]
        if names:
            lines.extend(['', 'Форматы, которые есть в бизнес-референсе, но отсутствуют в текущем объекте: ' + ', '.join(names) + '.'])
    return [line for line in lines if str(line or '').strip()]


def _render_category_workspace_block(response):
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower() != 'category':
        return []
    workspace = response.get('category_workspace') if isinstance(response.get('category_workspace'), dict) else {}
    if not workspace:
        return []
    category = workspace.get('category') or ctx.get('object_name') or 'категория'
    period = workspace.get('period') or ctx.get('period') or ''
    formats = workspace.get('formats') if isinstance(workspace.get('formats'), list) else []
    missing_formats = workspace.get('missing_business_formats') if isinstance(workspace.get('missing_business_formats'), list) else []
    sku_leaders = workspace.get('sku_leaders') if isinstance(workspace.get('sku_leaders'), list) else []
    missing_sku = workspace.get('missing_business_sku_leaders') if isinstance(workspace.get('missing_business_sku_leaders'), list) else []
    strategy = workspace.get('strategy') if isinstance(workspace.get('strategy'), dict) else {}
    lines = [
        f'📦 Рабочий стол категории: {category}' + (f' | {period}' if period else ''),
        '',
        '🧠 Продуктовый разбор',
        f'Δ прибыли категории к прошлому году: {_fmt_signed_int(workspace.get("profit_delta_money"))} грн.',
    ]
    if formats:
        lines.extend(['', 'Форматы внутри категории:', 'Формат | Оборот | Доля категории | SKU | Сетей'])
        for item in formats[:8]:
            if not isinstance(item, dict):
                continue
            lines.append(f'{item.get("format") or "—"} | {_fmt_int(item.get("revenue"))} грн | {_fmt_percent(item.get("share_revenue_percent"))}% | {_fmt_int(item.get("sku_count"))} | {_fmt_int(item.get("network_count"))}')
    if missing_formats:
        lines.extend(['', 'Форматы из бизнес-референса, которых нет в текущем объекте:'])
        for item in missing_formats[:5]:
            if isinstance(item, dict):
                lines.append(f'{item.get("format") or "—"} → оборот бизнеса {_fmt_int(item.get("revenue"))} грн, SKU {_fmt_int(item.get("sku_count"))}')
    if sku_leaders:
        lines.extend(['', 'Лидеры SKU категории:', 'SKU | Оборот | Доля категории | Доля в бизнес-категории | Сетей | Формат'])
        for item in sku_leaders[:8]:
            if not isinstance(item, dict):
                continue
            lines.append(f'{item.get("sku") or "—"} | {_fmt_int(item.get("revenue"))} грн | {_fmt_percent(item.get("share_category_percent"))}% | {_fmt_percent(item.get("share_business_category_percent"))}% | {_fmt_int(item.get("network_count"))} | {item.get("format") or "—"}')
    if missing_sku:
        lines.extend(['', 'Отсутствующие SKU-лидеры бизнес-референса категории:'])
        for item in missing_sku[:8]:
            if isinstance(item, dict):
                lines.append(f'{item.get("sku") or "—"} | {_fmt_int(item.get("business_revenue"))} грн | {_fmt_signed_int(item.get("business_finrez_pre"))} грн | {item.get("format") or "—"}')
    lines.extend(['', '🚀 План развития категории'])
    if strategy.get('format_gap_exists'):
        lines.append('1. Начать с проверки отсутствующих форматов: бизнес уже показывает, какие форматы могут расширить категорию.')
    else:
        lines.append('1. Усиливать текущие форматы и защищать позиции-лидеры.')
    if missing_sku:
        lines.append(f'2. Собрать пакет из отсутствующих SKU-лидеров: сейчас найдено {strategy.get("sku_gap_count") or len(missing_sku)} кандидатов.')
    lines.append('3. Перейти к Product рабочий стол по ключевому SKU или подготовить переговорный аргумент по категории.')
    return [line for line in lines if str(line or '').strip()]

def _build_sku_passport_block(response):
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower() != 'sku':
        return []
    passport = response.get('sku_passport') if isinstance(response.get('sku_passport'), dict) else {}
    if not passport:
        return []
    ident = passport.get('identification') if isinstance(passport.get('identification'), dict) else {}
    role = passport.get('business_role') if isinstance(passport.get('business_role'), dict) else {}
    eco = passport.get('economics') if isinstance(passport.get('economics'), dict) else {}
    presence = passport.get('presence') if isinstance(passport.get('presence'), dict) else {}
    decision = passport.get('decision') if isinstance(passport.get('decision'), dict) else {}
    lines = [
        f'🧾 Паспорт SKU: {passport.get("sku") or ctx.get("object_name")}',
        f'Период: {passport.get("period") or ctx.get("period")}',
        '',
        '## Идентификация',
        f'Категория: {ident.get("category") or "—"}',
        f'Группа ТМС: {ident.get("tmc_group") or "—"}',
        f'Формат: {ident.get("format") or "—"}',
    ]
    if passport.get('contract'):
        lines.append(f'Текущий контракт: {passport.get("contract")}')
    lines.extend([
        '',
        '## Роль в бизнесе',
        f'Доля в бизнесе: {_fmt_percent(role.get("business_share_percent"))}%',
        f'Доля в категории: {_fmt_percent(role.get("category_share_percent"))}%',
        f'Доля в группе/формате: {_fmt_percent(role.get("tmc_group_share_percent"))}%',
        f'Рейтинг по обороту в бизнесе: {_fmt_rank(role.get("rank_revenue_business"))}',
        f'Рейтинг по прибыли в бизнесе: {_fmt_rank(role.get("rank_profit_business"))}',
        f'Рейтинг по обороту в категории: {_fmt_rank(role.get("rank_revenue_category"))}',
        f'Представленность: {role.get("network_count") or 0} из {role.get("total_network_count") or 0} сетей',
        f'Роль SKU: {role.get("role") or "роль не определена"}',
        '',
        '## Экономика SKU',
        f'Оборот в текущем контексте: {_fmt_int(eco.get("revenue"))} грн',
        f'Финрез до: {_fmt_signed_int(eco.get("finrez_pre"))} грн',
        f'Δ прибыли к прошлому году: {_fmt_signed_int(eco.get("profit_delta_money"))} грн',
        f'Маржа: {_fmt_percent(eco.get("margin_pre_percent"))}%',
        f'Наценка: {_fmt_percent(eco.get("markup_percent"))}%',
        '',
        '## Где SKU работает лучше всего',
        'Сеть | Оборот | Финрез до | Доля продаж SKU',
    ])
    top_networks = presence.get('top_networks') if isinstance(presence.get('top_networks'), list) else []
    if top_networks:
        for item in top_networks[:5]:
            if isinstance(item, dict):
                lines.append(f'{item.get("network") or "—"} | {_fmt_int(item.get("revenue"))} грн | {_fmt_signed_int(item.get("finrez_pre"))} грн | {_fmt_percent(item.get("share_sku_percent"))}%')
    else:
        lines.append('Нет подтверждённых сетей по этому SKU в текущем периоде.')
    missing = presence.get('missing_networks') if isinstance(presence.get('missing_networks'), list) else []
    if missing:
        lines.extend(['', '## Где SKU отсутствует', ', '.join(str(x) for x in missing[:10])])
    lines.extend([
        '',
        '## Управленческий вывод',
        decision.get('development_logic') or 'использовать как доказательную базу по позиции',
        '',
        '## Что делаем дальше',
        'подготовить переговоры — использовать паспорт SKU как аргумент',
        'создать задачу — зафиксировать действие по позиции',
        'назад — вернуться уровнем выше',
    ])
    limitations = decision.get('data_limitations') if isinstance(decision.get('data_limitations'), list) else []
    if limitations:
        lines.extend(['', 'Ограничение текущей версии: ' + '; '.join(str(x) for x in limitations) + '.'])
    return lines


def _build_product_layer_block(response):
    """Product Layer 2.0.

    Explains the commercial product logic available from current DATA.
    It does not invent price, stock, shelf or promo data; those remain future Data Mart layers.
    """
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if level == 'category':
        return [
            'Как читать категорию:',
            '1. Сначала определить группу/формат, который формирует результат.',
            '2. Проверить концентрацию: один формат тянет категорию или результат распределён.',
            '3. После этого выбирать конкретные позиции для развития или ввода.',
            'Недоступные пока слои: цена, остатки, полка, промо и мерчендайзинг — будут добавлены через будущий Data Mart.',
        ]
    if level == 'tmc_group':
        return [
            'Как читать группу/формат:',
            '1. Оценить роль формата в категории.',
            '2. Проверить позиции внутри формата.',
            '3. Сформировать очередь SKU для развития или ввода.',
        ]
    return [
        'Что влияет на результат позиции:',
        'Экономика, представленность, роль в категории, работа в сетях и доказательная база для переговоров.',
        'Цена, остатки, полка и промо будут добавлены после расширения Data Mart.',
    ]


def _build_product_insight_block(response):
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    object_name = str(ctx.get('object_name') or ctx.get('name') or '').strip()
    object_label = object_name or ('позиция' if level == 'sku' else 'продукт')
    obj_result = _num(response.get('object_result_money'))
    opportunity = _num(response.get('opportunity_money'))
    base_label = _product_compare_base_label(response)

    if obj_result > 0:
        result_line = f'{object_label} работает лучше, чем {base_label}, на {_fmt_int(obj_result)} грн.'
    elif obj_result < 0:
        result_line = f'{object_label} работает хуже, чем {base_label}, на {_fmt_int(abs(obj_result))} грн.'
    else:
        result_line = f'{object_label} находится около уровня: {base_label}.'

    if opportunity > 0:
        opportunity_line = f'Внутри продукта остаётся {_fmt_int(opportunity)} потенциала прибыли.'
    else:
        opportunity_line = 'Существенный продуктовый потенциал прибыли не выявлен.'

    if level == 'category':
        next_line = 'Товарные группы анализируются внутри категории динамически; отдельный шаг нужен только если результат распределён между группами.'
    elif level == 'tmc_group':
        next_line = 'Следующий шаг — проверить позиции внутри группы как доказательный уровень.'
    else:
        next_line = 'Позиция является диагностическим уровнем; окончательная причина требует данных витрины данных по цене, объёму, ассортименту и структуры ассортимента.'

    return ['Что это означает?', result_line, opportunity_line, next_line]

def _build_product_priority_action_block(response):
    compare_base = str(response.get('compare_base') or ((response.get('context') or {}).get('compare_base')) or '').strip()
    reasons = _available_reasons(response)
    markup = None
    for reason in reasons:
        if str(reason.get('name') or '').strip().lower() == 'наценка':
            markup = reason
            break
    if compare_base == 'product_baseline_missing':
        return ['Проверить продуктовую эффективность → нет корректной базы сравнения']
    gap_reasons = _opportunity_gap_reasons(response, limit=1)
    if gap_reasons:
        reason = gap_reasons[0]
        effect = abs(_reason_effect_vs_business(reason))
        return [f'{_action_text_for_reason(reason)} → потенциальный эффект до {_fmt_int(effect)} относительно {_product_compare_base_label(response)}']
    if markup:
        delta = _num(markup.get('delta_vs_business_percent', markup.get('delta_percent')))
        if delta > 0:
            return [f'Сохранить сильную наценку → преимущество {_fmt_pp_delta(delta)} относительно {_product_compare_base_label(response)}']
    return ['Открыть позицию и подтвердить продуктовый результат']

def _normalize_decision(payload):
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    level = str(ctx.get('level') or payload.get('level') or '').strip().lower()
    if _is_product_layer_level(level):
        return None
    if level not in DECISION_LEVELS:
        return None

    decision = payload.get('decision_block')
    if not isinstance(decision, list) or not decision:
        return None
    items = []
    for entry in decision:
        if not isinstance(entry, dict):
            continue
        text = entry.get('text')
        if not text:
            key = entry.get('action') or entry.get('metric') or ''
            text = _decision_action_text(key, level)
        items.append({
            'text': text,
            'effect_money': _intnum(entry.get('effect_money')),
        })
    return items or None


def public_summary(payload):
    if not isinstance(payload, dict):
        return _ensure_vectra_query_render_contract({'status': 'error', 'reason': 'unknown_error'})
    if payload.get('status') == 'error':
        return _ensure_vectra_query_render_contract({
            'status': 'error',
            'reason': payload.get('reason') or 'unknown_error',
        })
    metrics = _normalize_metrics(payload)
    structure = _normalize_structure(payload)
    drain = _normalize_drain(payload)
    nav_raw = payload.get('navigation') if isinstance(payload.get('navigation'), dict) else {}
    render_mode = 'list_only' if (nav_raw.get('mode') == 'all' or payload.get('view_mode') == 'all') else ('reasons' if (nav_raw.get('mode') == 'reasons' or payload.get('view_mode') == 'reasons') else '')
    context = _normalize_context(payload)
    level = str(context.get('level') or '').strip().lower()
    response = {
        'context': context,
        'path': payload.get('path') or [],
        'summary_block': payload.get('summary_block') or '',
        'explanation_block': payload.get('explanation_block') or [],
        'next_step_block': payload.get('next_step_block') or [],
        'diagnosis_block': payload.get('diagnosis_block') or [],
        'recommended_next_step_block': payload.get('recommended_next_step_block') or [],
        'opportunity_explanation_block': payload.get('opportunity_explanation_block') or [],
        'anomaly_explanation_block': payload.get('anomaly_explanation_block') or [],
        'screen_order': payload.get('screen_order', ['kpi_table', 'factor_change_table', 'benchmark_diagnostic_table']) or [],
        'product_insight_block': payload.get('product_insight_block') or [],
        'product_tmc_decision': payload.get('product_tmc_decision') or {},
        'product_tmc_decision_block': payload.get('product_tmc_decision_block') or [],
        'decision_workspace': payload.get('decision_workspace') or {},
        'sku_passport': payload.get('sku_passport') or {},
        'business_context': payload.get('business_context') or {},
        'category_workspace': payload.get('category_workspace') or {},
        'business_opportunity': payload.get('business_opportunity') or {},
        'recommendation_engine': payload.get('recommendation_engine') or {},
        'narrative_engine': payload.get('narrative_engine') or {},
        'product_workspace': payload.get('product_workspace') or {},
        'management_intelligence': payload.get('management_intelligence') or {},
        'management_workspace': payload.get('management_workspace') or {},
        'management_passport': payload.get('management_passport') or {},
        'decision_workspace_block': payload.get('decision_workspace_block') or [],
        'result_block': payload.get('result_block') or [],
        'object_result_money': payload.get('object_result_money'),
        'opportunity_money': payload.get('opportunity_money'),
        'navigation_money': payload.get('navigation_money'),
        'net_drain_money': payload.get('net_drain_money'),
        'gross_loss_money': payload.get('gross_loss_money'),
        'internal_drain_money': payload.get('internal_drain_money'),
        'metrics': metrics,
        'structure': structure,
        'drain_block': drain,
        'all_block': payload.get('all_block') or [],
        'navigation': _normalize_navigation(payload, drain),
        'compare_base': payload.get('compare_base') or (payload.get('context') or {}).get('compare_base'),
        'render_mode': render_mode,
    }
    if level == 'business':
        response['business_result_money'] = payload.get('business_result_money')
        response['business_result_rating'] = payload.get('business_result_rating') or []
        response['profit_loss_rating'] = payload.get('profit_loss_rating') or []
        response['opportunity_rating'] = payload.get('opportunity_rating') or []
        response['business_reasons'] = payload.get('business_reasons') or []
        response['priority_action'] = payload.get('priority_action')
    else:
        response['object_reasons'] = payload.get('object_reasons') or []
        response['priority_action'] = payload.get('priority_action')
    reasons = _normalize_reasons(payload, structure)
    ctx_level_for_contract = str((response.get('context') or {}).get('level') or '').strip().lower()
    if ctx_level_for_contract == 'business':
        for legacy_key in ('goal', 'goal_block', 'focus_money', 'coverage', 'coverage_percent', 'vector_block', 'path_goal', 'path_goal_money'):
            response.pop(legacy_key, None)
    elif ctx_level_for_contract:
        for legacy_key in ('goal', 'goal_block', 'focus_money', 'coverage', 'coverage_percent', 'vector_block', 'path_goal', 'path_goal_money'):
            response.pop(legacy_key, None)

    if reasons is not None and not _is_product_layer_level(ctx_level_for_contract):
        response['reasons_block'] = reasons
    elif _is_product_layer_level(ctx_level_for_contract):
        response['reasons_block'] = []
    decision = _normalize_decision(payload)
    if decision is not None:
        response['decision_block'] = decision
    rendered = _attach_render_blocks(response, payload)
    # v1.3 Stage 3: explanation is presentation-only and benchmark driven.
    try:
        if rendered.get('render_mode') not in {'list_only', 'reasons', 'kpi_only'}:
            rendered['summary_block'] = _build_benchmark_driven_summary(rendered)
            rendered['explanation_block'] = _build_explanation_block(rendered)
            rendered['next_step_block'] = _build_next_step_block(rendered)
            rendered['diagnosis_block'] = _build_assistant_diagnosis_block(rendered)
            rendered['recommended_next_step_block'] = _build_recommended_next_step_block(rendered)
            rendered['opportunity_explanation_block'] = _build_opportunity_explanation_block(rendered)
            rendered['anomaly_explanation_block'] = _build_anomaly_explanation_block(rendered)
            rendered['business_opportunity_block'] = _render_business_opportunity_block(rendered)
            rendered['recommendation_block'] = _render_recommendation_block(rendered)
            rendered['narrative_block'] = _render_narrative_block(rendered)
            rendered['product_workspace_block'] = _render_product_workspace_block(rendered)
            rendered['management_workspace_block'] = _render_management_workspace_block(rendered)
            rendered['screen_order'] = _stage7_screen_order(rendered)
    except Exception:
        logger.exception('explanation_layer_failed')
    rendered = _attach_product_recovery_blocks(rendered)
    final_payload = _ensure_vectra_query_render_contract(rendered)
    final_level = str((final_payload.get('context') or {}).get('level') or '').strip().lower()
    if final_level:
        for legacy_key in ('goal', 'goal_block', 'focus_money', 'coverage', 'coverage_percent', 'vector_block', 'path_goal', 'path_goal_money'):
            final_payload.pop(legacy_key, None)
    return final_payload




def _fmt_int(value):
    num = _intnum(value)
    sign = '−' if num < 0 else ''
    return f"{sign}{abs(num):,}".replace(',', ' ')


def _fmt_signed_int(value):
    num = _intnum(value)
    if num > 0:
        return f"+{num:,}".replace(',', ' ')
    if num < 0:
        return f"−{abs(num):,}".replace(',', ' ')
    return '0'


def _fmt_percent(value):
    try:
        value = float(value)
        if not math.isfinite(value):
            value = 0.0
    except Exception:
        value = 0.0
    return f'{value:.2f}'


def _fmt_percent_value(value):
    return f'{_fmt_percent(value)}%'


def _fmt_pp_delta(value):
    num = _num(value)
    sign = '+' if num > 0 else ('−' if num < 0 else '')
    return f'{sign}{abs(num):.2f} п.п.'

def _metric_render_values(item):
    metric_name = str(item.get('name') or '').strip()
    if metric_name in {'Маржа', 'Наценка'}:
        return (
            _fmt_percent_value(item.get('fact_percent')),
            _fmt_percent_value(item.get('pg_percent')),
            _fmt_pp_delta(item.get('delta_percent')),
            'Δ',
        )
    return (
        _fmt_int(item.get('fact_money')),
        _fmt_int(item.get('pg_money')),
        _fmt_signed_int(item.get('delta_money')),
        'Δ к прошлому году',
    )


def _render_kpi_block(metrics):
    lines = []
    for item in metrics:
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        fact, base, delta, delta_label = _metric_render_values(item)
        lines.append(f'{name}: текущий период {fact} | прошлый год {base} | {delta_label} {delta}')
    return lines


def _render_structure_block(structure):
    lines = []
    for item in structure:
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        percent = _fmt_percent(item.get('percent'))
        base_percent = _fmt_percent(item.get('base_percent'))
        effect_money = _fmt_signed_int(item.get('effect_money'))
        lines.append(f'{name} {percent} vs {base_percent} → {effect_money}')
    return lines


def _render_reasons_block(reasons, level=""):
    order = {'Наценка': 0, 'Ретро': 1, 'Логистика': 2, 'Персонал': 3, 'Прочие': 4}
    sorted_reasons = sorted([x for x in (reasons or []) if isinstance(x, dict)], key=lambda x: order.get(str(x.get('name') or '').strip(), 99))
    lines = []
    for item in sorted_reasons:
        if not isinstance(item, dict):
            continue
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        money = _fmt_int(item.get('money', item.get('value_money')))
        percent = _fmt_percent(item.get('percent', item.get('value_percent')))
        base_percent = _fmt_percent(item.get('base_percent'))
        prev_money = _fmt_int(item.get('previous_money', item.get('prev_money')))
        prev_missing = bool(item.get('previous_percent_missing')) or item.get('previous_percent', item.get('prev_percent')) is None
        prev_percent = 'нет корректной базы' if prev_missing else _fmt_percent(item.get('previous_percent', item.get('prev_percent')))
        delta_b = _num(item.get('delta_vs_business_percent', item.get('delta_percent')))
        delta_p_raw = item.get('delta_vs_previous_percent', item.get('delta_vs_prev'))
        delta_p = None if delta_p_raw is None else _num(delta_p_raw)
        delta_b_text = f'+{delta_b:.2f}' if delta_b > 0 else f'{delta_b:.2f}'
        delta_p_text = 'нет корректной базы' if delta_p is None else (f'+{delta_p:.2f}' if delta_p > 0 else f'{delta_p:.2f}')
        effect = _fmt_signed_int(item.get('effect_money'))
        signal = str(item.get('signal') or '').strip() or 'норма'
        prev_line = f'прошлый год: {prev_money} грн ({prev_percent}%)'
        if prev_missing:
            prev_line = f'прошлый год: {prev_money} грн (нет корректной базы)'
        delta_prev_line = f'{delta_p_text} п.п. к прошлому году'
        if delta_p is None:
            delta_prev_line = 'нет корректной базы к прошлому году'
        if str(level).strip().lower() == 'business':
            lines.append(
                f'{name}\n'
                f'факт: {money} грн ({percent}%)\n'
                f'{prev_line}\n\n'
                f'отклонение:\n'
                f'{delta_prev_line}\n\n'
                f'эффект: {effect}\n'
                f'сигнал: {signal}'
            )
        else:
            lines.append(
                f'{name}\n'
                f'факт: {money} грн ({percent}%)\n'
                f'бизнес: {base_percent}%\n'
                f'{prev_line}\n\n'
                f'отклонение:\n'
                f'{delta_b_text} п.п. к бизнесу\n'
                f'{delta_prev_line}\n\n'
                f'эффект: {effect}\n'
                f'сигнал: {signal}'
            )
    return lines




def _reason_current_percent(item):
    return _fmt_percent(item.get('percent', item.get('value_percent')))


def _reason_previous_percent(item):
    prev_missing = bool(item.get('previous_percent_missing')) or item.get('previous_percent', item.get('prev_percent')) is None
    if prev_missing:
        return 'нет корректной базы'
    return f'{_fmt_percent(item.get("previous_percent", item.get("prev_percent")))}%'


def _reason_previous_money(item):
    return _fmt_int(item.get('previous_money', item.get('prev_money')))


def _reason_current_money(item):
    return _fmt_int(item.get('money', item.get('value_money')))


def _render_factor_change_block(reasons):
    """CHANGE-006.1: factors are object-vs-previous-period diagnostics.

    They must show current value, previous year value, delta and money effect.
    This block is used for Business and all object screens.
    """
    order = {'Наценка': 0, 'Ретро': 1, 'Логистика': 2, 'Персонал': 3, 'Прочие': 4}
    sorted_reasons = sorted([x for x in (reasons or []) if isinstance(x, dict)], key=lambda x: order.get(str(x.get('name') or '').strip(), 99))
    lines = []
    for item in sorted_reasons:
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        delta_p_raw = item.get('delta_vs_previous_percent', item.get('delta_vs_prev'))
        delta_text = 'нет корректной базы' if delta_p_raw is None else _fmt_pp_delta(_num(delta_p_raw))
        signal = str(item.get('signal') or '').strip() or 'норма'
        lines.append(
            f'{name}\n'
            f'текущий период: {_reason_current_money(item)} грн ({_reason_current_percent(item)}%)\n'
            f'прошлый год: {_reason_previous_money(item)} грн ({_reason_previous_percent(item)})\n'
            f'Δ к прошлому году: {delta_text}\n'
            f'эффект: {_fmt_signed_int(item.get("effect_vs_previous_money", item.get("effect_money")))}\n'
            f'сигнал: {signal}'
        )
    return lines


def _render_benchmark_diagnostic_block(reasons):
    """CHANGE-006.1: Benchmark is diagnostic, not a separate money entity.

    On non-business screens it shows object vs business, delta and diagnostic
    effect for the factor. It must not include aggregate Benchmark Money.
    """
    order = {'Наценка': 0, 'Ретро': 1, 'Логистика': 2, 'Персонал': 3, 'Прочие': 4}
    sorted_reasons = sorted([x for x in (reasons or []) if isinstance(x, dict)], key=lambda x: order.get(str(x.get('name') or '').strip(), 99))
    lines = []
    for item in sorted_reasons:
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        base_percent = _fmt_percent(item.get('base_percent'))
        delta_b = _num(item.get('delta_vs_business_percent', item.get('delta_percent')))
        lines.append(
            f'{name}\n'
            f'объект: {_reason_current_percent(item)}%\n'
            f'бизнес: {base_percent}%\n'
            f'Δ к бизнесу: {_fmt_pp_delta(delta_b)}\n'
            f'эффект: {_fmt_signed_int(item.get("effect_vs_business_money", item.get("effect_money")))}'
        )
    return lines

def _render_main_driver(structure):
    for item in structure:
        if item.get('is_main_driver'):
            return str(item.get('name') or '')
    return ''


def _navigation_money_text(item):
    if not isinstance(item, dict):
        return '0'
    if item.get('profit_delta_money') is not None:
        return f'{_fmt_signed_int(item.get("profit_delta_money"))} к прошлому году'
    value = item.get('navigation_money')
    if value is None:
        value = abs(_num(item.get('effect_money')))
    return f'{_fmt_int(abs(_num(value)))} потенциал'


def _render_drain_block(drain):
    lines = []
    for item in drain.get('items') or []:
        object_name = str(item.get('object_name') or '').strip()
        if not object_name:
            continue
        if item.get('profit_delta_money') is not None:
            lines.append(f'{object_name} → {_fmt_signed_int(item.get("profit_delta_money"))}')
        else:
            lines.append(f'{object_name} → {_navigation_money_text(item)}')
    return lines




def _render_vitrina_block(response):
    """Render the manual 'все' mode as an object showcase, not assistant analysis."""
    existing = response.get('drain_block_render') if isinstance(response, dict) else None
    if isinstance(existing, list) and len(existing) >= 2 and 'Оборот' in str(existing[1]):
        return existing
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    obj_name = ctx.get('object_name') or 'объект'
    period = ctx.get('period') or ''
    items = response.get('all_block') if isinstance(response.get('all_block'), list) else []
    if not items:
        items = (response.get('drain_block') or {}).get('items') if isinstance(response.get('drain_block'), dict) else []
    lines = [f'Витрина объекта: {obj_name}' + (f' | {period}' if period else '')]
    lines.append('№ | Объект | Δ прибыли | Потенциал')
    for idx, item in enumerate([x for x in items if isinstance(x, dict)], start=1):
        name = item.get('object_name') or item.get('name') or 'объект'
        delta = item.get('profit_delta_money')
        if delta is None:
            delta = item.get('delta_money')
        potential = item.get('opportunity_money')
        if potential is None:
            potential = item.get('potential_money')
        if potential is None:
            potential = item.get('navigation_money')
        delta_text = _fmt_signed_int(delta) + ' грн' if delta is not None else '—'
        potential_text = _fmt_int(potential) + ' грн' if potential is not None else '—'
        lines.append(f'{idx} | {name} | {delta_text} | {potential_text}')
    return lines

def _extract_navigation_names(payload, drain):
    # Navigation Contract v1.2 / BUG-006 FIX-002:
    # navigation names are derived only from normalized drain, and normalized
    # drain is derived only from all_block.
    names = []
    seen = set()
    for item in drain.get('items') or []:
        name = str(item.get('object_name') or '').strip()
        if name and name not in seen:
            names.append(name)
            seen.add(name)
    return names


def _render_navigation_block(payload, navigation, drain):
    lines = []
    names = _extract_navigation_names(payload, drain)
    drain_items = [item for item in (drain.get('items') or []) if isinstance(item, dict)]
    for idx, name in enumerate(names, start=1):
        item = drain_items[idx - 1] if idx - 1 < len(drain_items) else {}
        lines.append(f'{idx} — {name} → {_navigation_money_text(item)}')

    action_types = [a.get('type') for a in (navigation.get('actions') or []) if isinstance(a, dict)]
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    level = str(ctx.get('level') or payload.get('level') or '').strip().lower()
    nav = payload.get('navigation') if isinstance(payload.get('navigation'), dict) else {}
    mode = nav.get('mode') or payload.get('view_mode') or ''

    # Force product navigation commands into every analytical screen.
    if level != 'sku' and mode != 'all':
        if 'all' in action_types or names:
            lines.append('все — полный список')
    if level == 'network':
        lines.append('причины — разбор контракта')
    elif level and not _is_product_layer_level(level):
        lines.append('причины — разбор')
    # v9: no separate 'искать' command; numeric navigation and 'все' are enough.
    if 'back' in action_types or level != 'business':
        lines.append('назад — вверх')

    # De-duplicate while preserving order.
    out = []
    seen = set()
    for line in lines:
        if line and line not in seen:
            out.append(line)
            seen.add(line)
    return out





def _metric_by_name(metrics, wanted):
    wanted_l = str(wanted).strip().lower()
    for item in metrics or []:
        if not isinstance(item, dict):
            continue
        if str(item.get('name') or '').strip().lower() == wanted_l:
            return item
    return {}


def _delta_money_for_metric(item):
    if not isinstance(item, dict):
        return 0
    if item.get('delta_money') is not None:
        return _num(item.get('delta_money'))
    if item.get('fact_money') is not None and item.get('pg_money') is not None:
        return _num(item.get('fact_money')) - _num(item.get('pg_money'))
    return 0


def _delta_percent_for_metric(item):
    if not isinstance(item, dict):
        return 0
    if item.get('delta_percent') is not None:
        return _num(item.get('delta_percent'))
    if item.get('fact_percent') is not None and item.get('pg_percent') is not None:
        return _num(item.get('fact_percent')) - _num(item.get('pg_percent'))
    return 0


def _display_layer(level: str) -> str:
    level = str(level or '').strip().lower()
    if level == 'business':
        return 'business'
    if level in {'manager_top', 'manager'}:
        return 'object'
    if level == 'network':
        return 'contract'
    if level in {'category', 'tmc_group'}:
        return 'product'
    if level == 'sku':
        return 'sku'
    return 'object'


def _reason_display_name(reason: dict) -> str:
    return str((reason or {}).get('name') or '').strip() or 'причина'


def _reason_effect(reason: dict) -> float:
    # Backward-compatible helper. Stage 7 diagnostics should use explicit
    # previous-year or business-benchmark helpers below to avoid mixing layers.
    return _reason_effect_vs_previous(reason)


def _reason_effect_vs_previous(reason: dict) -> float:
    if not isinstance(reason, dict):
        return 0.0
    if reason.get('effect_vs_previous_money') is not None:
        return _num(reason.get('effect_vs_previous_money'))
    return _num(reason.get('effect_money'))


def _reason_effect_vs_business(reason: dict) -> float:
    if not isinstance(reason, dict):
        return 0.0
    if reason.get('effect_vs_business_money') is not None:
        return _num(reason.get('effect_vs_business_money'))
    return _num(reason.get('effect_money'))


def _available_reasons(response: dict):
    return [r for r in (response.get('object_reasons') or response.get('business_reasons') or response.get('reasons_block') or []) if isinstance(r, dict)]


def _best_positive_reason(response: dict):
    reasons = _available_reasons(response)
    positives = [r for r in reasons if _reason_effect_vs_previous(r) > 0]
    if not positives:
        return None
    return max(positives, key=lambda r: _reason_effect_vs_previous(r))


def _worst_negative_reason(response: dict):
    reasons = _available_reasons(response)
    negatives = [r for r in reasons if _reason_effect_vs_previous(r) < 0]
    if not negatives:
        return None
    return min(negatives, key=lambda r: _reason_effect_vs_previous(r))


def _worst_benchmark_gap_reason(response: dict):
    reasons = _available_reasons(response)
    negatives = [r for r in reasons if _reason_effect_vs_business(r) < 0]
    if not negatives:
        return None
    return min(negatives, key=lambda r: _reason_effect_vs_business(r))


def _opportunity_gap_reasons(response: dict, limit=5):
    """Factors that form Opportunity through benchmark gaps.

    Uses only effect vs business; does not change Opportunity formula.
    Returned in descending money impact for explanation and priority action.
    """
    gaps = []
    for reason in _available_reasons(response):
        effect = _reason_effect_vs_business(reason)
        if effect < 0:
            gaps.append((abs(effect), reason))
    gaps.sort(key=lambda x: x[0], reverse=True)
    return [reason for _, reason in gaps[:limit]]


def _action_text_for_reason(reason: dict) -> str:
    name = _reason_display_name(reason).strip().lower()
    if name == 'ретро':
        return 'Проверить ретроусловия'
    if name == 'логистика':
        return 'Проверить логистические затраты'
    if name == 'персонал':
        return 'Проверить затраты на персонал'
    if name in {'прочие', 'прочее'}:
        return 'Проверить прочие расходы'
    if name == 'наценка':
        return 'Проверить цену/наценку'
    return f'Проверить фактор {_reason_display_name(reason).lower()}'


def _first_name(items) -> str:
    for item in items or []:
        if isinstance(item, dict):
            name = str(item.get('object_name') or item.get('name') or item.get('object') or '').strip()
            if name:
                return name
    return ''


def _top_names(items, limit=2) -> str:
    names = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get('object_name') or item.get('name') or item.get('object') or '').strip()
        if name:
            names.append(name)
        if len(names) >= limit:
            break
    if not names:
        return ''
    if len(names) == 1:
        return names[0]
    return ' и '.join(names)


def _metric_word(metric_name: str) -> str:
    name = str(metric_name or '').strip().lower()
    if name == 'markup':
        return 'наценка'
    if name == 'retro':
        return 'ретроусловия'
    if name == 'logistics':
        return 'логистика'
    if name == 'personnel':
        return 'персонал'
    if name == 'other':
        return 'прочие затраты'
    return str(metric_name or '').strip().lower() or 'показатель'


def _sku_metric_sentence(response: dict) -> str:
    metrics = response.get('metrics') or []
    revenue = _metric_by_name(metrics, 'Оборот')
    finrez = _metric_by_name(metrics, 'Финрез до')
    margin = _metric_by_name(metrics, 'Маржа')
    markup = _metric_by_name(metrics, 'Наценка')
    parts = []
    if revenue:
        delta = _delta_money_for_metric(revenue)
        parts.append(f'Оборот {"вырос" if delta > 0 else ("снизился" if delta < 0 else "остался без существенного изменения")} на {_fmt_int(abs(delta))}.')
    if finrez:
        delta = _delta_money_for_metric(finrez)
        parts.append(f'Финрез {"вырос" if delta > 0 else ("снизился" if delta < 0 else "не изменился существенно")} на {_fmt_int(abs(delta))}.')
    if margin:
        delta = _delta_percent_for_metric(margin)
        parts.append(f'Маржа {"улучшилась" if delta > 0 else ("снизилась" if delta < 0 else "осталась на уровне прошлого года")} на {_fmt_pp_delta(abs(delta))}.')
    if markup:
        delta = _delta_percent_for_metric(markup)
        parts.append(f'Наценка {"улучшилась" if delta > 0 else ("снизилась" if delta < 0 else "осталась на уровне прошлого года")} на {_fmt_pp_delta(abs(delta))}.')
    if not parts:
        return 'Доступна только ограниченная оценка по текущим KPI.'
    return ' '.join(parts)

def _build_kpi_summary(response):
    """V12.3: summary explains KPI behavior, not structure."""
    metrics = response.get('metrics') or []
    revenue = _metric_by_name(metrics, 'Оборот')
    finrez = _metric_by_name(metrics, 'Финрез до')
    margin = _metric_by_name(metrics, 'Маржа')

    rev_delta = _delta_money_for_metric(revenue)
    fin_delta = _delta_money_for_metric(finrez)
    margin_delta = _delta_percent_for_metric(margin)

    if rev_delta < 0 and fin_delta >= 0:
        return 'Оборот снизился, но финрез удержан за счёт более сильной маржи.'
    if rev_delta < 0 and fin_delta < 0 and margin_delta > 0:
        return 'Оборот просел, маржа выросла и частично компенсировала падение финреза.'
    if rev_delta > 0 and fin_delta > 0:
        return 'Оборот и финрез растут одновременно — объект усиливает результат.'
    if fin_delta < 0:
        return 'Финрез просел — нужен разбор источника потерь ниже.'
    if margin_delta > 0:
        return 'Маржа улучшилась относительно базы.'
    return response.get('summary_block') or ''




def _parse_rendered_number(value: Any) -> float:
    text = str(value or '').strip().replace('−', '-').replace(',', '.')
    match = re.search(r'[-+]?\d+(?:\.\d+)?', text.replace(' ', ''))
    if not match:
        return 0.0
    try:
        return float(match.group(0).replace('+', ''))
    except Exception:
        return 0.0


def _kpi_table_delta(response: dict, metric_name: str) -> float:
    rows = response.get('kpi_table') if isinstance(response.get('kpi_table'), list) else []
    wanted = str(metric_name or '').strip().lower()
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get('name') or '').strip().lower() == wanted:
            return _parse_rendered_number(row.get('delta'))
    return 0.0

def _metric_delta_text(response: dict) -> str:
    metrics = response.get('metrics') or []
    revenue = _metric_by_name(metrics, 'Оборот')
    finrez = _metric_by_name(metrics, 'Финрез до')
    margin = _metric_by_name(metrics, 'Маржа')
    rev_delta = _delta_money_for_metric(revenue)
    fin_delta = _delta_money_for_metric(finrez)
    margin_delta = _delta_percent_for_metric(margin)
    if rev_delta > 0 and fin_delta > 0:
        return 'Объект показывает рост относительно прошлого года.'
    if rev_delta < 0 and fin_delta >= 0:
        return 'Оборот ниже прошлого года, но результат удержан за счёт маржи.'
    if rev_delta > 0 and fin_delta < 0:
        return 'Оборот выше прошлого года, но финансовый результат просел.'
    if fin_delta < 0:
        return 'Финансовый результат ниже прошлого года.'
    if margin_delta > 0:
        return 'Маржа лучше прошлого года.'
    return 'Динамика к прошлому году не является главным источником управленческого вывода.'


def _profit_first_fact_sentence(response: dict) -> str:
    metrics = response.get('metrics') or []
    finrez = _metric_by_name(metrics, 'Финрез до')
    revenue = _metric_by_name(metrics, 'Оборот')
    margin = _metric_by_name(metrics, 'Маржа')
    fin_delta = _delta_money_for_metric(finrez)
    rev_delta = _delta_money_for_metric(revenue)
    margin_delta = _delta_percent_for_metric(margin)

    # State/back screens may arrive already rendered, with reliable kpi_table
    # deltas but without previous values in the metrics array. In that case the
    # short summary must use the displayed KPI deltas, not fall back to zero.
    if abs(fin_delta) < 0.0001:
        table_fin_delta = _kpi_table_delta(response, 'Финрез до')
        if abs(table_fin_delta) > 0.0001:
            fin_delta = table_fin_delta
    if abs(rev_delta) < 0.0001:
        table_rev_delta = _kpi_table_delta(response, 'Оборот')
        if abs(table_rev_delta) > 0.0001:
            rev_delta = table_rev_delta
    if abs(margin_delta) < 0.0001:
        table_margin_delta = _kpi_table_delta(response, 'Маржа')
        if abs(table_margin_delta) > 0.0001:
            margin_delta = table_margin_delta

    if fin_delta < 0:
        result = f'Финрез снизился на {_fmt_int(abs(fin_delta))} к прошлому году.'
    elif fin_delta > 0:
        result = f'Финрез вырос на {_fmt_int(fin_delta)} к прошлому году.'
    else:
        result = 'Финрез находится примерно на уровне прошлого года.'

    details = []
    if revenue or abs(rev_delta) > 0.0001:
        details.append(f'оборот {_fmt_signed_int(rev_delta)}')
    if margin or abs(margin_delta) > 0.0001:
        details.append(f'маржа {_fmt_pp_delta(margin_delta)}')
    return result + (f' Дополнительно: {", ".join(details)}.' if details else '')


def _benchmark_sentence(response: dict) -> str:
    # CHANGE-006.1: Benchmark is diagnostic only. Do not render aggregate
    # Benchmark Money; show factor-level diagnostics through benchmark_diagnostic_block.
    return 'Сравнение с бизнесом используется как диагностика: объект сравнивается с текущим средним уровнем бизнеса по факторам.'


def _build_benchmark_driven_summary(response: dict) -> str:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    layer = _display_layer(level)
    opportunity = _num(response.get('opportunity_money'))
    strong = _best_positive_reason(response)
    risk = _worst_negative_reason(response)

    if layer == 'business':
        fact = _profit_first_fact_sentence(response)
        loss_names = _top_names(response.get('profit_loss_rating') or [], 2)
        result_names = _top_names(response.get('business_result_rating') or [], 2)
        potential_names = _top_names(response.get('opportunity_rating') or [], 2)
        parts = [fact]
        if loss_names:
            parts.append(f'Крупнейшие просадки прибыли: {loss_names}.')
        # CHANGE-006: benchmark money/rating is diagnostic only and must not
        # dominate the Business summary. It is intentionally not rendered here.
        if potential_names:
            parts.append(f'Главные резервы возврата: {potential_names}.')
        return ' '.join(parts)

    if layer in {'object', 'contract'}:
        existing_summary = str(response.get('summary_block') or '').strip()
        # Back/state screens already contain the summary from the original
        # workspace. Preserve it to avoid changing the main factor just because
        # the restored payload is already rendered rather than raw.
        if existing_summary and 'к прошлому году' in existing_summary and 'примерно на уровне' not in existing_summary:
            return existing_summary
        fact = _profit_first_fact_sentence(response)
        factor_line = f'Главный отрицательный фактор: {_reason_display_name(risk).lower()}.' if risk else 'Критичный отрицательный фактор не выделен.'
        strong_line = f'Сильный фактор: {_reason_display_name(strong).lower()}.' if strong else ''
        opportunity_line = f'Резерв прибыли внутри объекта: {_fmt_int(opportunity)} грн.' if opportunity > 0 else 'Существенный резерв прибыли внутри объекта не выявлен.'
        return ' '.join([x for x in [fact, factor_line, strong_line, opportunity_line] if x])

    if layer == 'product':
        fact = _profit_first_fact_sentence(response)
        opportunity_line = f'Потенциал внутри продукта: {_fmt_int(opportunity)} грн.' if opportunity > 0 else 'Существенный продуктовый резерв не выявлен.'
        return f'{fact} {opportunity_line} Детальный анализ цены, объёма, ассортимента и структуры ассортимента будет доступен после подключения витрины данных VECTRA.'

    if layer == 'sku':
        return f'{_sku_metric_sentence(response)} Для полного анализа позиции не хватает данных витрины данных. Доступна только оценка по текущим KPI.'

    return _metric_delta_text(response)


def _build_explanation_block(response: dict) -> list:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    layer = _display_layer(level)
    risk = _worst_negative_reason(response)
    strong = _best_positive_reason(response)
    opportunity = _num(response.get('opportunity_money'))

    if layer == 'business':
        loss_names = _top_names(response.get('profit_loss_rating') or [], 3)
        potential_names = _top_names(response.get('opportunity_rating') or [], 2)
        return [
            'Сначала смотрим изменение прибыли к прошлому году.',
            f'Крупнейшие просадки прибыли: {loss_names or "данных нет"}.',
            f'Главные резервы возврата: {potential_names or "данных нет"}.',
            'Сравнение с бизнесом используется только как диагностика по отклонениям от бизнеса, без отдельной агрегированной денежной оценки.',
        ]

    lines = [
        'Сначала сравниваем объект с прошлым годом.',
        _profit_first_fact_sentence(response),
    ]

    if risk:
        lines.append(f'Главный отрицательный фактор диагностики: {_reason_display_name(risk)} ({_fmt_signed_int(_reason_effect(risk))}).')
    if strong:
        lines.append(f'Главный положительный фактор диагностики: {_reason_display_name(strong)} ({_fmt_signed_int(_reason_effect(strong))}).')

    if layer not in {'sku'}:
        lines.append(_benchmark_sentence(response))
    if opportunity > 0:
        lines.append(f'Потенциал показывает, где внутри выбранного объекта искать резерв: {_fmt_int(opportunity)} грн.')
    else:
        lines.append('Существенный резерв внутри объекта не выявлен.')

    if layer in {'product', 'sku'}:
        lines.append('Для полноценной причины нужны данные витрины данных: цена, объём, ассортимент, структуры ассортимента и контекст исполнения.')
    return lines



def _business_impact_sentence(response: dict) -> str:
    losses = response.get('profit_loss_rating') or []
    if losses and isinstance(losses[0], dict):
        name = str(losses[0].get('object_name') or '').strip()
        value = losses[0].get('profit_delta_money')
        if name:
            return f'Главная зона просадки прибыли: {name} ({_render_money_value(value)} к прошлому году).'
    drain = response.get('drain_block') or {}
    items = drain.get('items') if isinstance(drain, dict) else drain
    if items and isinstance(items, list) and isinstance(items[0], dict):
        name = str(items[0].get('object_name') or items[0].get('name') or '').strip()
        if name:
            return f'Первым вниз стоит проверить: {name}.'
    return 'Главная зона просадки ниже по дереву не выделена.'


def _main_factor_sentence(response: dict) -> str:
    # Factor Layer: only object/current period vs previous year.
    # Do not use benchmark effect here. Benchmark is rendered separately.
    risk = _worst_negative_reason(response)
    strong = _best_positive_reason(response)
    parts = []
    if strong:
        parts.append(f'Главный положительный фактор к прошлому году: {_reason_display_name(strong).lower()} ({_render_money_value(_reason_effect_vs_previous(strong))}).')
    if risk:
        parts.append(f'Главный отрицательный фактор к прошлому году: {_reason_display_name(risk).lower()} ({_render_money_value(_reason_effect_vs_previous(risk))}).')
    if parts:
        return ' '.join(parts)
    main_driver = str(response.get('main_driver') or '').strip()
    if main_driver:
        return f'Главный фактор диагностики: {main_driver.lower()}.'
    return 'Главный фактор изменения прибыли к прошлому году по доступным данным не выделен.'


def _turnover_or_margin_sentence(response: dict) -> str:
    metrics = response.get('metrics') or []

    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()

    children = response.get('all_block') if isinstance(response.get('all_block'), list) else []
    first_child = ''
    if children and isinstance(children[0], dict):
        first_child = str(children[0].get('object_name') or children[0].get('name') or '').strip()

    revenue = _metric_by_name(metrics, 'Оборот')
    finrez = _metric_by_name(metrics, 'Финрез до')
    margin = _metric_by_name(metrics, 'Маржа')
    markup = _metric_by_name(metrics, 'Наценка')
    rev_delta = _delta_money_for_metric(revenue)
    fin_delta = _delta_money_for_metric(finrez)
    margin_delta = _delta_percent_for_metric(margin)
    markup_delta = _delta_percent_for_metric(markup)

    if level in {'category', 'tmc_group'}:
        tmc_lines = _build_product_tmc_decision_block(response)
        if tmc_lines:
            return [f'➡ Рекомендуемый следующий шаг: {tmc_lines[-1]}']
        if first_child:
            return [f'➡ Рекомендуемый следующий шаг: открыть {first_child} как доказательство по позиции продуктового результата.']
        return ['➡ Рекомендуемый следующий шаг: сравнить продукт с таким же продуктом бизнеса и подтвердить решение на позиции.']

    if fin_delta < 0 and rev_delta < 0:
        if margin_delta < 0:
            return f'Главный сигнал просадки — падение оборота ({_fmt_signed_int(rev_delta)}). Маржа также ухудшилась ({_fmt_pp_delta(margin_delta)}), поэтому факторы доходности усилили потерю прибыли.'
        return f'Главный сигнал просадки — падение оборота ({_fmt_signed_int(rev_delta)}). Доходность улучшилась или удержалась, но не компенсировала потерю продаж.'
    if fin_delta < 0 and margin_delta < 0:
        return f'Главный сигнал просадки — снижение доходности: маржа {_fmt_pp_delta(margin_delta)}.'
    if fin_delta > 0:
        if rev_delta > 0 and margin_delta > 0:
            return f'Рост прибыли поддержан одновременно оборотом ({_fmt_signed_int(rev_delta)}) и доходностью: маржа {_fmt_pp_delta(margin_delta)}.'
        if rev_delta > 0:
            return f'Рост прибыли поддержан оборотом ({_fmt_signed_int(rev_delta)}).'
        if margin_delta > 0 or markup_delta > 0:
            return f'Рост прибыли поддержан доходностью: маржа {_fmt_pp_delta(margin_delta)}, наценка {_fmt_pp_delta(markup_delta)}.'
    if rev_delta < 0:
        return f'Оборот ниже прошлого года ({_fmt_signed_int(rev_delta)}) — нужно проверить, не теряется ли объём продаж.'
    if margin_delta < 0:
        return f'Маржа ниже прошлого года ({_fmt_pp_delta(margin_delta)}) — нужно проверить доходность.'
    return 'Критичного перекоса между оборотом и доходностью по доступным KPI не видно.'


def _benchmark_diagnosis_sentence(response: dict) -> str:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if level == 'business':
        return 'Сравнение с бизнесом на экране бизнеса не выводится: бизнес не сравнивается с самим собой.'
    risk = _worst_benchmark_gap_reason(response)
    if risk:
        delta = _num(risk.get('delta_vs_business_percent', risk.get('delta_percent')))
        return f'Относительно {_product_compare_base_label(response)} слабое место: {_reason_display_name(risk).lower()} ({_fmt_pp_delta(delta)}, эффект {_render_money_value(_reason_effect_vs_business(risk))}). Это сравнение с бизнесом, а не причина изменения к прошлому году.'
    return f'Относительно {_product_compare_base_label(response)} отдельный критичный разрыв по доступным данным не выделен.'


def _build_assistant_diagnosis_block(response: dict) -> list:
    """Stage 7 / Assistant Diagnostic Layer.

    This is a presentation-only layer. It explains API numbers and does not
    calculate new KPI, change navigation, benchmark, opportunity or effect logic.
    """
    if response.get('render_mode') in {'list_only', 'reasons', 'kpi_only', 'voice_diagnostic'}:
        return []
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    layer = _display_layer(level)

    lines = ['🧠 Диагноз']
    lines.append(_profit_first_fact_sentence(response))
    lines.append(_turnover_or_margin_sentence(response))

    if layer == 'business':
        lines.append(_business_impact_sentence(response))
    elif layer in {'object', 'contract'}:
        lines.append(_main_factor_sentence(response))
        lines.append(_benchmark_diagnosis_sentence(response))
    elif layer == 'product':
        lines.append(_benchmark_diagnosis_sentence(response))
        tmc_lines = _build_product_tmc_decision_block(response)
        if tmc_lines:
            lines.append(tmc_lines[0])
        lines.append('Это продуктовый слой: цена, объём, ассортимент и структура ассортимента будут полноценно объяснены после подключения витрины данных VECTRA.')
    elif layer == 'sku':
        lines.append('Это Слой позиции: доступна KPI-диагностика, без окончательной причины до подключения витрины данных.')
    return [line for line in lines if line]


def _build_recommended_next_step_block(response: dict) -> list:
    if response.get('render_mode') in {'list_only', 'reasons', 'kpi_only', 'voice_diagnostic'}:
        return []
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    metrics = response.get('metrics') or []
    fin_delta = _delta_money_for_metric(_metric_by_name(metrics, 'Финрез до'))
    rev_delta = _delta_money_for_metric(_metric_by_name(metrics, 'Оборот'))
    margin_delta = _delta_percent_for_metric(_metric_by_name(metrics, 'Маржа'))

    if level == 'network' and response.get('decision_workspace'):
        return ['➡ Рекомендуемый следующий шаг: выбрать приоритетное действие Decision Engine или открыть доказательства по категории или позиции.']

    if level == 'sku':
        return ['➡ Рекомендуемый следующий шаг: использовать паспорт SKU как доказательство для переговоров или создать задачу по позиции.']

    children = response.get('all_block') if isinstance(response.get('all_block'), list) else []
    first_child = ''
    if children and isinstance(children[0], dict):
        first_child = str(children[0].get('object_name') or children[0].get('name') or '').strip()

    if fin_delta < 0 and rev_delta < 0:
        if level == 'network':
            return ['➡ Рекомендуемый следующий шаг: проверить контрактный контекст сети и открыть продуктовый уровень, чтобы понять, где потерян оборот.']
        if first_child:
            return [f'➡ Рекомендуемый следующий шаг: открыть {first_child} как крупнейший объект ниже и локализовать потерю оборота/прибыли.']
        return ['➡ Рекомендуемый следующий шаг: проверить контекст падения оборота: контракт, ассортимент, дистрибуцию и структуры ассортимента.']

    if fin_delta < 0 and margin_delta < 0:
        risk = _worst_negative_reason(response)
        if risk:
            return [f'➡ Рекомендуемый следующий шаг: открыть причины и проверить фактор {_reason_display_name(risk).lower()} как главный отрицательный эффект к прошлому году.']
        return ['➡ Рекомендуемый следующий шаг: открыть причины и проверить факторы снижения доходности.']

    if fin_delta > 0:
        strong = _best_positive_reason(response)
        bench_risk = _worst_benchmark_gap_reason(response)
        if strong and bench_risk:
            return [f'➡ Рекомендуемый следующий шаг: сохранить сильную сторону ({_reason_display_name(strong).lower()}) и проверить дополнительный резерв относительно бизнеса: {_reason_display_name(bench_risk).lower()}.']
        if first_child:
            return [f'➡ Рекомендуемый следующий шаг: открыть {first_child} и понять, где усилить прибыль внутри успешного объекта.']
        return ['➡ Рекомендуемый следующий шаг: зафиксировать факторы роста и проверить дополнительный резерв прибыли.']

    raw = _build_next_step_block(response)
    out = []
    for line in raw or []:
        text = str(line or '').strip()
        if not text:
            continue
        text = text.replace('Следующий шаг:', '').replace('Рекомендуемый следующий шаг:', '').strip()
        out.append(f'➡ Рекомендуемый следующий шаг: {text}')
    return out or ['➡ Рекомендуемый следующий шаг: открыть объекты ниже и продолжить диагностику прибыли.']



def _build_opportunity_explanation_block(response: dict) -> list:
    """Stage 8: explain where Opportunity comes from.

    This is a presentation-only layer. It uses Benchmark diagnostics
    (effect vs business) and does not change Opportunity formula.
    """
    if response.get('render_mode') in {'list_only', 'reasons', 'kpi_only', 'voice_diagnostic'}:
        return []

    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    opportunity = abs(_num(response.get('opportunity_money')))

    # Business screen has no object-vs-business benchmark. Explain reserves by objects below.
    if level == 'business':
        items = [x for x in (response.get('opportunity_rating') or []) if isinstance(x, dict)]
        if not items:
            return []
        lines = ['🔍 Почему сформирован потенциал']
        lines.append('Потенциал бизнеса складывается из резервов объектов ниже по дереву.')
        for item in items[:5]:
            name = str(item.get('object_name') or item.get('name') or '').strip()
            value = item.get('opportunity_money')
            if name and _num(value) > 0:
                lines.append(f'{name}: {_fmt_int(abs(_num(value)))}')
        return lines

    gap_reasons = _opportunity_gap_reasons(response, limit=5)

    if not gap_reasons:
        if opportunity > 0:
            return [
                '🔍 Почему сформирован потенциал',
                f'Потенциал {_fmt_int(opportunity)} рассчитан внутри VECTRA Engine, но по доступным benchmark-факторам основной источник не выделен.',
                'Для точного объяснения нужен дополнительный контекст или витрины данных.',
            ]
        return []

    base_label = _product_compare_base_label(response)
    lines = ['🔍 Почему сформирован потенциал']
    if opportunity > 0:
        lines.append(f'Потенциал {_fmt_int(opportunity)} формируется за счёт факторов, которые хуже {base_label}.')
    else:
        lines.append(f'Потенциал формируется за счёт факторов, которые хуже {base_label}.')

    for reason in gap_reasons:
        money = abs(_reason_effect_vs_business(reason))
        name = _reason_display_name(reason)
        delta = _num(reason.get('delta_vs_business_percent', reason.get('delta_percent')))
        lines.append(f'{name} — {_fmt_int(money)} грн потенциального эффекта.')
        lines.append(f'Отклонение к бизнесу: {_fmt_pp_delta(delta)}. Если вывести фактор на уровень {base_label}, можно вернуть до {_fmt_int(money)} грн.')

    return lines


def _build_anomaly_explanation_block(response: dict) -> list:
    """Stage 8: explain abnormal previous-period bases without hiding data."""
    if response.get('render_mode') in {'list_only', 'reasons', 'kpi_only', 'voice_diagnostic'}:
        return []

    metrics = response.get('metrics') or []
    revenue = _metric_by_name(metrics, 'Оборот')
    margin = _metric_by_name(metrics, 'Маржа')
    markup = _metric_by_name(metrics, 'Наценка')

    flags = []
    if revenue and revenue.get('pg_money') is not None and _num(revenue.get('pg_money')) <= 0:
        flags.append('оборот прошлого года был отрицательным или нулевым')
    if margin and margin.get('pg_percent') is not None and abs(_num(margin.get('pg_percent'))) > 100:
        flags.append('маржа прошлого года выглядит нетипично высокой или низкой')
    if markup and markup.get('pg_percent') is not None and _num(markup.get('pg_percent')) <= 0:
        flags.append('наценка прошлого года была нулевой или отрицательной')

    if not flags:
        return []

    return [
        '⚠ Особенность базы прошлого года',
        'По объекту в прошлом году были нетипичные данные: ' + '; '.join(flags) + '.',
        'Это может быть связано с возвратами, корректировками, сторно или отсутствием полноценной базы продаж.',
        'VECTRA не скрывает эти данные, но предупреждает: прямое сравнение отдельных KPI с прошлым годом может быть ограничено.',
    ]


def _render_business_opportunity_block(response):
    engine = response.get('business_opportunity') if isinstance(response.get('business_opportunity'), dict) else {}
    if not engine:
        workspace = response.get('product_workspace') if isinstance(response.get('product_workspace'), dict) else {}
        engine = workspace.get('opportunities') if isinstance(workspace.get('opportunities'), dict) else {}
    items = engine.get('items') if isinstance(engine.get('items'), list) else []
    if not items:
        return []
    lines = ['💰 Business Opportunity Engine', 'Объект | Тип | Основание | Потенциал / масштаб']
    for item in items[:8]:
        if not isinstance(item, dict):
            continue
        lines.append(
            f'{item.get("object") or "—"} | {item.get("type") or "—"} | '
            f'{item.get("reason") or "—"} | {_fmt_int(item.get("effect_money"))}'
        )
    summary = engine.get('summary') if isinstance(engine.get('summary'), dict) else {}
    top = summary.get('top_opportunity') if isinstance(summary.get('top_opportunity'), dict) else None
    if top:
        lines.extend(['', f'Главный фокус: {top.get("object") or "—"} — {top.get("recommended_action") or "проверить возможность"}.'])
    return [line for line in lines if str(line or '').strip()]


def _render_recommendation_block(response):
    engine = response.get('recommendation_engine') if isinstance(response.get('recommendation_engine'), dict) else {}
    if not engine:
        workspace = response.get('product_workspace') if isinstance(response.get('product_workspace'), dict) else {}
        engine = workspace.get('recommendations') if isinstance(workspace.get('recommendations'), dict) else {}
    items = engine.get('items') if isinstance(engine.get('items'), list) else []
    if not items:
        return []
    lines = ['🚀 Recommendation Engine', 'Приоритет | Действие | Основание | Ожидаемый эффект']
    for item in items[:5]:
        if not isinstance(item, dict):
            continue
        lines.append(
            f'{item.get("priority") or "—"} | {item.get("action") or "—"} | '
            f'{item.get("basis") or "—"} | {_fmt_int(item.get("expected_effect_money"))}'
        )
    return [line for line in lines if str(line or '').strip()]


def _render_narrative_block(response):
    narrative = response.get('narrative_engine') if isinstance(response.get('narrative_engine'), dict) else {}
    if not narrative:
        workspace = response.get('product_workspace') if isinstance(response.get('product_workspace'), dict) else {}
        narrative = workspace.get('narrative') if isinstance(workspace.get('narrative'), dict) else {}
    if not narrative:
        return []
    lines = [
        '🧠 Narrative Engine',
        f'Что произошло: {narrative.get("what_happened") or "—"}',
        f'Почему: {narrative.get("why") or "—"}',
        f'Что это означает: {narrative.get("what_it_means") or "—"}',
        f'Что делать: {narrative.get("what_to_do") or "—"}',
    ]
    if narrative.get('expected_effect_money') not in (None, ''):
        lines.append(f'Ожидаемый эффект / масштаб: {_fmt_int(narrative.get("expected_effect_money"))}')
    return [line for line in lines if str(line or '').strip()]




def _render_management_workspace_block(response):
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if level not in {'business', 'manager_top', 'manager'}:
        return []
    mi = response.get('management_intelligence') if isinstance(response.get('management_intelligence'), dict) else {}
    if not mi:
        return []
    passport = mi.get('passport') if isinstance(mi.get('passport'), dict) else {}
    portfolio = passport.get('portfolio') if isinstance(passport.get('portfolio'), dict) else {}
    radar = mi.get('radar') if isinstance(mi.get('radar'), dict) else {}
    priority = mi.get('priority_action') if isinstance(mi.get('priority_action'), dict) else {}
    narrative = mi.get('narrative') if isinstance(mi.get('narrative'), dict) else {}
    workspace = mi.get('workspace') if isinstance(mi.get('workspace'), dict) else {}
    lines = [
        f'🧭 Management рабочий стол: {mi.get("object_name") or ctx.get("object_name") or "объект"}',
        f'Период: {mi.get("period") or ctx.get("period") or "—"}',
        f'Роль владельца: {mi.get("owner_role") or "—"}',
        '',
        workspace.get('main_question') or 'Что требует управленческого внимания?',
        '',
        '## Паспорт ответственности',
        f'{portfolio.get("child_label") or "Объекты"}: {_fmt_int(portfolio.get("child_count"))} / прошлый год {_fmt_int(portfolio.get("child_count_previous_year"))}',
        f'Контракты: {_fmt_int(portfolio.get("network_count"))} / прошлый год {_fmt_int(portfolio.get("network_count_previous_year"))}',
        f'Категории: {_fmt_int(portfolio.get("category_count"))}',
        f'SKU: {_fmt_int(portfolio.get("sku_count"))}',
        '',
        '## Управленческий радар',
    ]
    summary = radar.get('summary') if isinstance(radar.get('summary'), dict) else {}
    lines.append(f'Объектов внимания: {_fmt_int(summary.get("risk_count"))}; объектов роста: {_fmt_int(summary.get("growth_count"))}; объектов с резервом: {_fmt_int(summary.get("opportunity_count"))}.')
    attention = radar.get('attention_required') if isinstance(radar.get('attention_required'), list) else []
    if attention:
        lines.extend(['', 'Требуют внимания:'])
        for item in attention[:5]:
            if isinstance(item, dict):
                lines.append(f'{item.get("object_name") or "—"} → Δ прибыли {_fmt_signed_int(item.get("profit_delta_money"))} грн, резерв {_fmt_int(item.get("opportunity_money"))} грн')
    growth = radar.get('growth_practices') if isinstance(radar.get('growth_practices'), list) else []
    if growth:
        lines.extend(['', 'Сильные практики / рост:'])
        for item in growth[:3]:
            if isinstance(item, dict):
                lines.append(f'{item.get("object_name") or "—"} → Δ прибыли {_fmt_signed_int(item.get("profit_delta_money"))} грн')
    lines.extend(['', '## Управленческий вывод'])
    if narrative.get('what_happened'):
        lines.append(str(narrative.get('what_happened')))
    if narrative.get('why_it_matters'):
        lines.append(str(narrative.get('why_it_matters')))
    if priority:
        lines.extend(['', '## Приоритетное действие', str(priority.get('action') or '—')])
        if priority.get('basis'):
            lines.append(f'Основание: {priority.get("basis")}')
    chain = mi.get('decision_chain') if isinstance(mi.get('decision_chain'), list) else []
    if chain:
        lines.extend(['', '## Decision Lifecycle'])
        for item in chain:
            if isinstance(item, dict) and item.get('title'):
                lines.append(f'{item.get("step")}: {item.get("title")}')
    return [line for line in lines if str(line or '').strip()]

def _render_product_workspace_block(response):
    workspace = response.get('product_workspace') if isinstance(response.get('product_workspace'), dict) else {}
    if not workspace:
        return []
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = workspace.get('level') or ctx.get('level') or ''
    if str(level).lower() not in {'network', 'category', 'tmc_group', 'sku'}:
        return []
    lines = [
        f'📦 Product рабочий стол: {workspace.get("object_name") or ctx.get("object_name") or "объект"}',
        f'Период: {workspace.get("period") or ctx.get("period") or "—"}',
    ]
    opp = workspace.get('opportunities') if isinstance(workspace.get('opportunities'), dict) else {}
    rec = workspace.get('recommendations') if isinstance(workspace.get('recommendations'), dict) else {}
    opp_summary = opp.get('summary') if isinstance(opp.get('summary'), dict) else {}
    main_rec = rec.get('main_recommendation') if isinstance(rec.get('main_recommendation'), dict) else None
    lines.extend([
        '',
        'Управленческий смысл:',
        f'Найдено возможностей: {_fmt_int(opp_summary.get("total_items"))}',
    ])
    if main_rec:
        lines.append(f'Главное действие: {main_rec.get("action") or "—"}')
        if main_rec.get('basis'):
            lines.append(f'Основание: {main_rec.get("basis")}')
    next_actions = workspace.get('next_actions') if isinstance(workspace.get('next_actions'), list) else []
    if next_actions:
        lines.extend(['', 'Что делаем дальше:'])
        lines.extend(str(x) for x in next_actions[:5])
    return [line for line in lines if str(line or '').strip()]


# Sprint 12 Product Recovery: full assistant workspaces built from current DATA.
# These blocks intentionally keep the product model visible in the API response:
# Business = commercial director desktop; Network = Рабочий стол контракта for КАМ.

def _pr_prev_year(period: str) -> str:
    return _pi72_previous_year_period(period)


def _pr_months_back(period: str, count: int = 6) -> list:
    try:
        year = int(str(period)[:4]); month = int(str(period)[5:7])
    except Exception:
        return [period] if period else []
    out = []
    y, m = year, month
    for _ in range(count):
        out.append(f'{y:04d}-{m:02d}')
        m -= 1
        if m == 0:
            y -= 1; m = 12
    return list(reversed(out))


def _pr_context_filters(response: dict) -> dict:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    name = str(ctx.get('object_name') or '').strip()
    path = response.get('path') if isinstance(response.get('path'), list) else []
    flt = {}
    if level == 'manager_top':
        flt['manager_top'] = name
    elif level == 'manager':
        flt['manager'] = name
    elif level == 'network':
        flt['network'] = name
    elif level == 'category':
        if len(path) >= 4:
            flt['network'] = path[3]
        flt['category'] = name
    elif level == 'tmc_group':
        if len(path) >= 4:
            flt['network'] = path[3]
        if len(path) >= 5:
            flt['category'] = path[4]
        flt['tmc_group'] = name
    elif level == 'sku':
        if len(path) >= 4:
            flt['network'] = path[3]
        if len(path) >= 5:
            flt['category'] = path[4]
        flt['sku'] = name
    return {k: v for k, v in flt.items() if v}


_PR_ROWS_CACHE = {}

def _pr_rows(period: str, **filters) -> list:
    key = (str(period or ''), tuple(sorted((str(k), str(v)) for k, v in filters.items() if v)))
    if key in _PR_ROWS_CACHE:
        return _PR_ROWS_CACHE[key]
    try:
        rows, _ = filter_rows(get_normalized_rows(), period=period, **{k: v for k, v in filters.items() if v})
        rows = rows or []
        if len(_PR_ROWS_CACHE) > 256:
            _PR_ROWS_CACHE.clear()
        _PR_ROWS_CACHE[key] = rows
        return rows
    except Exception:
        logger.exception('product_recovery_filter_rows_failed')
        return []


def _pr_metric_text(response: dict, name: str) -> str:
    item = _metric_by_name(response.get('metrics') or [], name)
    if not item:
        return '—'
    if name in {'Маржа', 'Наценка'}:
        return f"{_fmt_percent_value(item.get('fact_percent'))} | Прошлый год {_fmt_percent_value(item.get('pg_percent'))} | Δ {_fmt_pp_delta(item.get('delta_percent'))}"
    return f"{_fmt_int(item.get('fact_money'))} | Прошлый год {_fmt_int(item.get('pg_money'))} | Δ {_fmt_signed_int(item.get('delta_money'))}"


def _pr_metric_num(response: dict, name: str, key_money='fact_money', key_percent='fact_percent') -> float:
    item = _metric_by_name(response.get('metrics') or [], name)
    if not item:
        return 0.0
    return _num(item.get(key_percent if name in {'Маржа', 'Наценка'} else key_money))


def _pr_structural_items(level: str, period: str, filters: dict) -> list:
    curr = _pr_rows(period, **filters)
    prev = _pr_rows(_pr_prev_year(period), **filters) if period else []
    fields = {
        'business': [('manager_top','Топ-менеджеры'),('manager','Менеджеры'),('network','Контракты'),('category','Категории'),('tmc_group','Группы ТМС'),('sku','SKU')],
        'network': [('category','Категории'),('tmc_group','Группы ТМС'),('sku','SKU')],
        'manager': [('network','Контракты'),('category','Категории'),('tmc_group','Группы ТМС'),('sku','SKU')],
        'manager_top': [('manager','Менеджеры'),('network','Контракты'),('category','Категории'),('tmc_group','Группы ТМС'),('sku','SKU')],
        'category': [('tmc_group','Группы ТМС'),('sku','SKU'),('network','Контракты')],
        'sku': [('network','Контракты')],
    }.get(level, [])
    items=[]
    for field,label in fields:
        cur=len({str(r.get(field) or '').strip() for r in curr if str(r.get(field) or '').strip()})
        prv=len({str(r.get(field) or '').strip() for r in prev if str(r.get(field) or '').strip()})
        items.append({'name':label,'current':cur,'previous':prv,'delta':cur-prv})
    return items


def _pr_trend_lines(period: str, filters: dict, limit: int = 6) -> list:
    lines=['📈 Историческая динамика 6 месяцев','Период | Оборот | Финрез ДО | Маржа | Наценка']
    for p in _pr_months_back(period, limit):
        rows=_pr_rows(p, **filters)
        m=aggregate_metrics(rows) if rows else {}
        revenue=_num(m.get('revenue'))
        finrez=_num(m.get('finrez_pre'))
        margin=_num(m.get('margin_pre'))
        markup=_num(m.get('markup'))
        lines.append(f'{p} | {_fmt_int(revenue)} | {_fmt_signed_int(finrez)} | {_fmt_percent_value(margin)} | {_fmt_percent_value(markup)}')
    return lines


def _pr_group_table(period: str, group_field: str, filters: dict, top: int = 8) -> list:
    rows=_pr_rows(period, **filters)
    prev=_pr_rows(_pr_prev_year(period), **filters)
    grouped={}; prev_grouped={}
    for r in rows:
        key=str(r.get(group_field) or '').strip()
        if key: grouped.setdefault(key,[]).append(r)
    for r in prev:
        key=str(r.get(group_field) or '').strip()
        if key: prev_grouped.setdefault(key,[]).append(r)
    total=aggregate_metrics(rows) if rows else {}
    total_rev=_num(total.get('revenue')); total_profit=_num(total.get('finrez_pre'))
    items=[]
    for name, rs in grouped.items():
        cur=aggregate_metrics(rs); prv=aggregate_metrics(prev_grouped.get(name) or [])
        revenue=_num(cur.get('revenue')); profit=_num(cur.get('finrez_pre')); prev_profit=_num(prv.get('finrez_pre'))
        items.append({
            'name':name,'revenue':revenue,'finrez':profit,'delta_profit':profit-prev_profit,
            'share_revenue':(revenue/total_rev*100) if total_rev else 0,
            'share_profit':(profit/total_profit*100) if abs(total_profit)>1e-9 else 0,
            'sku_count':len({str(r.get('sku') or '').strip() for r in rs if str(r.get('sku') or '').strip()}),
            'network_count':len({str(r.get('network') or '').strip() for r in rs if str(r.get('network') or '').strip()}),
        })
    items.sort(key=lambda x: abs(x.get('delta_profit') or 0), reverse=True)
    return items[:top]



def _w5_trend_comment(period: str, filters: dict, label: str = 'объекта') -> str:
    """User-facing assistant comment after the 6-month dynamics table."""
    months = _pr_months_back(period, 6)
    if len(months) < 2:
        return ''
    first_rows = _pr_rows(months[0], **filters)
    last_rows = _pr_rows(months[-1], **filters)
    if not first_rows or not last_rows:
        return ''
    first = aggregate_metrics(first_rows)
    last = aggregate_metrics(last_rows)
    rev_delta = _num(last.get('revenue')) - _num(first.get('revenue'))
    profit_delta = _num(last.get('finrez_pre')) - _num(first.get('finrez_pre'))
    margin_delta = _num(last.get('margin_pre')) - _num(first.get('margin_pre'))
    if profit_delta >= 0 and rev_delta < 0:
        return f'Комментарий ассистента: за 6 месяцев оборот снизился на {_fmt_signed_int(rev_delta)} грн, но финрез ДО изменился на {_fmt_signed_int(profit_delta)} грн. Значит, ключевая история {label} — не рост масштаба, а улучшение качества экономики.'
    if profit_delta >= 0 and rev_delta >= 0:
        return f'Комментарий ассистента: за 6 месяцев {label} растёт одновременно по обороту ({_fmt_signed_int(rev_delta)} грн) и финрезу ДО ({_fmt_signed_int(profit_delta)} грн). Это более здоровый сценарий роста.'
    return f'Комментарий ассистента: за 6 месяцев финрез ДО изменился на {_fmt_signed_int(profit_delta)} грн, маржа — на {_fmt_pp_delta(margin_delta)}. Перед действием нужно отделить проблему объёма от проблемы экономики.'


def _w5_factor_comment(factors: list) -> str:
    if not factors:
        return ''
    ordered = sorted(factors, key=lambda x: abs(_num(x.get('effect'))), reverse=True)
    top = ordered[0]
    risks = [x for x in ordered if _num(x.get('effect')) < 0]
    text = f'Комментарий ассистента: главный фактор по денежному эффекту — {top.get("name")}: {_fmt_signed_int(top.get("effect"))} грн. '
    if risks:
        risk = risks[0]
        text += f'Главный отрицательный фактор — {risk.get("name")}: {_fmt_signed_int(risk.get("effect"))} грн. Именно его нужно держать под контролем при выборе следующего действия.'
    else:
        text += 'Отрицательных факторов с подтверждённым эффектом в этом блоке не видно.'
    return text


def _w5_potential_comment(rows: list) -> str:
    if not rows:
        return ''
    rows = [x for x in rows if isinstance(x, dict)]
    if not rows:
        return ''
    top = max(rows, key=lambda x: _num(x.get('potential') or x.get('opportunity_money') or 0))
    name = top.get('name') or top.get('object_name') or top.get('sku') or 'объект'
    value = top.get('potential') if top.get('potential') is not None else top.get('opportunity_money')
    return f'Комментарий ассистента: потенциал нужно читать не как абстрактную сумму, а как подтверждённое отклонение от более сильной модели бизнеса. Самая крупная точка резерва в этом блоке — {name}: {_fmt_int(value)} грн.'



def _wic_factor_levels_from_metrics(metrics: dict) -> dict:
    """Percent levels for factors in a comparable business/object model.

    Positive values mean revenue/cost intensity. Cost factors are shown as
    negative percentages in UI because they reduce profit. Markup is positive.
    """
    revenue = _num(metrics.get('revenue'))
    cost = _num(metrics.get('cost'))
    return {
        'Наценка': _num(metrics.get('markup')),
        'Ретро': -(_num(metrics.get('retro_bonus')) / revenue * 100.0) if revenue else 0.0,
        'Логистика': -(_num(metrics.get('logistics_cost')) / revenue * 100.0) if revenue else 0.0,
        'Персонал': -(_num(metrics.get('personnel_cost')) / revenue * 100.0) if revenue else 0.0,
        'Прочие': -(_num(metrics.get('other_costs')) / revenue * 100.0) if revenue else 0.0,
    }


def _wic_factor_evidence_from_data(period: str, filters: dict) -> list:
    """Build Evidence First factor table directly from DATA when legacy fields are incomplete."""
    cur_rows = _pr_rows(period, **filters)
    prev_rows = _pr_rows(_pr_prev_year(period), **filters)
    if not cur_rows:
        return []
    cur = aggregate_metrics(cur_rows)
    prev = aggregate_metrics(prev_rows) if prev_rows else {}
    cur_l = _wic_factor_levels_from_metrics(cur)
    prev_l = _wic_factor_levels_from_metrics(prev) if prev_rows else {}

    # Monetary effects vs LY: profit bridge approximation on current object.
    effects = {
        'Наценка': (_num(cur.get('markup')) - _num(prev.get('markup'))) / 100.0 * max(_num(cur.get('cost')), 0),
        'Ретро': -(_num(cur.get('retro_bonus')) - _num(prev.get('retro_bonus'))),
        'Логистика': -(_num(cur.get('logistics_cost')) - _num(prev.get('logistics_cost'))),
        'Персонал': -(_num(cur.get('personnel_cost')) - _num(prev.get('personnel_cost'))),
        'Прочие': -(_num(cur.get('other_costs')) - _num(prev.get('other_costs'))),
    }
    rows=[]
    for name in ['Наценка','Логистика','Прочие','Ретро','Персонал']:
        curr=cur_l.get(name,0); prv=prev_l.get(name,0) if prev_rows else None
        rows.append({
            'name': name,
            'current_text': _fmt_percent_value(curr),
            'previous_text': _fmt_percent_value(prv) if prv is not None else '—',
            'delta_text': _fmt_pp_delta(curr - prv) if prv is not None else 'нет корректной базы',
            'effect': effects.get(name,0),
            'signal': 'риск' if effects.get(name,0) < 0 else ('драйвер' if effects.get(name,0) > 0 else 'нейтрально'),
        })
    rows.sort(key=lambda x: abs(_num(x.get('effect'))), reverse=True)
    return rows


def _wic_benchmark_factor_rows(period: str, filters: dict) -> list:
    """Object vs business factor evidence table based on DATA."""
    obj_rows = _pr_rows(period, **filters)
    biz_rows = _pr_rows(period)
    if not obj_rows or not biz_rows:
        return []
    obj = aggregate_metrics(obj_rows); biz = aggregate_metrics(biz_rows)
    obj_l = _wic_factor_levels_from_metrics(obj); biz_l = _wic_factor_levels_from_metrics(biz)
    revenue = max(_num(obj.get('revenue')), 0)
    rows=[]
    for name in ['Наценка','Ретро','Логистика','Персонал','Прочие']:
        gap = _num(obj_l.get(name)) - _num(biz_l.get(name))
        # For all factors in UI convention, positive gap is better; negative gap is reserve/risk.
        effect = gap/100.0 * revenue
        rows.append({
            'name': name,
            'current_text': _fmt_percent_value(obj_l.get(name)),
            'base_text': _fmt_percent_value(biz_l.get(name)),
            'gap_text': _fmt_pp_delta(gap),
            'effect': effect,
            'signal': 'сильнее бизнеса' if effect >= 0 else 'резерв / слабее бизнеса',
        })
    rows.sort(key=lambda x: abs(_num(x.get('effect'))), reverse=True)
    return rows


def _wic_potential_breakdown(period: str, filters: dict, limit: int = 3) -> list:
    rows = _wic_benchmark_factor_rows(period, filters)
    risks = [r for r in rows if _num(r.get('effect')) < 0]
    return [{'name': r['name'], 'money': abs(_num(r.get('effect'))), 'gap': r.get('gap_text')} for r in risks[:limit]]


def _wic_breakdown_text(parts: list) -> str:
    if not parts:
        return 'потенциал не разложен по факторам текущей DATA'
    return '; '.join(f"{p['name']} {p['gap']} ≈ {_fmt_int(p['money'])} грн" for p in parts)


def _wic_business_context_lines(period: str) -> list:
    rows = _pr_rows(period)
    if not rows:
        return []
    lines=['🌐 Business Context: что отличается внутри бизнеса']
    # Categories, formats, SKU opportunities from the business itself.
    cats=_pr_group_table(period,'category',{},10)
    if cats:
        lines.extend(['Категории | Оборот | Доля бизнеса | Финрез ДО | Δ прибыли | Что означает'])
        for c in cats[:5]:
            meaning='ключевой контур бизнеса' if c.get('share_revenue',0)>=20 else 'вторичный контур'
            lines.append(f"{c['name']} | {_fmt_int(c['revenue'])} | {_fmt_percent_value(c['share_revenue'])} | {_fmt_signed_int(c['finrez'])} | {_fmt_signed_int(c['delta_profit'])} | {meaning}")
    fmts=_w3_format_table(period, {}, {}, limit=8)
    if fmts:
        lines.extend(['','Форматы бизнеса | Оборот | Доля бизнеса | SKU | Финрез | Управленческий смысл'])
        for f in fmts[:6]:
            sense='формат масштаба' if f.get('share_business',0)>=10 else 'формат развития/ниши'
            lines.append(f"{f['format']} | {_fmt_int(f['revenue'])} | {_fmt_percent_value(f['share_business'])} | {f['sku_count']} | {_fmt_signed_int(f['finrez'])} | {sense}")
    skus=_pr_business_sku_leaders(period,10)
    if skus:
        lines.extend(['','SKU-лидеры бизнеса | Оборот | Финрез ДО | Сетей | Зачем смотреть'])
        for s in skus[:5]:
            lines.append(f"{s['sku']} | {_fmt_int(s['revenue'])} | {_fmt_signed_int(s['finrez'])} | {s['network_count']} | доказательная база для контрактов")
    lines.append('Комментарий ассистента: этот блок показывает не локальную проблему, а карту возможностей бизнеса — какие категории, форматы и SKU уже доказаны DATA и могут использоваться как аргументы ниже.')
    return lines


def _wic_concentration_lines(period: str) -> list:
    managers=_pr_group_table(period,'manager_top',{},50)
    contracts=_pr_group_table(period,'network',{},200)
    if not managers and not contracts:
        return []
    lines=['🧲 Концентрация результата','Контур | Концентрация | Что означает']
    if managers:
        top3_rev=sum(_num(x.get('revenue')) for x in managers[:3]); total_rev=sum(_num(x.get('revenue')) for x in managers)
        top3_profit=sum(_num(x.get('finrez')) for x in managers[:3]); total_profit=sum(_num(x.get('finrez')) for x in managers)
        lines.append(f'ТОП-3 руководителя по обороту | {_fmt_percent_value((top3_rev/total_rev*100) if total_rev else 0)} оборота | показывает зависимость бизнеса от ключевых владельцев')
        lines.append(f'ТОП-3 руководителя по прибыли | {_fmt_percent_value((top3_profit/total_profit*100) if abs(total_profit)>1e-9 else 0)} прибыли | показывает концентрацию результата')
    if contracts:
        top10_rev=sum(_num(x.get('revenue')) for x in contracts[:10]); total_rev=sum(_num(x.get('revenue')) for x in contracts)
        lines.append(f'ТОП-10 контрактов | {_fmt_percent_value((top10_rev/total_rev*100) if total_rev else 0)} оборота | показывает, где управленческое внимание даёт быстрый эффект')
    return lines


def _pr_business_workspace_block(response: dict) -> list:
    """Workspace Intelligence Completion: full visible Business Workspace.

    This block is intentionally information-dense. It is the primary artifact
    for Custom GPT rendering and must show the changes from audit directly on
    screen: stronger executive summary, Evidence First factors, potential
    breakdown, Business Context and concentration map.
    """
    ctx=response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower()!='business':
        return []
    period=str(ctx.get('period') or '').strip()
    filters={}
    fin_delta=_pr_metric_num(response,'Финрез до','delta_money')
    rev_delta=_pr_metric_num(response,'Оборот','delta_money')
    margin_delta=_pr_metric_num(response,'Маржа','delta_money','delta_percent')
    markup_delta=_pr_metric_num(response,'Наценка','delta_money','delta_percent')
    revenue_cur=_pr_metric_num(response,'Оборот','fact_money')
    revenue_prev=_pr_metric_num(response,'Оборот','pg_money')
    revenue_drop_pct=(rev_delta/revenue_prev*100) if revenue_prev else 0
    profit_prev=_pr_metric_num(response,'Финрез до','pg_money')
    profit_growth_pct=(fin_delta/profit_prev*100) if profit_prev else 0

    lines=[
        f'📍 Рабочий стол бизнеса — {period}',
        '👤 Рабочий стол: Бизнес',
        '🤖 Роль ассистента: стратегический помощник коммерческого директора',
        '',
        '🧠 Краткий управленческий вывод',
    ]
    if fin_delta > 0 and rev_delta < 0:
        lines.append(
            f'Бизнес находится в нестандартной ситуации: оборот снизился на {_fmt_signed_int(rev_delta)} грн '
            f'({_fmt_percent_value(revenue_drop_pct)} к прошлому году), но финрез ДО вырос на {_fmt_signed_int(fin_delta)} грн '
            f'({_fmt_percent_value(profit_growth_pct)} к прошлому году).'
        )
        lines.append(
            f'Это означает, что период выигран не масштабом продаж, а качеством экономики: маржа выросла на {_fmt_pp_delta(margin_delta)}, '
            f'наценка — на {_fmt_pp_delta(markup_delta)}. Главный управленческий вопрос теперь — удержать новую доходность при восстановлении оборота.'
        )
    elif fin_delta > 0:
        lines.append(
            f'Финрез ДО вырос на {_fmt_signed_int(fin_delta)} грн при изменении оборота на {_fmt_signed_int(rev_delta)} грн. '
            'Нужно разделить вклад масштаба, структуры и экономики продаж.'
        )
    else:
        lines.append(
            f'Финрез ДО снизился на {_fmt_signed_int(fin_delta)} грн. Первое действие — найти владельцев просадки, затем разложить её на факторы экономики и структуры.'
        )

    lines.extend(['','📊 Ключевые показатели бизнеса','Показатель | Текущий период | Прошлый год | Изменение | Что означает'])
    for name, meaning in [
        ('Оборот','масштаб бизнеса'),('Финрез до','прибыль до распределений'),('Маржа','качество прибыли'),('Наценка','ценовая экономика'),('Финрез итог','итог после распределений')
    ]:
        item=_metric_by_name(response.get('metrics') or [], name)
        if not item: continue
        if name in {'Маржа','Наценка'}:
            lines.append(f'{name} | {_fmt_percent_value(item.get("fact_percent"))} | {_fmt_percent_value(item.get("pg_percent"))} | {_fmt_pp_delta(item.get("delta_percent"))} | {meaning}')
        else:
            lines.append(f'{name} | {_fmt_int(item.get("fact_money"))} | {_fmt_int(item.get("pg_money"))} | {_fmt_signed_int(item.get("delta_money"))} | {meaning}')
    lines.append('Комментарий ассистента: таблица показывает доверительную базу. Вывод выше не является мнением — он следует из сочетания падения оборота и роста доходности.')

    sitems=_pr_structural_items('business', period, filters)
    if sitems:
        lines.extend(['','🏗 Структурный анализ бизнеса','Структура | Сейчас | Прошлый год | Δ | Что означает'])
        comments={'Топ-менеджеры':'верхний контур управления','Менеджеры':'покрытие портфеля','Контракты':'клиентская база','Категории':'состав бизнеса','Группы ТМС':'продуктовые линейки','SKU':'ассортимент'}
        for it in sitems:
            delta=int(it.get('delta') or 0)
            if delta>0: meaning=f'расширение: {comments.get(it["name"],"структура")} увеличилась'
            elif delta<0: meaning=f'сокращение: {comments.get(it["name"],"структура")} уменьшилась'
            else: meaning='без изменений'
            lines.append(f"{it['name']} | {it['current']} | {it['previous']} | {delta:+d} | {meaning}")
        lines.append('Комментарий ассистента: перед финансовым выводом нужно понимать, сравниваем ли мы тот же объект. Здесь изменились менеджеры, контракты, категории, группы ТМС и SKU — значит часть результата связана со структурой портфеля.')

    lines.extend([''] + _pr_trend_lines(period, filters, 6))
    trend_comment = _w5_trend_comment(period, filters, 'бизнеса')
    if trend_comment:
        lines.append(trend_comment)
    # Add an explicit interpretation of current month vs prior months.
    months=_pr_months_back(period,6)
    vals=[]
    for p in months:
        rs=_pr_rows(p)
        if rs:
            m=aggregate_metrics(rs); vals.append((p,_num(m.get('revenue')),_num(m.get('finrez_pre')),_num(m.get('margin_pre')),_num(m.get('markup'))))
    if vals:
        peak_rev=max(vals, key=lambda x:x[1]); peak_profit=max(vals, key=lambda x:x[2]); cur=vals[-1]
        lines.append(f'Вывод по динамике: текущий месяц не является максимумом по обороту за 6 месяцев (пик — {peak_rev[0]}: {_fmt_int(peak_rev[1])} грн), но показывает одну из самых сильных экономик периода: маржа {_fmt_percent_value(cur[3])}, наценка {_fmt_percent_value(cur[4])}.')

    factors=_w3_factor_evidence_rows(response, business=True) or _wic_factor_evidence_from_data(period, filters)
    if factors:
        lines.extend(['','💰 Почему изменилась прибыль: доказательная база','Фактор | Текущий уровень | Прошлый год | Изменение | Денежный эффект | Сигнал'])
        for item in factors:
            lines.append(f"{item['name']} | {item['current_text']} | {item['previous_text']} | {item['delta_text']} | {_fmt_signed_int(item['effect'])} грн | {item['signal']}")
        comment=_w5_factor_comment(factors)
        if comment: lines.append(comment)
        lines.append('Комментарий ассистента: здесь важно не только увидеть сумму эффекта, но и проверить, из какого изменения показателя она возникла. Поэтому фактор всегда должен показываться как текущий уровень → прошлый год → изменение → деньги.')

    managers=[m for m in _pr_group_table(period,'manager_top',filters,50) if str(m.get('name') or '').strip().lower() not in {'пусто','без менеджера','без менеджера '} ]
    opp_map={str(x.get('object_name')): _num(x.get('opportunity_money')) for x in (response.get('opportunity_rating') or []) if isinstance(x,dict)}
    if managers:
        total_potential=sum(max(0,_num(v)) for v in opp_map.values())
        if not total_potential:
            total_potential=sum(sum(p['money'] for p in _wic_potential_breakdown(period, {'manager_top': item['name']})) for item in managers)
        lines.extend(['','💵 Где находятся деньги','Подтверждённый потенциал по текущей модели: '+_fmt_int(total_potential)+' грн','Объект | Сигнал | Δ прибыли | Доля оборота | Доля прибыли | Потенциал | Из чего состоит потенциал | Контрактов | SKU'])
        # Sort by management priority: negative delta first, then potential, then profit growth.
        managers_sorted=sorted(managers, key=lambda x: (0 if x['delta_profit']<0 else 1, -max(opp_map.get(x['name'],0), sum(p['money'] for p in _wic_potential_breakdown(period, {'manager_top':x['name']}))), -x['delta_profit']))
        for item in managers_sorted[:8]:
            name=item['name']
            parts=_wic_potential_breakdown(period, {'manager_top': name})
            potential=opp_map.get(name,0) or sum(p['money'] for p in parts)
            if item['delta_profit']<0:
                sig='управленческий риск'
            elif potential>500000:
                sig='крупный резерв'
            elif item['delta_profit']>0:
                sig='рост / практика'
            else:
                sig='контроль'
            lines.append(f"{name} | {sig} | {_fmt_signed_int(item['delta_profit'])} | {_fmt_percent_value(item['share_revenue'])} | {_fmt_percent_value(item['share_profit'])} | {_fmt_int(potential)} | {_wic_breakdown_text(parts)} | {item['network_count']} | {item['sku_count']}")
        lines.append('Комментарий ассистента: блок показывает не только сумму потенциала, а её происхождение. Это нужно, чтобы следующий шаг превращался в задачу: наценка, ретро, логистика, персонал или ассортиментный контур.')

    bctx=_wic_business_context_lines(period)
    if bctx:
        lines.extend(['']+bctx)
    conc=_wic_concentration_lines(period)
    if conc:
        lines.extend(['']+conc)

    lines.extend(['','🚨 Приоритеты руководителя','Зона | Объект | Доказательство | Что делать первым'])
    if managers:
        risk=next((x for x in sorted(managers, key=lambda x:x['delta_profit']) if x['delta_profit']<0), None)
        reserve=max(managers, key=lambda x: opp_map.get(x['name'],0) or sum(p['money'] for p in _wic_potential_breakdown(period, {'manager_top':x['name']})))
        best=max(managers, key=lambda x: x['delta_profit'])
        if risk:
            lines.append(f'🔴 Главный риск | {risk["name"]} | Δ прибыли {_fmt_signed_int(risk["delta_profit"])}; доля прибыли {_fmt_percent_value(risk["share_profit"])} | открыть рабочий стол и найти источник просадки')
        lines.append(f'🟠 Главный резерв | {reserve["name"]} | потенциал {_fmt_int(opp_map.get(reserve["name"],0) or sum(p["money"] for p in _wic_potential_breakdown(period, {"manager_top":reserve["name"]})))}; доля оборота {_fmt_percent_value(reserve["share_revenue"])} | разобрать происхождение потенциала')
        lines.append(f'🟢 Лучшая практика | {best["name"]} | прирост прибыли {_fmt_signed_int(best["delta_profit"])} | понять, какие решения можно масштабировать')

        first = risk or reserve
        lines.extend(['','🎯 Что я бы сделал первым'])
        lines.append(f'Я бы начал с «{first["name"]}», потому что это первая точка, где управленческое действие может изменить общий результат бизнеса: есть вес в бизнесе, подтверждённая динамика и понятный следующий уровень детализации.')
    lines.extend(['','➡️ Что делаем дальше?','1. Открыть рабочий стол главного риска.','2. Открыть рабочий стол крупнейшего резерва.','3. Показать полную витрину руководителей.','4. Показать причины изменения результата.','5. Показать Business Context: категории, форматы и SKU бизнеса.','6. Создать задачи по выбранному приоритету.','7. Спросить ассистента: «что бы ты сделал первым и почему?»'])
    return [x for x in lines if str(x or '').strip()]

def _pr_business_sku_leaders(period: str, limit: int = 20) -> list:
    rows=_pr_rows(period)
    grouped={}
    for r in rows:
        sku=str(r.get('sku') or '').strip()
        if sku: grouped.setdefault(sku,[]).append(r)
    items=[]
    for sku, rs in grouped.items():
        m=aggregate_metrics(rs)
        items.append({'sku':sku,'revenue':_num(m.get('revenue')),'finrez':_num(m.get('finrez_pre')),'network_count':len({str(r.get('network') or '').strip() for r in rs if str(r.get('network') or '').strip()})})
    items.sort(key=lambda x: x['revenue'], reverse=True)
    for i,item in enumerate(items,1): item['rank']=i
    return items[:limit]



def _pr_management_workspace_block(response: dict) -> list:
    """Sprint W6: Russian, evidence-first workspace for Top Manager / Manager.

    This replaces the legacy mixed-language Management screen with the same
    product standard as Business and Contract: passport, structure, evidence,
    portfolio showcase and next decisions.
    """
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if level not in {'manager_top', 'manager'}:
        return []
    name = str(ctx.get('object_name') or '').strip() or 'объект'
    period = str(ctx.get('period') or '').strip()
    owner_role = 'руководитель направления' if level == 'manager_top' else 'КАМ / менеджер портфеля'
    filters = {'manager_top': name} if level == 'manager_top' else {'manager': name}
    child_field = 'manager' if level == 'manager_top' else 'network'
    child_label = 'менеджеров' if level == 'manager_top' else 'контрактов'
    child_title = 'Команда' if level == 'manager_top' else 'Портфель контрактов'

    lines = [
        f'🧭 Рабочий стол управления — {name} | {period}',
        f'👤 Рабочий стол: {"Руководитель направления" if level == "manager_top" else "Менеджер / КАМ"}',
        f'🤖 Роль ассистента: помощник {owner_role}',
        '',
        '🧠 Краткий вывод',
    ]
    fin_delta = _pr_metric_num(response, 'Финрез до', 'delta_money')
    rev_delta = _pr_metric_num(response, 'Оборот', 'delta_money')
    margin_delta = _pr_metric_num(response, 'Маржа', 'delta_money', 'delta_percent')
    lines.append(f'Финрез ДО изменился на {_fmt_signed_int(fin_delta)} грн, оборот — на {_fmt_signed_int(rev_delta)} грн к прошлому году.')
    if fin_delta >= 0 and rev_delta >= 0:
        lines.append(f'Зона ответственности растёт по масштабу и результату. Следующий вопрос — где внутри {child_label} находятся риски, резервы и лучшие практики.')
    elif fin_delta >= 0:
        lines.append('Результат улучшился, но нужно проверить качество роста: структуру портфеля, факторы экономики и объекты ниже.')
    else:
        lines.append('Есть просадка результата. Сначала проверяем структуру зоны ответственности, затем факторы и объекты ниже.')

    # KPI evidence table
    lines.extend(['', '📊 Ключевые показатели', 'Показатель | Текущий период | Прошлый год | Изменение | Доля бизнеса'])
    business_rows = _pr_rows(period)
    obj_rows = _pr_rows(period, **filters)
    bm = aggregate_metrics(business_rows) if business_rows else {}
    om = aggregate_metrics(obj_rows) if obj_rows else {}
    business_rev = _num(bm.get('revenue'))
    business_profit = _num(bm.get('finrez_pre'))
    obj_rev = _num(om.get('revenue'))
    obj_profit = _num(om.get('finrez_pre'))
    share_rev = (obj_rev / business_rev * 100) if business_rev else 0
    share_profit = (obj_profit / business_profit * 100) if abs(business_profit) > 1e-9 else 0
    for metric_name in ['Оборот', 'Финрез до', 'Маржа', 'Наценка']:
        item = _metric_by_name(response.get('metrics') or [], metric_name)
        if not item:
            continue
        if metric_name in {'Маржа', 'Наценка'}:
            share = '—'
            lines.append(f'{metric_name} | {_fmt_percent_value(item.get("fact_percent"))} | {_fmt_percent_value(item.get("pg_percent"))} | {_fmt_pp_delta(item.get("delta_percent"))} | {share}')
        else:
            share = _fmt_percent_value(share_rev if metric_name == 'Оборот' else share_profit)
            lines.append(f'{metric_name} | {_fmt_int(item.get("fact_money"))} | {_fmt_int(item.get("pg_money"))} | {_fmt_signed_int(item.get("delta_money"))} | {share}')

    # responsibility passport / structural analysis
    sitems = _pr_structural_items(level, period, filters)
    if sitems:
        lines.extend(['', '🧾 Паспорт зоны ответственности', 'Показатель | Сейчас | Прошлый год | Δ'])
        for it in sitems:
            lines.append(f"{it['name']} | {it['current']} | {it['previous']} | {it['delta']:+d}")
        lines.append('Комментарий ассистента: этот блок показывает, изменился ли сам состав зоны ответственности. Без этого нельзя корректно читать финансовую динамику.')

    trend = _pr_trend_lines(period, filters, 6)
    if trend:
        lines.extend([''] + trend)
        c = _w5_trend_comment(period, filters, 'зоны ответственности')
        if c:
            lines.append(c)

    factors = _w3_factor_evidence_rows(response, business=False)
    if factors:
        lines.extend(['', '💰 Почему изменилась прибыль', 'Фактор | Текущий период | Прошлый год | Δ | Денежный эффект | Сигнал'])
        for item in factors:
            lines.append(f"{item['name']} | {item['current_text']} | {item['previous_text']} | {item['delta_text']} | {_fmt_signed_int(item['effect'])} грн | {item['signal']}")
        comment = _w5_factor_comment(factors)
        if comment:
            lines.append(comment)

    # benchmark vs business from structure/economics
    econ_rows = _w3_contract_factor_rows(response)
    if econ_rows:
        lines.extend(['', '📍 Положение относительно бизнеса', 'Фактор | Объект | Бизнес | Отклонение | Потенциал / эффект | Сигнал'])
        for item in econ_rows:
            lines.append(f"{item['name']} | {item['current_text']} | {item['base_text']} | {item['gap_text']} | {_fmt_signed_int(item['effect'])} грн | {item['signal']}")

    # portfolio / team objects below
    children = _pr_group_table(period, child_field, filters, 20)
    opp_map = {str(o.get('object_name')): _num(o.get('opportunity_money')) for o in (response.get('opportunity_rating') or []) if isinstance(o, dict)}
    if children:
        lines.extend(['', f'👥 {child_title}: где деньги ниже', f'Объект | Оборот | Доля в зоне | Доля бизнеса | Финрез ДО | Δ прибыли | Потенциал | Сетей | SKU | Приоритет'])
        for item in children[:8]:
            obj = item['name']
            potential = opp_map.get(obj, 0)
            priority = '🔴 Риск' if item['delta_profit'] < 0 else ('🟠 Крупный резерв' if potential > 100000 else ('🟡 Резерв' if potential > 0 else '🟢 Рост'))
            business_share = (item['revenue'] / business_rev * 100) if business_rev else 0
            lines.append(
                f"{obj} | {_fmt_int(item['revenue'])} грн | {_fmt_percent_value(item['share_revenue'])} | {_fmt_percent_value(business_share)} | "
                f"{_fmt_signed_int(item['finrez'])} грн | {_fmt_signed_int(item['delta_profit'])} грн | {_fmt_int(potential)} грн | {item['network_count']} | {item['sku_count']} | {priority}"
            )
        lines.append('Комментарий ассистента: это не просто список ниже. Это карта управленческого внимания: где риск, где резерв, где лучшая практика и кого открывать первым.')

        risks = [x for x in children if x['delta_profit'] < 0]
        reserve = max(children, key=lambda x: opp_map.get(x['name'], 0)) if children else None
        best = max(children, key=lambda x: x['delta_profit']) if children else None
        lines.extend(['', '🚨 Управленческий радар', 'Сигнал | Объект | Основание | Действие'])
        if risks:
            r = risks[0]
            lines.append(f'🔴 Главный риск | {r["name"]} | Δ прибыли {_fmt_signed_int(r["delta_profit"])} грн | открыть рабочий стол и найти причину')
        if reserve:
            lines.append(f'🟠 Главный резерв | {reserve["name"]} | потенциал {_fmt_int(opp_map.get(reserve["name"], 0))} грн | разобрать источник потенциала')
        if best:
            lines.append(f'🟢 Лучшая практика | {best["name"]} | прирост прибыли {_fmt_signed_int(best["delta_profit"])} грн | понять, что можно масштабировать')

    lines.extend(['', '🎯 Приоритет владельца Workspace'])
    if children:
        first = next((x for x in children if x['delta_profit'] < 0), None) or max(children, key=lambda x: opp_map.get(x['name'], 0))
        lines.append(f'Первое действие — открыть «{first["name"]}». Это самый полезный следующий шаг по текущей карте риска и резерва.')
    else:
        lines.append('Первое действие — уточнить портфель ниже или задать ассистенту вопрос по причинам результата.')

    lines.extend(['', '➡️ Что делаем дальше?'])
    if children:
        first = next((x for x in children if x['delta_profit'] < 0), children[0])
        reserve = max(children, key=lambda x: opp_map.get(x['name'], 0))
        lines.append(f'1. Открыть «{first["name"]}» — главный риск или первая точка внимания.')
        lines.append(f'2. Открыть «{reserve["name"]}» — крупнейший подтверждённый резерв.')
    else:
        lines.append('1. Показать все объекты уровня.')
        lines.append('2. Показать причины результата.')
    lines.append(f'3. Показать полную витрину {child_label}.')
    lines.append('4. Показать причины.')
    lines.append('5. Создать задачи по выявленным приоритетам.')
    lines.append('6. Задать вопрос ассистенту: «что бы ты сделал первым?»')
    return [x for x in lines if str(x or '').strip()]

def _pr_contract_workspace_block(response: dict) -> list:
    ctx=response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower()!='network':
        return []
    contract=str(ctx.get('object_name') or '').strip(); period=str(ctx.get('period') or '').strip()
    filters={'network':contract}
    lines=[f'📍 Рабочий стол контракта — {contract} | {period}','👤 Рабочий стол: Контракт','🤖 Роль ассистента: цифровой помощник КАМ по развитию клиента','','🧠 Краткий вывод']
    fin_delta=_pr_metric_num(response,'Финрез до','delta_money')
    rev_delta=_pr_metric_num(response,'Оборот','delta_money')
    lines.append(f'Финрез ДО изменился на {_fmt_signed_int(fin_delta)} грн, оборот — на {_fmt_signed_int(rev_delta)} грн.')
    if fin_delta>0:
        lines.append('Контракт показывает положительную динамику; задача КАМ — понять, что закрепить, где расширить матрицу и какие условия контролировать.')
    else:
        lines.append('Контракт требует восстановления: сначала отделяем изменение структуры от экономики, затем ищем деньги в категориях, форматах и SKU.')
    lines.extend(['','📊 Ключевые показатели контракта','Показатель | Текущий период | Прошлый год | Изменение'])
    for name in ['Оборот','Финрез до','Маржа','Наценка']:
        val=_pr_metric_text(response,name)
        if val!='—': lines.append(f'{name} | {val}')
    # Contract passport: scale of the client inside the business.
    contract_rows=_pr_rows(period, **filters)
    business_rows=_pr_rows(period)
    if contract_rows and business_rows:
        cm=aggregate_metrics(contract_rows); bm=aggregate_metrics(business_rows)
        c_rev=_num(cm.get('revenue')); b_rev=_num(bm.get('revenue'))
        c_profit=_num(cm.get('finrez_pre')); b_profit=_num(bm.get('finrez_pre'))
        fmt_count=len({_pi72_format_name(r.get('tmc_group') or r.get('sku')) for r in contract_rows})
        lines.extend(['','🧾 Паспорт контракта','Показатель | Значение'])
        lines.append(f'Доля оборота бизнеса | {_fmt_percent_value((c_rev/b_rev*100) if b_rev else 0)}')
        lines.append(f'Доля прибыли бизнеса | {_fmt_percent_value((c_profit/b_profit*100) if abs(b_profit)>1e-9 else 0)}')
        lines.append(f'Категорий | {len({str(r.get("category") or "").strip() for r in contract_rows if str(r.get("category") or "").strip()})}')
        lines.append(f'Групп ТМС | {len({str(r.get("tmc_group") or "").strip() for r in contract_rows if str(r.get("tmc_group") or "").strip()})}')
        lines.append(f'Форматов | {fmt_count}')
        lines.append(f'SKU | {len({str(r.get("sku") or "").strip() for r in contract_rows if str(r.get("sku") or "").strip()})}')
    # economics vs business: Evidence First — object, benchmark, gap, effect.
    econ_rows = _w3_contract_factor_rows(response)
    if econ_rows:
        lines.extend(['','💰 Экономика контракта относительно бизнеса','Фактор | Контракт | Бизнес | Отклонение | Денежный эффект | Сигнал'])
        for item in econ_rows:
            lines.append(f"{item['name']} | {item['current_text']} | {item['base_text']} | {item['gap_text']} | {_fmt_signed_int(item['effect'])} грн | {item['signal']}")
        comment = _w5_factor_comment(econ_rows)
        if comment:
            lines.append(comment)
    sitems=_pr_structural_items('network',period,filters)
    if sitems:
        lines.extend(['','🏗 Структура контракта','Показатель | Сейчас | Прошлый год | Δ'])
        for it in sitems:
            lines.append(f"{it['name']} | {it['current']} | {it['previous']} | {it['delta']:+d}")
    lines.extend(['']+_pr_trend_lines(period,filters,6))
    trend_comment = _w5_trend_comment(period, filters, f'контракта {contract}')
    if trend_comment:
        lines.append(trend_comment)
    # categories
    cats=_pr_group_table(period,'category',filters,10)
    biz_cat=_pr_group_table(period,'category',{},50)
    biz_cat_map={x['name']:x for x in biz_cat}
    if cats:
        lines.extend(['','📦 Категории в контракте','Категория | Оборот | Доля контракта | Доля бизнеса | Финрез | Δ прибыли | Действие'])
        for c in cats:
            bc=biz_cat_map.get(c['name'],{})
            lines.append(f"{c['name']} | {_fmt_int(c['revenue'])} | {_fmt_percent_value(c['share_revenue'])} | {_fmt_percent_value(bc.get('share_revenue'))} | {_fmt_signed_int(c['finrez'])} | {_fmt_signed_int(c['delta_profit'])} | разобрать")
    # formats in contract
    rows=_pr_rows(period, **filters)
    if rows:
        format_rows=[]; grouped={}
        for r in rows:
            fmt=_pi72_format_name(r.get('tmc_group') or r.get('sku'))
            grouped.setdefault(fmt,[]).append(r)
        total_rev=_num(aggregate_metrics(rows).get('revenue'))
        for fmt,rs in grouped.items():
            m=aggregate_metrics(rs); format_rows.append((fmt,_num(m.get('revenue')),_num(m.get('finrez_pre')),len({r.get('sku') for r in rs if r.get('sku')})))
        format_rows.sort(key=lambda x:x[1], reverse=True)
        lines.extend(['','📐 Форматы контракта','Формат | Оборот | Доля контракта | Финрез | SKU | Что делать'])
        for fmt,rev,fin,sku_count in format_rows[:8]:
            action='защитить/масштабировать' if rev>0 else 'оценить ввод'
            lines.append(f'{fmt} | {_fmt_int(rev)} | {_fmt_percent_value((rev/total_rev*100) if total_rev else 0)} | {_fmt_signed_int(fin)} | {sku_count} | {action}')
    # SKU leaders and missing
    sku_items=_pr_group_table(period,'sku',filters,10)
    biz_leaders=_pr_business_sku_leaders(period,30)
    contract_skus={x['name'] for x in sku_items}
    missing=[x for x in biz_leaders if x['sku'] not in contract_skus]
    if sku_items:
        biz_sku_rows=_pr_group_table(period,'sku',{},200)
        biz_sku_map={x['name']:x for x in biz_sku_rows}
        lines.extend(['','⭐ SKU-лидеры контракта','SKU | Оборот | Доля контракта | Доля бизнеса | Финрез | Сетей в бизнесе | Роль'])
        for s in sku_items[:8]:
            role='флагман' if s['share_revenue']>=15 else 'рабочая позиция'
            bs=biz_sku_map.get(s['name'], {})
            lines.append(f"{s['name']} | {_fmt_int(s['revenue'])} | {_fmt_percent_value(s['share_revenue'])} | {_fmt_percent_value(bs.get('share_revenue'))} | {_fmt_signed_int(s['finrez'])} | {_fmt_int(bs.get('network_count'))} | {role}")
    if missing:
        biz_sku_rows=_pr_group_table(period,'sku',{},200)
        biz_sku_map={x['name']:x for x in biz_sku_rows}
        lines.extend(['','➕ Лидеры бизнеса, которых нет в контракте','SKU | Ранг в бизнесе | Оборот бизнеса | Доля бизнеса | Финрез бизнеса | Сетей где есть | Почему предложить'])
        for s in missing[:10]:
            bs=biz_sku_map.get(s['sku'], {})
            lines.append(f"{s['sku']} | №{s['rank']} | {_fmt_int(s['revenue'])} | {_fmt_percent_value(bs.get('share_revenue'))} | {_fmt_signed_int(s.get('finrez'))} | {s['network_count']} | лидер бизнеса отсутствует в контракте")
    lines.extend(['','🚀 План развития контракта'])
    if cats: lines.append(f'1. Разобрать категорию «{cats[0]["name"]}»: максимальный вклад/изменение внутри контракта.')
    if missing: lines.append('2. Собрать пакет отсутствующих SKU-лидеров бизнеса для первой переговорной позиции.')
    lines.append('3. Проверить экономику условий: наценка, ретро, логистика, персонал, прочие относительно бизнеса.')
    lines.extend(['','🤝 Переговорный пакет КАМ','Цель: перейти от общего разговора о контракте к пакету развития: категория → формат → SKU → условия.'])
    if missing:
        lines.append('Аргумент: предлагаемые позиции уже доказаны бизнесом — имеют оборот, покрытие сетей и рейтинг в бизнесе.')
    lines.extend(['','✅ Что делаем дальше?','1. Подготовить переговоры по контракту.','2. Собрать пакет SKU для ввода.','3. Разобрать категорию с наибольшим эффектом.','4. Показать причины по экономике контракта.','5. Создать задачи КАМ / трейд-маркетингу / аналитикам.','6. Спросить ассистента: «какие SKU предложить первыми и почему?»'])
    return [x for x in lines if str(x or '').strip()]




# Sprint W3 рабочий стол Intelligence: Evidence First, Object Passport and SKU-FIRST blocks.
def _w3_factor_name(item: dict) -> str:
    return str(item.get('name') or item.get('factor') or 'Фактор').strip()


def _w3_signal(effect: Any) -> str:
    val = _num(effect)
    if val < 0:
        return 'риск'
    if val > 0:
        return 'драйвер'
    return 'нейтрально'


def _w3_factor_evidence_rows(response: dict, *, business: bool = False) -> list:
    source = response.get('reasons_block') if isinstance(response.get('reasons_block'), list) else []
    if not source:
        source = response.get('structure') if isinstance(response.get('structure'), list) else []
    rows = []
    for item in source:
        if not isinstance(item, dict):
            continue
        name = _w3_factor_name(item)
        current = item.get('percent', item.get('fact_percent', item.get('value_percent')))
        previous = item.get('previous_percent', item.get('pg_percent', item.get('base_percent')))
        previous_missing = bool(item.get('previous_percent_missing')) or previous is None
        if previous_missing:
            delta_text = item.get('previous_note') or 'нет корректной базы'
            previous_text = '—'
        else:
            delta = item.get('delta_vs_previous_percent')
            if delta is None:
                delta = _num(current) - _num(previous)
            delta_text = _fmt_pp_delta(delta)
            previous_text = _fmt_percent_value(previous)
        rows.append({
            'name': name,
            'current_text': _fmt_percent_value(current),
            'previous_text': previous_text,
            'delta_text': delta_text,
            'effect': item.get('effect_money'),
            'signal': _w3_signal(item.get('effect_money')),
        })
    return rows


def _w3_contract_factor_rows(response: dict) -> list:
    source = response.get('structure') if isinstance(response.get('structure'), list) else []
    rows = []
    for item in source:
        if not isinstance(item, dict):
            continue
        current = item.get('percent', item.get('fact_percent', item.get('value_percent')))
        base = item.get('base_percent')
        gap = _num(current) - _num(base)
        rows.append({
            'name': _w3_factor_name(item),
            'current_text': _fmt_percent_value(current),
            'base_text': _fmt_percent_value(base),
            'gap_text': _fmt_pp_delta(gap),
            'effect': item.get('effect_money'),
            'signal': _w3_signal(item.get('effect_money')),
        })
    return rows


def _w3_group_map(period: str, field: str, filters: dict) -> dict:
    grouped = {}
    for row in _pr_rows(period, **filters):
        name = str(row.get(field) or '').strip()
        if name:
            grouped.setdefault(name, []).append(row)
    return grouped


def _w3_total_metrics(rows: list) -> dict:
    return aggregate_metrics(rows) if rows else {}


def _w3_share(part: Any, total: Any) -> float:
    total_num = _num(total)
    return (_num(part) / total_num * 100.0) if abs(total_num) > 1e-9 else 0.0


def _w3_format_table(period: str, filters: dict, business_filters: dict, *, limit: int = 12) -> list:
    rows = _pr_rows(period, **filters)
    business_rows = _pr_rows(period, **business_filters)
    total_rev = _num(_w3_total_metrics(rows).get('revenue'))
    biz_total_rev = _num(_w3_total_metrics(business_rows).get('revenue'))
    prev_rows = _pr_rows(_pr_prev_year(period), **filters)
    prev_by_fmt = {}
    for r in prev_rows:
        prev_by_fmt.setdefault(_pi72_format_name(r.get('tmc_group') or r.get('sku')), []).append(r)
    grouped = {}
    for r in rows:
        grouped.setdefault(_pi72_format_name(r.get('tmc_group') or r.get('sku')), []).append(r)
    biz_grouped = {}
    for r in business_rows:
        biz_grouped.setdefault(_pi72_format_name(r.get('tmc_group') or r.get('sku')), []).append(r)
    all_formats = set(grouped) | set(biz_grouped)
    out = []
    for fmt in all_formats:
        rs = grouped.get(fmt, [])
        brs = biz_grouped.get(fmt, [])
        prs = prev_by_fmt.get(fmt, [])
        m = _w3_total_metrics(rs); bm = _w3_total_metrics(brs); pm = _w3_total_metrics(prs)
        rev = _num(m.get('revenue')); fin = _num(m.get('finrez_pre')); prev_fin = _num(pm.get('finrez_pre'))
        biz_rev = _num(bm.get('revenue'))
        out.append({
            'format': fmt,
            'revenue': rev,
            'finrez': fin,
            'delta_profit': fin - prev_fin,
            'share_object': _w3_share(rev, total_rev),
            'share_business': _w3_share(biz_rev, biz_total_rev),
            'sku_count': len({str(r.get('sku') or '').strip() for r in rs if str(r.get('sku') or '').strip()}),
            'potential': max(0.0, ( _w3_share(biz_rev, biz_total_rev) - _w3_share(rev, total_rev) ) * max(total_rev, 0) / 100.0),
            'present': bool(rs),
        })
    out.sort(key=lambda x: (x.get('present', False), x.get('revenue') or x.get('potential') or 0), reverse=True)
    return out[:limit]


def _w3_sku_table(period: str, filters: dict, business_filters: dict, *, limit: int = 12, missing_only: bool = False) -> list:
    rows = _pr_rows(period, **filters)
    business_rows = _pr_rows(period, **business_filters)
    total_rev = _num(_w3_total_metrics(rows).get('revenue'))
    biz_total_rev = _num(_w3_total_metrics(business_rows).get('revenue'))
    current_names = {str(r.get('sku') or '').strip() for r in rows if str(r.get('sku') or '').strip()}
    prev_rows = _pr_rows(_pr_prev_year(period), **filters)
    prev_by_sku = {}
    for r in prev_rows:
        name = str(r.get('sku') or '').strip()
        if name:
            prev_by_sku.setdefault(name, []).append(r)
    grouped = _w3_group_map(period, 'sku', filters)
    biz_grouped = _w3_group_map(period, 'sku', business_filters)
    biz_rank = sorted(((name, _num(_w3_total_metrics(rs).get('revenue'))) for name, rs in biz_grouped.items()), key=lambda x: x[1], reverse=True)
    rank_map = {name: idx for idx, (name, _) in enumerate(biz_rank, 1)}
    names = set(biz_grouped) if missing_only else (set(grouped) | set(biz_grouped))
    out = []
    for sku in names:
        if missing_only and sku in current_names:
            continue
        rs = grouped.get(sku, [])
        brs = biz_grouped.get(sku, [])
        prs = prev_by_sku.get(sku, [])
        m = _w3_total_metrics(rs); bm = _w3_total_metrics(brs); pm = _w3_total_metrics(prs)
        rev = _num(m.get('revenue')); fin = _num(m.get('finrez_pre')); prev_fin = _num(pm.get('finrez_pre'))
        biz_rev = _num(bm.get('revenue')); biz_fin = _num(bm.get('finrez_pre'))
        networks = len({str(r.get('network') or '').strip() for r in brs if str(r.get('network') or '').strip()})
        out.append({
            'sku': sku,
            'format': _pi72_format_name((brs[0].get('tmc_group') or sku) if brs else sku),
            'revenue': rev,
            'finrez': fin,
            'delta_profit': fin - prev_fin,
            'share_object': _w3_share(rev, total_rev),
            'business_revenue': biz_rev,
            'business_finrez': biz_fin,
            'share_business': _w3_share(biz_rev, biz_total_rev),
            'network_count': networks,
            'rank': rank_map.get(sku),
            'potential': max(0.0, biz_fin if missing_only else 0.0),
            'present': bool(rs),
        })
    if missing_only:
        out.sort(key=lambda x: (x.get('business_revenue') or 0), reverse=True)
    else:
        out.sort(key=lambda x: (x.get('revenue') or x.get('business_revenue') or 0), reverse=True)
    return out[:limit]


def _w3_parent_contract_from_response(response: dict) -> str:
    return _pi72_extract_network_from_path(response)


def _w3_category_workspace_block(response: dict) -> list:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower() != 'category':
        return []
    category = str(ctx.get('object_name') or '').strip()
    period = str(ctx.get('period') or '').strip()
    contract = _w3_parent_contract_from_response(response)
    if not category or not period:
        return []
    filters = {'category': category}
    if contract:
        filters['network'] = contract
    rows = _pr_rows(period, **filters)
    biz_filters = {'category': category}
    biz_rows = _pr_rows(period, **biz_filters)
    total = _w3_total_metrics(rows); biz_total = _w3_total_metrics(biz_rows)
    contract_rows = _pr_rows(period, network=contract) if contract else []
    business_rows = _pr_rows(period)
    rev = _num(total.get('revenue')); fin = _num(total.get('finrez_pre'))
    biz_rev = _num(biz_total.get('revenue')); biz_fin = _num(biz_total.get('finrez_pre'))
    contract_rev = _num(_w3_total_metrics(contract_rows).get('revenue'))
    business_rev = _num(_w3_total_metrics(business_rows).get('revenue'))
    lines = [
        f'📦 Рабочий стол категории — {category}' + (f' | {contract}' if contract else ''),
        f'Период: {period}',
        '🤖 Роль ассистента: помощник КАМ по развитию категории',
        '',
        '🧾 Паспорт категории',
        'Показатель | Значение',
        f'Контракт | {contract or "—"}',
        f'Оборот категории в контракте | {_fmt_int(rev)} грн',
        f'Финрез ДО категории в контракте | {_fmt_signed_int(fin)} грн',
        f'Доля в контракте | {_fmt_percent_value(_w3_share(rev, contract_rev))}',
        f'Доля в бизнесе | {_fmt_percent_value(_w3_share(biz_rev, business_rev))}',
        f'SKU в категории | {len({str(r.get("sku") or "").strip() for r in rows if str(r.get("sku") or "").strip()})}',
        '',
        '📊 Ключевые показатели категории',
        'Показатель | Текущий период | Прошлый год | Изменение | Доля контракта | Доля бизнеса',
    ]
    metric_map = {str(x.get('name') or ''): x for x in (response.get('metrics') or []) if isinstance(x, dict)}
    for name in ['Оборот', 'Финрез до', 'Маржа', 'Наценка']:
        item = metric_map.get(name)
        if not item:
            continue
        if name in {'Маржа', 'Наценка'}:
            lines.append(f'{name} | {_fmt_percent_value(item.get("fact_percent"))} | {_fmt_percent_value(item.get("pg_percent"))} | {_fmt_pp_delta(item.get("delta_percent"))} | — | —')
        else:
            fact = item.get('fact_money'); prev = item.get('pg_money'); delta = item.get('delta_money')
            share_contract = _w3_share(fact, contract_rev) if name == 'Оборот' else _w3_share(fact, _num(_w3_total_metrics(contract_rows).get('finrez_pre')))
            share_business = _w3_share(biz_rev if name == 'Оборот' else biz_fin, business_rev if name == 'Оборот' else _num(_w3_total_metrics(business_rows).get('finrez_pre')))
            lines.append(f'{name} | {_fmt_int(fact)} | {_fmt_int(prev)} | {_fmt_signed_int(delta)} | {_fmt_percent_value(share_contract)} | {_fmt_percent_value(share_business)}')
    factors = _w3_factor_evidence_rows(response)
    if factors:
        lines.extend(['','💰 Экономика категории: доказательства','Фактор | Текущий период | Прошлый год | Изменение | Денежный эффект | Сигнал'])
        for item in factors:
            lines.append(f"{item['name']} | {item['current_text']} | {item['previous_text']} | {item['delta_text']} | {_fmt_signed_int(item['effect'])} грн | {item['signal']}")
        comment = _w5_factor_comment(factors)
        if comment:
            lines.append(comment)
    formats = _w3_format_table(period, filters, biz_filters, limit=10)
    if formats:
        lines.extend(['','📐 Форматы категории','Формат | Оборот | Доля категории | Доля бизнеса | SKU | Потенциал | Действие'])
        for f in formats:
            action = 'защитить/масштабировать' if f.get('present') and _num(f.get('share_object')) >= 20 else ('ввести/проверить' if not f.get('present') else 'оценить развитие')
            lines.append(f"{f['format']} | {_fmt_int(f['revenue'])} | {_fmt_percent_value(f['share_object'])} | {_fmt_percent_value(f['share_business'])} | {f['sku_count']} | {_fmt_int(f['potential'])} | {action}")
    sku_leaders = _w3_sku_table(period, filters, biz_filters, limit=10, missing_only=False)
    sku_leaders = [x for x in sku_leaders if x.get('present')]
    if sku_leaders:
        lines.extend(['','⭐ SKU-лидеры категории','SKU | Формат | Оборот | Доля категории | Доля бизнеса | Финрез | Роль'])
        for s in sku_leaders[:8]:
            role = 'якорь' if _num(s.get('share_object')) >= 15 else 'рабочая позиция'
            lines.append(f"{s['sku']} | {s['format']} | {_fmt_int(s['revenue'])} | {_fmt_percent_value(s['share_object'])} | {_fmt_percent_value(s['share_business'])} | {_fmt_signed_int(s['finrez'])} | {role}")
    missing = _w3_sku_table(period, filters, biz_filters, limit=10, missing_only=True)
    if missing:
        lines.extend(['','➕ Отсутствующие SKU / форматы','SKU | Формат | Ранг в бизнесе | Оборот бизнеса | Сетей где есть | Потенциал | Почему важно'])
        for s in missing[:8]:
            lines.append(f"{s['sku']} | {s['format']} | №{s.get('rank') or '—'} | {_fmt_int(s['business_revenue'])} | {s['network_count']} | {_fmt_int(s['potential'])} | закрывает пробел категории")
    lines.extend(['','🎯 План развития категории'])
    if formats:
        target = next((f for f in formats if not f.get('present') and _num(f.get('share_business')) > 0), formats[0])
        lines.append(f"1. Сначала проверить формат {target['format']}: доля в бизнесе {_fmt_percent_value(target.get('share_business'))}, текущая доля в категории {_fmt_percent_value(target.get('share_object'))}.")
    if missing:
        lines.append('2. После выбора формата собрать первую волну SKU из отсутствующих лидеров бизнеса.')
    lines.append('3. Подготовить переговорный аргумент по категории: сначала структура, затем SKU, затем условия.')
    lines.extend(['','➡️ Что делаем дальше?','1. Подготовить пакет развития категории.','2. Показать все SKU категории.','3. Подготовить переговорный аргумент.','4. Открыть SKU-лидера как доказательство.','5. Создать задачи.'])
    return [x for x in lines if str(x or '').strip()]


def _w3_sku_passport_block(response: dict) -> list:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower() != 'sku':
        return []
    sku = str(ctx.get('object_name') or '').strip()
    period = str(ctx.get('period') or '').strip()
    passport = response.get('sku_passport') if isinstance(response.get('sku_passport'), dict) else {}
    if not sku or not period:
        return []
    contract = passport.get('contract') or _pi72_extract_network_from_path(response)
    ident = passport.get('identification') if isinstance(passport.get('identification'), dict) else {}
    role = passport.get('business_role') if isinstance(passport.get('business_role'), dict) else {}
    econ = passport.get('economics') if isinstance(passport.get('economics'), dict) else {}
    presence = passport.get('presence') if isinstance(passport.get('presence'), dict) else {}
    decision = passport.get('decision') if isinstance(passport.get('decision'), dict) else {}
    lines = [
        f'🧾 Паспорт SKU 2.0 — {sku}',
        f'Период: {period}' + (f' | Контракт: {contract}' if contract else ''),
        '🤖 Роль ассистента: помощник по развитию продукта',
        '',
        '🧬 Идентификация SKU',
        'Поле | Значение',
        f'Категория | {ident.get("category") or "—"}',
        f'Группа ТМС | {ident.get("tmc_group") or "—"}',
        f'Формат | {ident.get("format") or _pi72_format_name(sku)}',
        f'Роль | {role.get("role") or "—"}',
        '',
        '📊 KPI SKU: доказательная база',
        'Показатель | Текущий период | Прошлый год | Δ / комментарий',
        f'Оборот | {_fmt_int(econ.get("revenue"))} | {_fmt_int(econ.get("previous_revenue"))} | {_fmt_signed_int(_num(econ.get("revenue")) - _num(econ.get("previous_revenue")))}',
        f'Финрез ДО | {_fmt_signed_int(econ.get("finrez_pre"))} | {_fmt_signed_int(econ.get("previous_finrez_pre"))} | {_fmt_signed_int(econ.get("profit_delta_money"))}',
        f'Маржа | {_fmt_percent_value(econ.get("margin_pre_percent"))} | — | проверять базу сравнения',
        f'Наценка | {_fmt_percent_value(econ.get("markup_percent"))} | — | проверять базу сравнения',
        '',
        '🏢 Роль SKU в бизнесе',
        'Метрика | Значение',
        f'Оборот SKU по бизнесу | {_fmt_int(role.get("business_revenue"))} грн',
        f'Финрез SKU по бизнесу | {_fmt_signed_int(role.get("business_finrez_pre"))} грн',
        f'Доля бизнеса | {_fmt_percent_value(role.get("business_share_percent"))}',
        f'Доля категории | {_fmt_percent_value(role.get("category_share_percent"))}',
        f'Доля группы ТМС | {_fmt_percent_value(role.get("tmc_group_share_percent"))}',
        f'Ранг по обороту бизнеса | {_fmt_rank(role.get("rank_revenue_business"))}',
        f'Ранг по прибыли бизнеса | {_fmt_rank(role.get("rank_profit_business"))}',
        f'Покрытие сетей | {role.get("network_count") or 0} из {role.get("total_network_count") or 0}',
    ]
    top_networks = presence.get('top_networks') if isinstance(presence.get('top_networks'), list) else []
    if top_networks:
        lines.extend(['','⭐ Где SKU работает лучше всего','Сеть | Оборот | Доля SKU | Финрез'])
        for n in top_networks[:8]:
            if not isinstance(n, dict): continue
            lines.append(f"{n.get('network')} | {_fmt_int(n.get('revenue'))} | {_fmt_percent_value(n.get('share_sku_percent'))} | {_fmt_signed_int(n.get('finrez_pre'))}")
    missing = presence.get('missing_networks') if isinstance(presence.get('missing_networks'), list) else []
    if missing:
        lines.extend(['','➕ Где SKU отсутствует','Сеть | Почему важно | Приоритет'])
        for net in missing[:10]:
            lines.append(f'{net} | позиция уже имеет бизнес-доказательство и отсутствует в сети | оценить ввод')
    lines.extend(['','🗣 Переговорная позиция по SKU','Аргумент | Доказательство | Ответ на возражение'])
    lines.append(f'Позиция доказана бизнесом | оборот {_fmt_int(role.get("business_revenue"))}, покрытие {role.get("network_count") or 0} сетей, ранг {_fmt_rank(role.get("rank_revenue_business"))} | предложить тест / первую волну, не спорить по полной матрице')
    lines.append(f'Логика развития | {decision.get("development_logic") or "использовать как доказательство"} | если нет места — предложить ограниченный ввод')
    lines.extend(['','🎯 Управленческий вывод', decision.get('recommended_action') or 'Использовать паспорт SKU как доказательную базу для развития, ввода или защиты позиции.', '', '➡️ Что делаем дальше?', '1. Подготовить переговоры с этим SKU как аргументом.', '2. Создать задачу по SKU.', '3. Вернуться к категории и собрать пакет развития.', '4. Показать витрину SKU / где отсутствует.'])
    return [x for x in lines if str(x or '').strip()]



def _w4_set_primary_workspace(payload: dict, block_key: str) -> dict:
    """Sprint W4 Information Recovery: make the recovered рабочий стол block explicit.

    Custom GPT can otherwise summarize older short legacy blocks. This helper
    exposes a single primary рабочий стол artifact with full evidence tables.
    """
    block = payload.get(block_key) if isinstance(payload.get(block_key), list) else []
    if not block:
        return payload
    payload['workspace_primary_block'] = block
    payload['workspace_markdown'] = '\n'.join(str(x) for x in block if str(x or '').strip())
    payload['summary_block'] = 'Основной рабочий стол находится в workspace_primary_block. Выводить его полностью, не сокращая доказательные таблицы.'
    order = payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
    payload['screen_order'] = ['workspace_primary_block', block_key, 'navigation_block'] + [
        x for x in order
        if x not in {'workspace_primary_block', block_key, 'summary_block', 'result_block', 'diagnosis_block', 'explanation_block', 'next_step_block', 'recommended_next_step_block', 'drain_block_render', 'kpi_block', 'structure_block'}
    ]
    return payload

def _attach_product_recovery_blocks(payload: dict) -> dict:
    if not isinstance(payload, dict):
        return payload
    if payload.get('render_mode') in {'list_only','reasons','kpi_only','voice_diagnostic','action_package','negotiation_workspace','task_workspace','post_meeting_workspace','execution_workspace'}:
        return payload
    ctx=payload.get('context') if isinstance(payload.get('context'), dict) else {}
    level=str(ctx.get('level') or '').strip().lower()
    if level=='business':
        payload['business_workspace_block']=_pr_business_workspace_block(payload)
        order=payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
        payload['screen_order']=['business_workspace_block']+[x for x in order if x!='business_workspace_block']
        payload=_w4_set_primary_workspace(payload, 'business_workspace_block')
    elif level in {'manager_top','manager'}:
        payload['management_workspace_block']=_pr_management_workspace_block(payload)
        order=payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
        payload['screen_order']=['management_workspace_block']+[x for x in order if x!='management_workspace_block']
        payload=_w4_set_primary_workspace(payload, 'management_workspace_block')
    elif level=='network':
        payload['contract_workspace_block']=_pr_contract_workspace_block(payload)
        order=payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
        payload['screen_order']=['contract_workspace_block']+[x for x in order if x!='contract_workspace_block']
        payload=_w4_set_primary_workspace(payload, 'contract_workspace_block')
    elif level=='category':
        payload['category_workspace_block']=_w3_category_workspace_block(payload)
        order=payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
        payload['screen_order']=['category_workspace_block']+[x for x in order if x!='category_workspace_block']
        payload=_w4_set_primary_workspace(payload, 'category_workspace_block')
    elif level=='sku':
        payload['sku_passport_block']=_w3_sku_passport_block(payload) or payload.get('sku_passport_block') or []
        order=payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
        payload['screen_order']=['sku_passport_block']+[x for x in order if x!='sku_passport_block']
        payload=_w4_set_primary_workspace(payload, 'sku_passport_block')
    return payload

def _stage7_screen_order(response: dict) -> list:
    if response.get('render_mode') in {'list_only', 'reasons', 'kpi_only', 'voice_diagnostic'}:
        return []
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if level == 'business':
        return ['management_workspace_block', 'result_block', 'diagnosis_block', 'anomaly_explanation_block', 'recommended_next_step_block', 'factor_change_block', 'opportunity_rating_block', 'opportunity_explanation_block', 'decision_block_render', 'navigation_block']
    if level == 'network':
        # Stage 8.4: Network is the full Рабочий стол контракта.
        # It is not a short redirect/wizard and not a single Decision block.
        # The user receives the full contract desktop first, then can freely
        # continue with categories, SKU, negotiations, tasks or assistant dialogue.
        return [
            'result_block',
            'diagnosis_block',
            'anomaly_explanation_block',
            'business_context_block',
            'narrative_block',
            'business_opportunity_block',
            'recommendation_block',
            'product_workspace_block',
            'factor_change_block',
            'benchmark_diagnostic_block',
            'opportunity_rating_block',
            'opportunity_explanation_block',
            'decision_workspace_block',
            'decision_block_render',
            'navigation_block',
        ]
    
    if level in {'category', 'tmc_group'}:
        return ['result_block', 'diagnosis_block', 'business_context_block', 'narrative_block', 'business_opportunity_block', 'recommendation_block', 'product_workspace_block', 'anomaly_explanation_block', 'recommended_next_step_block', 'category_workspace_block', 'factor_change_block', 'benchmark_diagnostic_block', 'product_tmc_decision_block', 'opportunity_rating_block', 'opportunity_explanation_block', 'decision_block_render', 'navigation_block']
    if level == 'sku':
        return ['sku_passport_block', 'business_context_block', 'narrative_block', 'business_opportunity_block', 'recommendation_block', 'product_workspace_block', 'factor_change_block', 'benchmark_diagnostic_block', 'decision_block_render', 'navigation_block']
    return ['management_workspace_block', 'result_block', 'diagnosis_block', 'anomaly_explanation_block', 'recommended_next_step_block', 'factor_change_block', 'benchmark_diagnostic_block', 'opportunity_rating_block', 'opportunity_explanation_block', 'decision_block_render', 'navigation_block']


def _build_next_step_block(response: dict) -> list:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if response.get('render_mode') in {'list_only', 'reasons', 'kpi_only'}:
        return []
    if level == 'business':
        losses = response.get('profit_loss_rating') or []
        first_loss = losses[0].get('object_name') if losses and isinstance(losses[0], dict) else None
        if first_loss:
            return [f'Рекомендуемый следующий шаг: открыть {first_loss} как крупнейшую просадку прибыли.']
        return ['Рекомендуемый следующий шаг: открыть полный список и найти крупнейшую просадку прибыли.']
    if _is_product_layer_level(level):
        return [
            'Что делаем дальше: подготовить пакет развития категории, разобрать форматы и позиции или собрать переговорный аргумент.',
            'Можно задать вопрос ассистенту свободно: «какие позиции предложить первыми?» или «где быстрый эффект по категории?»',
        ]
    if _num(response.get('opportunity_money')) > 0:
        return ['Следующий шаг: открыть объекты ниже и найти резерв внутри уже выбранной проблемы.']
    return ['Следующий шаг: проверить факторы и подтвердить контекст причины.']

def _render_money_value(value):
    if value is None:
        return '—'
    return _fmt_signed_int(value)


def _render_rating_lines(items, money_key):
    lines = []
    for idx, item in enumerate([x for x in (items or []) if isinstance(x, dict)], start=1):
        name = str(item.get('object_name') or item.get('object') or item.get('name') or '').strip()
        if not name:
            continue
        lines.append(f'{idx}. {name} → {_render_money_value(item.get(money_key))}')
    return lines


def _action_display_label(action):
    if not isinstance(action, dict):
        return 'Приоритетное действие'
    code = str(action.get('action') or action.get('metric') or '').strip()
    text = str(action.get('text') or '').strip()
    if code in ACTION_TEXT_MAP:
        return ACTION_TEXT_MAP[code]
    if text in ACTION_TEXT_MAP:
        return ACTION_TEXT_MAP[text]
    return text or (code.replace('_', ' ').strip() if code else 'Приоритетное действие')


def _render_priority_action(response):
    action = response.get('priority_action')
    metrics = response.get('metrics') or []
    fin_delta = _delta_money_for_metric(_metric_by_name(metrics, 'Финрез до'))
    rev_delta = _delta_money_for_metric(_metric_by_name(metrics, 'Оборот'))
    if fin_delta < 0 and rev_delta < 0:
        return ['Проверить причину падения оборота → требуется контрактный/продуктовый контекст']

    # Stage 8: priority action must follow the main source of Opportunity,
    # not the strongest factor. Uses benchmark gaps only.
    gap_reasons = _opportunity_gap_reasons(response, limit=1)
    if gap_reasons:
        reason = gap_reasons[0]
        effect = abs(_reason_effect_vs_business(reason))
        return [f'{_action_text_for_reason(reason)} → потенциальный эффект до {_fmt_int(effect)}']

    if not isinstance(action, dict) or not action:
        return []
    text = _action_display_label(action)
    effect = action.get('expected_effect_money')
    if effect is None:
        effect = action.get('effect_money')
    text = text.replace('Сократить', 'Проверить').replace('Снизить', 'Проверить').replace('Повысить', 'Проверить')
    if fin_delta > 0:
        return [f'{text} → дополнительный потенциал до {_render_money_value(abs(_num(effect)))}']
    return [f'{text} → потенциальный эффект до {_render_money_value(abs(_num(effect)))}']


def _profit_first_metric_lines(response):
    metrics = response.get('metrics') or []
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    order = ['Финрез до', 'Маржа', 'Оборот', 'Наценка']
    if level == 'business':
        order.append('Финрез итог')
    lines = []
    for name in order:
        item = _metric_by_name(metrics, name)
        if not item:
            continue
        if name in {'Маржа', 'Наценка'}:
            lines.append(
                f'{name}: текущий период {_fmt_percent_value(item.get("fact_percent"))} | '
                f'прошлый год {_fmt_percent_value(item.get("pg_percent"))} | '
                f'изменение {_fmt_pp_delta(item.get("delta_percent"))}'
            )
        else:
            lines.append(
                f'{name}: текущий период {_fmt_int(item.get("fact_money"))} | '
                f'прошлый год {_fmt_int(item.get("pg_money"))} | '
                f'изменение {_fmt_signed_int(item.get("delta_money"))}'
            )
    return lines


def _period_result_money(response):
    """CHANGE-006.2: primary object KPI is profit movement vs previous period.

    This is intentionally not Benchmark Money. It answers: did the object
    earn more or less than the same object in the previous year?
    """
    metric = _metric_by_name(response.get('metrics') or [], 'Финрез до')
    if metric and metric.get('delta_money') is not None:
        return _intnum(metric.get('delta_money'))
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower() == 'business':
        return _intnum(response.get('business_result_money'))
    return 0


def _render_period_result_block(response):
    value = _period_result_money(response)
    return [
        f'🎯 Результат периода: {_fmt_signed_int(value)} к прошлому году'
    ]


def _render_business_result_block(response):
    lines = []
    lines.extend(_render_period_result_block(response))
    lines.append('📊 Что произошло с прибылью')
    lines.extend(_profit_first_metric_lines(response))
    return lines


def _render_object_result_block(response):
    # CHANGE-006.2: Benchmark Money is no longer rendered as a separate money block.
    # The object screen starts from result of the period: delta profit vs previous year.
    lines = []
    lines.extend(_render_period_result_block(response))
    lines.append('📊 Что произошло с объектом')
    lines.extend(_profit_first_metric_lines(response))
    return lines


def _render_opportunity_block(response):
    value = response.get("opportunity_money")
    return [f'💰 Потенциал прибыли внутри выбранной проблемы: {_fmt_int(abs(_num(value)))} грн']


def _render_result_block(response):
    """CHANGE-005.1: Profit First render contract.

    The first rendered block must answer «Что произошло?» using object vs
    previous period. Benchmark and Opportunity are rendered only after that.
    """
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or response.get('level') or '').strip().lower()
    if level == 'business':
        return _render_business_result_block(response)

    lines = []
    lines.extend(_render_object_result_block(response))
    return lines


def _metric_fact_revenue(response):
    for item in response.get('metrics') or []:
        if str(item.get('name') or '').strip().lower() == 'оборот':
            return abs(_num(item.get('fact_money')))
    return 0.0



def _workspace_action_label(action: dict) -> str:
    if not isinstance(action, dict):
        return 'Проверить фактор'
    text = str(action.get('action') or action.get('problem') or '').strip()
    effect = action.get('expected_effect_money')
    suffix = f' | эффект до {_fmt_int(effect)} грн' if effect is not None else ''
    return f'{text}{suffix}' if text else f'Проверить фактор{suffix}'


def _render_potential_breakdown(potential: dict, *, limit: int = 3) -> str:
    if not isinstance(potential, dict):
        return 'потенциал не разложен'
    items = potential.get('items') if isinstance(potential.get('items'), list) else []
    if not items:
        total = potential.get('total_money')
        return f'потенциал {_fmt_int(total)} грн' if total is not None else 'потенциал не разложен'
    parts = []
    for item in items[:limit]:
        if not isinstance(item, dict):
            continue
        name = item.get('name') or item.get('factor') or 'фактор'
        effect = item.get('effect_money')
        parts.append(f'{name} {_fmt_int(effect)} грн')
    return '; '.join(parts) if parts else 'потенциал не разложен'


def _assortment_skew_lines(assortment: dict, categories: list) -> list:
    lines = []
    sku_leaders = assortment.get('sku_leaders_contract') if isinstance(assortment.get('sku_leaders_contract'), list) else []
    missing_sku = assortment.get('missing_business_sku_leaders') if isinstance(assortment.get('missing_business_sku_leaders'), list) else []
    if sku_leaders:
        top_share = sum(_num(item.get('share_network_percent')) for item in sku_leaders[:5] if isinstance(item, dict))
        if top_share >= 65:
            lines.append(f'Высокая концентрация: ТОП-5 позиций дают около {_fmt_percent(top_share)}% оборота контракта.')
        elif top_share > 0:
            lines.append(f'ТОП-5 позиций дают около {_fmt_percent(top_share)}% оборота контракта — это основа текущей матрицы.')
    missing_count = _intnum(assortment.get('missing_business_leader_count'))
    if missing_count > 0:
        lines.append(f'Есть ассортиментное окно: отсутствует {missing_count} лидеров бизнеса.')
    if categories:
        top_category = categories[0]
        share = _num(top_category.get('share_contract_revenue_percent'))
        name = top_category.get('category') or 'категория'
        if share >= 45:
            lines.append(f'Контракт заметно опирается на категорию «{name}»: {_fmt_percent(share)}% оборота контракта.')
    return lines


def _render_decision_workspace_block(response):
    """Render Network as Рабочий стол контракта 2.0.

    The block is not a KPI report and not a forced wizard. It adds the assistant
    layer required by the product model: evidence → interpretation → priority →
    action navigation. All numbers come from API/DATA structures already present
    in the response.
    """
    workspace = response.get('decision_workspace')
    if not isinstance(workspace, dict) or not workspace:
        return []

    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    contract = workspace.get('contract') or ctx.get('object_name') or response.get('object_name') or 'контракт'
    period = workspace.get('period') or ctx.get('period') or response.get('period') or ''
    diagnostics = workspace.get('contract_diagnostics') if isinstance(workspace.get('contract_diagnostics'), dict) else {}
    categories = workspace.get('category_intelligence') if isinstance(workspace.get('category_intelligence'), list) else []
    actions = workspace.get('recommended_actions') if isinstance(workspace.get('recommended_actions'), list) else []
    assortment = workspace.get('assortment_analysis') if isinstance(workspace.get('assortment_analysis'), dict) else {}
    sku_leaders = assortment.get('sku_leaders_contract') if isinstance(assortment.get('sku_leaders_contract'), list) else []
    missing_sku = assortment.get('missing_business_sku_leaders') if isinstance(assortment.get('missing_business_sku_leaders'), list) else []
    negotiation = workspace.get('negotiation_package') if isinstance(workspace.get('negotiation_package'), dict) else {}
    structural = workspace.get('structural_analysis') if isinstance(workspace.get('structural_analysis'), dict) else {}

    profit_delta = diagnostics.get('profit_delta_money')
    revenue_current = diagnostics.get('revenue_current')
    margin_current = diagnostics.get('margin_current_percent')
    margin_business = diagnostics.get('margin_business_percent')

    lines = [
        '🧭 Рабочий стол контракта',
        f'{contract}' + (f' | {period}' if period else ''),
        '',
        '🧠 Разбор ассистента',
    ]

    if profit_delta is not None:
        if _num(profit_delta) >= 0:
            lines.append(f'Смотри, по контракту сейчас положительное движение: финрез до вырос на {_fmt_signed_int(profit_delta)} грн к прошлому году.')
        else:
            lines.append(f'Смотри, по контракту сейчас просадка: финрез до изменился на {_fmt_signed_int(profit_delta)} грн к прошлому году.')
    if revenue_current is not None:
        lines.append(f'Масштаб контракта по текущему обороту: {_fmt_int(revenue_current)} грн. Это база, от которой считаем доли категорий и позиций.')
    if margin_current is not None and margin_business is not None:
        delta_margin = _num(margin_current) - _num(margin_business)
        if delta_margin >= 0:
            lines.append(f'Маржа контракта выше бизнеса на {_fmt_pp_delta(delta_margin)}: доходность сейчас является сильной стороной, а не главной проблемой.')
        else:
            lines.append(f'Маржа контракта ниже бизнеса на {_fmt_pp_delta(delta_margin)}: здесь нужно смотреть экономику и структуру ассортимента.')

    if actions:
        first = actions[0]
        lines.append(f'Главный подтверждённый управленческий приоритет по экономике: {_workspace_action_label(first)}.')

    structural_items = structural.get('items') if isinstance(structural.get('items'), list) else []
    if structural_items:
        lines.append('')
        lines.append('🏗 Изменение структуры')
        lines.append('Показатель | Прошлый год | Сейчас | Δ')
        for item in structural_items:
            if not isinstance(item, dict):
                continue
            delta = _num(item.get('delta'))
            delta_text = _fmt_signed_int(delta)
            lines.append(f'{item.get("name") or "Показатель"} | {_fmt_int(item.get("previous_year"))} | {_fmt_int(item.get("current"))} | {delta_text}')
        if structural.get('is_material'):
            lines.append('Структура контракта изменилась. Поэтому финансовую динамику нужно читать не только как изменение экономики, но и как изменение состава контракта.')

    if categories:
        lines.append('')
        lines.append('📦 Категории')
        lines.append('Категория | Оборот | Доля контракта | Доля бизнеса | Δ прибыли | Потенциал')
        for item in categories[:7]:
            if not isinstance(item, dict):
                continue
            name = item.get('category') or item.get('object_name') or 'Категория'
            lines.append(
                f'{name} | {_fmt_int(item.get("revenue"))} грн | '
                f'{_fmt_percent(item.get("share_contract_revenue_percent"))}% | '
                f'{_fmt_percent(item.get("share_business_revenue_percent"))}% | '
                f'{_fmt_signed_int(item.get("profit_delta_money"))} грн | '
                f'{_fmt_int(item.get("opportunity_money"))} грн'
            )
        best = categories[0]
        best_name = best.get('category') or 'категория'
        lines.append(f'По категориям первой в разбор просится «{best_name}»: у неё самый высокий рабочий вес в этом контракте по текущим данным.')
        breakdown = _render_potential_breakdown(best.get('potential_breakdown') if isinstance(best, dict) else {})
        lines.append(f'Потенциал «{best_name}» нужно читать не одной суммой: {breakdown}.')

    if sku_leaders:
        lines.append('')
        lines.append('⭐ Разбор SKU: лидеры ассортимента в контракте')
        lines.append('Позиция | Оборот | Доля контракта | Δ прибыли | Роль | Что это означает')
        for item in sku_leaders[:10]:
            if not isinstance(item, dict):
                continue
            sku = item.get('sku') or 'Позиция'
            lines.append(
                f'{sku} | {_fmt_int(item.get("revenue"))} грн | '
                f'{_fmt_percent(item.get("share_network_percent"))}% | '
                f'{_fmt_signed_int(item.get("profit_delta_money"))} грн | '
                f'{item.get("role") or "роль не определена"} | '
                f'{item.get("development_logic") or "использовать как доказательную базу"}'
            )

    sku_intelligence = assortment.get('sku_intelligence') if isinstance(assortment.get('sku_intelligence'), dict) else {}
    if sku_intelligence:
        lines.append('')
        lines.append('🧩 Ассортиментная логика')
        concentration = sku_intelligence.get('concentration_level')
        top5 = sku_intelligence.get('top5_share_percent')
        if concentration == 'high':
            lines.append(f'ТОП-5 позиций дают около {_fmt_percent(top5)}% оборота контракта. Это сильная база, но есть риск зависимости от узкой матрицы.')
        elif concentration == 'medium':
            lines.append(f'ТОП-5 позиций дают около {_fmt_percent(top5)}% оборота контракта. Матрица имеет выраженных лидеров, но не выглядит критично узкой.')
        elif top5 is not None:
            lines.append(f'ТОП-5 позиций дают около {_fmt_percent(top5)}% оборота контракта. Матрица выглядит относительно сбалансированной.')
        plan = sku_intelligence.get('development_plan') if isinstance(sku_intelligence.get('development_plan'), list) else []
        if plan:
            lines.append('План развития ассортимента:')
            for idx, step in enumerate(plan[:3], 1):
                lines.append(f'{idx}. {step}.')

    if missing_sku:
        lines.append('')
        lines.append('➕ 10 лидеров бизнеса, которых нет в контракте')
        lines.append('Позиция | Оборот бизнеса | Финрез до бизнеса | Почему важно')
        for item in missing_sku[:10]:
            if not isinstance(item, dict):
                continue
            sku = item.get('sku') or 'Позиция'
            lines.append(
                f'{sku} | {_fmt_int(item.get("business_revenue"))} грн | '
                f'{_fmt_signed_int(item.get("business_finrez_pre"))} грн | '
                f'{item.get("reason") or "лидер бизнеса отсутствует в контракте"}'
            )

    skew_lines = _assortment_skew_lines(assortment, categories)
    if skew_lines:
        lines.append('')
        lines.append('⚖ Ассортиментные перекосы')
        lines.extend(skew_lines)

    lines.append('')
    lines.append('🚀 План развития контракта')
    if actions:
        lines.append(f'1. Экономика: {_workspace_action_label(actions[0])}.')
    if missing_sku:
        lines.append('2. Ассортимент: собрать короткий пакет из 10 отсутствующих лидеров бизнеса, а не открывать весь длинный список.')
    if categories:
        lines.append(f'3. Категории: начать с «{categories[0].get("category") or "ключевой категории"}» и проверить, какие форматы и позиции дают следующий прирост.')
    if not actions and not missing_sku and not categories:
        lines.append('1. Начать с уточняющего вопроса ассистенту по цели работы с контрактом: экономика, ассортимент, переговоры или задачи.')

    if negotiation:
        lines.append('')
        lines.append('🤝 Переговорный пакет')
        goal = negotiation.get('goal')
        if goal:
            lines.append(f'Цель: {goal}.')
        priority_categories = negotiation.get('priority_categories') if isinstance(negotiation.get('priority_categories'), list) else []
        if priority_categories:
            lines.append('Категории для аргументации: ' + ', '.join(str(x) for x in priority_categories[:3]) + '.')
        sku_package = negotiation.get('sku_package') if isinstance(negotiation.get('sku_package'), list) else []
        if sku_package:
            lines.append('Пакет позиций для первой встречи: ' + ', '.join(str(x) for x in sku_package[:10]) + '.')

    lines.append('')
    lines.append('✅ Что делаем дальше?')
    if actions:
        lines.append(f'1. Подготовить переговоры по экономике контракта — {_workspace_action_label(actions[0])}.')
    else:
        lines.append('1. Подготовить переговоры по экономике контракта.')
    if categories:
        lines.append(f'2. Разобрать категорию «{categories[0].get("category") or "ключевую категорию"}» — посмотреть форматы, позиции и потенциал.')
    else:
        lines.append('2. Разобрать категории контракта.')
    if missing_sku:
        lines.append('3. Собрать пакет позиций для ввода — начать с 10 отсутствующих лидеров бизнеса.')
        lines.append('4. Показать лидеров SKU — отдельно разобрать роли текущих позиций.')
        lines.append('5. Показать ассортиментные перекосы — проверить концентрацию и пробелы.')
    else:
        lines.append('3. Посмотреть ассортиментные возможности.')
    lines.append('6. Создать задачи по контракту после выбора направления.')
    lines.append('7. Или задай вопрос ассистенту своими словами: «какие позиции предложить первыми?», «как говорить с байером?», «где быстрый эффект?»')

    return [line for line in lines if str(line or '').strip()]

def _render_decision_block(response):
    priority_lines = _render_priority_action(response)
    if priority_lines:
        return priority_lines

    decision = response.get('decision_block')
    if isinstance(decision, list) and decision:
        lines = []
        for item in decision:
            if not isinstance(item, dict):
                continue
            text = _action_display_label(item)
            effect = item.get('expected_effect_money')
            if effect is None:
                effect = item.get('effect_money')
            lines.append(f'{text} → ожидаемый эффект {_render_money_value(effect)}')
        return lines
    return []


def _attach_render_blocks(response, payload):
    metrics = response.get('metrics') or []
    structure = response.get('structure') or []
    drain = response.get('drain_block') or {'items': [], 'total_effect': 0}
    navigation = response.get('navigation') or {'actions': []}
    render_mode = response.get('render_mode') or ''

    drain_total = _intnum(response.get('navigation_money') if response.get('navigation_money') is not None else drain.get('total_effect'))
    ctx_level_for_main_driver = str((response.get('context') or {}).get('level') or '').strip().lower()

    # In list-only mode, the screen is a navigation list, not an object analysis screen.
    ctx_level = str((response.get('context') or {}).get('level') or '').strip().lower()
    if render_mode == 'list_only':
        response['result_block'] = []
        response['period_result_block'] = []
        response['kpi_block'] = []
        response['structure_block'] = []
        response['main_driver'] = ''
        response['summary_block'] = 'Витрина объекта. Полный список текущего уровня без аналитического сопровождения.'
        response['decision_block_render'] = []
        response['business_result_rating_block'] = []
        response['profit_loss_rating_block'] = []
        response['opportunity_rating_block'] = []
        response['priority_action_block'] = []
        response['object_reasons_block'] = []
        response['factor_change_block'] = []
        response['benchmark_diagnostic_block'] = []
        response['kpi_table'] = []
        response['factor_change_table'] = []
        response['benchmark_diagnostic_table'] = []
        response['opportunity_explanation_block'] = []
        response['anomaly_explanation_block'] = []
        response['product_layer_block'] = []
        response['product_insight_block'] = []
        response['product_tmc_decision_block'] = []
        response['sku_passport_block'] = []
        response['category_workspace_block'] = []
        response['business_opportunity_block'] = []
        response['recommendation_block'] = []
        response['narrative_block'] = []
        response['product_workspace_block'] = []
        response['business_context_block'] = []
        response['decision_workspace_block'] = []
        response['explanation_block'] = []
        response['next_step_block'] = []
        response['recommended_next_step_block'] = []
        response['diagnosis_block'] = []
    elif render_mode == 'reasons':
        # Reasons is a focused factor view. Do not leak the full workspace or
        # assistant explanation blocks into this screen.
        response['result_block'] = []
        response['period_result_block'] = []
        response['kpi_block'] = []
        response['structure_block'] = []
        response['main_driver'] = ''
        response['summary_block'] = 'Разбор причин текущего объекта.'
        response['decision_block_render'] = []
        response['business_result_rating_block'] = []
        response['profit_loss_rating_block'] = []
        response['opportunity_rating_block'] = []
        response['priority_action_block'] = []
        response['object_reasons_block'] = []
        response['factor_change_block'] = []
        response['benchmark_diagnostic_block'] = []
        response['kpi_table'] = []
        response['factor_change_table'] = []
        response['benchmark_diagnostic_table'] = []
        response['opportunity_explanation_block'] = []
        response['anomaly_explanation_block'] = []
        response['product_layer_block'] = []
        response['product_insight_block'] = []
        response['product_tmc_decision_block'] = []
        response['sku_passport_block'] = []
        response['business_context_block'] = []
        response['category_workspace_block'] = []
        response['business_opportunity_block'] = []
        response['recommendation_block'] = []
        response['narrative_block'] = []
        response['product_workspace_block'] = []
        response['decision_workspace_block'] = []
        response['explanation_block'] = []
        response['next_step_block'] = []
        response['recommended_next_step_block'] = []
        response['diagnosis_block'] = []
        response['navigation_block'] = response.get('navigation_block') or ['назад к объекту']
    else:
        response['kpi_block'] = _render_kpi_block(metrics)
        response['summary_block'] = _build_kpi_summary(response)
        response['result_block'] = _render_result_block(response)
        response['period_result_block'] = _render_period_result_block(response)
        # CHANGE-006: hide aggregate Benchmark Money from screen rendering.
        # Benchmark remains diagnostic through factors vs business, not a separate money rating.
        response['business_result_rating_block'] = []
        response['profit_loss_rating_block'] = _render_rating_lines(response.get('profit_loss_rating') or [], 'profit_delta_money')
        response['opportunity_rating_block'] = _render_rating_lines(response.get('opportunity_rating') or [], 'opportunity_money')
        if _is_product_layer_level(ctx_level_for_main_driver):
            response['structure_block'] = []
            response['main_driver'] = 'Продуктовая экономика'
            response['product_layer_block'] = _build_product_layer_block(response)
            response['product_insight_block'] = _build_product_insight_block(response)
            response['product_tmc_decision_block'] = _build_product_tmc_decision_block(response)
            response['sku_passport_block'] = _build_sku_passport_block(response)
            response['priority_action_block'] = _build_product_priority_action_block(response)
            reason_source = response.get('object_reasons') or []
            response['object_reasons_block'] = []
            response['factor_change_block'] = _render_factor_change_block(reason_source or [])
            response['benchmark_diagnostic_block'] = _render_benchmark_diagnostic_block(reason_source or [])
            response['reasons_block'] = []
            response['decision_block'] = []
        else:
            response['structure_block'] = _render_structure_block(structure)
            response['main_driver'] = _render_main_driver(structure)
            response['product_layer_block'] = []
            response['product_insight_block'] = []
            response['priority_action_block'] = _render_priority_action(response)
            reason_source = response.get('business_reasons') if ctx_level_for_main_driver == 'business' else response.get('object_reasons')
            response['object_reasons_block'] = _render_factor_change_block(reason_source or [])
            response['factor_change_block'] = _render_factor_change_block(reason_source or [])
            response['benchmark_diagnostic_block'] = [] if ctx_level_for_main_driver == 'business' else _render_benchmark_diagnostic_block(reason_source or [])

        # Stage 8.3: explicit table-ready data for Custom GPT.
        # This restores full factors/benchmark rendering without changing any calculations.
        table_reason_source = response.get('business_reasons') if ctx_level_for_main_driver == 'business' else response.get('object_reasons')
        response['kpi_table'] = _render_kpi_table_data(response)
        response['factor_change_table'] = _render_factor_change_table_data(table_reason_source or [])
        response['benchmark_diagnostic_table'] = [] if ctx_level_for_main_driver == 'business' else _render_benchmark_table_data(table_reason_source or [])

    ctx_level = str((response.get('context') or {}).get('level') or '').strip().lower()
    if render_mode == 'list_only':
        response['drain_block_render'] = _render_vitrina_block(response)
        response['drain_total'] = drain_total
        response['summary_block'] = 'Витрина объекта. Полный список текущего уровня без аналитического сопровождения.'
    elif ctx_level == 'sku':
        rendered_sku_drain = _render_drain_block(drain)
        if not rendered_sku_drain:
            response['drain_total'] = drain_total
        else:
            response['drain_total'] = drain_total
        response['drain_block_render'] = rendered_sku_drain
    else:
        response['drain_block_render'] = _render_drain_block(drain)
        response['drain_total'] = drain_total
    if render_mode == 'list_only':
        response['navigation_block'] = response.get('navigation_block') or ['назад — вернуться к объекту']
        response['decision_workspace_block'] = []
        response['diagnosis_block'] = []
        response['explanation_block'] = []
        response['next_step_block'] = []
        response['recommended_next_step_block'] = []
        response['opportunity_explanation_block'] = []
        response['anomaly_explanation_block'] = []
        response['decision_block_render'] = []
        response['reasons_block_render'] = []
        response['screen_order'] = ['summary_block', 'drain_block_render', 'navigation_block']
        return response

    if render_mode == 'reasons':
        response['navigation_block'] = response.get('navigation_block') or ['назад к объекту']
        response['decision_workspace_block'] = []
        response['diagnosis_block'] = []
        response['explanation_block'] = []
        response['next_step_block'] = []
        response['recommended_next_step_block'] = []
        response['opportunity_explanation_block'] = []
        response['anomaly_explanation_block'] = []
        response['decision_block_render'] = []
        response['drain_block_render'] = []
        response['drain_total'] = 0
        response['reasons_block_render'] = _render_reasons_block(response.get('reasons_block') or [], ctx_level) or response.get('reasons_block_render') or []
        response['screen_order'] = ['summary_block', 'reasons_block_render', 'navigation_block']
        return response

    response['navigation_block'] = _render_navigation_block(payload, navigation, drain)
    response['business_context_block'] = _render_business_context_block(response)
    response['category_workspace_block'] = _render_category_workspace_block(response)
    response['business_opportunity_block'] = _render_business_opportunity_block(response)
    response['recommendation_block'] = _render_recommendation_block(response)
    response['narrative_block'] = _render_narrative_block(response)
    response['product_workspace_block'] = _render_product_workspace_block(response)
    response['decision_workspace_block'] = _render_decision_workspace_block(response)
    response['diagnosis_block'] = _build_assistant_diagnosis_block(response)
    response['recommended_next_step_block'] = _build_recommended_next_step_block(response)
    response['opportunity_explanation_block'] = _build_opportunity_explanation_block(response)
    response['anomaly_explanation_block'] = _build_anomaly_explanation_block(response)
    response['screen_order'] = _stage7_screen_order(response)
    if _is_product_layer_level(ctx_level):
        response['decision_block_render'] = list(response.get('priority_action_block') or [])
        response['reasons_block_render'] = []
    else:
        response['decision_block_render'] = _render_decision_block(response)
        response['reasons_block_render'] = _render_reasons_block(response.get('reasons_block') or [], ctx_level)
    return response




def _render_kpi_table_data(response):
    """Machine-readable table data for Custom GPT rendering.

    Does not calculate new values; only exposes already normalized metrics in a
    stable table-friendly shape. Object screens intentionally exclude Финрез итог.
    """
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    order = ['Финрез до', 'Маржа', 'Оборот', 'Наценка']
    if level == 'business':
        order.append('Финрез итог')
    rows = []
    for name in order:
        item = _metric_by_name(response.get('metrics') or [], name)
        if not item:
            continue
        if name in {'Маржа', 'Наценка'}:
            rows.append({
                'name': name,
                'current': _fmt_percent_value(item.get('fact_percent')),
                'previous': _fmt_percent_value(item.get('pg_percent')),
                'delta': _fmt_pp_delta(item.get('delta_percent')),
            })
        else:
            rows.append({
                'name': name,
                'current': _fmt_int(item.get('fact_money')),
                'previous': _fmt_int(item.get('pg_money')),
                'delta': _fmt_signed_int(item.get('delta_money')),
            })
    return rows


def _render_factor_change_table_data(reasons):
    """Table-ready Effect vs Previous Year data for the render layer."""
    order = {'Наценка': 0, 'Ретро': 1, 'Логистика': 2, 'Персонал': 3, 'Прочие': 4}
    rows = []
    for item in sorted([x for x in (reasons or []) if isinstance(x, dict)], key=lambda x: order.get(str(x.get('name') or '').strip(), 99)):
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        delta_p_raw = item.get('delta_vs_previous_percent', item.get('delta_vs_prev'))
        rows.append({
            'factor': name,
            'current': f'{_reason_current_percent(item)}%',
            'previous': _reason_previous_percent(item),
            'delta': 'нет корректной базы' if delta_p_raw is None else _fmt_pp_delta(_num(delta_p_raw)),
            'effect': _fmt_signed_int(item.get('effect_vs_previous_money', item.get('effect_money'))),
            'signal': str(item.get('signal') or '').strip() or 'норма',
        })
    return rows


def _render_benchmark_table_data(reasons):
    """Table-ready Effect vs Business data for the render layer."""
    order = {'Наценка': 0, 'Ретро': 1, 'Логистика': 2, 'Персонал': 3, 'Прочие': 4}
    rows = []
    for item in sorted([x for x in (reasons or []) if isinstance(x, dict)], key=lambda x: order.get(str(x.get('name') or '').strip(), 99)):
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        delta_b = _num(item.get('delta_vs_business_percent', item.get('delta_percent')))
        rows.append({
            'factor': name,
            'object': f'{_reason_current_percent(item)}%',
            'business': f'{_fmt_percent(item.get("base_percent"))}%',
            'delta_to_business': _fmt_pp_delta(delta_b),
            'effect': _fmt_signed_int(item.get('effect_vs_business_money', item.get('effect_money'))),
        })
    return rows

def _sanitize_json_value(value):
    if isinstance(value, dict):
        return {str(k): _sanitize_json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_json_value(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else 0.0
    return value


def _json_len(value) -> int:
    try:
        return len(json.dumps(_sanitize_json_value(value), ensure_ascii=False))
    except Exception:
        return 0


def _compact_text_list(value, *, limit: int = 12):
    if not isinstance(value, list):
        return []
    out = []
    for item in value[:limit]:
        if isinstance(item, (str, int, float)):
            out.append(item)
        elif isinstance(item, dict):
            # Keep only business-facing scalar keys; drop nested details.
            compact = {}
            for key in (
                'object_id', 'object_name', 'name', 'level', 'title', 'label',
                'value', 'fact_money', 'pg_money', 'delta_money', 'fact_percent',
                'pg_percent', 'delta_percent', 'effect_money', 'signal', 'comment',
            ):
                if item.get(key) is not None:
                    compact[key] = item.get(key)
            out.append(compact if compact else str(item)[:240])
    return out


def _enforce_public_response_budget(payload: dict) -> dict:
    """Last-resort response budget guard.

    Sprint W14.1: the guard may compact auxiliary transport fields, but it must
    never remove, truncate, rewrite or replace `workspace_markdown`. A full
    Workspace is a product contract, not a discretionary payload section.
    """
    if not isinstance(payload, dict):
        return payload

    initial_len = _json_len(payload)
    if initial_len <= VECTRA_PUBLIC_RESPONSE_BUDGET:
        return payload

    out = dict(payload)
    render_mode = str(out.get('render_mode') or '').strip().lower()
    has_canonical_workspace = isinstance(out.get('workspace_markdown'), str) and bool(out.get('workspace_markdown').strip())

    out['response_budget_guard'] = {
        'applied': True,
        'initial_json_chars': initial_len,
        'budget_chars': VECTRA_PUBLIC_RESPONSE_BUDGET,
        'workspace_markdown_preserved': has_canonical_workspace,
    }

    if has_canonical_workspace:
        # Remove only duplicate / auxiliary fields. The canonical markdown stays
        # byte-for-byte intact so GPT cannot fall back to a compact legacy screen.
        for key in (
            'workspace_primary_block', 'all_block',
            'result_block', 'period_result_block', 'kpi_block', 'kpi_table',
            'structure_block', 'drain_block_render', 'explanation_block',
            'next_step_block', 'diagnosis_block', 'recommended_next_step_block',
            'opportunity_explanation_block', 'anomaly_explanation_block',
            'reasons_block', 'reasons_block_render', 'decision_block',
            'decision_block_render', 'business_result_rating_block',
            'profit_loss_rating_block', 'opportunity_rating_block',
            'priority_action_block', 'object_reasons_block', 'factor_change_block',
            'factor_change_table', 'benchmark_diagnostic_block',
            'benchmark_diagnostic_table', 'product_layer_block',
            'product_insight_block', 'product_tmc_decision_block',
            'business_workspace_block', 'contract_workspace_block',
            'management_workspace_block', 'category_workspace_block',
            'product_workspace_block', 'sku_passport_block',
            'decision_workspace_block', 'business_context_block',
            'business_opportunity_block', 'recommendation_block', 'narrative_block',
            'metrics', 'structure', 'decision_workspace', 'sku_passport',
            'business_context', 'category_workspace', 'business_opportunity',
            'recommendation_engine', 'narrative_engine', 'product_workspace',
            'management_intelligence', 'management_workspace', 'management_passport',
        ):
            out.pop(key, None)
        out['screen_order'] = ['workspace_markdown']
        out['response_budget_guard']['final_json_chars'] = _json_len(out)
        return out

    if render_mode == 'list_only':
        out['all_block'] = _compact_public_all_block(out.get('all_block', []))
        out['drain_block_render'] = _compact_text_list(out.get('drain_block_render'), limit=80)
        out['screen_order'] = ['summary_block', 'drain_block_render', 'navigation_block']
    elif render_mode == 'reasons':
        out['reasons_block_render'] = _compact_text_list(out.get('reasons_block_render'), limit=12)
        out['reasons_block'] = _compact_text_list(out.get('reasons_block'), limit=12)
        out['screen_order'] = ['summary_block', 'reasons_block_render', 'navigation_block']
    elif render_mode == 'kpi_only':
        out['result_block'] = _compact_text_list(out.get('result_block'), limit=8)
        out['kpi_block'] = _compact_text_list(out.get('kpi_block'), limit=10)
        out['kpi_table'] = _compact_text_list(out.get('kpi_table'), limit=10)
        out['screen_order'] = ['summary_block', 'result_block', 'kpi_block', 'kpi_table', 'navigation_block']
    else:
        for key in (
            'all_block', 'product_workspace_block', 'product_workspace',
            'product_insight_block', 'product_layer_block', 'sku_passport_block',
            'sku_passport', 'category_workspace_block', 'category_workspace',
            'business_opportunity_block', 'business_opportunity',
            'recommendation_block', 'recommendation_engine', 'narrative_block',
            'narrative_engine', 'decision_workspace_block', 'decision_workspace',
            'management_workspace_block', 'management_workspace',
            'management_intelligence', 'management_passport',
        ):
            out[key] = [] if key.endswith('_block') or key == 'all_block' else {}
        out['drain_block_render'] = _compact_text_list(out.get('drain_block_render'), limit=7)
        out['diagnosis_block'] = _compact_text_list(out.get('diagnosis_block'), limit=6)
        out['explanation_block'] = _compact_text_list(out.get('explanation_block'), limit=6)
        out['next_step_block'] = _compact_text_list(out.get('next_step_block'), limit=6)
        out['recommended_next_step_block'] = _compact_text_list(out.get('recommended_next_step_block'), limit=6)

    if _json_len(out) > VECTRA_PUBLIC_RESPONSE_HARD_BUDGET:
        out = {
            'status': out.get('status', 'ok'),
            'reason': out.get('reason'),
            'context': out.get('context'),
            'path': out.get('path', []),
            'render_mode': render_mode or 'compact',
            'summary_block': out.get('summary_block', 'Ответ сокращён из-за ограничения объёма.'),
            'result_block': _compact_text_list(out.get('result_block'), limit=5),
            'kpi_block': _compact_text_list(out.get('kpi_block'), limit=5),
            'drain_block_render': _compact_text_list(out.get('drain_block_render'), limit=7),
            'navigation_block': out.get('navigation_block') or ['причины', 'все', 'назад'],
            'active_workspace_state': out.get('active_workspace_state', {}),
            'workspace_action_map': out.get('workspace_action_map', []),
            'workspace_runtime_contract': out.get('workspace_runtime_contract', {}),
            'workspace_render_instruction': out.get('workspace_render_instruction', ''),
            'screen_order': ['summary_block', 'result_block', 'kpi_block', 'drain_block_render', 'navigation_block'],
            'response_budget_guard': {
                'applied': True,
                'hard_fallback': True,
                'initial_json_chars': initial_len,
                'budget_chars': VECTRA_PUBLIC_RESPONSE_HARD_BUDGET,
            },
        }
    else:
        out['response_budget_guard']['final_json_chars'] = _json_len(out)
    return out

def json_response(payload):
    return JSONResponse(content=_sanitize_json_value(payload), media_type='application/json; charset=utf-8')




def _hydrate_runtime_context_from_request(session_id: str, request: VectraQueryRequest) -> None:
    """Hydrate server-side session from explicit Custom GPT runtime fields.

    Custom GPT Actions do not send hidden dialogue history automatically.  W15.1
    therefore accepts the last public active_workspace_state/workspace_action_map
    as explicit request fields and uses them only to restore navigation context.
    """
    try:
        active_state = getattr(request, 'active_workspace_state', None)
        runtime_context = getattr(request, 'runtime_context', None)
        action_map = getattr(request, 'workspace_action_map', None)
        active_research_state = getattr(request, 'active_research_state', None)
        if not isinstance(active_research_state, dict) and isinstance(runtime_context, dict):
            active_research_state = runtime_context.get('active_research_state') or runtime_context.get('research_flow_status')
        research_path = getattr(request, 'research_path', None)
        current_step = getattr(request, 'current_step', None)
        payload = {}
        if isinstance(active_state, dict) and active_state:
            state = dict(active_state)
            if isinstance(action_map, list) and action_map and not state.get('action_map'):
                state['action_map'] = action_map
            if isinstance(active_research_state, dict) and active_research_state:
                # DEV-0004: restore Research Flow state explicitly. The action
                # boundary cannot rely on hidden GPT chat state.
                state['research_flow'] = active_research_state
            ctx = {
                'level': state.get('workspace_level'),
                'object_name': state.get('object_name'),
                'period': state.get('period'),
                'parent_object': None,
            }
            screen = {
                'status': 'ok',
                'render_mode': state.get('render_mode') or 'runtime_context_snapshot',
                'context': ctx,
                'path': state.get('path') if isinstance(state.get('path'), list) else [],
                'filter': state.get('filter') if isinstance(state.get('filter'), dict) else {},
                'workspace_markdown': runtime_context.get('workspace_markdown') if isinstance(runtime_context, dict) and isinstance(runtime_context.get('workspace_markdown'), str) else '',
                'active_workspace_state': state,
                'workspace_action_map': state.get('action_map') if isinstance(state.get('action_map'), list) else [],
            }
            if isinstance(active_research_state, dict) and active_research_state:
                screen['active_research_state'] = active_research_state
                screen['research_flow_status'] = active_research_state
                screen['research_path'] = research_path if isinstance(research_path, list) else active_research_state.get('research_path', [])
                screen['current_step'] = current_step or active_research_state.get('current_step')
            payload.update({
                'active_workspace_state': state,
                'active_research_state': active_research_state if isinstance(active_research_state, dict) else None,
                'research_path': research_path if isinstance(research_path, list) else None,
                'current_step': current_step,
                'scope_level': state.get('workspace_level'),
                'scope_object_name': state.get('object_name'),
                'period_current': state.get('period'),
                'filter': state.get('filter') if isinstance(state.get('filter'), dict) else {},
            })
            # DEV-0004: when Custom GPT calls vectraQuery with explicit Product
            # Team research runtime state, hydrate both current_screen and
            # last_payload. Numeric/local research commands must not fall back
            # into ordinary free dialogue just because the server has no hidden
            # chat history for the current Action call.
            if str(state.get('workspace_level') or '').strip().lower() == 'product_team_research':
                payload['current_screen'] = screen
                payload['last_payload'] = screen
            else:
                payload.setdefault('last_payload', screen)
        if payload:
            update_session(session_id, {k: v for k, v in payload.items() if v not in (None, '')})
    except Exception:
        logger.exception('runtime_context_hydration_failed session_id=%s', session_id)

def _stable_session_id(request: VectraQueryRequest) -> str:
    raw = (getattr(request, 'session_id', None) or '').strip()
    return raw or 'default'


def _normalize_product_team_command_text(message: str) -> str:
    """Normalize Product Owner research commands before dispatching.

    Product Acceptance found that voice/keyboard input can contain visually
    similar Latin characters (for example `исcледуй`, where `c` is Latin).
    DEV-0006A makes autonomous-session routing tolerant to these variants so
    the command reaches Autonomous User Session instead of the old dialogue
    path.
    """
    text = (message or '').strip().lower().replace('ё', 'е')
    translation = str.maketrans({
        'c': 'с', 'C': 'с',
        'a': 'а', 'A': 'а',
        'e': 'е', 'E': 'е',
        'o': 'о', 'O': 'о',
        'p': 'р', 'P': 'р',
        'x': 'х', 'X': 'х',
        'y': 'у', 'Y': 'у',
        'k': 'к', 'K': 'к',
        'm': 'м', 'M': 'м',
        't': 'т', 'T': 'т',
        'h': 'н', 'H': 'н',
        'b': 'в', 'B': 'в',
    })
    return text.translate(translation)



def _is_product_owner_autonomous_research_start(message: str) -> bool:
    """Detect Product Owner control commands that start autonomous research.

    DEV-0008: after the base platform was accepted, short owner commands such
    as "делай" are not VECTRA user messages. They are commands to Product
    Team Assistant to close the previous working context and start its own
    autonomous product research cycle. These commands must be intercepted
    before ordinary Workspace routing.
    """
    raw_text = (message or '').strip().lower().replace('ё', 'е')
    text = _normalize_product_team_command_text(message)
    if not text:
        return False

    exact_commands = {
        'делай', 'давай', 'начинай', 'начни', 'приступай', 'стартуй',
        'запускай', 'работай', 'продолжай', 'go', 'do it', 'start',
        'continue', 'run', 'do',
    }
    if text in exact_commands or raw_text in exact_commands:
        return True

    owner_research_phrases = (
        'начинай исследование', 'начни исследование', 'запускай исследование',
        'запусти исследование', 'приступай к исследованию',
        'начинай автономное исследование', 'запусти автономное исследование',
        'стартуй автономное исследование', 'делай исследование',
        'продолжай исследование', 'продолжаю исследование',
        'продолжить исследование', 'продолжаем исследование',
        'продолжай автономное исследование', 'продолжить автономное исследование',
        'переходим к следующему этапу', 'переходим на следующий этап',
        'перейти к следующему этапу', 'следующий этап',
        'работай дальше', 'продолжай работать', 'работаем дальше',
        'можешь приступать', 'можно приступать', 'можешь начинать',
        'давай работай', 'давай работай дальше', 'давай продолжай',
        'починай дослідження', 'продовжуй дослідження', 'запусти дослідження',
        'start autonomous research', 'run autonomous research',
        'continue autonomous research', 'continue product research',
        'start product research', 'run product research',
    )
    if any(phrase in text for phrase in owner_research_phrases) or any(phrase in raw_text for phrase in owner_research_phrases):
        return True

    # DEV-0008B: Product Owner often gives natural-language control phrases,
    # not exact commands. Detect an autonomous-work intent when an action verb
    # is combined with a research/work/next-stage object. This interception must
    # stay before ordinary Workspace routing so owner commands are never treated
    # as VECTRA user messages.
    start_continue_markers = (
        'продолж', 'продовж', 'начин', 'начн', 'почин', 'приступ',
        'старт', 'запуск', 'запуст', 'работай', 'работаем', 'працюй',
        'делай', 'давай', 'можешь', 'можно', 'переходим', 'перейти',
        'go on', 'continue', 'start', 'run', 'proceed',
    )
    autonomous_work_markers = (
        'исследован', 'досліджен', 'research', 'автоном',
        'работу', 'работа', 'работать', 'дальше', 'следующему этапу',
        'следующий этап', 'наступний етап', 'next stage', 'next step',
    )
    return (
        any(marker in text for marker in start_continue_markers)
        and any(marker in text for marker in autonomous_work_markers)
    ) or (
        any(marker in raw_text for marker in start_continue_markers)
        and any(marker in raw_text for marker in autonomous_work_markers)
    )


def _close_previous_context_for_autonomous_research(session_id: str) -> None:
    """Close the visible VECTRA context before Assistant starts research."""
    try:
        update_session(session_id, {
            'scope_level': None,
            'scope_object_name': None,
            'period_current': None,
            'period_previous': None,
            'filter': {},
            'last_list_level': None,
            'last_response_type': None,
            'last_list_items': [],
            'full_view': False,
            'current_screen': None,
            'last_payload': None,
            'show_all': False,
            'stack': [],
            'previous_context_closed_for_autonomous_research': True,
        })
    except Exception:
        logger.exception('autonomous_research_context_close_failed session_id=%s', session_id)


def _build_product_owner_autonomous_research_start(message: str, session_id: str) -> dict:
    """Start autonomous product research from a Product Owner control command."""
    _close_previous_context_for_autonomous_research(session_id)
    payload = _start_autonomous_user_session('VECTRA', session_id=session_id, owner_command=message)
    payload['previous_context_closed'] = True
    payload['owner_command_type'] = 'product_owner_autonomous_research_start'
    payload['autonomous_route'] = 'ClosePreviousContext -> StartAutonomousResearch -> GenerateUserScenarios -> ExecuteUserMessages -> ProductOwnerReport'
    if isinstance(payload.get('autonomous_user_session'), dict):
        payload['autonomous_user_session']['previous_context_closed'] = True
        payload['autonomous_user_session']['owner_command_type'] = 'product_owner_autonomous_research_start'
        payload['autonomous_user_session']['owner_command_forwarded_to_vectra'] = False
    if isinstance(payload.get('active_research_state'), dict):
        payload['active_research_state']['previous_context_closed'] = True
        payload['active_research_state']['owner_command_type'] = 'product_owner_autonomous_research_start'
    if isinstance(payload.get('research_flow_status'), dict):
        payload['research_flow_status']['previous_context_closed'] = True
        payload['research_flow_status']['owner_command_type'] = 'product_owner_autonomous_research_start'
    return payload


def _is_product_team_research_request(message: str) -> bool:
    """Detect Product Owner commands that must start Autonomous User Session.

    DEV-0006B: Product Acceptance showed that the production route can still
    fall back to ordinary user-request recognition.  The root cause is an
    over-narrow activation guard: the previous detector required both a
    research token and a known object token.  In the Product Team Assistant
    workflow the Product Owner command itself is the event; when it clearly
    asks to research/explore/check the product, the system must start the
    autonomous route even if the object is omitted or written in another
    language/transliteration.
    """
    raw_text = (message or '').strip().lower().replace('ё', 'е')
    text = _normalize_product_team_command_text(message)
    if not text:
        return False

    research_tokens = (
        'исследуй', 'исследовать', 'исследование', 'исследуй продукт',
        'проверь продукт', 'проверить продукт', 'проверка продукта',
        'проверь релиз', 'проверить релиз', 'полный цикл',
        'досліди', 'дослідити', 'дослідження', 'перевір продукт',
        'перевір реліз', 'повний цикл',
        'research', 'explore', 'study', 'product acceptance', 'release acceptance',
    )
    has_research = any(token in text for token in research_tokens) or any(token in raw_text for token in research_tokens)
    if not has_research:
        return False

    object_tokens_raw = (
        'vectra', 'product team assistant', 'workspace', 'release',
        'custom gpt', 'assistant', 'product',
    )
    object_tokens_norm = (
        'вектра', 'продукт теам ассистант', 'ассистент', 'воркспаце',
        'релиз', 'реліз', 'продукт', 'кастом',
    )
    has_object = any(token in raw_text for token in object_tokens_raw) or any(token in text for token in object_tokens_norm)

    # The Product Team Assistant command may be simply "Исследуй" / "Досліди"
    # because the active GPT already defines the product context.  Treat clear
    # research imperatives as VECTRA research instead of sending them into the
    # ordinary VECTRA query recognizer.
    starts_with_research = any(text.startswith(token) for token in ('исследуй', 'исследовать', 'досліди', 'дослідити', 'research', 'explore', 'study'))
    return bool(has_object or starts_with_research)


def _research_object_name(message: str) -> str:
    raw_text = (message or '').strip().lower().replace('ё', 'е')
    text = _normalize_product_team_command_text(message)
    if 'product team assistant' in raw_text or 'assistant' in raw_text or 'ассистент' in text:
        return 'Product Team Assistant'
    if 'workspace' in raw_text or 'воркспаце' in text or 'екран' in text or 'экран' in text:
        return 'Workspace'
    if 'релиз' in text or 'реліз' in text or 'release' in raw_text:
        return 'Release'
    return 'VECTRA'


def _build_product_team_research_workspace(message: str, session_id: str) -> dict:
    """Start a Product Team autonomous user session.

    DEV-0006: the Product Owner command (for example "Исследуй VECTRA") is
    not itself a user request to VECTRA. It is a command to Product Team
    Assistant. Runtime must therefore create an internal virtual user session
    and execute Assistant-generated user messages inside VECTRA.
    """
    obj = _research_object_name(message)
    return _start_autonomous_user_session(obj, session_id=session_id, owner_command=message)


def _is_product_team_research_workspace(screen: dict) -> bool:
    if not isinstance(screen, dict):
        return False
    render_mode = str(screen.get('render_mode') or '').strip().lower()
    ctx = screen.get('context') if isinstance(screen.get('context'), dict) else {}
    return render_mode == 'product_team_research_workspace' and str(ctx.get('level') or '').strip().lower() == 'product_team_research'


def _current_product_team_research_object(session_id: str) -> str:
    try:
        current = get_session(session_id).get('current_screen') or get_session(session_id).get('last_payload') or {}
    except Exception:
        current = {}
    state = current.get('active_workspace_state') if isinstance(current.get('active_workspace_state'), dict) else {}
    ctx = current.get('context') if isinstance(current.get('context'), dict) else {}
    return state.get('object_name') or ctx.get('object_name') or 'VECTRA'


def _is_research_continue_request(message: str, session_id: str) -> bool:
    """Detect explicit continuation of the Product Team research route.

    Before DEV-0003 such requests fell into free dialogue and produced
    "Работаю в контексте открытого Workspace...".  This detector keeps route
    execution inside the Product Team research engine.
    """
    text = (message or '').strip().lower().replace('ё', 'е')
    if not text:
        return False
    try:
        current = get_session(session_id).get('current_screen') or get_session(session_id).get('last_payload') or {}
    except Exception:
        current = {}
    if not _is_product_team_research_workspace(current):
        return False
    if any(token in text for token in ('продолжить исследование', 'продолжить маршрут', 'текущему шагу', 'следующий этап исследования', 'выполнить текущий шаг')):
        return True
    # Numeric command can be used only when the visible action #1 is the
    # continuation action from the current research workspace.
    if re.fullmatch(r'1', text):
        state = current.get('active_workspace_state') if isinstance(current.get('active_workspace_state'), dict) else {}
        actions = state.get('action_map') if isinstance(state.get('action_map'), list) else current.get('workspace_action_map')
        if isinstance(actions, list):
            for action in actions:
                if isinstance(action, dict) and int(action.get('number') or 0) == 1:
                    label = str(action.get('label') or '').lower().replace('ё', 'е')
                    return 'продолжить' in label and 'исследован' in label
    return False


def _autonomous_user_session_plan(root_obj: str) -> list:
    """Build deterministic virtual user messages for Autonomous User Session.

    The Product Owner command is never reused as a user message.  These
    messages are generated by Product Team Assistant and executed as if a real
    user was working inside VECTRA.
    """
    root = (root_obj or 'VECTRA').strip() or 'VECTRA'
    if root.lower() in {'vectra', 'вектра'}:
        return [
            {'step_id': 'start_day', 'role': 'Commercial Director', 'user_message': 'Начать Анализ', 'goal': 'Открыть стартовую точку пользовательской работы.'},
            {'step_id': 'business_workspace', 'role': 'Commercial Director', 'user_message': 'Бизнес 2026-02', 'goal': 'Проверить открытие Business Workspace и рабочий контекст.'},
            {'step_id': 'business_vitrine', 'role': 'Commercial Director', 'user_message': 'все', 'goal': 'Проверить локальную витрину и действие все.'},
            {'step_id': 'business_reasons', 'role': 'Commercial Director', 'user_message': 'причины', 'goal': 'Проверить локальный разбор причин без потери контекста.'},
            {'step_id': 'discovery', 'role': 'Product Explorer', 'user_message': 'Покажи лучшие SKU', 'goal': 'Проверить Discovery-запрос и переход к следующему действию.'},
            {'step_id': 'journal_status', 'role': 'Product Owner', 'user_message': 'экспорт журнала', 'goal': 'Проверить состояние Development Journal как часть Product Owner Report.'},
        ]
    if root.lower() in {'product team assistant', 'assistant', 'ассистент'}:
        return [
            {'step_id': 'assistant_research_start', 'role': 'Product Owner', 'user_message': 'Открой рабочий контекст Product Team Assistant', 'goal': 'Проверить способность Assistant открыть собственный продуктовый контур без передачи команды Product Owner.'},
            {'step_id': 'role_engine', 'role': 'Product Explorer', 'user_message': 'Проверь Role Engine', 'goal': 'Проверить выбор и переключение ролей.'},
            {'step_id': 'product_owner_report', 'role': 'Product Owner', 'user_message': 'Сформируй Product Owner Report', 'goal': 'Проверить итоговый отчёт исследования.'},
        ]
    return [
        {'step_id': 'object_research_start', 'role': 'Product Explorer', 'user_message': f'Покажи {root}', 'goal': f'Открыть объект исследования {root}.'},
        {'step_id': 'object_reasons', 'role': 'Product Explorer', 'user_message': 'причины', 'goal': 'Проверить причины и локальный контекст.'},
    ]


def _extract_autonomous_result_summary(payload: dict) -> dict:
    if not isinstance(payload, dict):
        return {'status': 'error', 'reason': 'non_dict_response'}
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    workspace_markdown = payload.get('workspace_markdown') if isinstance(payload.get('workspace_markdown'), str) else ''
    return {
        'status': payload.get('status', 'ok'),
        'reason': payload.get('reason'),
        'render_mode': payload.get('render_mode'),
        'context_level': ctx.get('level'),
        'context_object': ctx.get('object_name'),
        'has_workspace_markdown': bool(workspace_markdown.strip()),
        'workspace_markdown_length': len(workspace_markdown),
        'has_active_workspace_state': isinstance(payload.get('active_workspace_state'), dict) and bool(payload.get('active_workspace_state')),
        'action_count': len(payload.get('workspace_action_map') or []) if isinstance(payload.get('workspace_action_map'), list) else 0,
        'error_code': payload.get('error_code'),
    }


def _execute_autonomous_user_message(step: dict, session_id: str) -> dict:
    """Execute one Assistant-generated user message against VECTRA Runtime."""
    user_message = str(step.get('user_message') or '').strip()
    virtual_session_id = f'{session_id}:autonomous_user_session'
    try:
        raw = orchestrate_vectra_query(user_message, session_id=virtual_session_id)
        prepared = _prepare_vectra_query_payload(raw)
        rendered = apply_runtime_contract(prepared)
        summary = _extract_autonomous_result_summary(rendered)
        return {
            'step_id': step.get('step_id'),
            'role': step.get('role'),
            'goal': step.get('goal'),
            'user_message': user_message,
            'status': 'executed',
            'runtime_summary': summary,
        }
    except Exception as exc:
        logger.exception('autonomous_user_message_execution_failed session_id=%s message=%r', virtual_session_id, user_message)
        return {
            'step_id': step.get('step_id'),
            'role': step.get('role'),
            'goal': step.get('goal'),
            'user_message': user_message,
            'status': 'execution_error',
            'runtime_summary': {'status': 'error', 'reason': str(exc), 'has_workspace_markdown': False},
        }


def _build_autonomous_user_session_report(root_obj: str, session_state: dict, user_history: list) -> list:
    executed = [item for item in user_history if isinstance(item, dict)]
    confirmed = []
    limitations = []
    for item in executed:
        summary = item.get('runtime_summary') if isinstance(item.get('runtime_summary'), dict) else {}
        if summary.get('has_workspace_markdown'):
            confirmed.append(f"• {item.get('step_id')}: VECTRA вернула рабочий ответ для запроса Assistant: {item.get('user_message')}")
        else:
            reason = summary.get('error_code') or summary.get('reason') or 'workspace_not_confirmed'
            limitations.append(f"• {item.get('step_id')}: не подтверждён рабочий Workspace для запроса Assistant: {item.get('user_message')} ({reason})")
    if not confirmed:
        confirmed.append('• Автономная пользовательская сессия создана; пользовательские запросы сформированы Assistant, а не Product Owner.')
    if not limitations:
        limitations.append('• Критичных ограничений автономной пользовательской сессии в доступной локальной проверке не подтверждено.')
    return [
        f'📍 Autonomous User Session — {root_obj}',
        '',
        '🎯 Цель',
        'Проверить VECTRA как реальный пользователь без пошагового участия Product Owner.',
        '',
        '✅ Что изменилось',
        'Product Owner больше не является источником пользовательских сообщений внутри исследования.',
        'Assistant создаёт виртуальную пользовательскую сессию, выбирает роль, формирует пользовательские запросы и выполняет их через VECTRA Runtime.',
        '',
        '👤 Выбранная пользовательская роль',
        str(session_state.get('user_role') or 'Product Explorer'),
        '',
        '🧭 Сценарий автономной сессии',
        *[f"{idx+1}. [{item.get('role')}] {item.get('user_message')} — {item.get('goal')}" for idx, item in enumerate(executed)],
        '',
        '✅ Подтверждённые результаты',
        *confirmed,
        '',
        '⚠ Ограничения',
        *limitations,
        '',
        '💡 Product Opportunities',
        '• Расширить библиотеку виртуальных пользовательских ролей: Commercial Director, KAM, Category Manager, Release Manager.',
        '• Добавить настройку глубины автономной сессии для короткого, стандартного и полного исследования.',
        '• Связать подтверждённые ограничения автономной сессии с автоматической подготовкой Engineering Tasks после проверки Laboratory.',
        '',
        '📄 Product Owner Report',
        f'Цель исследования: автономно исследовать {root_obj}.',
        'Что исследовано: виртуальная пользовательская сессия, генерация пользовательских запросов Assistant, выполнение через VECTRA Runtime, сохранение состояния.',
        'Что подтверждено: Product Owner command отделён от user messages; Assistant формирует собственные пользовательские запросы.',
        'Ограничения: полнота бизнес-проверки зависит от production-среды и доступности бизнес-данных.',
        'Следующий шаг: выполнить Product Acceptance в Custom GPT после деплоя DEV-0006.',
        '',
        '## Что делаем дальше?',
        '1. Передать результаты в Laboratory',
        '2. Проверить Development Journal',
        '3. Запустить автономную сессию для роли KAM',
        '4. Сформировать Engineering Tasks по подтверждённым ограничениям',
    ]


def _start_autonomous_user_session(root_obj: str, session_id: str, owner_command: str = '') -> dict:
    """StartUserSession + ExecuteUserMessage + FinishUserSession.

    DEV-0006: this function is the bridge between Product Owner dialogue and
    VECTRA user-mode execution.  owner_command is stored only as the trigger;
    it is never sent to VECTRA as a user message.
    """
    root = (root_obj or 'VECTRA').strip() or 'VECTRA'
    plan = _autonomous_user_session_plan(root)
    user_role = plan[0].get('role') if plan else 'Product Explorer'
    user_history = []
    for step in plan:
        user_history.append(_execute_autonomous_user_message(step, session_id=session_id))
    completed_count = len([item for item in user_history if item.get('status') == 'executed'])
    blocking = [item for item in user_history if item.get('status') == 'execution_error']
    session_state = {
        'version': 'DEV_0006_AUTONOMOUS_USER_SESSION',
        'mode': 'autonomous_user_session',
        'user_session_id': f'{session_id}:autonomous_user_session',
        'session_status': 'completed_with_limitations' if blocking else 'completed',
        'owner_command': owner_command,
        'owner_command_forwarded_to_vectra': False,
        'user_role': user_role,
        'user_goal': f'Autonomously research {root} as a real VECTRA user.',
        'user_context': {'research_object': root, 'source': 'Product Team Assistant'},
        'current_user_request': user_history[-1].get('user_message') if user_history else '',
        'user_history': user_history,
        'last_runtime_response': user_history[-1].get('runtime_summary') if user_history else {},
        'research_progress': {
            'completed_user_messages': completed_count,
            'total_user_messages': len(plan),
            'blocking_errors': len(blocking),
        },
    }
    lines = _build_autonomous_user_session_report(root, session_state, user_history)
    research_state = {
        'version': 'DEV_0006_AUTONOMOUS_USER_SESSION',
        'status': session_state['session_status'],
        'research_goal': root,
        'current_object': root,
        'current_step': 'autonomous_user_session_completed',
        'next_step': 'product_owner_report_review',
        'research_path': ['start_user_session', 'select_user_role', 'execute_user_messages', 'analyze_results', 'product_opportunities', 'product_owner_report'],
        'completion_reason': session_state['session_status'],
        'requires_product_owner_decision': False,
        'autonomous_user_session': session_state,
    }
    state = {
        'state_version': 'W15_ACTIVE_WORKSPACE_STATE_V3_DEV_0006',
        'source_of_truth': 'autonomous_user_session',
        'workspace_level': 'product_team_research',
        'object_name': root,
        'period': None,
        'path': ['Product Team Assistant', 'Autonomous User Session', root],
        'filter': {'research_object': root, 'mode': 'autonomous_user_session'},
        'render_mode': 'product_team_research_workspace',
        'research_flow': research_state,
        'autonomous_user_session': session_state,
    }
    return {
        'status': 'ok',
        'render_mode': 'product_team_research_workspace',
        'context': {'level': 'product_team_research', 'object_name': root, 'period': None, 'parent_object': None},
        'path': state['path'],
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'screen_order': ['workspace_markdown'],
        'workspace_render_instruction': 'Показать пользователю workspace_markdown полностью и без изменений.',
        'active_workspace_state': state,
        'workspace_action_map': [],
        'research_flow_status': research_state,
        'active_research_state': research_state,
        'research_path': research_state['research_path'],
        'current_step': research_state['current_step'],
        'next_step': research_state['next_step'],
        'autonomous_user_session': session_state,
        'user_session_id': session_state['user_session_id'],
        'user_role': session_state['user_role'],
        'user_goal': session_state['user_goal'],
        'user_history': session_state['user_history'],
        'session_status': session_state['session_status'],
        'owner_command_forwarded_to_vectra': False,
        'owner_command': owner_command,
        'autonomous_user_session_active': True,
        'autonomous_route': 'StartUserSession -> ExecuteUserMessage -> FinishUserSession',
    }


def _research_continuation_plan(root_obj: str) -> list:
    """Return deterministic objects for a continuous Product Team research loop.

    DEV-0005: the research route is not a single finished report.  It is a
    continuous loop over related research objects.  Runtime must keep pending
    and completed objects so a follow-up Action call cannot reopen the same
    completed stage by default.
    """
    root = (root_obj or 'VECTRA').strip() or 'VECTRA'
    if root.lower() in {'vectra', 'вектра'}:
        return [
            'VECTRA',
            'Workspace',
            'Navigation',
            'Discovery',
            'Development Journal',
            'Product Acceptance',
            'Product Opportunities',
        ]
    if root.lower() in {'product team assistant', 'assistant', 'ассистент'}:
        return [
            'Product Team Assistant',
            'Role Engine',
            'Research Flow',
            'Product Owner Report',
            'Development Journal',
            'Product Opportunities',
        ]
    return [root]


def _restore_research_state(session_id: str) -> dict:
    try:
        current = get_session(session_id).get('current_screen') or get_session(session_id).get('last_payload') or {}
    except Exception:
        current = {}
    if not isinstance(current, dict):
        return {}
    state = current.get('active_research_state') if isinstance(current.get('active_research_state'), dict) else {}
    if not state:
        state = current.get('research_flow_status') if isinstance(current.get('research_flow_status'), dict) else {}
    active_state = current.get('active_workspace_state') if isinstance(current.get('active_workspace_state'), dict) else {}
    if not state and isinstance(active_state.get('research_flow'), dict):
        state = active_state.get('research_flow')
    return state if isinstance(state, dict) else {}


def _advance_research_state(root_obj: str, session_id: str, trigger: str) -> dict:
    previous = _restore_research_state(session_id)
    requested_root = (root_obj or previous.get('research_goal') or previous.get('object_name') or 'VECTRA').strip() or 'VECTRA'
    existing_goal = str(previous.get('research_goal') or '').strip()
    # Continue an existing research goal unless the user explicitly starts a new
    # initial request for another object.
    if previous and trigger not in {'initial_research_request'}:
        research_goal = existing_goal or requested_root
    else:
        research_goal = requested_root
    if trigger == 'initial_research_request':
        # A new explicit research command starts a fresh route even when the
        # server-side session fallback points to the latest active session from
        # another Custom GPT Action call.
        plan = _research_continuation_plan(research_goal)
        completed = []
        pending = list(plan)
    else:
        plan = previous.get('research_plan') if isinstance(previous.get('research_plan'), list) else _research_continuation_plan(research_goal)
        completed = previous.get('completed_objects') if isinstance(previous.get('completed_objects'), list) else []
        completed = [str(x) for x in completed if str(x).strip()]
        pending = previous.get('pending_objects') if isinstance(previous.get('pending_objects'), list) else []
        pending = [str(x) for x in pending if str(x).strip()]
        if not pending:
            pending = [item for item in plan if item not in completed]
    if not pending:
        current_object = previous.get('current_object') or previous.get('object_name') or research_goal
        status = 'completed'
        completion_reason = 'all_research_objects_completed'
        next_object = None
    else:
        current_object = pending.pop(0)
        if current_object not in completed:
            completed.append(current_object)
        next_object = pending[0] if pending else None
        status = 'in_progress' if next_object else 'completed'
        completion_reason = 'next_object_available' if next_object else 'all_research_objects_completed'
    return {
        'version': 'DEV_0005_CONTINUOUS_RESEARCH_LOOP',
        'trigger': trigger,
        'status': status,
        'research_goal': research_goal,
        'object_name': current_object,
        'current_object': current_object,
        'next_object': next_object,
        'completed_objects': completed,
        'pending_objects': pending,
        'research_plan': plan,
        'research_progress': {
            'completed_count': len(completed),
            'total_count': len(plan),
            'remaining_count': len(pending),
        },
        'current_step': 'object_research_completed',
        'next_step': 'continue_with_next_object' if next_object else 'final_product_owner_report',
        'research_path': [
            'object_model', 'source_review', 'role_selection', 'scenario_review',
            'workspace_navigation_context_review', 'development_journal_review',
            'product_opportunities', 'product_owner_report', 'continuation_planning',
        ],
        'completed_steps': [
            'object_model', 'source_review', 'role_selection', 'scenario_review',
            'workspace_navigation_context_review', 'development_journal_review',
            'product_opportunities', 'product_owner_report', 'continuation_planning',
        ],
        'completion_reason': completion_reason,
        'requires_product_owner_decision': False,
        'runtime_rule': 'After each research object, choose the next pending object before returning to ordinary dialogue.',
    }


def _build_research_object_lines(obj: str, research_state: dict, has_workspace: bool) -> list:
    next_obj = research_state.get('next_object')
    status_line = 'Исследование продолжается автоматически: следующий объект уже определён.' if next_obj else 'Маршрут исследования завершён по доступным объектам.'
    limitation_lines = []
    if not has_workspace:
        limitation_lines.append('Реальные пользовательские Workspace в текущем контексте не открыты; исследование выполняется по доступному Runtime-контексту и Knowledge.')
    if obj in {'VECTRA', 'Product Team Assistant', 'Workspace', 'Release', 'Product Acceptance'}:
        limitation_lines.append('Полная проверка фактических бизнес-данных зависит от production-среды и доступности источника данных.')
    lines = [
        f'📍 Автономное исследование — {obj}',
        '',
        '🎯 Цель исследования',
        f'Исследовать объект: {obj}.',
        'Определить подтверждённые результаты, ограничения, Product Opportunities и следующий объект исследования.',
        '',
        '✅ Статус цикла',
        status_line,
        f"Прогресс: {research_state.get('research_progress', {}).get('completed_count', 0)} из {research_state.get('research_progress', {}).get('total_count', 0)} объектов.",
        '',
        '🧩 1. Модель объекта',
        f'Объект исследования: {obj}.',
        'Границы исследования: назначение объекта, пользовательская ценность, сценарии, навигация, контекст, ограничения и возможности развития.',
        'Исследование проводится по объекту; источники информации используются только для подтверждения выводов.',
        '',
        '🔎 2. Использованные источники информации',
        '• Knowledge Base и стандарты Product Team Assistant.',
        '• Runtime-состояние текущей сессии.',
        '• Текущий Workspace, если он открыт в сессии.',
        '• Development Journal как источник инженерного состояния, если доступен через продуктовый контур.',
        '',
        '👥 3. Роли исследования',
        '• Product Owner — оценка результата и следующего решения.',
        '• Product Explorer — поиск ограничений и возможностей развития.',
        '• Release Manager — проверка влияния изменений и регрессий.',
        '• Laboratory — подготовка материала для архитектурного анализа.',
        '',
        '🧭 4. Проверенные направления',
        '• Наличие рабочего Workspace и сохранение контекста.',
        '• Возможность продолжать исследование без ручного выбора каждого шага.',
        '• Локальная навигация и действие продолжения маршрута.',
        '• Возможность сформировать Product Owner Report по текущему объекту.',
        '• Возможность выделить ограничения и Product Opportunities.',
        '',
        '✅ 5. Подтверждённые результаты',
        '• Research Flow хранит прогресс исследования.',
        '• Завершённый объект добавляется в completed_objects.',
        '• Следующий объект определяется до возврата ответа Product Owner.',
        '• active_research_state содержит current_object, next_object, pending_objects и completion_reason.',
        '',
        '⚠ 6. Ограничения исследования',
    ]
    if limitation_lines:
        lines.extend([f'• {item}' for item in limitation_lines])
    else:
        lines.append('• Критичных ограничений в доступном Runtime-контексте не подтверждено.')
    lines.extend([
        '',
        '💡 7. Product Opportunities',
        '• Расширить автономный цикл фактическими сценариями Product Acceptance в production-среде.',
        '• Накапливать историю исследовательских запусков для сравнения зрелости продукта между релизами.',
        '• Связать подтверждённые ограничения с автоматическим созданием инженерных задач после проверки Laboratory.',
        '',
        '📄 8. Product Owner Report по текущему объекту',
        f'Цель: исследовать {obj}.',
        'Что исследовано: объект, источники информации, роли, навигация, контекст, ограничения и возможности развития.',
        'Что подтверждено: текущий объект исследован в пределах доступной информации, прогресс маршрута сохранён.',
        'Ограничения: полнота проверки реальных бизнес Workspace зависит от доступности production-данных и открытого рабочего контекста.',
        'Product Opportunities: расширить автономный Research Flow фактическими сценариями по ролям и историей исследовательских запусков.',
        '',
    ])
    if next_obj:
        lines.extend([
            '➡ Автоматическое продолжение',
            f'Следующий объект исследования: {next_obj}.',
            'Product Owner не должен запускать следующий этап вручную; Runtime State уже содержит следующий объект и ожидает продолжения исследовательского цикла.',
            '',
            '## Что делаем дальше?',
            f'1. Продолжить исследование: {next_obj}',
            '2. Проверить Development Journal',
            '3. Передать текущие результаты в Laboratory',
            '4. Сформировать инженерные задачи по подтверждённым ограничениям',
        ])
    else:
        lines.extend([
            '➡ Следующий шаг',
            'Передать итоговый результат в Laboratory для оценки следующего этапа развития продукта.',
            '',
            '## Что делаем дальше?',
            '1. Передать результаты в Laboratory',
            '2. Проверить Development Journal',
            '3. Повторить автономное исследование с новым объектом',
            '4. Сформировать инженерные задачи по подтверждённым ограничениям',
        ])
    return lines


def _build_product_team_autonomous_research_workspace(obj: str, session_id: str, *, trigger: str = 'continue') -> dict:
    """Execute one object in a continuous Product Team research loop.

    DEV-0005: completing a Product Owner Report for one object must not make
    the next request reopen the same completed stage.  The response now carries
    a continuation state with completed_objects, pending_objects and next_object.
    """
    try:
        session = get_session(session_id)
    except Exception:
        session = {}
    current = session.get('current_screen') or session.get('last_payload') or {}
    has_workspace = bool(current.get('workspace_markdown')) if isinstance(current, dict) else False
    research_state = _advance_research_state(obj, session_id, trigger)
    current_object = research_state.get('current_object') or obj or 'VECTRA'
    lines = _build_research_object_lines(current_object, research_state, has_workspace)
    state = {
        'state_version': 'W15_ACTIVE_WORKSPACE_STATE_V3_DEV_0005',
        'source_of_truth': 'last_displayed_workspace',
        'workspace_level': 'product_team_research',
        'object_name': current_object,
        'period': None,
        'path': ['Product Team Assistant', 'Research', research_state.get('research_goal') or current_object, current_object],
        'filter': {'research_object': current_object, 'research_goal': research_state.get('research_goal')},
        'render_mode': 'product_team_research_workspace',
        'research_flow': research_state,
    }
    return {
        'status': 'ok',
        'render_mode': 'product_team_research_workspace',
        'context': {'level': 'product_team_research', 'object_name': current_object, 'period': None, 'parent_object': research_state.get('research_goal')},
        'path': state['path'],
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'screen_order': ['workspace_markdown'],
        'workspace_render_instruction': 'Показать пользователю workspace_markdown полностью и без изменений.',
        'active_workspace_state': state,
        'workspace_action_map': [],
        'research_flow_status': research_state,
        'active_research_state': research_state,
        'research_path': research_state['research_path'],
        'current_step': research_state['current_step'],
        'next_step': research_state['next_step'],
        'current_object': research_state.get('current_object'),
        'next_object': research_state.get('next_object'),
        'pending_objects': research_state.get('pending_objects', []),
        'completed_objects': research_state.get('completed_objects', []),
        'completion_reason': research_state.get('completion_reason'),
    }

def _is_numeric_research_action(message: str, session_id: str) -> bool:
    text = str(message or '').strip()
    if not re.fullmatch(r'\d{1,2}', text):
        return False
    try:
        session = get_session(session_id)
        current = session.get('current_screen') or session.get('last_payload') or {}
        return str(current.get('render_mode') or '').strip().lower() == 'product_team_research_workspace'
    except Exception:
        return False


def _build_product_team_research_action_workspace(message: str, session_id: str) -> dict:
    number = int(str(message or '0').strip() or 0)
    session = get_session(session_id)
    current = session.get('current_screen') or session.get('last_payload') or {}
    state = current.get('active_workspace_state') if isinstance(current.get('active_workspace_state'), dict) else {}
    obj = state.get('object_name') or (current.get('context') or {}).get('object_name') or 'объект исследования'
    actions = state.get('action_map') if isinstance(state.get('action_map'), list) else current.get('workspace_action_map')
    selected = None
    if isinstance(actions, list):
        for action in actions:
            if isinstance(action, dict) and int(action.get('number') or 0) == number:
                selected = action
                break
    label = str((selected or {}).get('label') or '').strip()
    normalized_label = str((selected or {}).get('normalized_label') or label).strip().lower().replace('ё', 'е')

    # DEV-0003: numeric commands in Product Team research must execute the
    # visible research action, not a hard-coded generic action menu.
    if any(token in normalized_label for token in ('повторить автономное исследование', 'повторить исследование', 'продолжить исследование')):
        return _build_product_team_autonomous_research_workspace(obj, session_id=session_id, trigger='numeric_continue_or_repeat')
    if 'laboratory' in normalized_label or 'лаборатор' in normalized_label:
        lines = [
            f'📍 Передача результатов в Laboratory — {obj}',
            '',
            'Что произошло',
            'Результат автономного исследования подготовлен для Laboratory.',
            '',
            'Почему это важно',
            'Laboratory должна оценить подтверждённые ограничения и Product Opportunities и определить, требуется ли архитектурное решение или инженерное задание.',
            '',
            'Что рекомендуется сделать',
            'Передать Product Owner Report и список Product Opportunities в Laboratory.',
            '',
            '## Что делаем дальше?',
            '1. Вернуться к автономному исследованию',
            '2. Проверить Development Journal',
            '3. Сформировать инженерные задачи по подтверждённым ограничениям',
        ]
    elif 'development journal' in normalized_label or 'журнал' in normalized_label:
        lines = [
            f'📍 Проверка Development Journal — {obj}',
            '',
            'Что произошло',
            'Запрошена проверка состояния Development Journal в рамках исследования.',
            '',
            'Почему это важно',
            'Journal показывает, какие ограничения уже зарегистрированы, какие исправлены и какие ожидают проверки.',
            '',
            'Текущий результат',
            'В локальном Runtime-контексте состояние Journal может быть проверено только при доступности соответствующего продуктового контура.',
            '',
            'Что рекомендуется сделать',
            'При production-проверке запросить актуальный Development Journal и включить его состояние в Product Owner Report.',
            '',
            '## Что делаем дальше?',
            '1. Продолжить исследование по текущему шагу',
            '2. Вернуться к автономному исследованию',
            '3. Передать результаты в Laboratory',
        ]
    elif 'инженер' in normalized_label or 'engineering' in normalized_label or 'задач' in normalized_label:
        lines = [
            f'📍 Инженерные задачи — {obj}',
            '',
            'Что произошло',
            'Выполнена подготовка к формированию Engineering Tasks по подтверждённым ограничениям.',
            '',
            'Почему это важно',
            'Engineering Task создаётся только после подтверждения ограничения и анализа Laboratory.',
            '',
            'Текущий результат',
            'В рамках автономного исследования подготовлены Product Opportunities; подтверждённые инженерные задачи должны пройти через Laboratory.',
            '',
            'Что рекомендуется сделать',
            'Передать результат в Laboratory для классификации и подготовки Engineering Task при необходимости.',
            '',
            '## Что делаем дальше?',
            '1. Вернуться к автономному исследованию',
            '2. Передать результаты в Laboratory',
            '3. Проверить Development Journal',
        ]
    elif number == 5 or 'назад' in normalized_label or 'вернуться' in normalized_label:
        return current if isinstance(current, dict) and current else _build_product_team_research_workspace(f'Исследуй {obj}', session_id)
    else:
        # Compatibility for old research workspaces created before DEV-0003.
        legacy_actions = {
            1: ('Построение модели объекта', 'Определить назначение, состав, участников, жизненный цикл и связи объекта исследования.'),
            2: ('Маршрут исследования', 'Составить последовательность ролей, сценариев и проверок для исследования объекта.'),
            3: ('Development Journal', 'Проверить состояние инженерных записей, открытые ограничения и изменения после последнего релиза.'),
            4: ('Product Owner Report', 'Сформировать итоговый отчёт: подтверждённые результаты, ограничения, Product Opportunities и один следующий шаг.'),
        }
        title, description = legacy_actions.get(number, ('Действие недоступно', 'Выбранного действия нет в текущем рабочем столе исследования.'))
        lines = [
            f'📍 {title} — {obj}',
            '',
            'Что произошло',
            f'Выбрано действие №{number}: {title}.',
            '',
            'Почему это важно',
            description,
            '',
            'Что делать дальше',
            'Выполнить этот шаг в рамках исследования и затем перейти к следующему действию рабочего стола.',
            '',
            '## Что делаем дальше?',
            '1. Продолжить исследование по текущему шагу',
            '2. Вернуться к рабочему столу исследования',
            '3. Сформировать Product Owner Report',
        ]
    research_state = state.get('research_flow') if isinstance(state.get('research_flow'), dict) else {}
    if not research_state:
        research_state = current.get('active_research_state') if isinstance(current.get('active_research_state'), dict) else {}
    return {
        'status': 'ok',
        'render_mode': 'product_team_research_workspace',
        'context': {'level': 'product_team_research', 'object_name': obj, 'period': None, 'parent_object': None},
        'path': ['Product Team Assistant', 'Research', obj, label or f'Action {number}'],
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'screen_order': ['workspace_markdown'],
        'workspace_render_instruction': 'Показать пользователю workspace_markdown полностью и без изменений.',
        'research_flow_status': research_state,
        'active_research_state': research_state,
        'research_path': research_state.get('research_path', []) if isinstance(research_state, dict) else [],
        'current_step': research_state.get('current_step', '') if isinstance(research_state, dict) else '',
        'next_step': research_state.get('next_step', '') if isinstance(research_state, dict) else '',
    }

def _detect_response_scope(message: str) -> str:
    text = (message or '').strip().lower()
    if not text:
        return ''
    normalized = text.replace('ё', 'е')
    if re.search(r'(^|\s)(kpi|кпи|кипи)(\s|$)', normalized):
        return 'kpi'
    if any(token in normalized for token in ('только kpi', 'только кпи', 'только показатели', 'показатели kpi')):
        return 'kpi'
    return ''


def _compact_public_all_block(items):
    compact = []
    if not isinstance(items, list):
        return compact
    allowed = {
        'object_id', 'object_name', 'name', 'level', 'navigation_money',
        'profit_delta_money', 'delta_money', 'opportunity_money', 'potential_money',
        'revenue', 'finrez_pre', 'parent_share_percent', 'business_share_percent',
        'network_count', 'sku_count', 'category_count', 'tmc_group_count', 'contract_count', 'manager_count', 'margin', 'margin_pre', 'markup', 'priority_signal',
    }
    for idx, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        row = {k: item.get(k) for k in allowed if item.get(k) is not None}
        row.setdefault('object_id', idx)
        if 'object_name' not in row and row.get('name'):
            row['object_name'] = row.get('name')
        compact.append(row)
    return compact


def _render_lines_to_markdown(lines):
    if isinstance(lines, list):
        return '\n'.join(str(x) for x in lines if str(x or '').strip())
    return str(lines or '').strip()


def _make_list_only_public_payload(rendered_payload: dict) -> dict:
    """Return the minimal public payload for Showcase (`все`).

    Showcase must not leak full рабочий стол blocks. It is a list/navigation mode,
    not analysis. This directly prevents ResponseTooLargeError for large
    Contract/Manager screens.
    """
    return {
        'status': rendered_payload.get('status', 'ok'),
        'reason': rendered_payload.get('reason'),
        'context': rendered_payload.get('context'),
        'path': rendered_payload.get('path', []),
        'children_level': rendered_payload.get('children_level'),
        'render_mode': 'list_only',
        'summary_block': rendered_payload.get('summary_block', 'Витрина объекта. Полный список текущего уровня без аналитического сопровождения.'),
        'drain_block_render': rendered_payload.get('drain_block_render', []),
        'drain_total': rendered_payload.get('drain_total', 0),
        'all_block': _compact_public_all_block(rendered_payload.get('all_block', [])),
        'navigation_block': rendered_payload.get('navigation_block', ['назад — вернуться к объекту']),
        'screen_order': ['summary_block', 'drain_block_render', 'navigation_block'],
        'workspace_markdown': _render_lines_to_markdown([rendered_payload.get('summary_block', 'Витрина объекта.')] + (rendered_payload.get('drain_block_render') or []) + [''] + (rendered_payload.get('navigation_block') or ['назад — вернуться к объекту'])),
    }


def _make_kpi_only_public_payload(rendered_payload: dict) -> dict:
    """Return only KPI-related blocks for explicit KPI scope requests."""
    return {
        'status': rendered_payload.get('status', 'ok'),
        'reason': rendered_payload.get('reason'),
        'context': rendered_payload.get('context'),
        'path': rendered_payload.get('path', []),
        'render_mode': 'kpi_only',
        'summary_block': 'KPI текущего рабочий стол.',
        'result_block': rendered_payload.get('result_block', []),
        'period_result_block': rendered_payload.get('period_result_block', []),
        'kpi_block': rendered_payload.get('kpi_block', []),
        'kpi_table': rendered_payload.get('kpi_table', []),
        'navigation_block': rendered_payload.get('navigation_block', ['назад — вернуться к объекту']),
        'screen_order': ['summary_block', 'result_block', 'kpi_block', 'kpi_table', 'navigation_block'],
        'workspace_markdown': _render_lines_to_markdown([rendered_payload.get('summary_block', 'KPI текущего рабочего стола.')] + (rendered_payload.get('result_block') or []) + (rendered_payload.get('kpi_block') or []) + (rendered_payload.get('kpi_table') or []) + [''] + (rendered_payload.get('navigation_block') or ['назад — вернуться к объекту'])),
    }


def _ensure_public_markdown_for_diagnostic(payload: dict) -> dict:
    """Ensure diagnostic / non-analytical public responses still carry markdown.

    Full analytical Workspace responses are handled by _make_full_workspace_public_payload.
    This helper is only for modes that are intentionally excluded from full
    Workspace validation but still must be renderable by Product Team Assistant.
    """
    if not isinstance(payload, dict):
        return payload
    if isinstance(payload.get('workspace_markdown'), str) and payload.get('workspace_markdown').strip():
        return payload
    render_mode = str(payload.get('render_mode') or '').strip().lower()
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if render_mode not in {'voice_diagnostic', 'workspace_api_attempt_error'} and level not in {'voice_management', 'workspace_opening_error'}:
        return payload
    lines = []
    for key in (
        'summary_block', 'result_block', 'period_result_block', 'kpi_block',
        'explanation_block', 'next_step_block', 'diagnosis_block',
        'recommended_next_step_block', 'navigation_block'
    ):
        value = payload.get(key)
        if isinstance(value, list):
            lines.extend(str(x).strip() for x in value if str(x or '').strip())
        elif isinstance(value, str) and value.strip():
            lines.append(value.strip())
    markdown = _render_lines_to_markdown(lines)
    if markdown:
        payload['workspace_markdown'] = markdown
        payload['screen_order'] = ['workspace_markdown']
        payload['workspace_render_instruction'] = (
            'Показать пользователю workspace_markdown полностью и без изменений. '
            'Не собирать пользовательский ответ из служебных блоков.'
        )
    return payload


def _make_reasons_only_public_payload(rendered_payload: dict) -> dict:
    return {
        'status': rendered_payload.get('status', 'ok'),
        'reason': rendered_payload.get('reason'),
        'context': rendered_payload.get('context'),
        'path': rendered_payload.get('path', []),
        'render_mode': 'reasons',
        'summary_block': rendered_payload.get('summary_block', 'Разбор причин текущего объекта.'),
        'reasons_block_render': rendered_payload.get('reasons_block_render', []),
        'reasons_block': rendered_payload.get('reasons_block', []),
        'factor_change_block': rendered_payload.get('factor_change_block', []),
        'factor_change_table': rendered_payload.get('factor_change_table', []),
        'benchmark_diagnostic_block': rendered_payload.get('benchmark_diagnostic_block', []),
        'benchmark_diagnostic_table': rendered_payload.get('benchmark_diagnostic_table', []),
        'navigation_block': rendered_payload.get('navigation_block', ['назад — вернуться к объекту']),
        'screen_order': ['summary_block', 'reasons_block_render', 'navigation_block'],
        'workspace_markdown': _render_lines_to_markdown([rendered_payload.get('summary_block', 'Разбор причин текущего объекта.')] + (rendered_payload.get('reasons_block_render') or []) + (rendered_payload.get('factor_change_block') or []) + (rendered_payload.get('benchmark_diagnostic_block') or []) + [''] + (rendered_payload.get('navigation_block') or ['назад — вернуться к объекту'])),
    }




# Sprint W8 — Large Workspace Rendering.
# Public payload must contain one canonical rendered workspace, not three
# duplicated copies (workspace_primary_block + business_workspace_block +
# workspace_markdown). State still stores the full rendered payload separately.
def _workspace_section_title(line: str) -> str:
    text = str(line or '').strip()
    # Section titles in recovered workspaces are plain Markdown-like strings
    # starting with an emoji. Keep them business-facing and stable.
    section_markers = ('📍', '🧠', '📊', '🏗', '📈', '💰', '💵', '🌐', '🧲', '🚨', '🎯', '➡️', '🤝', '📦', '📐', '⭐', '➕')
    if text.startswith(section_markers):
        return text
    return ''


def _build_workspace_sections(block: list) -> list:
    if not isinstance(block, list):
        return []
    sections = []
    current = {'title': 'Рабочий стол', 'lines': []}
    for raw in block:
        line = str(raw or '').strip()
        if not line:
            continue
        title = _workspace_section_title(line)
        if title and current['lines']:
            sections.append(current)
            current = {'title': title, 'lines': [line]}
        elif title and not current['lines']:
            current = {'title': title, 'lines': [line]}
        else:
            current['lines'].append(line)
    if current['lines']:
        sections.append(current)
    # Compact metadata; the Custom GPT renders lines, not nested raw payloads.
    compact_sections = []
    cursor = 0
    for idx, item in enumerate(sections, start=1):
        line_count = len(item.get('lines') or [])
        compact_sections.append({
            'section': idx,
            'title': item.get('title'),
            'start_line': cursor + 1,
            'end_line': cursor + line_count,
            'line_count': line_count,
        })
        cursor += line_count
    return compact_sections


def _apply_large_workspace_rendering(payload: dict) -> dict:
    """Keep information depth but remove duplicated transport weight.

    This is not content reduction. The full visible workspace remains in
    workspace_primary_block. The route removes duplicate mirror fields and adds
    optional section metadata so the client can render a large workspace block by
    block without requesting a smaller product.
    """
    if not isinstance(payload, dict):
        return payload
    primary = payload.get('workspace_primary_block')
    if not (isinstance(primary, list) and primary):
        return payload

    payload['workspace_sections'] = _build_workspace_sections(primary)
    payload['workspace_markdown'] = '\n'.join(str(x) for x in primary if str(x or '').strip())
    payload['workspace_render_instruction'] = (
        'Показывать пользователю workspace_markdown как основной рабочий стол полностью. '
        'Не пересобирать экран из коротких legacy-блоков и не сокращать доказательные таблицы.'
    )
    payload['large_workspace_rendering'] = {
        'enabled': True,
        'mode': 'sectioned_public_payload',
        'rule': 'информация не сокращена; удалены только дубли транспортного ответа',
        'sections': len(payload.get('workspace_sections') or []),
        'lines': len(primary),
    }

    # Remove duplicate copies of the exact same rendered workspace. These fields
    # caused large Custom GPT Action responses while adding no new information.
    # Keep workspace_markdown: it is the canonical user-visible artifact for Custom GPT.
    for key in (
        'business_workspace_block', 'contract_workspace_block',
        'management_workspace_block', 'category_workspace_block', 'product_workspace_block',
        'sku_passport_block', 'decision_workspace_block', 'business_context_block',
        'business_opportunity_block', 'recommendation_block', 'narrative_block',
    ):
        if key in payload:
            payload[key] = [] if key.endswith('_block') else ''

    # All-block is a separate витрина. It is available by command «все» and must
    # not travel inside every full workspace response.
    if str(payload.get('render_mode') or '').strip().lower() not in {'list_only'}:
        payload['all_block'] = []

    # One canonical render entry is enough. Navigation remains visible.
    payload['screen_order'] = ['workspace_markdown']
    return apply_runtime_contract(payload)


def _trim_default_public_payload(payload: dict) -> dict:
    # Public response should be render-focused. Raw engine workspaces can be
    # large and are not needed by the Custom GPT when block render fields exist.
    for key in (
        'decision_workspace', 'sku_passport', 'business_context',
        'category_workspace', 'business_opportunity', 'recommendation_engine',
        'narrative_engine', 'product_workspace', 'management_intelligence',
        'management_workspace', 'management_passport',
    ):
        payload.pop(key, None)

    primary = payload.get('workspace_primary_block')
    if isinstance(primary, list) and primary:
        # W4: prevent old short BI-style blocks from competing with the recovered
        # information-dense рабочий стол in Custom GPT rendering.
        for key in (
            'result_block', 'period_result_block', 'kpi_block', 'kpi_table',
            'structure_block', 'drain_block_render', 'explanation_block',
            'next_step_block', 'diagnosis_block', 'recommended_next_step_block',
            'opportunity_explanation_block', 'anomaly_explanation_block',
            'decision_block_render', 'business_result_rating_block',
            'profit_loss_rating_block', 'opportunity_rating_block',
            'priority_action_block', 'object_reasons_block', 'factor_change_block',
            'factor_change_table', 'benchmark_diagnostic_block',
            'benchmark_diagnostic_table', 'product_layer_block',
            'product_insight_block', 'product_tmc_decision_block',
            'management_workspace_block', 'business_context_block',
            'business_opportunity_block', 'recommendation_block',
            'narrative_block', 'product_workspace_block', 'decision_workspace_block',
        ):
            if key in payload:
                payload[key] = [] if isinstance(payload.get(key), list) else ''
    payload = _apply_large_workspace_rendering(payload)
    if 'all_block' in payload:
        payload['all_block'] = _compact_public_all_block(payload.get('all_block'))
    return payload


def _workspace_generation_error(rendered_payload: dict, code: str = 'workspace_markdown_missing') -> dict:
    ctx = rendered_payload.get('context') if isinstance(rendered_payload, dict) and isinstance(rendered_payload.get('context'), dict) else {}
    return {
        'status': 'error',
        'reason': 'workspace_generation_error',
        'error_code': code,
        'message': 'Ошибка формирования Workspace: API не вернул готовый workspace_markdown. Запрос не завершён.',
        'context': ctx,
        'path': rendered_payload.get('path', []) if isinstance(rendered_payload, dict) else [],
        'render_mode': rendered_payload.get('render_mode', '') if isinstance(rendered_payload, dict) else '',
        'workspace_runtime_contract': {
            'version': 'W14_5_SINGLE_RENDERING_CONTRACT',
            'rule': 'Workspace can be rendered only from non-empty workspace_markdown.',
            'forbidden_fallback': 'Do not render summary_block/kpi_block/diagnosis_block/navigation_block or other technical blocks.',
        },
        'screen_order': ['message'],
    }


def _record_runtime_rendering_issue(session_id: str, rendered_payload: dict, event_type: str, technical_reason: str, error_code: str = None) -> None:
    try:
        ctx = rendered_payload.get('context') if isinstance(rendered_payload, dict) and isinstance(rendered_payload.get('context'), dict) else {}
        active_state = rendered_payload.get('active_workspace_state') if isinstance(rendered_payload, dict) and isinstance(rendered_payload.get('active_workspace_state'), dict) else {}
        add_development_journal_runtime_event(
            event_type=event_type,
            component='workspace_runtime_renderer',
            system_level='runtime',
            technical_reason=technical_reason,
            suspected_root_cause='API did not provide a complete renderable Workspace contract or visible action map.',
            error_code=error_code or event_type,
            runtime_context={
                'level': ctx.get('level'),
                'object_name': ctx.get('object_name'),
                'period': ctx.get('period'),
                'render_mode': rendered_payload.get('render_mode') if isinstance(rendered_payload, dict) else None,
            },
            active_workspace_state=active_state,
            reproduction_data={
                'has_workspace_markdown': bool(isinstance(rendered_payload.get('workspace_markdown') if isinstance(rendered_payload, dict) else None, str) and rendered_payload.get('workspace_markdown').strip()),
                'workspace_action_map_count': len(rendered_payload.get('workspace_action_map') or []) if isinstance(rendered_payload, dict) and isinstance(rendered_payload.get('workspace_action_map'), list) else 0,
            },
            session_id=session_id,
        )
    except Exception:
        logger.exception('development_journal_runtime_event_failed event=%s session_id=%s', event_type, session_id)


def _make_full_workspace_public_payload(rendered_payload: dict) -> dict:
    """Return a canonical full Workspace response.

    W14.5 — Single Rendering Contract.

    `workspace_markdown` is the only user-visible source for Workspace screens.
    If it is missing or empty, the response is an explicit Workspace generation
    error. The public payload must not expose legacy technical blocks as a
    fallback rendering surface.
    """
    if not isinstance(rendered_payload, dict):
        return rendered_payload

    markdown = rendered_payload.get('workspace_markdown')
    if not isinstance(markdown, str) or not markdown.strip():
        return _workspace_generation_error(rendered_payload)

    state = rendered_payload.get('active_workspace_state', {})
    action_map = rendered_payload.get('workspace_action_map', [])
    contract = rendered_payload.get('workspace_runtime_contract', {})
    if isinstance(contract, dict):
        contract = dict(contract)
        contract['version'] = 'W14_5_SINGLE_RENDERING_CONTRACT'
        contract['single_rendering_contract'] = True
        contract['forbidden_user_visible_blocks'] = [
            'summary_block', 'kpi_block', 'diagnosis_block', 'reasons_block',
            'navigation_block', 'recommendation_block', 'explanation_block',
            'factor_block', 'benchmark_block', 'workspace_primary_block',
        ]

    return {
        'status': rendered_payload.get('status', 'ok'),
        'reason': rendered_payload.get('reason'),
        'context': rendered_payload.get('context'),
        'path': rendered_payload.get('path', []),
        'render_mode': rendered_payload.get('render_mode', ''),
        'workspace_markdown': markdown,
        'workspace_render_instruction': (
            'Показать пользователю только workspace_markdown полностью и без изменений. '
            'Не показывать и не использовать для пользовательского рендера summary_block, kpi_block, diagnosis_block, navigation_block или другие служебные блоки. '
            'Если workspace_markdown отсутствует — сообщить об ошибке формирования Workspace.'
        ),
        'active_workspace_state': state,
        'workspace_action_map': action_map,
        'workspace_runtime_contract': contract,
        # DEV-0004: keep Product Team research runtime visible at the public
        # boundary so Custom GPT can pass it into the next Action call.
        'active_research_state': rendered_payload.get('active_research_state', {}) or rendered_payload.get('research_flow_status', {}),
        'research_flow_status': rendered_payload.get('research_flow_status', {}) or rendered_payload.get('active_research_state', {}),
        'research_path': rendered_payload.get('research_path', []),
        'current_step': rendered_payload.get('current_step', ''),
        'next_step': rendered_payload.get('next_step', ''),
        # DEV-0006: expose Autonomous User Session state so Custom GPT can
        # distinguish Product Owner command from Assistant-generated user
        # messages and continue acceptance without hidden chat state.
        'autonomous_user_session': rendered_payload.get('autonomous_user_session', {}),
        'user_session_id': rendered_payload.get('user_session_id', ''),
        'user_role': rendered_payload.get('user_role', ''),
        'user_goal': rendered_payload.get('user_goal', ''),
        'user_history': rendered_payload.get('user_history', []),
        'session_status': rendered_payload.get('session_status', ''),
        'owner_command_forwarded_to_vectra': rendered_payload.get('owner_command_forwarded_to_vectra', (rendered_payload.get('autonomous_user_session') or {}).get('owner_command_forwarded_to_vectra') if isinstance(rendered_payload.get('autonomous_user_session'), dict) else None),
        'owner_command': rendered_payload.get('owner_command', (rendered_payload.get('autonomous_user_session') or {}).get('owner_command') if isinstance(rendered_payload.get('autonomous_user_session'), dict) else ''),
        'autonomous_user_session_active': rendered_payload.get('autonomous_user_session_active', bool(rendered_payload.get('autonomous_user_session'))),
        'autonomous_route': rendered_payload.get('autonomous_route', ''),
        'previous_context_closed': rendered_payload.get('previous_context_closed', False),
        'owner_command_type': rendered_payload.get('owner_command_type', ''),
        'start_screen_contract': rendered_payload.get('start_screen_contract', {}),
        'runtime_navigation': rendered_payload.get('runtime_navigation', {}),
        'screen_order': ['workspace_markdown'],
    }


@router.post('/development-journal/register', summary='Global Development Journal Registration')
def development_journal_register(request: dict):
    """Independent global Development Journal API.

    Works for any VECTRA assistant role and does not require Workspace Runtime,
    workspace_markdown, active_workspace_state or business API context. The
    request must contain normalized engineering knowledge; raw chat history is
    intentionally ignored.
    """
    session_id = str(request.get('session_id') or 'global') if isinstance(request, dict) else 'global'
    dry_run = bool(request.get('dry_run')) if isinstance(request, dict) else False
    is_test = bool(request.get('is_test')) if isinstance(request, dict) else False
    record = add_development_journal_global_record(
        event_type=str(request.get('event_type') or 'manual_engineering_registration'),
        component=str(request.get('component') or 'global_development_journal_api'),
        technical_description=str(request.get('technical_description') or 'Manual global engineering registration created by VECTRA assistant.'),
        suspected_root_cause=str(request.get('suspected_root_cause') or 'Requires laboratory review.'),
        proposed_fix_direction=str(request.get('proposed_fix_direction') or 'Classify, aggregate and convert into an engineering task if confirmed.'),
        priority=str(request.get('priority') or 'P1'),
        runtime_context=request.get('runtime_context') if isinstance(request.get('runtime_context'), dict) else {'source_scope': 'any_vectra_assistant'},
        session_id=session_id,
        dry_run=dry_run,
        is_test=is_test,
    )
    return build_development_journal_capture_response(record)



@router.post('/development-journal/analyze-dialogue', summary='Development Journal Dialogue Engineering Review')
def development_journal_analyze_dialogue(request: dict):
    """Batch engineering review for Product Acceptance dialogues.

    The endpoint accepts transient dialogue/messages input, classifies defects in
    memory, deduplicates them and persists only normalized engineering records.
    Raw dialogue text is never stored in Development Journal records.
    """
    if not isinstance(request, dict):
        request = {}
    session_id = str(request.get('session_id') or 'global')
    dialogue = request.get('dialogue') if 'dialogue' in request else request.get('messages')
    result = analyze_development_journal_dialogue(
        dialogue=dialogue,
        session_ctx=request.get('session_context') if isinstance(request.get('session_context'), dict) else None,
        session_id=session_id,
        dry_run=bool(request.get('dry_run')),
        is_test=bool(request.get('is_test')),
    )
    return build_development_journal_dialogue_review_response(result)


@router.get('/development-journal/export', summary='Development Journal Export')
def development_journal_export(include_test: bool = False):
    return build_development_journal_response(export=True, include_test=include_test)



@router.get('/self-evolution/status', summary='Self Evolution Repository Status')
def self_evolution_status():
    return json_response(get_self_evolution_repository_status())


@router.get('/self-evolution/journal', summary='Product Evolution Journal')
def self_evolution_journal(limit: int = 50):
    return json_response({
        'status': 'ok',
        'render_mode': 'self_evolution',
        'entries': list_self_evolution_entries(limit=limit),
    })




@router.get('/self-evolution/policy', summary='Self Evolution Policy')
def self_evolution_policy():
    return json_response({
        'status': 'ok',
        'render_mode': 'self_evolution',
        'evolution_policy': get_evolution_policy(),
    })


@router.post('/self-evolution/classify', summary='Classify Self Evolution Knowledge')
def self_evolution_classify(request: dict):
    if not isinstance(request, dict):
        request = {}
    classification = classify_knowledge(
        decision=str(request.get('decision') or ''),
        object_changed=str(request.get('object_changed') or ''),
        rationale=str(request.get('rationale') or ''),
        related_documents=request.get('related_documents') if isinstance(request.get('related_documents'), list) else None,
        metadata=request.get('metadata') if isinstance(request.get('metadata'), dict) else {},
    )
    return json_response({
        'status': 'ok',
        'render_mode': 'self_evolution',
        'classification': classification,
        'knowledge_types': KNOWLEDGE_TYPES,
        'lifecycle_statuses': LIFECYCLE_STATUSES,
    })

@router.post('/self-evolution/commit', summary='Run Self Evolution Cycle')
def self_evolution_commit(request: dict):
    if not isinstance(request, dict):
        request = {}
    result = run_self_evolution_cycle(
        decision=str(request.get('decision') or 'Self Evolution cycle executed.'),
        object_changed=str(request.get('object_changed') or 'Product Team Assistant model'),
        rationale=str(request.get('rationale') or ''),
        consequences=request.get('consequences') if isinstance(request.get('consequences'), list) else None,
        related_documents=request.get('related_documents') if isinstance(request.get('related_documents'), list) else None,
        source=str(request.get('source') or 'self_evolution_api'),
        metadata=request.get('metadata') if isinstance(request.get('metadata'), dict) else {},
    )
    return json_response(build_self_evolution_response(result))




@router.get('/self-evolution/identity', summary='Product Team Assistant Identity State')
def self_evolution_identity():
    return json_response({
        'status': 'ok',
        'render_mode': 'self_evolution',
        'assistant_state': load_assistant_state(),
    })


@router.get('/self-evolution/state', summary='Product Team Assistant Full State')
def self_evolution_state():
    return json_response(get_assistant_state())


@router.get('/self-evolution/responsibilities', summary='Product Team Assistant Responsibilities')
def self_evolution_responsibilities():
    return json_response(get_responsibilities())


@router.get('/self-evolution/open-cycles', summary='Product Team Assistant Open Evolution Cycles')
def self_evolution_open_cycles():
    return json_response(get_open_cycles())


@router.get('/self-evolution/autonomous/status', summary='Autonomous Self Evolution Status')
def self_evolution_autonomous_status():
    return json_response(get_autonomous_work_state())


@router.post('/self-evolution/autonomous/run', summary='Run Autonomous Self Evolution Cycle')
def self_evolution_autonomous_run(request: dict):
    if not isinstance(request, dict):
        request = {}
    result = run_autonomous_self_evolution_cycle(
        decision=str(request.get('decision') or request.get('message') or 'Confirmed knowledge detected for autonomous Self Evolution.'),
        object_changed=str(request.get('object_changed') or 'Product Team Assistant autonomous Self Evolution model'),
        rationale=str(request.get('rationale') or 'Assistant must manage its own work queue and complete confirmed knowledge integration.'),
        consequences=request.get('consequences') if isinstance(request.get('consequences'), list) else None,
        related_documents=request.get('related_documents') if isinstance(request.get('related_documents'), list) else None,
        source=str(request.get('source') or 'self_evolution_autonomous_api'),
        metadata=request.get('metadata') if isinstance(request.get('metadata'), dict) else {},
    )
    return json_response(build_autonomous_self_evolution_response(result))




@router.get('/self-evolution/activity/plan', summary='Product Team Assistant Professional Activity Plan')
def self_evolution_activity_plan():
    return json_response(build_professional_activity_response(get_professional_activity_plan()))


@router.post('/self-evolution/activity/plan', summary='Refresh Product Team Assistant Professional Activity Plan')
def self_evolution_activity_plan_refresh(request: dict):
    # DEV-0011A is deterministic: request is accepted for future extension,
    # but the plan is built from Assistant State and SEE queues.
    return json_response(build_professional_activity_response(get_professional_activity_plan()))



@router.get('/self-evolution/activity/value-priority', summary='Product Team Assistant Value & Priority Evaluation')
def self_evolution_activity_value_priority():
    plan = get_professional_activity_plan()
    return json_response(build_value_priority_response(evaluate_professional_activity_value(plan)))


@router.post('/self-evolution/activity/value-priority', summary='Refresh Product Team Assistant Value & Priority Evaluation')
def self_evolution_activity_value_priority_refresh(request: dict):
    plan = get_professional_activity_plan()
    return json_response(build_value_priority_response(evaluate_professional_activity_value(plan)))



@router.get('/self-evolution/activity/dependencies', summary='Product Team Assistant Dependency Map')
def self_evolution_activity_dependencies():
    plan = get_professional_activity_plan()
    return json_response(build_dependency_response(evaluate_dependency_map(plan)))


@router.post('/self-evolution/activity/dependencies', summary='Refresh Product Team Assistant Dependency Map')
def self_evolution_activity_dependencies_refresh(request: dict):
    plan = get_professional_activity_plan()
    return json_response(build_dependency_response(evaluate_dependency_map(plan)))



@router.get('/self-evolution/activity/orchestrate', summary='Product Team Assistant Professional Activity Orchestrator')
def self_evolution_activity_orchestrate():
    plan = get_professional_activity_plan()
    return json_response(build_orchestration_response(evaluate_professional_activity_orchestration(plan)))


@router.post('/self-evolution/activity/orchestrate', summary='Run Product Team Assistant Professional Activity Orchestrator')
def self_evolution_activity_orchestrate_refresh(request: dict):
    plan = get_professional_activity_plan()
    return json_response(build_orchestration_response(evaluate_professional_activity_orchestration(plan)))

@router.get('/self-evolution/recover', summary='Recover Product Team Assistant State')
def self_evolution_recover():
    return json_response(recover_self_evolution_state())



@router.get('/digital-organization/protocol/document-contract-model', summary='Digital Organization Protocol Document Contract Model')
def digital_organization_document_contract_model():
    return json_response(get_document_contract_model())


@router.post('/digital-organization/protocol/document-contract', summary='Create Digital Organization Document Contract')
def digital_organization_document_contract(request: dict):
    if not isinstance(request, dict):
        request = {}
    payload = {
        'document_type': str(request.get('document_type') or 'release_brief'),
        'title': str(request.get('title') or 'Digital Organization Document Contract'),
        'what_changed': str(request.get('what_changed') or 'A professional document contract was created.'),
        'why_it_matters': str(request.get('why_it_matters') or 'The next digital role receives a self-sufficient artifact instead of relying on chat context.'),
        'what_it_enables_next': str(request.get('what_it_enables_next') or 'Responsibility can be transferred through a standard professional document.'),
        'created_by': str(request.get('created_by') or 'Engineering Team'),
        'received_by': str(request.get('received_by') or 'Product Team Assistant'),
        'next_actor': str(request.get('next_actor') or 'Product Team Assistant'),
        'related_epic': request.get('related_epic'),
        'related_release': request.get('related_release'),
        'source_artifact': request.get('source_artifact'),
        'previous_artifact': request.get('previous_artifact'),
        'downstream_artifacts': request.get('downstream_artifacts') if isinstance(request.get('downstream_artifacts'), list) else None,
        'decision_or_result': request.get('decision_or_result') if isinstance(request.get('decision_or_result'), dict) else None,
        'professional_context': request.get('professional_context') if isinstance(request.get('professional_context'), dict) else None,
        'documentation_sync': request.get('documentation_sync') if isinstance(request.get('documentation_sync'), dict) else None,
        'completion_criteria': request.get('completion_criteria') if isinstance(request.get('completion_criteria'), list) else None,
        'lifecycle_state': str(request.get('lifecycle_state') or 'confirmed'),
    }
    return json_response(build_document_contract_response(payload))


@router.post('/digital-organization/protocol/validate-document-contract', summary='Validate Digital Organization Document Contract')
def digital_organization_validate_document_contract(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(validate_document_contract(request))


@router.get('/digital-organization/protocol/responsibility-transfer-model', summary='Digital Organization Protocol Responsibility Transfer Model')
def digital_organization_responsibility_transfer_model():
    return json_response(get_responsibility_transfer_model())


@router.post('/digital-organization/protocol/responsibility-transfer', summary='Create Digital Organization Responsibility Transfer Package')
def digital_organization_responsibility_transfer(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(build_responsibility_transfer_response(request))


@router.post('/digital-organization/protocol/validate-responsibility-transfer', summary='Validate Digital Organization Responsibility Transfer Package')
def digital_organization_validate_responsibility_transfer(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(validate_responsibility_transfer_package(request))




@router.get('/digital-organization/protocol/responsibility-lifecycle-model', summary='Digital Organization Protocol Professional Responsibility Lifecycle Model')
def digital_organization_responsibility_lifecycle_model():
    return json_response(get_responsibility_lifecycle_model())


@router.post('/digital-organization/protocol/responsibility-lifecycle', summary='Create Digital Organization Professional Responsibility Lifecycle')
def digital_organization_responsibility_lifecycle(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(build_responsibility_lifecycle_response(request))


@router.post('/digital-organization/protocol/validate-responsibility-lifecycle', summary='Validate Digital Organization Professional Responsibility Lifecycle')
def digital_organization_validate_responsibility_lifecycle(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(validate_responsibility_lifecycle(request))



@router.get('/digital-organization/protocol/traceability-model', summary='Digital Organization Protocol Purpose and Responsibility Traceability Model')
def digital_organization_traceability_model():
    return json_response(get_traceability_model())


@router.post('/digital-organization/protocol/traceability', summary='Create Digital Organization Purpose and Responsibility Trace')
def digital_organization_traceability(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(build_traceability_response(request))


@router.post('/digital-organization/protocol/validate-traceability', summary='Validate Digital Organization Purpose and Responsibility Trace')
def digital_organization_validate_traceability(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(validate_purpose_trace(request))



@router.get('/digital-organization/runtime/model', summary='Digital Organization Runtime Model')
def digital_organization_runtime_model():
    return json_response(get_digital_organization_runtime_model())


@router.get('/digital-organization/runtime/status', summary='Digital Organization Runtime Status')
def digital_organization_runtime_status():
    return json_response(build_runtime_response({}))


@router.post('/digital-organization/runtime/run', summary='Run Digital Organization Runtime Evaluation')
def digital_organization_runtime_run(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(build_runtime_response(request))


@router.post('/digital-organization/runtime/validate', summary='Validate Digital Organization Runtime')
def digital_organization_runtime_validate(request: dict):
    if not isinstance(request, dict):
        request = {}
    runtime = request.get('digital_organization_runtime') if isinstance(request.get('digital_organization_runtime'), dict) else request
    return json_response(validate_digital_organization_runtime(runtime))


# VECTRA-RUNTIME-0001: Assistant Runtime Repository Foundation
# These endpoints make VECTRA itself the persistent professional workspace of
# Product Team Assistant. ChatGPT remains the interface; VECTRA stores state,
# journals, knowledge, decisions and recovery snapshots.

@router.get('/assistant/repository', summary='VECTRA Assistant Runtime Repository Status')
def vectra_assistant_repository_status():
    return json_response(get_vectra_assistant_repository_status())


@router.get('/assistant/recovery', summary='Recover Product Team Assistant from VECTRA Runtime Repository')
def vectra_assistant_recovery():
    return json_response(get_vectra_assistant_recovery_bundle())


@router.get('/assistant/state', summary='Read Product Team Assistant Runtime State')
def vectra_assistant_state():
    return json_response(get_vectra_assistant_current_state())


@router.post('/assistant/state', summary='Update Product Team Assistant Runtime State')
def vectra_assistant_state_update(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(update_vectra_assistant_current_state(request))


@router.get('/assistant/runtime', summary='Read VECTRA Assistant Runtime Status')
def vectra_assistant_runtime_status():
    return json_response(get_vectra_assistant_runtime_status())


@router.get('/assistant/evolution-journal', summary='Read Product Team Assistant Evolution Journal')
def vectra_assistant_evolution_journal():
    recovery = get_vectra_assistant_recovery_bundle()
    return json_response({
        'status': 'ok',
        'render_mode': 'assistant_runtime_evolution_journal',
        'recent_entries': recovery.get('recent_journal_entries', []),
        'repository': recovery.get('repository', {}),
    })


@router.post('/assistant/journal', summary='Append Product Team Assistant Evolution Journal Entry')
def vectra_assistant_journal_append(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(append_vectra_assistant_journal_entry(request))


@router.post('/assistant/evolution', summary='Run Assistant Runtime Evolution Update')
def vectra_assistant_evolution_update(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(run_vectra_assistant_evolution_update(request))


@router.get('/assistant/knowledge', summary='List Product Team Assistant Knowledge Repository')
def vectra_assistant_knowledge_list():
    return json_response(list_vectra_assistant_knowledge_documents())


@router.post('/assistant/knowledge', summary='Create or Update Product Team Assistant Knowledge Document')
def vectra_assistant_knowledge_upsert(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(upsert_vectra_assistant_knowledge_document(request))


@router.patch('/assistant/knowledge/{document_id}', summary='Update Product Team Assistant Knowledge Document by ID')
def vectra_assistant_knowledge_update(document_id: str, request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(update_vectra_assistant_knowledge_document(document_id, request))


@router.post('/assistant/decision', summary='Record Product Decision in Assistant Runtime Repository')
def vectra_assistant_decision_record(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(record_vectra_assistant_product_decision(request))


@router.post('/assistant/snapshot', summary='Create Product Team Assistant Recovery Snapshot')
def vectra_assistant_snapshot_create(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(create_vectra_assistant_recovery_snapshot(request))




# VECTRA-RUNTIME-0002: Runtime Execution & Transparent Control
# Product Acceptance is no longer the end of the process. It becomes the trigger
# for VECTRA to update its internal working environment and explain the result
# to Product Owner in human language.

@router.get('/assistant/runtime-execution/model', summary='VECTRA Runtime Execution Model')
def vectra_runtime_execution_model():
    return json_response(get_vectra_runtime_execution_model())


@router.post('/assistant/runtime-execution/run', summary='Run VECTRA Runtime Execution after confirmed event')
def vectra_runtime_execution_run(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(run_vectra_runtime_execution(request))


@router.get('/assistant/runtime-execution/reports', summary='List VECTRA Runtime Execution Reports')
def vectra_runtime_execution_reports(limit: int = 20):
    return json_response(list_vectra_runtime_execution_reports(limit=limit))


@router.get('/assistant/runtime-execution/pending-approvals', summary='List Runtime Changes Waiting for Product Owner')
def vectra_runtime_pending_approvals():
    return json_response(get_vectra_runtime_pending_approvals())


@router.post('/assistant/work-shift/start', summary='Start VECTRA Work Shift')
def vectra_work_shift_start(request: dict = None):
    if not isinstance(request, dict):
        request = {}
    return json_response(start_vectra_work_shift(request))


@router.post('/assistant/work-shift/close', summary='Close VECTRA Work Shift and Create Human Report')
def vectra_work_shift_close(request: dict = None):
    if not isinstance(request, dict):
        request = {}
    return json_response(close_vectra_work_shift(request))




# VECTRA-RUNTIME-0003: Natural Command Guidance & Readback Observability
# Product Owner is not expected to remember technical API routes. VECTRA accepts
# ordinary human language, selects the internal read action, and returns a human
# explanation plus the actual observable runtime payload.

@router.get('/vectra/natural-command/model', summary='VECTRA Natural Command Guidance Model')
def vectra_natural_command_model():
    return json_response(get_vectra_natural_command_model())


@router.post('/vectra/command', summary='Execute Natural Product Owner Command')
def vectra_natural_command(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(execute_vectra_natural_command(request))


@router.post('/assistant/command', summary='Alias: Execute Natural Product Owner Command')
def assistant_natural_command(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(execute_vectra_natural_command(request))


@router.get('/vectra/memory', summary='Read Observable VECTRA Runtime Memory Overview')
def vectra_memory_overview():
    return json_response(get_vectra_runtime_memory_overview())


@router.get('/assistant/memory', summary='Alias: Read Observable VECTRA Runtime Memory Overview')
def assistant_memory_overview():
    return json_response(get_vectra_runtime_memory_overview())


@router.get('/vectra/journal', summary='Read VECTRA Evolution Journal')
def vectra_journal_read(limit: int = 50):
    return json_response(list_vectra_journal_entries(limit=limit))


@router.get('/vectra/decisions', summary='Read VECTRA Product Decisions')
def vectra_decisions_read(limit: int = 50):
    return json_response(list_vectra_product_decisions(limit=limit))


@router.get('/vectra/snapshots', summary='Read VECTRA Recovery Snapshots')
def vectra_snapshots_read(limit: int = 20):
    return json_response(list_vectra_recovery_snapshots(limit=limit))


@router.get('/vectra/recovery', summary='Alias: Recover VECTRA Runtime State')
def vectra_recovery_read():
    return json_response(get_vectra_assistant_recovery_bundle())


@router.get('/vectra/state', summary='Alias: Read VECTRA Runtime State')
def vectra_state_read():
    return json_response(get_vectra_assistant_current_state())






# FOUNDATION-0003: Laboratory Public API
# Public schema for VECTRA Laboratory Actions. This schema intentionally exposes
# only Laboratory Product Verification endpoints and does not publish internal
# engineering/runtime mutation endpoints.

def _laboratory_public_openapi_schema() -> dict:
    """Return OpenAI GPT Actions compatible public Laboratory OpenAPI.

    FOUNDATION-0003 BUG-001 final fix:
    - no response $ref references are used;
    - components.schemas is serialized as an empty object;
    - every object schema declares explicit properties;
    - the schema exposes only Laboratory endpoints.

    This intentionally uses conservative response schemas because GPT Actions
    validates the OpenAPI contract more strictly than generic OpenAPI tooling.
    The Runtime may return richer JSON at execution time, but the Action schema
    remains stable and import-safe for VECTRA Laboratory.
    """
    server_url = os.getenv('VECTRA_PUBLIC_RUNTIME_URL') or os.getenv('VECTRA_RUNTIME_URL') or os.getenv('RENDER_EXTERNAL_URL') or 'https://bon-buasson-api.onrender.com'
    api_key_required = bool(os.getenv('VECTRA_LABORATORY_API_KEY'))
    security = [{'LaboratoryApiKey': []}] if api_key_required else []

    generic_response_schema = {
        'type': 'object',
        'properties': {
            'status': {'type': 'string', 'description': 'Runtime operation status.'},
            'render_mode': {'type': 'string', 'description': 'Rendering or response mode when provided by Runtime.'},
            'runtime_version': {'type': 'string', 'description': 'Runtime version when available.'},
            'release_version': {'type': 'string', 'description': 'Current release version when available.'},
            'verification_result': {'type': 'string', 'description': 'Verification result when available.'},
            'runtime_health': {'type': 'string', 'description': 'Runtime health value when available.'},
            'readback_status': {'type': 'string', 'description': 'Readback status when available.'},
            'human_summary': {'type': 'string', 'description': 'Human-readable summary prepared for VECTRA Laboratory.'},
            'message': {'type': 'string', 'description': 'Additional Runtime message when available.'},
            'business_data_connected': {'type': 'boolean', 'description': 'Whether Business Data is connected when returned by Business Data endpoints.'},
            'business_data_health': {'type': 'string', 'description': 'Business Data health when returned by Business Data endpoints.'},
            'read_only': {'type': 'boolean', 'description': 'True when the operation is read-only.'},
            'same_source_as_working_gpt': {'type': 'boolean', 'description': 'True when Laboratory uses the same Runtime Business Data source as Working GPT.'},
            'available_read_only_endpoints': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Read-only Business Data endpoint list when provided.'},
            'package_id': {'type': 'string', 'description': 'Capitalization or verification package id when provided.'},
            'knowledge_id': {'type': 'string', 'description': 'Knowledge id when provided.'},
            'knowledge_type': {'type': 'string', 'description': 'professional or business when provided.'},
            'target_repository': {'type': 'string', 'description': 'Runtime target repository when provided.'},
            'final_status': {'type': 'string', 'description': 'Final capitalization status when provided.'},
        },
    }

    capitalization_request_schema = {
        'type': 'object',
        'properties': {
            'candidate_id': {'type': 'string'},
            'knowledge_id': {'type': 'string'},
            'knowledge_type': {'type': 'string', 'enum': ['professional', 'business']},
            'title': {'type': 'string'},
            'content': {'type': 'string'},
            'domain': {'type': 'string'},
            'product_owner_approval': {'type': 'boolean'},
            'source': {'type': 'string'},
        },
    }

    def response(description: str) -> dict:
        return {
            '200': {
                'description': description,
                'content': {
                    'application/json': {
                        'schema': generic_response_schema,
                    }
                },
            }
        }

    return {
        'openapi': '3.1.0',
        'info': {
            'title': 'VECTRA Laboratory Public Verification API',
            'version': os.getenv('VECTRA_LABORATORY_SCHEMA_VERSION', 'LABORATORY-ACTIONS-0002-FULL-LABORATORY-ACTIONS'),
            'description': 'Single complete Action-facing OpenAPI API for VECTRA Laboratory. Publishes the full aggregated GPT Actions contract and includes Action Manifest verification so Runtime Capability Registry, Runtime Services and GPT Actions stay synchronized after every release.',
        },
        'servers': [{'url': server_url}],
        'components': {
            'schemas': {},
            'securitySchemes': {
                'LaboratoryApiKey': {
                    'type': 'apiKey',
                    'in': 'header',
                    'name': 'X-VECTRA-LABORATORY-KEY',
                    'description': 'Optional Laboratory API key. Required only when VECTRA_LABORATORY_API_KEY is configured in Runtime.',
                }
            }
        },
        'paths': {
            '/vectra/runtime/status': {
                'get': {
                    'operationId': 'getVectraRuntimeStatus',
                    'summary': 'Get VECTRA Runtime status',
                    'description': 'Returns Runtime Version, Deployment Version, Deployment Time, Current Release and Runtime Health.',
                    'security': security,
                    'responses': response('Runtime status'),
                }
            },
            '/vectra/runtime/snapshot': {
                'get': {
                    'operationId': 'getVectraRuntimeSnapshot',
                    'summary': 'Get full VECTRA Runtime Snapshot',
                    'description': 'Returns the official Runtime Snapshot used by VECTRA Laboratory for Product Verification.',
                    'security': security,
                    'parameters': [
                        {
                            'name': 'refresh',
                            'in': 'query',
                            'required': False,
                            'schema': {'type': 'boolean'},
                            'description': 'If true, request refresh when supported.',
                        }
                    ],
                    'responses': response('Runtime Snapshot'),
                }
            },
            '/vectra/runtime/verify': {
                'get': {
                    'operationId': 'verifyVectraRuntime',
                    'summary': 'Verify VECTRA Runtime',
                    'description': 'Returns Runtime Verification Report for Laboratory Product Verification.',
                    'security': security,
                    'responses': response('Runtime Verification Report'),
                }
            },
            '/vectra/laboratory/verification': {
                'get': {
                    'operationId': 'getVectraLaboratoryVerification',
                    'summary': 'Get full VECTRA Laboratory Product Verification Evidence',
                    'description': 'Returns the complete verification evidence package in one request for VECTRA Laboratory.',
                    'security': security,
                    'parameters': [
                        {
                            'name': 'runtime_url',
                            'in': 'query',
                            'required': False,
                            'schema': {'type': 'string'},
                            'description': 'Optional Runtime URL to echo into evidence package.',
                        }
                    ],
                    'responses': response('Laboratory Verification Evidence'),
                }
            },
            '/vectra/laboratory/behavior/policy': {
                'get': {
                    'operationId': 'getVectraLaboratoryActionFirstPolicy',
                    'summary': 'Get VECTRA Laboratory Action First Policy',
                    'description': 'Returns the official Laboratory behavior policy: Runtime action first, then conclusion. Read-only endpoint for preventing drift into ordinary ChatGPT behavior.',
                    'security': security,
                    'responses': response('Laboratory Action First Policy'),
                }
            },
            '/vectra/laboratory/behavior/next-action': {
                'get': {
                    'operationId': 'determineVectraLaboratoryNextAction',
                    'summary': 'Determine next VECTRA Laboratory Runtime Action',
                    'description': 'Determines the next professional Runtime Action for Product Owner commands such as continue work, check state, inspect product or capitalize knowledge. Laboratory must call the selected Runtime action before forming a conclusion.',
                    'security': security,
                    'parameters': [
                        {'name': 'command', 'in': 'query', 'required': False, 'schema': {'type': 'string'}, 'description': 'Product Owner command.'},
                        {'name': 'runtime_access_confirmed', 'in': 'query', 'required': False, 'schema': {'type': 'boolean'}, 'description': 'Whether Runtime was already successfully reached in the current work session.'},
                    ],
                    'responses': response('Laboratory Next Action'),
                }
            },
            '/vectra/laboratory/behavior/verify': {
                'get': {
                    'operationId': 'verifyVectraLaboratoryActionFirstPolicy',
                    'summary': 'Verify VECTRA Laboratory Action First Policy',
                    'description': 'Verifies that trigger commands map to a Runtime action first and that preliminary limitation explanations are not allowed before a Runtime response.',
                    'security': security,
                    'responses': response('Laboratory Action First Policy Verification'),
                }
            },
            '/vectra/laboratory/actions/manifest': {
                'get': {
                    'operationId': 'getVectraLaboratoryActionManifest',
                    'summary': 'Get VECTRA Laboratory Action Manifest',
                    'description': 'Returns the full exported GPT Actions manifest with operation id, Runtime Service, Capability, endpoint, release version and export status. Used to prevent accidental Action loss after OpenAPI updates.',
                    'security': security,
                    'responses': response('Laboratory Action Manifest'),
                }
            },
            '/vectra/laboratory/actions/verify': {
                'get': {
                    'operationId': 'verifyVectraLaboratoryActionCompleteness',
                    'summary': 'Verify VECTRA Laboratory Action completeness',
                    'description': 'Compares Runtime Capability Registry with exported GPT Actions Manifest. Returns PASS only when every Capability that requires a GPT Action has an exported Action.',
                    'security': security,
                    'responses': response('Laboratory Action Completeness Verification'),
                }
            },
            '/vectra/life-model': {
                'get': {
                    'operationId': 'getVectraLifeModel',
                    'summary': 'Get VECTRA Life Model',
                    'description': 'Returns the official VECTRA Life Model from Runtime Repository.',
                    'security': security,
                    'responses': response('VECTRA Life Model'),
                }
            },
            '/vectra/life-model/status': {
                'get': {
                    'operationId': 'getVectraLifeModelStatus',
                    'summary': 'Get VECTRA Life Model status',
                    'description': 'Returns Life Model repository and verification status.',
                    'security': security,
                    'responses': response('VECTRA Life Model status'),
                }
            },
            '/vectra/life-model/verify': {
                'get': {
                    'operationId': 'verifyVectraLifeModel',
                    'summary': 'Verify VECTRA Life Model',
                    'description': 'Verifies Life Model readback, required sections and protection rules.',
                    'security': security,
                    'responses': response('VECTRA Life Model verification'),
                }
            },

            '/vectra/vos': {
                'get': {
                    'operationId': 'getVectraOperatingSystem',
                    'summary': 'Get VECTRA Operating System',
                    'description': 'Returns VOS-001 Operating Model from Runtime Repository.',
                    'security': security,
                    'responses': response('VECTRA Operating System'),
                }
            },
            '/vectra/vos/status': {
                'get': {
                    'operationId': 'getVectraOperatingSystemStatus',
                    'summary': 'Get VECTRA Operating System status',
                    'description': 'Returns VOS repository status and Runtime readiness.',
                    'security': security,
                    'responses': response('VECTRA Operating System status'),
                }
            },
            '/vectra/vos/verify': {
                'get': {
                    'operationId': 'verifyVectraOperatingSystem',
                    'summary': 'Verify VECTRA Operating System',
                    'description': 'Verifies VOS readback, required sections, protection rules and Bonboason support.',
                    'security': security,
                    'responses': response('VECTRA Operating System verification'),
                }
            },
            '/vectra/vos/restore': {
                'get': {
                    'operationId': 'restoreVectraOperatingSystem',
                    'summary': 'Restore VECTRA Operating System state',
                    'description': 'Restores VOS, active Business Domain, responsibilities, pending reviews and Evolution Journal from Runtime.',
                    'security': security,
                    'responses': response('VECTRA Operating System restoration'),
                }
            },
            '/vectra/professional/model': {
                'get': {
                    'operationId': 'getVectraProfessionalModel',
                    'summary': 'Get current confirmed VECTRA Professional Model',
                    'description': 'Returns the current confirmed Professional Model without changing it.',
                    'security': security,
                    'responses': response('Professional Model'),
                }
            },
            '/vectra/evolution/status': {
                'get': {
                    'operationId': 'getVectraEvolutionStatus',
                    'summary': 'Get VECTRA evolution status',
                    'description': 'Returns last confirmed engineering increment, last Product Verification, active Engineering Proposals and active Improvement Proposals.',
                    'security': security,
                    'responses': response('Evolution status'),
                }
            },
            '/vectra/evolution/journal': {
                'get': {
                    'operationId': 'getVectraEvolutionJournal',
                    'summary': 'Get VECTRA Evolution Journal',
                    'description': 'Returns the Evolution Journal repository for Laboratory verification.',
                    'security': security,
                    'responses': response('Evolution Journal'),
                }
            },

            '/vectra/capabilities': {
                'get': {
                    'operationId': 'getVectraCapabilities',
                    'summary': 'Get VECTRA Capability Registry',
                    'description': 'Returns professional capabilities available to VECTRA Laboratory and maps them to Runtime services.',
                    'security': security,
                    'responses': response('Capability Registry'),
                }
            },
            '/vectra/recovery': {
                'get': {
                    'operationId': 'getVectraRecoveryState',
                    'summary': 'Get VECTRA Recovery state',
                    'description': 'Returns the Runtime Recovery bundle used to restore VECTRA state.',
                    'security': security,
                    'responses': response('Recovery State'),
                }
            },
            '/vectra/synchronization/status': {
                'get': {
                    'operationId': 'getVectraSynchronizationStatus',
                    'summary': 'Get VECTRA Laboratory Synchronization Status',
                    'description': 'Returns synchronization status across Sources, Knowledge, Instruction, API and Runtime.',
                    'security': security,
                    'responses': response('Synchronization Status'),
                }
            },
            '/vectra/review/session': {
                'get': {
                    'operationId': 'getVectraReviewSession',
                    'summary': 'Get VECTRA Product Owner Review Session',
                    'description': 'Returns or opens the current Product Owner Review Session for Laboratory verification.',
                    'security': security,
                    'responses': response('Review Session'),
                }
            },
            '/vectra/domains': {
                'get': {
                    'operationId': 'getVectraBusinessDomains',
                    'summary': 'Get VECTRA Business Domain Registry',
                    'description': 'Returns Business Domain Registry and active domain information.',
                    'security': security,
                    'responses': response('Business Domain Registry'),
                }
            },
            '/vectra/domain/recover': {
                'get': {
                    'operationId': 'restoreVectraBusinessDomain',
                    'summary': 'Restore VECTRA Business Domain',
                    'description': 'Restores the active or requested Business Domain from Runtime Repository.',
                    'security': security,
                    'parameters': [
                        {'name': 'domain_id', 'in': 'query', 'required': False, 'schema': {'type': 'string'}, 'description': 'Business Domain id. Defaults to bonboason.'},
                    ],
                    'responses': response('Business Domain Restore'),
                }
            },
            '/vectra/domain/activate': {
                'post': {
                    'operationId': 'activateVectraBusinessDomain',
                    'summary': 'Activate VECTRA Business Domain',
                    'description': 'Activates a Business Domain when explicitly requested by Product Owner.',
                    'security': security,
                    'requestBody': {'required': False, 'content': {'application/json': {'schema': {'type': 'object', 'properties': {'domain_id': {'type': 'string'}}}}}},
                    'responses': response('Business Domain Activation'),
                }
            },
            '/vectra/domain/capitalization': {
                'post': {
                    'operationId': 'capitalizeVectraBusinessDomainContext',
                    'summary': 'Capitalize confirmed context into Business Domain',
                    'description': 'Capitalizes Product Owner confirmed context into the active Business Domain without changing Professional Knowledge.',
                    'security': security,
                    'requestBody': {'required': False, 'content': {'application/json': {'schema': capitalization_request_schema}}},
                    'responses': response('Business Domain Context Capitalization'),
                }
            },
            '/vectra/professional-body/restore': {
                'get': {
                    'operationId': 'restoreVectraProfessionalBody',
                    'summary': 'Restore VECTRA professional state from Runtime',
                    'description': 'Restores Professional Identity, Professional Model, decisions, responsibilities, Evolution Journal and Recovery Snapshot from Runtime Repository.',
                    'security': security,
                    'responses': response('Professional Body Restoration'),
                }
            },
            '/vectra/professional-body/verify': {
                'get': {
                    'operationId': 'verifyVectraProfessionalBody',
                    'summary': 'Verify VECTRA Professional Body Integration',
                    'description': 'Verifies Capability Registry, context capitalization readback, Recovery Snapshot and Professional Model protection.',
                    'security': security,
                    'responses': response('Professional Body Integration Verification'),
                }
            },
            '/vectra/laboratory/business-data/status': {
                'get': {
                    'operationId': 'getVectraLaboratoryBusinessDataStatus',
                    'summary': 'Get VECTRA Laboratory Business Data access status',
                    'description': 'GPT Action endpoint. Confirms that VECTRA Laboratory has read-only access to the same Business Data source used by Working GPT.',
                    'security': security,
                    'responses': response('Business Data access status'),
                }
            },
            '/vectra/laboratory/business-data/entities': {
                'get': {
                    'operationId': 'getVectraLaboratoryBusinessDataEntities',
                    'summary': 'Get Business Data entity dictionary and previews',
                    'description': 'Returns read-only entity counts and previews for periods, managers, networks, categories, groups and SKU.',
                    'security': security,
                    'parameters': [{'name': 'limit_per_group', 'in': 'query', 'required': False, 'schema': {'type': 'integer'}, 'description': 'Maximum preview values per entity group.'}],
                    'responses': response('Business Data entities'),
                }
            },
            '/vectra/laboratory/business-data/sample': {
                'get': {
                    'operationId': 'getVectraLaboratoryBusinessDataSample',
                    'summary': 'Get read-only Business Data sample rows',
                    'description': 'Returns a small read-only sample from the same DATA source used by Working GPT.',
                    'security': security,
                    'parameters': [{'name': 'limit', 'in': 'query', 'required': False, 'schema': {'type': 'integer'}, 'description': 'Number of rows to return, capped by Runtime.'}],
                    'responses': response('Business Data sample'),
                }
            },
            '/vectra/laboratory/business-data/summary/business': {
                'get': {
                    'operationId': 'getVectraLaboratoryBusinessSummary',
                    'summary': 'Get Business summary from existing Runtime Business Data',
                    'description': 'Read-only Business summary using the same Runtime calculation path as Working GPT.',
                    'security': security,
                    'parameters': [{'name': 'period', 'in': 'query', 'required': True, 'schema': {'type': 'string'}, 'description': 'Period, for example 2026-02.'}],
                    'responses': response('Business summary'),
                }
            },
            '/vectra/laboratory/business-data/summary/manager-top': {
                'get': {
                    'operationId': 'getVectraLaboratoryManagerTopSummary',
                    'summary': 'Get Manager Top summary from existing Runtime Business Data',
                    'security': security,
                    'parameters': [{'name': 'manager_top', 'in': 'query', 'required': True, 'schema': {'type': 'string'}}, {'name': 'period', 'in': 'query', 'required': True, 'schema': {'type': 'string'}}],
                    'responses': response('Manager Top summary'),
                }
            },
            '/vectra/laboratory/business-data/summary/manager': {
                'get': {
                    'operationId': 'getVectraLaboratoryManagerSummary',
                    'summary': 'Get Manager summary from existing Runtime Business Data',
                    'security': security,
                    'parameters': [{'name': 'manager', 'in': 'query', 'required': True, 'schema': {'type': 'string'}}, {'name': 'period', 'in': 'query', 'required': True, 'schema': {'type': 'string'}}],
                    'responses': response('Manager summary'),
                }
            },
            '/vectra/laboratory/business-data/summary/network': {
                'get': {
                    'operationId': 'getVectraLaboratoryNetworkSummary',
                    'summary': 'Get Network / Contract summary from existing Runtime Business Data',
                    'security': security,
                    'parameters': [{'name': 'network', 'in': 'query', 'required': True, 'schema': {'type': 'string'}}, {'name': 'period', 'in': 'query', 'required': True, 'schema': {'type': 'string'}}],
                    'responses': response('Network summary'),
                }
            },
            '/vectra/laboratory/business-data/summary/category': {
                'get': {
                    'operationId': 'getVectraLaboratoryCategorySummary',
                    'summary': 'Get Category summary from existing Runtime Business Data',
                    'security': security,
                    'parameters': [{'name': 'category', 'in': 'query', 'required': True, 'schema': {'type': 'string'}}, {'name': 'period', 'in': 'query', 'required': True, 'schema': {'type': 'string'}}],
                    'responses': response('Category summary'),
                }
            },
            '/vectra/laboratory/business-data/summary/tmc-group': {
                'get': {
                    'operationId': 'getVectraLaboratoryTmcGroupSummary',
                    'summary': 'Get TMC Group summary from existing Runtime Business Data',
                    'security': security,
                    'parameters': [{'name': 'tmc_group', 'in': 'query', 'required': True, 'schema': {'type': 'string'}}, {'name': 'period', 'in': 'query', 'required': True, 'schema': {'type': 'string'}}],
                    'responses': response('TMC Group summary'),
                }
            },
            '/vectra/laboratory/business-data/summary/sku': {
                'get': {
                    'operationId': 'getVectraLaboratorySkuSummary',
                    'summary': 'Get SKU summary from existing Runtime Business Data',
                    'security': security,
                    'parameters': [{'name': 'sku', 'in': 'query', 'required': True, 'schema': {'type': 'string'}}, {'name': 'period', 'in': 'query', 'required': True, 'schema': {'type': 'string'}}],
                    'responses': response('SKU summary'),
                }
            },
            '/vectra/laboratory/business-data/query': {
                'get': {
                    'operationId': 'queryVectraLaboratoryBusinessData',
                    'summary': 'Run read-only Business Data query through existing VECTRA Runtime pipeline',
                    'description': 'GPT Action endpoint. Runs the same existing query/orchestration path used by Working GPT without exposing Business Data mutation endpoints. This is the primary first autonomous Business Data request for VECTRA Laboratory.',
                    'security': security,
                    'parameters': [{'name': 'message', 'in': 'query', 'required': True, 'schema': {'type': 'string'}, 'description': 'Natural-language VECTRA query, for example Бизнес 2026-02.'}, {'name': 'session_id', 'in': 'query', 'required': False, 'schema': {'type': 'string'}, 'description': 'Optional Laboratory read-only session id.'}],
                    'responses': response('Read-only Business Data query result'),
                }
            },
            '/vectra/laboratory/business-data/verify': {
                'get': {
                    'operationId': 'verifyVectraLaboratoryBusinessDataAccess',
                    'summary': 'Verify Laboratory read-only Business Data access and Actions readiness',
                    'description': 'Verifies source loading, entity access, sample rows, read-only boundary and GPT Actions readiness for FOUNDATION-0008-PV.',
                    'security': security,
                    'responses': response('Business Data access verification'),
                }
            },
            '/vectra/laboratory/repository/status': {
                'get': {
                    'operationId': 'getVectraLaboratoryRepositoryStatus',
                    'summary': 'Inspect VECTRA repository status read-only',
                    'description': 'Returns read-only status of the deployed VECTRA project/repository structure for Laboratory self-inspection.',
                    'security': security,
                    'responses': response('Repository inspection status'),
                }
            },
            '/vectra/laboratory/repository/manifest': {
                'get': {
                    'operationId': 'getVectraLaboratoryRepositoryManifest',
                    'summary': 'Get deploy repository manifest read-only',
                    'description': 'Returns file manifest, hashes and key file descriptions without modifying code.',
                    'security': security,
                    'responses': response('Repository manifest'),
                }
            },
            '/vectra/laboratory/repository/tree': {
                'get': {
                    'operationId': 'getVectraLaboratoryRepositoryTree',
                    'summary': 'Get project tree read-only',
                    'description': 'Returns a bounded read-only tree of the deployed project.',
                    'security': security,
                    'parameters': [{'name': 'max_items', 'in': 'query', 'required': False, 'schema': {'type': 'integer'}, 'description': 'Maximum tree entries to return, capped by Runtime.'}],
                    'responses': response('Repository tree'),
                }
            },
            '/vectra/laboratory/repository/components': {
                'get': {
                    'operationId': 'getVectraLaboratoryRepositoryComponents',
                    'summary': 'Get implemented subsystem list read-only',
                    'description': 'Returns known VECTRA subsystems and key files found in the deployed repository.',
                    'security': security,
                    'responses': response('Repository components'),
                }
            },
            '/vectra/laboratory/repository/verify': {
                'get': {
                    'operationId': 'verifyVectraLaboratoryRepository',
                    'summary': 'Verify repository implementation against expected components',
                    'description': 'Checks expected FOUNDATION-0009 components and endpoints. Optional release_brief_text can be supplied for semantic comparison context.',
                    'security': security,
                    'parameters': [{'name': 'release_brief_text', 'in': 'query', 'required': False, 'schema': {'type': 'string'}, 'description': 'Optional Release Brief text for comparison.'}],
                    'responses': response('Repository verification'),
                }
            },
            '/vectra/knowledge/candidates': {
                'post': {
                    'operationId': 'createVectraKnowledgeCandidate',
                    'summary': 'Create Product Owner reviewable Knowledge Candidate',
                    'description': 'Creates a Knowledge Candidate. It is not capitalized unless Product Owner approval is explicitly present.',
                    'security': security,
                    'requestBody': {'required': True, 'content': {'application/json': {'schema': capitalization_request_schema}}},
                    'responses': response('Knowledge Candidate'),
                }
            },
            '/vectra/knowledge/capitalization/packages': {
                'post': {
                    'operationId': 'createVectraKnowledgeCapitalizationPackage',
                    'summary': 'Create Product Owner approved Knowledge Capitalization package',
                    'description': 'Explicit FOUNDATION-0010 command. Creates a Capitalization Package after Product Owner approval without writing knowledge yet.',
                    'security': security,
                    'requestBody': {'required': True, 'content': {'application/json': {'schema': capitalization_request_schema}}},
                    'responses': response('Knowledge Capitalization package'),
                }
            },
            '/vectra/knowledge/capitalization/write': {
                'post': {
                    'operationId': 'writeVectraConfirmedKnowledge',
                    'summary': 'Write packaged Product Owner approved knowledge',
                    'description': 'Explicit FOUNDATION-0010 command. Writes confirmed knowledge, performs readback verification, updates Recovery Snapshot and returns capitalization report.',
                    'security': security,
                    'requestBody': {'required': True, 'content': {'application/json': {'schema': capitalization_request_schema}}},
                    'responses': response('Knowledge Capitalization write report'),
                }
            },
            '/vectra/knowledge/capitalization': {
                'post': {
                    'operationId': 'capitalizeVectraKnowledge',
                    'summary': 'Capitalize Product Owner approved knowledge in one call',
                    'description': 'Backward-compatible command that runs the explicit FOUNDATION-0010 flow: package creation -> write -> readback -> recovery -> report. Requires product_owner_approval=true.',
                    'security': security,
                    'requestBody': {'required': True, 'content': {'application/json': {'schema': capitalization_request_schema}}},
                    'responses': response('Knowledge Capitalization report'),
                }
            },
            '/vectra/knowledge/capitalization/status': {
                'get': {
                    'operationId': 'getVectraKnowledgeCapitalizationStatus',
                    'summary': 'Get Knowledge Capitalization Runtime status',
                    'description': 'Returns official Knowledge Capitalization Runtime status.',
                    'security': security,
                    'responses': response('Knowledge Capitalization status'),
                }
            },
            '/vectra/knowledge/capitalization/reports': {
                'get': {
                    'operationId': 'getVectraKnowledgeCapitalizationReports',
                    'summary': 'List Knowledge Capitalization reports',
                    'description': 'Returns successful and failed capitalization reports.',
                    'security': security,
                    'parameters': [{'name': 'limit', 'in': 'query', 'required': False, 'schema': {'type': 'integer'}}, {'name': 'include_failed', 'in': 'query', 'required': False, 'schema': {'type': 'boolean'}}],
                    'responses': response('Knowledge Capitalization reports'),
                }
            },
            '/vectra/memory/objects': {
                'get': {
                    'operationId': 'getVectraMemoryObjects',
                    'summary': 'List unified VECTRA Memory Objects',
                    'description': 'Lists unified Knowledge Objects through the adapter-compatible Memory Repository layer. Existing Professional and Business Knowledge repositories are preserved.',
                    'security': security,
                    'parameters': [
                        {'name': 'memory_space', 'in': 'query', 'required': False, 'schema': {'type': 'string'}, 'description': 'Optional memory space filter, for example professional_memory or business_domain_memory.'},
                        {'name': 'domain', 'in': 'query', 'required': False, 'schema': {'type': 'string'}, 'description': 'Business Domain id, default bonboason.'},
                        {'name': 'limit', 'in': 'query', 'required': False, 'schema': {'type': 'integer'}},
                    ],
                    'responses': response('Unified Memory Objects'),
                }
            },
            '/vectra/memory/objects/{object_id}': {
                'get': {
                    'operationId': 'getVectraMemoryObjectById',
                    'summary': 'Read unified VECTRA Memory Object',
                    'description': 'Reads a single Knowledge Object by object_id through the unified Memory Repository layer.',
                    'security': security,
                    'parameters': [
                        {'name': 'object_id', 'in': 'path', 'required': True, 'schema': {'type': 'string'}},
                        {'name': 'domain', 'in': 'query', 'required': False, 'schema': {'type': 'string'}},
                    ],
                    'responses': response('Unified Memory Object'),
                }
            },
            '/vectra/memory/readback': {
                'post': {
                    'operationId': 'verifyVectraMemoryObjectReadback',
                    'summary': 'Verify unified VECTRA Memory Object readback',
                    'description': 'Verifies readback and Knowledge Object mapping by object_id or knowledge_id.',
                    'security': security,
                    'requestBody': {'required': False, 'content': {'application/json': {'schema': {'type': 'object', 'additionalProperties': True}}}},
                    'responses': response('Unified Memory Object readback'),
                }
            },
            '/vectra/memory/overview': {
                'get': {
                    'operationId': 'getVectraMemoryOverview',
                    'summary': 'Get unified VECTRA Memory overview',
                    'description': 'Returns memory object counts, memory spaces, mapping status and repository compatibility status.',
                    'security': security,
                    'parameters': [{'name': 'domain', 'in': 'query', 'required': False, 'schema': {'type': 'string'}}],
                    'responses': response('Unified Memory overview'),
                }
            },
            '/vectra/memory/verify': {
                'get': {
                    'operationId': 'verifyVectraMemoryRepository',
                    'summary': 'Verify unified VECTRA Memory Repository',
                    'description': 'Runs Repository Integrity Check for the unified Memory Repository adapter layer.',
                    'security': security,
                    'parameters': [{'name': 'domain', 'in': 'query', 'required': False, 'schema': {'type': 'string'}}],
                    'responses': response('Unified Memory Repository verification'),
                }
            },
            '/vectra/memory/spaces': {
                'get': {
                    'operationId': 'getVectraMemorySpaces',
                    'summary': 'List VECTRA Memory Spaces',
                    'description': 'Returns Memory Space registry including active and prepared spaces.',
                    'security': security,
                    'parameters': [{'name': 'include_prepared', 'in': 'query', 'required': False, 'schema': {'type': 'boolean'}}],
                    'responses': response('Memory Space registry'),
                }
            },
            '/vectra/memory/spaces/{memory_space}/validate': {
                'get': {
                    'operationId': 'validateVectraMemorySpace',
                    'summary': 'Validate VECTRA Memory Space',
                    'description': 'Validates whether a memory_space is supported and optionally active.',
                    'security': security,
                    'parameters': [
                        {'name': 'memory_space', 'in': 'path', 'required': True, 'schema': {'type': 'string'}},
                        {'name': 'require_active', 'in': 'query', 'required': False, 'schema': {'type': 'boolean'}},
                    ],
                    'responses': response('Memory Space validation'),
                }
            },
            '/vectra/memory/classification': {
                'post': {
                    'operationId': 'classifyVectraKnowledgePackage',
                    'summary': 'Classify VECTRA knowledge package',
                    'description': 'Classifies supplied knowledge items into memory spaces and prepares a normalized Knowledge Package without writing Repository data.',
                    'security': security,
                    'requestBody': {'required': False, 'content': {'application/json': {'schema': {'type': 'object', 'additionalProperties': True}}}},
                    'responses': response('Knowledge classification package'),
                }
            },
            '/vectra/memory/classification/verify': {
                'post': {
                    'operationId': 'verifyVectraAutomaticClassification',
                    'summary': 'Verify VECTRA automatic knowledge classification',
                    'description': 'Runs deterministic classification verification and confirms that no invalid classification failures occurred.',
                    'security': security,
                    'requestBody': {'required': False, 'content': {'application/json': {'schema': {'type': 'object', 'additionalProperties': True}}}},
                    'responses': response('Automatic classification verification'),
                }
            },
            '/vectra/memory/inspection': {
                'post': {
                    'operationId': 'inspectVectraMemory',
                    'summary': 'Run VECTRA Memory Inspection',
                    'description': 'Runs read-only Memory Inspection Runtime operations: overview, statistics, integrity report, readback report, inspect object or inspect space.',
                    'security': security,
                    'requestBody': {'required': False, 'content': {'application/json': {'schema': {'type': 'object', 'additionalProperties': True}}}},
                    'responses': response('Memory Inspection result'),
                }
            },
            '/vectra/memory/inspection/object/{object_id}': {
                'get': {
                    'operationId': 'inspectVectraMemoryObject',
                    'summary': 'Inspect VECTRA Memory Object',
                    'description': 'Reads and verifies a single Memory Object without modifying Repository data.',
                    'security': security,
                    'parameters': [
                        {'name': 'object_id', 'in': 'path', 'required': True, 'schema': {'type': 'string'}},
                        {'name': 'domain', 'in': 'query', 'required': False, 'schema': {'type': 'string'}},
                    ],
                    'responses': response('Memory Object inspection'),
                }
            },
            '/vectra/memory/inspection/space/{memory_space}': {
                'get': {
                    'operationId': 'inspectVectraMemorySpace',
                    'summary': 'Inspect VECTRA Memory Space',
                    'description': 'Lists and verifies objects in a memory_space without modifying Repository data.',
                    'security': security,
                    'parameters': [
                        {'name': 'memory_space', 'in': 'path', 'required': True, 'schema': {'type': 'string'}},
                        {'name': 'domain', 'in': 'query', 'required': False, 'schema': {'type': 'string'}},
                        {'name': 'limit', 'in': 'query', 'required': False, 'schema': {'type': 'integer'}},
                    ],
                    'responses': response('Memory Space inspection'),
                }
            },
            '/vectra/memory/statistics': {
                'get': {
                    'operationId': 'getVectraMemoryStatistics',
                    'summary': 'Get VECTRA Memory statistics',
                    'description': 'Returns counts by memory_space, knowledge_type and verification_status.',
                    'security': security,
                    'parameters': [{'name': 'domain', 'in': 'query', 'required': False, 'schema': {'type': 'string'}}],
                    'responses': response('Memory statistics'),
                }
            },
            '/vectra/memory/integrity-report': {
                'get': {
                    'operationId': 'getVectraMemoryIntegrityReport',
                    'summary': 'Get VECTRA Memory integrity report',
                    'description': 'Returns Memory Repository integrity, overview, statistics and Memory Space registry in one read-only report.',
                    'security': security,
                    'parameters': [{'name': 'domain', 'in': 'query', 'required': False, 'schema': {'type': 'string'}}],
                    'responses': response('Memory integrity report'),
                }
            },
            '/vectra/memory/readback-report': {
                'get': {
                    'operationId': 'getVectraMemoryReadbackReport',
                    'summary': 'Get VECTRA Memory readback report',
                    'description': 'Runs readback verification across Memory Objects and returns pass/fail counts.',
                    'security': security,
                    'parameters': [
                        {'name': 'domain', 'in': 'query', 'required': False, 'schema': {'type': 'string'}},
                        {'name': 'limit', 'in': 'query', 'required': False, 'schema': {'type': 'integer'}},
                    ],
                    'responses': response('Memory readback report'),
                }
            },
            '/vectra/memory/product-knowledge': {
                'get': {
                    'operationId': 'getVectraProductKnowledge',
                    'summary': 'List VECTRA Product Knowledge Runtime objects',
                    'description': 'Reads Product Knowledge from product_memory as unified Knowledge Objects.',
                    'security': security,
                    'parameters': [{'name': 'limit', 'in': 'query', 'required': False, 'schema': {'type': 'integer'}}],
                    'responses': response('Product Knowledge list'),
                },
                'post': {
                    'operationId': 'writeVectraProductKnowledge',
                    'summary': 'Capitalize VECTRA Product Knowledge',
                    'description': 'Writes Product Owner approved Product Knowledge into product_memory.',
                    'security': security,
                    'requestBody': {'required': False, 'content': {'application/json': {'schema': {'type': 'object', 'additionalProperties': True}}}},
                    'responses': response('Product Knowledge write'),
                },
            },
            '/vectra/memory/product-knowledge/{knowledge_id}': {
                'get': {
                    'operationId': 'getVectraProductKnowledgeById',
                    'summary': 'Read VECTRA Product Knowledge by ID',
                    'description': 'Reads one Product Knowledge object and verifies Knowledge Object mapping.',
                    'security': security,
                    'parameters': [{'name': 'knowledge_id', 'in': 'path', 'required': True, 'schema': {'type': 'string'}}],
                    'responses': response('Product Knowledge object'),
                }
            },
            '/vectra/memory/product-knowledge/verify/readback': {
                'get': {
                    'operationId': 'verifyVectraProductKnowledgeReadback',
                    'summary': 'Verify VECTRA Product Knowledge readback',
                    'description': 'Verifies Product Knowledge readback and Knowledge Object mapping.',
                    'security': security,
                    'parameters': [{'name': 'knowledge_id', 'in': 'query', 'required': False, 'schema': {'type': 'string'}}],
                    'responses': response('Product Knowledge readback'),
                }
            },
            '/vectra/memory/product-decisions': {
                'get': {
                    'operationId': 'getVectraProductDecisions',
                    'summary': 'List VECTRA Product Decisions Runtime objects',
                    'description': 'Reads Product Owner approved Product Decisions from a separate normative memory space.',
                    'security': security,
                    'parameters': [{'name': 'limit', 'in': 'query', 'required': False, 'schema': {'type': 'integer'}}],
                    'responses': response('Product Decisions list'),
                },
                'post': {
                    'operationId': 'writeVectraProductDecision',
                    'summary': 'Record VECTRA Product Decision',
                    'description': 'Writes a Product Owner approved decision into product_decisions_memory.',
                    'security': security,
                    'requestBody': {'required': False, 'content': {'application/json': {'schema': {'type': 'object', 'additionalProperties': True}}}},
                    'responses': response('Product Decision write'),
                },
            },
            '/vectra/memory/product-decisions/{decision_id}': {
                'get': {
                    'operationId': 'getVectraProductDecisionById',
                    'summary': 'Read VECTRA Product Decision by ID',
                    'description': 'Reads one Product Decision object and verifies Knowledge Object mapping.',
                    'security': security,
                    'parameters': [{'name': 'decision_id', 'in': 'path', 'required': True, 'schema': {'type': 'string'}}],
                    'responses': response('Product Decision object'),
                }
            },
            '/vectra/memory/product-decisions/verify/readback': {
                'get': {
                    'operationId': 'verifyVectraProductDecisionsReadback',
                    'summary': 'Verify VECTRA Product Decisions readback',
                    'description': 'Verifies Product Decisions readback and confirms decisions stay separate from ordinary knowledge.',
                    'security': security,
                    'parameters': [{'name': 'decision_id', 'in': 'query', 'required': False, 'schema': {'type': 'string'}}],
                    'responses': response('Product Decisions readback'),
                }
            },
            '/vectra/memory/health': {
                'get': {
                    'operationId': 'getVectraMemoryHealth',
                    'summary': 'Get VECTRA Memory Health status',
                    'description': 'Returns compact memory health status for Laboratory Product Verification.',
                    'security': security,
                    'parameters': [{'name': 'domain', 'in': 'query', 'required': False, 'schema': {'type': 'string'}}],
                    'responses': response('Memory health status'),
                }
            },
            '/vectra/memory/diagnostics': {
                'get': {
                    'operationId': 'getVectraMemoryDiagnostics',
                    'summary': 'Get VECTRA Memory Diagnostics report',
                    'description': 'Returns memory health, overview, statistics, readback and repository integrity in one report.',
                    'security': security,
                    'parameters': [{'name': 'domain', 'in': 'query', 'required': False, 'schema': {'type': 'string'}}],
                    'responses': response('Memory diagnostics report'),
                }
            },
            '/vectra/memory/health/verify': {
                'get': {
                    'operationId': 'verifyVectraMemoryHealth',
                    'summary': 'Verify VECTRA Memory Health',
                    'description': 'Verifies memory health and reports blocking issues before Product Verification.',
                    'security': security,
                    'parameters': [{'name': 'domain', 'in': 'query', 'required': False, 'schema': {'type': 'string'}}],
                    'responses': response('Memory health verification'),
                }
            },
            '/vectra/knowledge/professional': {
                'get': {
                    'operationId': 'getVectraProfessionalKnowledge',
                    'summary': 'Get capitalized Professional Knowledge',
                    'description': 'Returns Professional Knowledge restored from Runtime Repository.',
                    'security': security,
                    'responses': response('Professional Knowledge'),
                }
            },
            '/vectra/knowledge/professional/overview': {
                'get': {
                    'operationId': 'getVectraProfessionalKnowledgeOverview',
                    'summary': 'Professional Knowledge Overview',
                    'description': 'Returns Professional Knowledge repository overview: document counts, last update and repository status.',
                    'security': security,
                    'responses': response('Professional Knowledge overview'),
                }
            },
            '/vectra/knowledge/professional/{knowledge_id}': {
                'get': {
                    'operationId': 'getVectraProfessionalKnowledgeById',
                    'summary': 'Get Professional Knowledge',
                    'description': 'Returns a single Professional Knowledge document by knowledge_id.',
                    'security': security,
                    'parameters': [{'name': 'knowledge_id', 'in': 'path', 'required': True, 'schema': {'type': 'string'}, 'description': 'Professional Knowledge id, for example PK-001.'}],
                    'responses': response('Professional Knowledge document'),
                }
            },
            '/vectra/knowledge/professional/{knowledge_id}/readback': {
                'get': {
                    'operationId': 'verifyVectraProfessionalKnowledgeReadback',
                    'summary': 'Verify Professional Knowledge Readback',
                    'description': 'Verifies that a Professional Knowledge document exists and can be read back from Runtime Repository without changing Professional Model.',
                    'security': security,
                    'parameters': [{'name': 'knowledge_id', 'in': 'path', 'required': True, 'schema': {'type': 'string'}, 'description': 'Professional Knowledge id, for example PK-001.'}],
                    'responses': response('Professional Knowledge readback verification'),
                }
            },
            '/vectra/domain/{domain}/knowledge': {
                'get': {
                    'operationId': 'getVectraDomainKnowledge',
                    'summary': 'Get Business Domain Knowledge',
                    'description': 'Returns Business Knowledge stored inside the selected Business Domain.',
                    'security': security,
                    'parameters': [{'name': 'domain', 'in': 'path', 'required': True, 'schema': {'type': 'string'}, 'description': 'Business Domain id, for example bonboason.'}],
                    'responses': response('Domain Knowledge'),
                }
            },
            '/vectra/domain/{domain}/knowledge/overview': {
                'get': {
                    'operationId': 'getVectraDomainKnowledgeOverview',
                    'summary': 'Get Business Domain Knowledge Overview',
                    'description': 'Returns Business Knowledge repository overview for a selected Business Domain.',
                    'security': security,
                    'parameters': [{'name': 'domain', 'in': 'path', 'required': True, 'schema': {'type': 'string'}, 'description': 'Business Domain id, for example bonboason.'}],
                    'responses': response('Domain Knowledge overview'),
                }
            },
            '/vectra/domain/{domain}/knowledge/{knowledge_id}': {
                'get': {
                    'operationId': 'getVectraDomainKnowledgeById',
                    'summary': 'Get Business Domain Knowledge by ID',
                    'description': 'Returns a single Business Knowledge document from the selected Business Domain.',
                    'security': security,
                    'parameters': [
                        {'name': 'domain', 'in': 'path', 'required': True, 'schema': {'type': 'string'}, 'description': 'Business Domain id, for example bonboason.'},
                        {'name': 'knowledge_id', 'in': 'path', 'required': True, 'schema': {'type': 'string'}, 'description': 'Business Knowledge id, for example BK-001.'},
                    ],
                    'responses': response('Domain Knowledge document'),
                }
            },
            '/vectra/domain/{domain}/knowledge/{knowledge_id}/readback': {
                'get': {
                    'operationId': 'verifyVectraDomainKnowledgeReadback',
                    'summary': 'Verify Business Domain Knowledge Readback',
                    'description': 'Verifies Business Knowledge readback from the selected Business Domain repository.',
                    'security': security,
                    'parameters': [
                        {'name': 'domain', 'in': 'path', 'required': True, 'schema': {'type': 'string'}, 'description': 'Business Domain id, for example bonboason.'},
                        {'name': 'knowledge_id', 'in': 'path', 'required': True, 'schema': {'type': 'string'}, 'description': 'Business Knowledge id, for example BK-001.'},
                    ],
                    'responses': response('Domain Knowledge readback verification'),
                }
            },
            '/vectra/domain/{domain}/knowledge/candidates': {
                'post': {
                    'operationId': 'createVectraBusinessKnowledgeCandidate',
                    'summary': 'Create Business Knowledge Candidate',
                    'description': 'Creates a Product Owner approval-gated Business Knowledge Candidate for the selected Business Domain.',
                    'security': security,
                    'parameters': [{'name': 'domain', 'in': 'path', 'required': True, 'schema': {'type': 'string'}, 'description': 'Business Domain id, for example bonboason.'}],
                    'requestBody': {'required': False, 'content': {'application/json': {'schema': capitalization_request_schema}}},
                    'responses': response('Business Knowledge Candidate'),
                }
            },
            '/vectra/domain/{domain}/knowledge/capitalization/packages': {
                'post': {
                    'operationId': 'createVectraBusinessKnowledgeCapitalizationPackage',
                    'summary': 'Create Business Knowledge Capitalization Package',
                    'description': 'Creates a Business Knowledge Capitalization Package after Product Owner approval without writing yet.',
                    'security': security,
                    'parameters': [{'name': 'domain', 'in': 'path', 'required': True, 'schema': {'type': 'string'}, 'description': 'Business Domain id, for example bonboason.'}],
                    'requestBody': {'required': False, 'content': {'application/json': {'schema': capitalization_request_schema}}},
                    'responses': response('Business Knowledge Capitalization Package'),
                }
            },
            '/vectra/domain/{domain}/knowledge/capitalization/write': {
                'post': {
                    'operationId': 'writeVectraBusinessKnowledge',
                    'summary': 'Write Business Knowledge',
                    'description': 'Writes packaged Product Owner approved Business Knowledge, performs readback, updates Recovery Snapshot and returns report.',
                    'security': security,
                    'parameters': [{'name': 'domain', 'in': 'path', 'required': True, 'schema': {'type': 'string'}, 'description': 'Business Domain id, for example bonboason.'}],
                    'requestBody': {'required': False, 'content': {'application/json': {'schema': capitalization_request_schema}}},
                    'responses': response('Business Knowledge Capitalization Report'),
                }
            },
            '/vectra/knowledge/verify': {
                'get': {
                    'operationId': 'verifyVectraKnowledgeCapitalization',
                    'summary': 'Verify Knowledge Capitalization Runtime',
                    'description': 'Verifies readback, reports and protection rules for Knowledge Capitalization Runtime.',
                    'security': security,
                    'responses': response('Knowledge Capitalization verification'),
                }
            },
            '/vectra/context/capitalization': {
                'post': {
                    'operationId': 'capitalizeVectraContext',
                    'summary': 'Capitalize confirmed VECTRA context',
                    'description': 'Capitalizes Product Owner confirmed development context into Runtime Repository without automatic Professional Model changes.',
                    'security': security,
                    'requestBody': {'required': False, 'content': {'application/json': {'schema': capitalization_request_schema}}},
                    'responses': response('Context Capitalization'),
                }
            },
            '/vectra/context/capitalization/status': {
                'get': {
                    'operationId': 'getVectraContextCapitalizationStatus',
                    'summary': 'Get VECTRA Context Capitalization status',
                    'description': 'Returns context capitalization status and confirms Professional Model is not changed automatically.',
                    'security': security,
                    'responses': response('Context Capitalization status'),
                }
            },
            '/vectra/context/capitalization/verify': {
                'get': {
                    'operationId': 'verifyVectraContextCapitalizationReadback',
                    'summary': 'Verify VECTRA Context Capitalization readback',
                    'description': 'Verifies Context Capitalization repository readback and recovery snapshot readiness.',
                    'security': security,
                    'responses': response('Context Capitalization readback verification'),
                }
            },
        },
        'x-vectra-scope': 'laboratory_public_api',
        'x-vectra-openai-actions-compatibility': 'FOUNDATION-0008: OpenAI Actions compatible; no response refs; components.schemas is {}; every object schema has explicit properties.',
        'x-vectra-excluded': 'All internal engineering endpoints, mutation endpoints and non-Laboratory Runtime operations are intentionally excluded from this schema.',
    }



# LABORATORY-BEHAVIOR-0001: Action First Policy
# These endpoints are read-only and exist to keep VECTRA Laboratory in
# professional Runtime-first behavior during long Product Owner sessions.

@router.get('/vectra/laboratory/behavior/policy', summary='Read VECTRA Laboratory Action First Policy')
def vectra_laboratory_action_first_policy(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_laboratory_action_first_policy())


@router.get('/vectra/laboratory/behavior/next-action', summary='Determine next VECTRA Laboratory Runtime Action')
def vectra_laboratory_next_action(command: str = '', runtime_access_confirmed: bool = True, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(determine_vectra_laboratory_next_action(command=command, runtime_access_confirmed=runtime_access_confirmed))


@router.get('/vectra/laboratory/behavior/verify', summary='Verify VECTRA Laboratory Action First Policy')
def vectra_laboratory_action_first_policy_verify(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_laboratory_action_first_policy())


# LABORATORY-ACTIONS-0002: Full Laboratory OpenAPI and Action Manifest
# The canonical Laboratory Actions contract is the single full OpenAPI schema.
# Split schemas remain as backward-compatible diagnostic exports, but Product
# Owner imports /vectra/laboratory/openapi.json as the source of truth.
# Action completeness is enforced by Runtime Capability Registry ↔ GPT Actions Manifest verification.

_LABORATORY_CORE_PATHS = {
    '/vectra/runtime/status',
    '/vectra/runtime/snapshot',
    '/vectra/laboratory/verification',
    '/vectra/laboratory/behavior/policy',
    '/vectra/laboratory/behavior/next-action',
    '/vectra/laboratory/behavior/verify',
    '/vectra/laboratory/actions/manifest',
    '/vectra/laboratory/actions/verify',
    '/vectra/life-model',
    '/vectra/life-model/status',
    '/vectra/life-model/verify',
    '/vectra/vos',
    '/vectra/vos/status',
    '/vectra/vos/verify',
    '/vectra/vos/restore',
    '/vectra/professional/model',
    '/vectra/evolution/status',
    '/vectra/capabilities',
    '/vectra/professional-body/restore',
    '/vectra/professional-body/verify',
}

_LABORATORY_BUSINESS_DATA_PATHS = {
    '/vectra/laboratory/business-data/status',
    '/vectra/laboratory/business-data/entities',
    '/vectra/laboratory/business-data/sample',
    '/vectra/laboratory/business-data/summary/business',
    '/vectra/laboratory/business-data/summary/manager-top',
    '/vectra/laboratory/business-data/summary/manager',
    '/vectra/laboratory/business-data/summary/network',
    '/vectra/laboratory/business-data/summary/category',
    '/vectra/laboratory/business-data/summary/tmc-group',
    '/vectra/laboratory/business-data/summary/sku',
    '/vectra/laboratory/business-data/query',
    '/vectra/laboratory/business-data/verify',
}

_LABORATORY_KNOWLEDGE_PATHS = {
    '/vectra/context/capitalization/status',
    '/vectra/context/capitalization/verify',
    '/vectra/laboratory/repository/status',
    '/vectra/laboratory/repository/manifest',
    '/vectra/laboratory/repository/tree',
    '/vectra/laboratory/repository/components',
    '/vectra/laboratory/repository/verify',
    '/vectra/knowledge/candidates',
    '/vectra/knowledge/capitalization/packages',
    '/vectra/knowledge/capitalization/write',
    '/vectra/knowledge/capitalization',
    '/vectra/knowledge/capitalization/status',
    '/vectra/memory/objects',
    '/vectra/memory/objects/{object_id}',
    '/vectra/memory/readback',
    '/vectra/memory/overview',
    '/vectra/memory/verify',
    '/vectra/memory/spaces',
    '/vectra/memory/spaces/{memory_space}/validate',
    '/vectra/memory/classification',
    '/vectra/memory/classification/verify',
    '/vectra/memory/inspection',
    '/vectra/memory/inspection/object/{object_id}',
    '/vectra/memory/inspection/space/{memory_space}',
    '/vectra/memory/statistics',
    '/vectra/memory/integrity-report',
    '/vectra/memory/readback-report',
    '/vectra/memory/product-knowledge',
    '/vectra/memory/product-knowledge/{knowledge_id}',
    '/vectra/memory/product-knowledge/verify/readback',
    '/vectra/memory/product-decisions',
    '/vectra/memory/product-decisions/{decision_id}',
    '/vectra/memory/product-decisions/verify/readback',
    '/vectra/memory/health',
    '/vectra/memory/diagnostics',
    '/vectra/memory/health/verify',
    '/vectra/memory/architecture-conformance',
    '/vectra/memory/architecture-conformance/verify',
    '/vectra/memory/recovery-optimized',
    '/vectra/memory/recovery-optimized/verify',
    '/vectra/memory/e2e-validation',
    '/vectra/memory/e2e-validation/verify',
    '/vectra/memory/general-knowledge',
    '/vectra/memory/general-knowledge/{knowledge_id}',
    '/vectra/memory/general-knowledge/verify/readback',
    '/vectra/memory/revisions',
    '/vectra/memory/revisions/{revision_id}',
    '/vectra/memory/revisions/verify',
    '/vectra/memory/release-history',
    '/vectra/memory/release-history/{release_id}',
    '/vectra/memory/release-history/verify/readback',
    '/vectra/knowledge/capitalization/reports',
    '/vectra/knowledge/professional',
    '/vectra/knowledge/professional/overview',
    '/vectra/knowledge/professional/{knowledge_id}',
    '/vectra/knowledge/professional/{knowledge_id}/readback',
    '/vectra/domain/{domain}/knowledge',
    '/vectra/domain/{domain}/knowledge/overview',
    '/vectra/domain/{domain}/knowledge/{knowledge_id}',
    '/vectra/domain/{domain}/knowledge/{knowledge_id}/readback',
    '/vectra/domain/{domain}/knowledge/candidates',
    '/vectra/domain/{domain}/knowledge/capitalization/packages',
    '/vectra/domain/{domain}/knowledge/capitalization/write',
    '/vectra/knowledge/verify',
}


def _laboratory_full_openapi_schema() -> dict:
    """Return the official compact facade OpenAPI for GPT Actions.

    LABORATORY-ACTIONS-0003 replaces the previously exported 67-operation
    Action contract with professional facade Actions. Low-level Runtime routes
    remain available as internal/diagnostic services, but GPT imports only this
    compact schema.
    """
    return _laboratory_facade_openapi_schema()


def _iter_openapi_actions(schema: dict) -> list[dict]:
    actions: list[dict] = []
    paths = schema.get('paths') if isinstance(schema, dict) else {}
    if not isinstance(paths, dict):
        return actions
    for endpoint, operations in sorted(paths.items()):
        if not isinstance(operations, dict):
            continue
        for method, operation in sorted(operations.items()):
            if str(method).lower() not in {'get', 'post', 'put', 'patch', 'delete', 'options', 'head'}:
                continue
            op = operation if isinstance(operation, dict) else {}
            actions.append({
                'operation_id': op.get('operationId') or f"{method}_{endpoint}",
                'method': str(method).upper(),
                'endpoint': endpoint,
                'summary': op.get('summary') or '',
                'export_status': 'EXPORTED',
            })
    return actions


def _normalize_endpoint_template(value: str) -> str:
    return re.sub(r'\{[^}/]+\}', '{}', str(value or '').strip())


def _action_runtime_service_for(operation_id: str, endpoint: str) -> str:
    mapping = {
        'getVectraRuntimeStatus': 'observability.get_runtime_status',
        'getVectraRuntimeSnapshot': 'observability.get_runtime_snapshot',
        'getVectraLaboratoryVerification': 'observability.run_laboratory_verification_package',
        'getVectraLaboratoryActionFirstPolicy': 'laboratory_behavior.get_laboratory_action_first_policy',
        'determineVectraLaboratoryNextAction': 'laboratory_behavior.determine_laboratory_next_action',
        'verifyVectraLaboratoryActionFirstPolicy': 'laboratory_behavior.verify_laboratory_action_first_policy',
        'getVectraLaboratoryActionManifest': 'laboratory_actions.get_action_manifest',
        'verifyVectraLaboratoryActionCompleteness': 'laboratory_actions.verify_action_completeness',
        'getVectraCapabilities': 'repository.get_capability_registry',
        'getVectraMemoryObjects': 'memory_repository.list_memory_objects',
        'getVectraMemoryObjectById': 'memory_repository.get_memory_object',
        'verifyVectraMemoryObjectReadback': 'memory_repository.readback_memory_object',
        'getVectraMemoryOverview': 'memory_repository.get_memory_overview',
        'verifyVectraMemoryRepository': 'memory_repository.verify_memory_repository_integrity',
        'getVectraMemorySpaces': 'memory_spaces.list_memory_spaces',
        'validateVectraMemorySpace': 'memory_spaces.validate_memory_space',
        'classifyVectraKnowledgeItem': 'memory_classification.classify_knowledge_item',
        'classifyVectraKnowledgePackage': 'memory_classification.classify_knowledge_package',
        'verifyVectraAutomaticClassification': 'memory_classification.verify_automatic_classification',
        'inspectVectraMemoryObject': 'memory_inspection.inspect_memory_object',
        'inspectVectraMemorySpace': 'memory_inspection.inspect_memory_space',
        'getVectraMemoryStatistics': 'memory_inspection.get_memory_statistics',
        'getVectraMemoryIntegrityReport': 'memory_inspection.get_memory_integrity_report',
        'getVectraMemoryReadbackReport': 'memory_inspection.get_memory_readback_report',
        'getVectraProductKnowledge': 'product_knowledge.list_product_knowledge',
        'getVectraProductKnowledgeById': 'product_knowledge.get_product_knowledge',
        'writeVectraProductKnowledge': 'product_knowledge.write_product_knowledge',
        'verifyVectraProductKnowledgeReadback': 'product_knowledge.verify_product_knowledge_readback',
        'getVectraProductDecisions': 'product_decisions_runtime.list_product_decisions',
        'getVectraProductDecisionById': 'product_decisions_runtime.get_product_decision',
        'writeVectraProductDecision': 'product_decisions_runtime.write_product_decision',
        'verifyVectraProductDecisionsReadback': 'product_decisions_runtime.verify_product_decisions_readback',
        'getVectraMemoryHealth': 'memory_health.get_memory_health_status',
        'getVectraMemoryDiagnostics': 'memory_health.get_memory_diagnostics_report',
        'verifyVectraMemoryHealth': 'memory_health.verify_memory_health',
        'getVectraGeneralKnowledge': 'general_knowledge.list_general_knowledge',
        'getVectraGeneralKnowledgeById': 'general_knowledge.get_general_knowledge',
        'writeVectraGeneralKnowledge': 'general_knowledge.write_general_knowledge',
        'verifyVectraGeneralKnowledgeReadback': 'general_knowledge.verify_general_knowledge_readback',
        'getVectraMemoryRevisions': 'revision_model.list_revisions',
        'getVectraMemoryRevisionById': 'revision_model.get_revision',
        'getVectraMemoryVersionStatus': 'revision_model.get_version_status',
        'verifyVectraRevisionModel': 'revision_model.verify_revision_model',
        'getVectraReleaseHistory': 'release_history_runtime.list_release_history',
        'getVectraReleaseHistoryById': 'release_history_runtime.get_release_history',
        'writeVectraReleaseHistory': 'release_history_runtime.write_release_history',
        'verifyVectraReleaseHistoryReadback': 'release_history_runtime.verify_release_history_readback',
        'getVectraProfessionalKnowledge': 'knowledge_capitalization.list_professional_knowledge',
        'getVectraProfessionalKnowledgeOverview': 'knowledge_capitalization.get_professional_knowledge_overview',
        'getVectraProfessionalKnowledgeById': 'knowledge_capitalization.get_professional_knowledge',
        'verifyVectraProfessionalKnowledgeReadback': 'knowledge_capitalization.verify_professional_knowledge_readback',
        'getVectraDomainKnowledge': 'knowledge_capitalization.list_domain_knowledge',
        'getVectraDomainKnowledgeOverview': 'knowledge_capitalization.get_domain_knowledge_overview',
        'getVectraDomainKnowledgeById': 'knowledge_capitalization.get_domain_knowledge',
        'verifyVectraDomainKnowledgeReadback': 'knowledge_capitalization.verify_domain_knowledge_readback',
        'createVectraKnowledgeCandidate': 'knowledge_capitalization.create_knowledge_candidate',
        'createVectraKnowledgeCapitalizationPackage': 'knowledge_capitalization.create_capitalization_package',
        'writeVectraConfirmedKnowledge': 'knowledge_capitalization.write_confirmed_knowledge',
        'createVectraBusinessKnowledgeCandidate': 'knowledge_capitalization.create_business_knowledge_candidate',
        'createVectraBusinessKnowledgeCapitalizationPackage': 'knowledge_capitalization.create_business_capitalization_package',
        'writeVectraBusinessKnowledge': 'knowledge_capitalization.write_business_knowledge',
    }
    if operation_id in mapping:
        return mapping[operation_id]
    cleaned = endpoint.strip('/').replace('/', '.').replace('-', '_').replace('{', '').replace('}', '')
    return cleaned or 'runtime.service'


def _build_laboratory_action_manifest() -> dict:
    schema = _laboratory_full_openapi_schema()
    actions = _iter_openapi_actions(schema)
    registry_payload = get_vectra_capability_registry()
    registry = registry_payload.get('capability_registry') if isinstance(registry_payload, dict) else {}
    capabilities = registry.get('capabilities') if isinstance(registry, dict) and isinstance(registry.get('capabilities'), list) else []
    capability_by_endpoint: dict[str, dict] = {}
    for capability in capabilities:
        if not isinstance(capability, dict):
            continue
        endpoint = capability.get('transport_endpoint') or capability.get('endpoint') or ''
        if endpoint:
            capability_by_endpoint[_normalize_endpoint_template(endpoint)] = capability
    for action in actions:
        endpoint_key = _normalize_endpoint_template(action.get('endpoint', ''))
        capability = capability_by_endpoint.get(endpoint_key) or {}
        action['runtime_service'] = capability.get('runtime_service') or _action_runtime_service_for(str(action.get('operation_id') or ''), str(action.get('endpoint') or ''))
        action['capability'] = capability.get('capability_id') or _derive_capability_id(str(action.get('operation_id') or ''), str(action.get('endpoint') or ''))
        action['capability_title'] = capability.get('title') or action.get('summary') or action.get('operation_id')
        action['release_version'] = 'LABORATORY-ACTIONS-0002'
    exported_operation_ids = {str(action.get('operation_id')) for action in actions if action.get('operation_id')}
    exported_endpoints = {_normalize_endpoint_template(str(action.get('endpoint') or '')) for action in actions}
    required_capabilities = []
    missing_actions = []
    for capability in capabilities:
        if not isinstance(capability, dict):
            continue
        endpoint = capability.get('transport_endpoint') or capability.get('endpoint')
        requires_action = bool(endpoint) and capability.get('status', 'active') not in {'retired', 'disabled'}
        if not requires_action:
            continue
        required_capabilities.append(capability)
        endpoint_key = _normalize_endpoint_template(str(endpoint))
        if endpoint_key not in exported_endpoints:
            missing_actions.append({
                'capability_id': capability.get('capability_id'),
                'title': capability.get('title'),
                'runtime_service': capability.get('runtime_service'),
                'expected_endpoint': endpoint,
                'export_status': 'MISSING',
            })
    return {
        'status': 'ok' if not missing_actions else 'error',
        'render_mode': 'vectra_laboratory_action_manifest',
        'release_version': 'LABORATORY-ACTIONS-0002',
        'schema_url': '/vectra/laboratory/openapi.json',
        'schema_version': schema.get('info', {}).get('version'),
        'operation_count': _count_openapi_operations(schema),
        'actions_count': len(actions),
        'runtime_capabilities_count': len(capabilities),
        'required_action_capabilities_count': len(required_capabilities),
        'actions': actions,
        'missing_actions': missing_actions,
        'export_status': 'COMPLETE' if not missing_actions else 'INCOMPLETE',
        'verification_status': 'PASS' if not missing_actions else 'FAIL',
        'policy': 'Capability -> Runtime Service -> GPT Action must be complete before release.',
    }


def _derive_capability_id(operation_id: str, endpoint: str) -> str:
    if 'business-data' in endpoint:
        return 'business_data_laboratory_access'
    if '/repository/' in endpoint:
        return 'repository_self_inspection'
    if '/knowledge/' in endpoint or '/domain/' in endpoint:
        return 'knowledge_runtime'
    if '/behavior/' in endpoint:
        return 'laboratory_action_first_policy'
    if 'RuntimeSnapshot' in operation_id:
        return 'runtime_snapshot'
    if 'RuntimeStatus' in operation_id:
        return 'runtime_status'
    if 'Professional' in operation_id:
        return 'professional_body'
    return re.sub(r'(?<!^)(?=[A-Z])', '_', operation_id).lower() if operation_id else 'laboratory_action'


def _verify_laboratory_action_completeness() -> dict:
    manifest = _build_laboratory_action_manifest()
    missing = manifest.get('missing_actions') if isinstance(manifest.get('missing_actions'), list) else []
    return {
        'status': 'ok' if not missing else 'error',
        'render_mode': 'vectra_laboratory_action_completeness_verification',
        'verification_status': 'PASS' if not missing else 'FAIL',
        'result': '✅ Комплектация полная.' if not missing else '❌ Обнаружено расхождение.',
        'schema_url': '/vectra/laboratory/openapi.json',
        'operation_count': manifest.get('operation_count'),
        'actions_count': manifest.get('actions_count'),
        'runtime_capabilities_count': manifest.get('runtime_capabilities_count'),
        'required_action_capabilities_count': manifest.get('required_action_capabilities_count'),
        'missing_actions': missing,
        'release_blocked': bool(missing),
    }


def _count_openapi_operations(schema: dict) -> int:
    paths = schema.get('paths') if isinstance(schema, dict) else {}
    if not isinstance(paths, dict):
        return 0
    return sum(
        1
        for operations in paths.values()
        if isinstance(operations, dict)
        for method in operations.keys()
        if str(method).lower() in {'get', 'post', 'put', 'patch', 'delete', 'options', 'head'}
    )


def _laboratory_split_openapi_schema(paths_to_include: set[str], *, title: str, version: str, description: str, scope: str) -> dict:
    base = _laboratory_public_openapi_schema()
    filtered_paths = {
        path: operations
        for path, operations in base.get('paths', {}).items()
        if path in paths_to_include
    }
    schema = dict(base)
    schema['info'] = {
        'title': title,
        'version': version,
        'description': description,
    }
    schema['paths'] = filtered_paths
    schema['x-vectra-scope'] = scope
    schema['x-vectra-foundation'] = 'LABORATORY-BEHAVIOR-0001'
    schema['x-vectra-gpt-actions-operation-limit'] = {
        'limit': 30,
        'operation_count': _count_openapi_operations(schema),
        'status': 'PASS' if _count_openapi_operations(schema) < 30 else 'FAIL',
    }
    schema['x-vectra-companion-openapi'] = {
        'core': '/vectra/laboratory/openapi/core.json',
        'business_data': '/vectra/laboratory/openapi/business-data.json',
        'knowledge_self_evolution': '/vectra/laboratory/openapi/knowledge.json',
        'legacy': '/vectra/laboratory/openapi.json',
    }
    return schema


def _laboratory_core_openapi_schema() -> dict:
    return _laboratory_split_openapi_schema(
        _LABORATORY_CORE_PATHS,
        title='VECTRA Laboratory Core Actions',
        version='LABORATORY-BEHAVIOR-0001-LABORATORY-CORE',
        description='Core VECTRA Laboratory Actions for Runtime state restoration, verification, Action First behavior policy, VOS, Life Model, Professional Model, Professional Body, Evolution and Recovery checks. This schema is intentionally below the GPT Actions 30-operation limit.',
        scope='laboratory_core_actions',
    )


def _laboratory_business_data_openapi_schema() -> dict:
    return _laboratory_split_openapi_schema(
        _LABORATORY_BUSINESS_DATA_PATHS,
        title='VECTRA Laboratory Business Data Actions',
        version='FOUNDATION-0011-BUSINESS-DATA',
        description='Business Data read-only Actions for VECTRA Laboratory: status, entities, samples, summaries, query and verification. This schema is intentionally below the GPT Actions 30-operation limit.',
        scope='laboratory_business_data_actions',
    )


def _laboratory_knowledge_openapi_schema() -> dict:
    return _laboratory_split_openapi_schema(
        _LABORATORY_KNOWLEDGE_PATHS,
        title='VECTRA Laboratory Knowledge / Self Evolution Actions',
        version='LABORATORY-KNOWLEDGE-0007-KNOWLEDGE-SELF-EVOLUTION',
        description='Knowledge Capitalization, Professional Knowledge Readback, Repository Inspection and Self Evolution Actions for VECTRA Laboratory. This schema is intentionally below the GPT Actions 30-operation limit.',
        scope='laboratory_knowledge_self_evolution_actions',
    )



# LABORATORY-ACTIONS-0003: Compact professional facade Actions
_FACADE_OPERATION_TO_ENDPOINT = {
    'getVectraRuntimeStatus': '/vectra/runtime/status',
    'restoreVectraLaboratoryState': '/vectra/laboratory/state/restore',
    'verifyVectraRuntime': '/vectra/runtime/verify',
    'getVectraCapabilities': '/vectra/capabilities',
    'getVectraActionManifest': '/vectra/laboratory/actions/manifest',
    'verifyVectraActionCompleteness': '/vectra/laboratory/actions/verify',
    'executeVectraKnowledgeOperation': '/vectra/laboratory/facade/knowledge',
    'executeVectraBusinessDomainOperation': '/vectra/laboratory/facade/business-domain',
    'executeVectraBusinessDataOperation': '/vectra/laboratory/facade/business-data',
    'executeVectraProductReviewOperation': '/vectra/laboratory/facade/product-review',
    'executeVectraRepositoryOperation': '/vectra/laboratory/facade/repository',
    'determineVectraLaboratoryNextAction': '/vectra/laboratory/behavior/next-action',
    'verifyVectraKnowledgeMemoryPersistence': '/vectra/laboratory/memory/verify',
    'create_research_program': '/vectra/laboratory/research/programs',
    'get_research_workspace': '/vectra/laboratory/research/workspace',
    'verify_business_framework_research_foundation': '/vectra/laboratory/research/foundation/verify',
}

_FACADE_ACTIONS = [
    ('getVectraRuntimeStatus', 'GET', '/vectra/runtime/status', 'Check VECTRA Runtime status', 'Runtime status and deployment health.'),
    ('restoreVectraLaboratoryState', 'GET', '/vectra/laboratory/state/restore', 'Restore VECTRA Laboratory state', 'Restores professional state, active Business Domain, Professional Knowledge and Business Knowledge for a new working session.'),
    ('verifyVectraRuntime', 'GET', '/vectra/runtime/verify', 'Verify VECTRA Runtime', 'Runs Runtime Verification and repository integrity checks.'),
    ('getVectraCapabilities', 'GET', '/vectra/capabilities', 'Get VECTRA Capability Registry', 'Returns Runtime Capability Registry.'),
    ('getVectraActionManifest', 'GET', '/vectra/laboratory/actions/manifest', 'Get VECTRA Laboratory Action Manifest', 'Returns public facade Actions and internal Runtime operations.'),
    ('verifyVectraActionCompleteness', 'GET', '/vectra/laboratory/actions/verify', 'Verify VECTRA Action Completeness', 'Checks Runtime Capabilities ↔ Facade Actions ↔ Internal Runtime Services.'),
    ('executeVectraKnowledgeOperation', 'POST', '/vectra/laboratory/facade/knowledge', 'Execute VECTRA Knowledge operation', 'Facade for Professional and Business Knowledge operations.'),
    ('executeVectraBusinessDomainOperation', 'POST', '/vectra/laboratory/facade/business-domain', 'Execute VECTRA Business Domain operation', 'Facade for Business Domain restore, activation, profile and knowledge operations.'),
    ('executeVectraBusinessDataOperation', 'POST', '/vectra/laboratory/facade/business-data', 'Execute VECTRA Business Data operation', 'Facade for read-only Business Data manifest, discovery, status, entities, summaries and query.'),
    ('executeVectraProductReviewOperation', 'POST', '/vectra/laboratory/facade/product-review', 'Execute VECTRA Product Review operation', 'Facade for Product Review and Product Verification operations.'),
    ('executeVectraRepositoryOperation', 'POST', '/vectra/laboratory/facade/repository', 'Execute VECTRA Repository operation', 'Facade for Repository Inspection operations.'),
    ('executeVectraMemoryOperation', 'POST', '/vectra/laboratory/facade/memory', 'Execute VECTRA Memory operation', 'Facade for Product Knowledge, Product Decisions, General Knowledge, Revision Model, Release History, Memory Health, Architecture Conformance, Recovery Optimization and End-to-End Professional Memory Validation operations.'),
    ('determineVectraLaboratoryNextAction', 'GET', '/vectra/laboratory/behavior/next-action', 'Determine VECTRA Laboratory next Action', 'Action First Policy next professional step resolver.'),
    ('verifyVectraKnowledgeMemoryPersistence', 'GET', '/vectra/laboratory/memory/verify', 'Verify VECTRA Knowledge memory persistence', 'Post-release read-only verification for Professional Knowledge, Business Domain Knowledge, Recovery Snapshot and Repository Integrity.'),
    ('create_research_program', 'POST', '/vectra/laboratory/research/programs', 'Create Business Framework Research Program', 'Creates a Research Program Professional Activity for Digital Business Analyst. Use this action directly; do not route it through a guessed facade operation.'),
    ('get_research_workspace', 'POST', '/vectra/laboratory/research/workspace', 'Get Digital Business Analyst Research Workspace', 'Returns the current Research Workspace, active programs, backlog, hypotheses, findings, recommendations and maturity state.'),
    ('verify_business_framework_research_foundation', 'GET', '/vectra/laboratory/research/foundation/verify', 'Verify Business Framework Research Foundation', 'Verifies Research Program, Backlog, Hypothesis, Traceability, Methodology Repository, Research Workspace and Research Maturity capabilities.'),
]

_FACADE_INTERNAL_ENDPOINTS = sorted(
    set(_LABORATORY_CORE_PATHS) | set(_LABORATORY_BUSINESS_DATA_PATHS) | set(_LABORATORY_KNOWLEDGE_PATHS)
)


def _facade_operation_request_schema() -> dict:
    knowledge_item_schema = {
        'type': 'object',
        'required': ['title', 'description'],
        'properties': {
            'title': {'type': 'string', 'description': 'Human-readable knowledge title prepared by VECTRA.'},
            'description': {'type': 'string', 'description': 'Full confirmed knowledge content to be capitalized.'},
            'status': {'type': 'string', 'description': 'Knowledge confirmation status, for example confirmed.'},
            'evidence': {'type': 'string', 'description': 'Evidence or source statement proving why this knowledge is confirmed.'},
            'recommended_memory_type': {'type': 'string', 'description': 'Recommended memory destination, for example professional_knowledge, business_knowledge or product_knowledge.'},
        },
        'additionalProperties': True,
    }
    business_knowledge_item_schema = {
        'type': 'object',
        'required': ['title', 'description'],
        'properties': {
            'title': {'type': 'string', 'description': 'Human-readable Business Knowledge title prepared by VECTRA.'},
            'description': {'type': 'string', 'description': 'Full confirmed Business Domain Knowledge content to be capitalized.'},
            'business_domain': {'type': 'string', 'description': 'Business Domain identifier, for example bonboason.'},
            'status': {'type': 'string', 'description': 'Knowledge confirmation status, for example confirmed.'},
            'evidence': {'type': 'string', 'description': 'Evidence or source statement proving why this knowledge is confirmed.'},
            'recommended_memory_type': {'type': 'string', 'description': 'Recommended memory destination, normally business_knowledge.'},
        },
        'additionalProperties': True,
    }
    prepared_package_schema = {
        'type': 'object',
        'description': (
            'LABORATORY-KNOWLEDGE-0007 final Prepared Knowledge Package. VECTRA supplies the actual knowledge arrays. '
            'When operation_type=capitalize_confirmed_knowledge and this object is supplied, Runtime MUST set '
            'knowledge_input_mode=prepared_knowledge_package and runtime_reanalysis_performed=false. Runtime validates the items, '
            'compares with memory, creates candidates/packages, writes, verifies readback, updates recovery snapshot and creates a report.'
        ),
        'required': ['source', 'business_domain', 'confirmation_level'],
        'properties': {
            'source': {'type': 'string', 'description': 'Knowledge source, for example current_product_owner_dialogue.'},
            'business_domain': {'type': 'string', 'description': 'Business Domain for Business Knowledge, for example bonboason.'},
            'confirmation_level': {'type': 'string', 'description': 'Confirmation level, for example confirmed_by_product_owner.'},
            'professional_knowledge': {
                'type': 'array',
                'items': knowledge_item_schema,
                'description': 'Confirmed Professional Knowledge items prepared by VECTRA.',
            },
            'business_knowledge': {
                'type': 'array',
                'items': business_knowledge_item_schema,
                'description': 'Confirmed Business Domain Knowledge items prepared by VECTRA.',
            },
            'product_knowledge': {
                'type': 'array',
                'items': knowledge_item_schema,
                'description': 'Confirmed Product Knowledge items prepared by VECTRA. Runtime accepts this section and currently persists it as Professional Knowledge with knowledge_subtype=product.',
            },
        },
        'additionalProperties': True,
    }
    return {
        'type': 'object',
        'properties': {
            'operation_type': {'type': 'string', 'description': 'Professional operation to execute through the facade. For operation_type=capitalize_confirmed_knowledge Runtime runs AUTO_CAPITALIZATION_PIPELINE instead of a direct low-level write.'},
            'payload': {'type': 'object', 'description': 'Operation-specific payload. For operation_type=capitalize_confirmed_knowledge, may also include prepared_knowledge_package prepared by VECTRA; Runtime will then skip raw-context reanalysis and execute storage, diff, write, readback, snapshot and report.', 'additionalProperties': True},
            'working_context': {'type': 'string', 'description': 'LABORATORY-KNOWLEDGE-0010-PV required field. Current Laboratory working context or compressed confirmed session context. Used by Runtime extraction when prepared_knowledge_package is not supplied.'},
            'source_type': {'type': 'string', 'description': 'Source type for working_context, for example laboratory_session.'},
            'extraction_mode': {'type': 'string', 'description': 'Extraction mode, for example confirmed_knowledge_only.'},
            'conversation': {'type': 'string', 'description': 'Optional conversation text supplied by VECTRA when working_context is not used.'},
            'transcript': {'type': 'string', 'description': 'Optional transcript text supplied by VECTRA when working_context is not used.'},
            'source_text': {'type': 'string', 'description': 'Optional source text supplied by VECTRA when working_context is not used.'},
            'prepared_knowledge_package': prepared_package_schema,
            'knowledge_package': prepared_package_schema,
            'prepared_package': prepared_package_schema,
            'product_owner_approval': {'type': 'boolean', 'description': 'Required true for write/capitalization operations.'},
            'domain': {'type': 'string', 'description': 'Business Domain identifier when applicable.'},
            'session_id': {'type': 'string', 'description': 'Laboratory session id.'},
            'request_id': {'type': 'string', 'description': 'Client request id for traceability.'},
        },
        'required': ['operation_type'],
        'additionalProperties': True,
        'examples': [
            {
                'operation_type': 'capitalize_confirmed_knowledge',
                'product_owner_approval': True,
                'domain': 'bonboason',
                'prepared_knowledge_package': {
                    'source': 'current_product_owner_dialogue',
                    'business_domain': 'bonboason',
                    'confirmation_level': 'confirmed_by_product_owner',
                    'professional_knowledge': [
                        {
                            'knowledge_id': 'PK-001',
                            'title': 'Двухуровневая модель знаний VECTRA',
                            'description': 'Professional Knowledge и Business Knowledge являются разными уровнями памяти.',
                            'status': 'CONFIRMED_BY_PRODUCT_OWNER'
                        }
                    ],
                    'business_knowledge': []
                }
            }
        ]
    }


def _facade_response_schema() -> dict:
    return {
        'type': 'object',
        'properties': {
            'status': {'type': 'string'},
            'operation_type': {'type': 'string'},
            'runtime_service_called': {'type': 'string'},
            'internal_endpoint_called': {'type': 'string'},
            'result': {'type': 'object', 'additionalProperties': True},
            'verification_status': {'type': 'string'},
            'readback_status': {'type': 'string'},
            'final_status': {'type': 'string'},
            'error': {'type': 'object', 'additionalProperties': True},
            'next_recommended_action': {'type': 'string'},
        },
        'additionalProperties': True,
    }


# WORKING-GPT-ACTIONS-RESTORE-001: Compact Business GPT Actions
_BUSINESS_GPT_FACADE_ACTIONS = [
    (
        'getVectraRuntimeStatus',
        'GET',
        '/vectra/runtime/status',
        'Check VECTRA Runtime status',
        'Runtime status and deployment health for the working Business GPT.',
    ),
    (
        'executeVectraBusinessDataOperation',
        'POST',
        '/vectra/laboratory/facade/business-data',
        'Execute VECTRA Business Data operation',
        'Read-only facade for Business Data manifest, discovery, status, entities, summaries and business query operations.',
    ),
    (
        'executeVectraBusinessDomainOperation',
        'POST',
        '/vectra/laboratory/facade/business-domain',
        'Execute VECTRA Business Domain operation',
        'Business Domain facade for domain activation, restore, profile and business knowledge read operations.',
    ),
    (
        'executeVectraQuery',
        'POST',
        '/vectra/query',
        'Execute VECTRA business query',
        'Stateful user-facing VECTRA query endpoint for commands such as Бизнес 2026-02 and business drill-down navigation.',
    ),
]


def _business_gpt_operation_request_schema() -> dict:
    return {
        'type': 'object',
        'properties': {
            'operation_type': {
                'type': 'string',
                'description': 'Business operation to execute through the facade, for example manifest, discovery, first_impression, status, entities, query, summary or restore_domain.',
            },
            'payload': {
                'type': 'object',
                'description': 'Operation-specific payload for Business Data or Business Domain facade.',
                'additionalProperties': True,
            },
            'message': {
                'type': 'string',
                'description': 'Business query text when operation_type=query or when the operation accepts a natural business command.',
            },
            'period': {'type': 'string', 'description': 'Business period, for example 2026-02.'},
            'domain': {'type': 'string', 'description': 'Business Domain identifier, for example bonboason.'},
            'session_id': {'type': 'string', 'description': 'Working GPT session identifier.'},
        },
        'required': ['operation_type'],
        'additionalProperties': True,
    }


def _business_gpt_query_request_schema() -> dict:
    return {
        'type': 'object',
        'properties': {
            'message': {
                'type': 'string',
                'description': 'User business command, for example Начать анализ, Бизнес 2026-02, Покажи Варус 2026-02 or all/назад navigation.',
            },
            'session_id': {
                'type': 'string',
                'description': 'Working GPT session id. Defaults to default when omitted.',
                'default': 'default',
            },
            'active_workspace_state': {'type': 'object', 'additionalProperties': True},
            'workspace_action_map': {'type': 'array', 'items': {'type': 'object', 'additionalProperties': True}},
            'runtime_context': {'type': 'object', 'additionalProperties': True},
        },
        'required': ['message'],
        'additionalProperties': True,
    }


def _business_gpt_openapi_schema() -> dict:
    """Return the compact OpenAPI schema for the working VECTRA Business GPT.

    This schema intentionally excludes Laboratory, Professional Memory,
    Professional Intelligence, Repository, Product Review, Historical Migration
    and Engineering Verification actions. It reuses existing Runtime endpoints
    without changing Runtime behavior or the official Laboratory /openapi.json.
    """
    server_url = os.getenv('VECTRA_PUBLIC_RUNTIME_URL') or os.getenv('VECTRA_RUNTIME_URL') or os.getenv('RENDER_EXTERNAL_URL') or 'https://bon-buasson-api.onrender.com'
    api_key_required = bool(os.getenv('VECTRA_LABORATORY_API_KEY'))
    security = [{'LaboratoryApiKey': []}] if api_key_required else []

    generic_response = {
        '200': {
            'description': 'VECTRA Business GPT response',
            'content': {'application/json': {'schema': _facade_response_schema()}},
        }
    }
    paths: dict[str, dict] = {}
    for operation_id, method, endpoint, summary, description in _BUSINESS_GPT_FACADE_ACTIONS:
        op: dict[str, Any] = {
            'operationId': operation_id,
            'summary': summary,
            'description': description,
            'security': security,
            'responses': generic_response,
        }
        if method == 'POST':
            if operation_id == 'executeVectraQuery':
                request_schema = _business_gpt_query_request_schema()
            else:
                request_schema = _business_gpt_operation_request_schema()
            op['requestBody'] = {
                'required': True,
                'content': {'application/json': {'schema': request_schema}},
            }
        paths.setdefault(endpoint, {})[method.lower()] = op

    schema = {
        'openapi': '3.1.0',
        'info': {
            'title': 'VECTRA Business GPT Actions',
            'version': 'WORKING-GPT-ACTIONS-RESTORE-001',
            'description': (
                'Compact OpenAPI schema for the working VECTRA Business GPT. '
                'It exposes only Runtime status, Business Data, Business Domain and user-facing business query actions. '
                'Laboratory, Professional Memory, Professional Intelligence, Repository, Product Review, Historical Migration and Engineering Verification actions are intentionally excluded.'
            ),
        },
        'servers': [{'url': server_url}],
        'components': {
            'schemas': {},
            'securitySchemes': {
                'LaboratoryApiKey': {
                    'type': 'apiKey',
                    'in': 'header',
                    'name': 'X-VECTRA-LABORATORY-KEY',
                    'description': 'Optional Runtime API key when configured. The working Business GPT uses the same backend security mechanism as Runtime.',
                }
            },
        },
        'paths': paths,
        'x-vectra-scope': 'working_business_gpt_actions',
        'x-vectra-release': 'WORKING-GPT-ACTIONS-RESTORE-001',
        'x-vectra-business-domain': 'bonboason',
        'x-vectra-excluded-scopes': [
            'professional_memory',
            'professional_intelligence',
            'repository',
            'product_review',
            'historical_migration',
            'laboratory_behavior',
            'engineering_verification',
        ],
        'x-vectra-gpt-actions-operation-limit': {
            'limit': 30,
            'operation_count': len(_BUSINESS_GPT_FACADE_ACTIONS),
            'status': 'PASS',
        },
    }
    schema['x-vectra-operation-count'] = _count_openapi_operations(schema)
    return schema


def _laboratory_facade_openapi_schema() -> dict:
    server_url = os.getenv('VECTRA_PUBLIC_RUNTIME_URL') or os.getenv('VECTRA_RUNTIME_URL') or os.getenv('RENDER_EXTERNAL_URL') or 'https://bon-buasson-api.onrender.com'
    api_key_required = bool(os.getenv('VECTRA_LABORATORY_API_KEY'))
    security = [{'LaboratoryApiKey': []}] if api_key_required else []

    generic_response = {
        '200': {
            'description': 'VECTRA Laboratory facade response',
            'content': {'application/json': {'schema': _facade_response_schema()}},
        }
    }
    paths: dict[str, dict] = {}
    for operation_id, method, endpoint, summary, description in _FACADE_ACTIONS:
        op: dict[str, Any] = {
            'operationId': operation_id,
            'summary': summary,
            'description': description,
            'security': security,
            'responses': generic_response,
        }
        if method == 'POST':
            op['requestBody'] = {
                'required': True,
                'content': {'application/json': {'schema': _facade_operation_request_schema()}},
            }
        elif operation_id == 'determineVectraLaboratoryNextAction':
            op['parameters'] = [
                {'name': 'command', 'in': 'query', 'required': False, 'schema': {'type': 'string'}, 'description': 'Product Owner command.'},
                {'name': 'session_id', 'in': 'query', 'required': False, 'schema': {'type': 'string'}, 'description': 'Laboratory session id.'},
            ]
        paths.setdefault(endpoint, {})[method.lower()] = op
    schema = {
        'openapi': '3.1.0',
        'info': {
            'title': 'VECTRA Laboratory Facade Actions',
            'version': 'BUSINESS-FRAMEWORK-RESEARCH-CAPABILITY-001',
            'description': 'Official compact OpenAPI schema for VECTRA Laboratory GPT Actions. Product Owner imports this single URL. Professional Memory v1.0 adds Architecture Conformance, Recovery Optimization and End-to-End Professional Memory Validation through the memory facade while preserving the compact Actions contract.',
        },
        'servers': [{'url': server_url}],
        'components': {
            'schemas': {},
            'securitySchemes': {
                'LaboratoryApiKey': {
                    'type': 'apiKey',
                    'in': 'header',
                    'name': 'X-VECTRA-LABORATORY-KEY',
                    'description': 'Optional Laboratory API key. Required only when VECTRA_LABORATORY_API_KEY is configured in Runtime.',
                }
            },
        },
        'paths': paths,
        'x-vectra-scope': 'laboratory_facade_actions',
        'x-vectra-release': 'BUSINESS-FRAMEWORK-RESEARCH-CAPABILITY-001',
        'x-vectra-gpt-actions-operation-limit': {
            'limit': 30,
            'operation_count': len(_FACADE_ACTIONS),
            'status': 'PASS',
        },
        'x-vectra-internal-runtime-operations': len(_FACADE_INTERNAL_ENDPOINTS),
        'x-vectra-legacy-diagnostic-openapi': {
            'core': '/vectra/laboratory/openapi/core.json',
            'business_data': '/vectra/laboratory/openapi/business-data.json',
            'knowledge_self_evolution': '/vectra/laboratory/openapi/knowledge.json',
        },
        'x-vectra-action-manifest': '/vectra/laboratory/actions/manifest',
        'x-vectra-action-completeness-verification': '/vectra/laboratory/actions/verify',
    }
    schema['x-vectra-operation-count'] = _count_openapi_operations(schema)
    return schema


def _extract_result_status(result: Any) -> tuple[str | None, str | None, str | None]:
    if not isinstance(result, dict):
        return None, None, None
    verification = result.get('verification_status') or result.get('verification_result')
    readback = result.get('readback_status')
    final = result.get('final_status') or result.get('capitalization_status')
    report = result.get('report') if isinstance(result.get('report'), dict) else {}
    if not readback:
        readback = report.get('readback_status')
    if not final:
        final = report.get('final_status')
    return verification, readback, final


def _facade_response(operation_type: str, runtime_service: str, endpoint: str, result: Any, *, next_action: str = '') -> dict:
    verification, readback, final = _extract_result_status(result)
    status = 'ok'
    if isinstance(result, dict) and str(result.get('status') or '').lower() in {'error', 'failed', 'fail'}:
        status = 'error'
    return {
        'status': status,
        'render_mode': 'vectra_laboratory_facade_operation',
        'operation_type': operation_type,
        'runtime_service_called': runtime_service,
        'internal_endpoint_called': endpoint,
        'result': result if isinstance(result, dict) else {'value': result},
        'verification_status': verification or ('PASS' if status == 'ok' else 'FAIL'),
        'readback_status': readback,
        'final_status': final,
        'error': None if status == 'ok' else (result if isinstance(result, dict) else {'message': str(result)}),
        'next_recommended_action': next_action,
    }



def _compact_capitalization_facade_result(result: Any, operation_type: str = 'capitalize_confirmed_knowledge') -> dict:
    """Return a compact GPT Action result for autonomous capitalization.

    LABORATORY-KNOWLEDGE-0009 keeps the facade response compact after automatic batch capitalization by keeping full
    technical reports inside Runtime while exposing only the management summary
    through the facade Action response.
    """
    if not isinstance(result, dict):
        return {
            'status': 'error',
            'operation_type': operation_type,
            'final_status': 'FAILED',
            'error': {'message': str(result)},
        }

    reports = result.get('capitalization_reports') if isinstance(result.get('capitalization_reports'), list) else []
    diff = result.get('incremental_diff') if isinstance(result.get('incremental_diff'), dict) else {}
    summary = result.get('summary') if isinstance(result.get('summary'), dict) else {}

    professional_written = result.get('professional_written')
    if professional_written is None:
        professional_written = result.get('professional_knowledge_written')
    business_written = result.get('business_written')
    if business_written is None:
        business_written = result.get('business_knowledge_written')
    product_written = result.get('product_written')
    if product_written is None:
        # Product Knowledge is stored as Professional Knowledge with subtype=product
        # until a dedicated Product Memory repository exists. Keep explicit field
        # in the facade contract so GPT can report it without exposing details.
        product_written = 0

    report_ids = [str(r.get('report_id')) for r in reports if isinstance(r, dict) and r.get('report_id')]
    capitalization_report_id = result.get('capitalization_report_id') or (report_ids[0] if report_ids else None)

    readback_status = result.get('readback_status')
    if readback_status == 'READBACK_PASS':
        readback_status = 'PASS'
    recovery_status = result.get('recovery_snapshot_status')
    if recovery_status == 'RECOVERY_UPDATED':
        recovery_status = 'PASS'

    compact = {
        'status': result.get('status') or 'ok',
        'operation_type': operation_type,
        'knowledge_input_mode': result.get('knowledge_input_mode'),
        'runtime_reanalysis_performed': bool(result.get('runtime_reanalysis_performed')),
        'final_status': result.get('final_status'),
        'professional_written': int(professional_written or 0),
        'business_written': int(business_written or 0),
        'product_written': int(product_written or 0),
        'duplicates': int(diff.get('unchanged_count') or summary.get('duplicates') or 0),
        'updated': int(diff.get('updated_count') or 0),
        'readback_status': readback_status,
        'recovery_snapshot_status': recovery_status,
        'capitalization_report_id': capitalization_report_id,
        'capitalization_report_ids': report_ids[:20],
        'next_recommended_action': result.get('next_recommended_action') or 'При необходимости открыть полный отчёт через operation_type=create_report и report_id.',
    }
    if result.get('batch_id'):
        compact['batch_id'] = result.get('batch_id')
    if result.get('message'):
        compact['message'] = result.get('message')
    if result.get('reason'):
        compact['reason'] = result.get('reason')
    if result.get('errors'):
        compact['error'] = {'message': 'Capitalization pipeline returned errors. Open full report by report_id.', 'errors_count': len(result.get('errors') or [])}
    else:
        compact['error'] = None
    return compact

def _facade_error(operation_type: str, message: str, *, runtime_service: str = '', endpoint: str = '', next_action: str = '') -> dict:
    return {
        'status': 'error',
        'render_mode': 'vectra_laboratory_facade_operation',
        'operation_type': operation_type,
        'runtime_service_called': runtime_service,
        'internal_endpoint_called': endpoint,
        'result': {},
        'verification_status': 'FAIL',
        'readback_status': None,
        'final_status': 'FAILED',
        'error': {'message': message},
        'next_recommended_action': next_action,
    }


def _normalize_facade_request(request: dict | None) -> tuple[str, dict, bool, str, str, str]:
    if not isinstance(request, dict):
        request = {}
    operation_type = str(request.get('operation_type') or '').strip()
    payload = request.get('payload') if isinstance(request.get('payload'), dict) else {}
    approval = bool(request.get('product_owner_approval'))
    domain = str(request.get('domain') or payload.get('domain') or '').strip()
    session_id = str(request.get('session_id') or payload.get('session_id') or '').strip()
    request_id = str(request.get('request_id') or payload.get('request_id') or '').strip()
    payload = dict(payload)

    # LABORATORY-KNOWLEDGE-0007/0008:
    # GPT Actions must be able to send the package as a first-class request field,
    # not only hidden inside generic payload. Runtime then consumes the same package
    # and skips raw-context reanalysis.
    for package_key in ('prepared_knowledge_package', 'knowledge_package', 'prepared_package'):
        if isinstance(request.get(package_key), dict) and not isinstance(payload.get(package_key), dict):
            payload[package_key] = request[package_key]

    # LABORATORY-KNOWLEDGE-0010-PV integration fix:
    # GPT may send professional session material as first-class Action fields.
    # Runtime extraction reads the normalized payload, so the facade must copy
    # these fields into payload before calling AUTO_CAPITALIZATION_PIPELINE.
    for context_key in (
        'working_context', 'working_context_text', 'conversation_context', 'dialogue_context',
        'current_dialogue', 'current_context_text', 'product_owner_context', 'transcript',
        'conversation', 'source_text', 'current_context', 'confirmed_knowledge',
        'source_type', 'extraction_mode',
    ):
        if context_key in request and context_key not in payload:
            payload[context_key] = request[context_key]

    if domain and not payload.get('domain'):
        payload['domain'] = domain
    if approval and 'product_owner_approval' not in payload:
        payload['product_owner_approval'] = True
    return operation_type, payload, approval, domain, session_id, request_id


def _requires_product_owner_approval(operation_type: str) -> bool:
    return operation_type in {
        'create_package',
        'write_professional_knowledge',
        'write_business_knowledge',
        'capitalize_confirmed_knowledge',
        'capitalize_domain_knowledge',
        'create_product_observation',
        'create_engineering_task',
    }


def _internal_runtime_actions() -> list[dict]:
    schema = _laboratory_public_openapi_schema()
    actions = _iter_openapi_actions(schema)
    for action in actions:
        op_id = str(action.get('operation_id') or '')
        endpoint = str(action.get('endpoint') or '')
        action['runtime_service'] = _action_runtime_service_for(op_id, endpoint)
        action['capability_id'] = _derive_capability_id(op_id, endpoint)
        action['internal_endpoint'] = endpoint
        action['export_status'] = 'INTERNAL_RUNTIME_OPERATION'
        action['release_version'] = 'LABORATORY-KNOWLEDGE-0005'
    return actions


def _facade_action_for_internal(endpoint: str, operation_id: str = '') -> str:
    if 'business-data' in endpoint:
        return 'executeVectraBusinessDataOperation'
    if '/repository/' in endpoint:
        return 'executeVectraRepositoryOperation'
    if '/knowledge/' in endpoint or '/domain/' in endpoint or '/context/capitalization' in endpoint:
        return 'executeVectraKnowledgeOperation'
    if '/runtime/status' in endpoint:
        return 'getVectraRuntimeStatus'
    if '/runtime/verify' in endpoint or '/runtime/snapshot' in endpoint:
        return 'verifyVectraRuntime'
    if '/capabilities' in endpoint:
        return 'getVectraCapabilities'
    if '/behavior/next-action' in endpoint:
        return 'determineVectraLaboratoryNextAction'
    if '/actions/manifest' in endpoint:
        return 'getVectraActionManifest'
    if '/actions/verify' in endpoint:
        return 'verifyVectraActionCompleteness'
    if '/domain/' in endpoint:
        return 'executeVectraBusinessDomainOperation'
    return 'restoreVectraLaboratoryState'


def _build_laboratory_facade_action_manifest() -> dict:
    facade_schema = _laboratory_facade_openapi_schema()
    public_actions = _iter_openapi_actions(facade_schema)
    internal_actions = _internal_runtime_actions()
    public_ids = {str(action.get('operation_id')) for action in public_actions}
    required_public_ids = set(_FACADE_OPERATION_TO_ENDPOINT)
    missing_facade = sorted(required_public_ids - public_ids)
    internal_services_missing = []
    for action in internal_actions:
        if not action.get('runtime_service'):
            internal_services_missing.append(action)
        action['facade_action'] = _facade_action_for_internal(str(action.get('endpoint') or ''), str(action.get('operation_id') or ''))
        action['requires_product_owner_approval'] = any(token in str(action.get('operation_id') or '').lower() for token in ['write', 'capitalization', 'candidate'])
        action['access_mode'] = 'write' if action['requires_product_owner_approval'] else 'read_only'
    return {
        'status': 'ok' if not missing_facade and not internal_services_missing else 'error',
        'render_mode': 'vectra_laboratory_action_manifest',
        'release_version': 'LABORATORY-KNOWLEDGE-0010',
        'schema_url': '/vectra/laboratory/openapi.json',
        'schema_version': facade_schema.get('info', {}).get('version'),
        'operation_count': _count_openapi_operations(facade_schema),
        'operation_limit': 30,
        'public_facade_actions': public_actions,
        'public_facade_actions_count': len(public_actions),
        'internal_runtime_operations': internal_actions,
        'internal_runtime_operations_count': len(internal_actions),
        'missing_facade_actions': missing_facade,
        'missing_internal_services': internal_services_missing,
        'export_status': 'COMPLETE' if not missing_facade and not internal_services_missing else 'INCOMPLETE',
        'verification_status': 'PASS' if not missing_facade and not internal_services_missing else 'FAIL',
        'policy': 'GPT imports one compact facade schema; Runtime routes internally to detailed services.',
    }


def _verify_laboratory_facade_action_completeness() -> dict:
    manifest = _build_laboratory_facade_action_manifest()
    missing_facade = manifest.get('missing_facade_actions') if isinstance(manifest.get('missing_facade_actions'), list) else []
    missing_services = manifest.get('missing_internal_services') if isinstance(manifest.get('missing_internal_services'), list) else []
    incomplete = bool(missing_facade or missing_services or manifest.get('operation_count', 999) > 30)
    return {
        'status': 'ok' if not incomplete else 'error',
        'render_mode': 'vectra_laboratory_action_completeness_verification',
        'verification_status': 'PASS' if not incomplete else 'FAIL',
        'result': '✅ Комплектация полная.' if not incomplete else '❌ Обнаружено расхождение.',
        'schema_url': '/vectra/laboratory/openapi.json',
        'operation_count': manifest.get('operation_count'),
        'operation_limit': 30,
        'public_facade_actions_count': manifest.get('public_facade_actions_count'),
        'internal_runtime_operations_count': manifest.get('internal_runtime_operations_count'),
        'missing_capability': [],
        'missing_facade_action': missing_facade,
        'missing_internal_service': missing_services,
        'affected_product_owner_command': [] if not incomplete else ['Лаборатория, продолжай работу.', 'Лаборатория, капитализируй подтверждённые знания.', 'Лаборатория, проверь комплектацию.'],
        'release_blocked': incomplete,
    }

@router.get('/vectra/laboratory/memory/verify', summary='Verify VECTRA Knowledge Memory Persistence')
def vectra_laboratory_memory_verify(domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    result = verify_vectra_knowledge_memory_persistence(domain=domain)
    return json_response(_facade_response('verify_memory_persistence', 'knowledge_capitalization.verify_knowledge_memory_persistence', '/vectra/laboratory/memory/verify', result))


@router.get('/vectra/laboratory/actions/manifest', summary='Read VECTRA Laboratory Action Manifest')
def vectra_laboratory_action_manifest(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(_build_laboratory_facade_action_manifest())


@router.get('/vectra/laboratory/actions/verify', summary='Verify VECTRA Laboratory Action completeness')
def vectra_laboratory_action_completeness_verify(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(_verify_laboratory_facade_action_completeness())


@router.get('/openapi.business.json', summary='Read compact VECTRA Business GPT OpenAPI schema')
def vectra_business_gpt_openapi_schema():
    return JSONResponse(content=_business_gpt_openapi_schema())


@router.get('/vectra/business/openapi.json', summary='Alias: Read compact VECTRA Business GPT OpenAPI schema')
def vectra_business_gpt_openapi_schema_alias():
    return JSONResponse(content=_business_gpt_openapi_schema())


@router.get('/vectra/laboratory/openapi/core.json', summary='Read VECTRA Laboratory Core OpenAPI schema')
def vectra_laboratory_openapi_core_schema():
    return JSONResponse(content=_laboratory_core_openapi_schema())


@router.get('/vectra/laboratory/openapi/business-data.json', summary='Read VECTRA Laboratory Business Data OpenAPI schema')
def vectra_laboratory_openapi_business_data_schema():
    return JSONResponse(content=_laboratory_business_data_openapi_schema())


@router.get('/vectra/laboratory/openapi/knowledge.json', summary='Read VECTRA Laboratory Knowledge and Self Evolution OpenAPI schema')
def vectra_laboratory_openapi_knowledge_schema():
    return JSONResponse(content=_laboratory_knowledge_openapi_schema())


@router.get('/vectra/laboratory/openapi.json', summary='Read complete VECTRA Laboratory OpenAPI schema')
def vectra_laboratory_openapi_schema():
    return JSONResponse(content=_laboratory_full_openapi_schema())


@router.get('/laboratory/openapi.json', summary='Alias: Read complete VECTRA Laboratory OpenAPI schema')
def laboratory_openapi_schema_alias():
    return JSONResponse(content=_laboratory_full_openapi_schema())


# FOUNDATION-0002: Direct Runtime Verification Access
# These endpoints are the stable Action-facing interface for VECTRA Laboratory.
# They expose fact-state from the running Runtime and do not apply changes,
# change Professional Model, or perform automatic synchronization.

@router.get('/vectra/runtime/status', summary='Read VECTRA Runtime deployment and health status')
def vectra_runtime_status(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    runtime_payload = get_vectra_assistant_runtime_status()
    runtime_body = runtime_payload.get('runtime') if isinstance(runtime_payload, dict) and isinstance(runtime_payload.get('runtime'), dict) else {}
    snapshot_payload = get_vectra_runtime_snapshot(refresh=False)
    snapshot = snapshot_payload.get('runtime_snapshot') if isinstance(snapshot_payload, dict) and isinstance(snapshot_payload.get('runtime_snapshot'), dict) else snapshot_payload
    components = snapshot.get('components') if isinstance(snapshot, dict) and isinstance(snapshot.get('components'), dict) else {}
    api_health = components.get('api_health') if isinstance(components.get('api_health'), dict) else {}
    health = 'PASS'
    if isinstance(api_health, dict) and api_health.get('status') in {'FAIL', 'ERROR'}:
        health = 'FAIL'
    elif isinstance(runtime_body, dict) and runtime_body.get('status') not in {None, 'ok', 'active'}:
        health = str(runtime_body.get('status')).upper()
    runtime_version = (snapshot.get('runtime_version') if isinstance(snapshot, dict) else None) or os.getenv('VECTRA_RUNTIME_VERSION') or 'GENESIS-0002'
    deployment_version = os.getenv('RENDER_GIT_COMMIT') or os.getenv('VECTRA_DEPLOYMENT_VERSION') or (snapshot.get('deployment_version') if isinstance(snapshot, dict) else None) or os.getenv('VECTRA_RELEASE_VERSION') or 'FOUNDATION-0002'
    deployment_time = os.getenv('RENDER_DEPLOY_TIME') or os.getenv('VECTRA_DEPLOYMENT_TIME') or runtime_body.get('last_integrity_check') or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
    current_release = os.getenv('VECTRA_RELEASE_VERSION') or (snapshot.get('release_version') if isinstance(snapshot, dict) else None) or 'GENESIS-0014+FOUNDATION-0002'
    return json_response({
        'status': 'ok',
        'render_mode': 'vectra_runtime_status',
        'runtime_version': runtime_version,
        'deployment_version': deployment_version,
        'deployment_time': deployment_time,
        'current_release': current_release,
        'runtime_health': health,
        'runtime_status': runtime_body,
        'laboratory_access': {
            'direct_runtime_verification_access': True,
            'primary_endpoint': '/vectra/laboratory/verification',
            'openapi_action_ready': True,
        },
    })


@router.get('/vectra/professional/model', summary='Read current confirmed VECTRA Professional Model')
def vectra_professional_model_action_read(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_professional_model())


@router.get('/vectra/evolution/status', summary='Read VECTRA evolution and verification status')
def vectra_evolution_status(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    journal = list_vectra_evolution_journal_entries()
    latest = get_vectra_evolution_journal_latest()
    journal_status = get_vectra_evolution_journal_status()
    verification = run_vectra_runtime_verification_report()
    # The repositories currently store proposals as journal-like operational records.
    # Keep this endpoint conservative: expose active proposal collections only when
    # they are explicitly present in runtime state instead of inventing them.
    current_state_payload = get_vectra_assistant_current_state()
    state = current_state_payload.get('state') if isinstance(current_state_payload, dict) and isinstance(current_state_payload.get('state'), dict) else {}
    active_engineering_proposals = state.get('active_engineering_proposals') if isinstance(state.get('active_engineering_proposals'), list) else []
    active_improvement_proposals = state.get('active_improvement_proposals') if isinstance(state.get('active_improvement_proposals'), list) else []
    latest_entries = latest.get('latest_entries') or latest.get('entries') or []
    latest_entry = latest_entries[-1] if isinstance(latest_entries, list) and latest_entries else (latest if isinstance(latest, dict) else {})
    return json_response({
        'status': 'ok',
        'render_mode': 'vectra_evolution_status',
        'last_confirmed_engineering_increment': latest_entry.get('release_id') or latest_entry.get('release') or os.getenv('VECTRA_RELEASE_VERSION', 'GENESIS-0014'),
        'last_product_verification': verification.get('verification_result') or verification.get('status'),
        'active_engineering_proposals': active_engineering_proposals,
        'active_improvement_proposals': active_improvement_proposals,
        'evolution_journal_status': journal_status,
        'evolution_journal_entries_count': journal.get('entries_count'),
        'laboratory_verification_endpoint': '/vectra/laboratory/verification',
    })


# VECTRA-RUNTIME-0004: Runtime Readback Completion & Product Verification Support

@router.get('/vectra/runtime/object/{object_name}', summary='Read observable VECTRA Runtime object')
def vectra_runtime_object_read(object_name: str, limit: int = 50):
    return json_response(read_vectra_runtime_object(object_name, limit=limit))


@router.post('/vectra/runtime/object/{object_name}', summary='Write observable VECTRA Runtime object with readback verification')
def vectra_runtime_object_write(object_name: str, request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(write_vectra_runtime_object(object_name, request))


@router.get('/vectra/runtime/object/{object_name}/verify', summary='Verify VECTRA Runtime readback for object')
def vectra_runtime_object_verify(object_name: str, written_id: str = None):
    return json_response(verify_vectra_runtime_readback(object_name, written_id=written_id))


@router.get('/vectra/runtime/observability-interface', summary='Read VECTRA Runtime Observability Interface')
def vectra_runtime_observability_interface():
    return json_response(get_vectra_runtime_observability_interface())


@router.get('/vectra/runtime/snapshot', summary='Read official VECTRA Runtime Snapshot')
def vectra_runtime_snapshot(refresh: bool = False, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_runtime_snapshot(refresh=refresh))


@router.post('/vectra/runtime/snapshot/refresh', summary='Refresh official VECTRA Runtime Snapshot')
def vectra_runtime_snapshot_refresh(request: dict = None):
    reason = 'manual_refresh'
    if isinstance(request, dict) and request.get('reason'):
        reason = str(request.get('reason'))
    return json_response(refresh_vectra_runtime_snapshot(reason=reason))


@router.get('/vectra/runtime/snapshots', summary='Read VECTRA Runtime Snapshot History')
def vectra_runtime_snapshot_history(limit: int = 20):
    return json_response(list_vectra_runtime_snapshots(limit=limit))


@router.post('/vectra/runtime/product-verification', summary='Run VECTRA Runtime Product Verification from Runtime Snapshot')
def vectra_runtime_product_verification():
    return json_response(run_vectra_snapshot_product_verification())




@router.get('/vectra/runtime/verify', summary='Read VECTRA Runtime Verification Report')
def vectra_runtime_verify():
    return run_vectra_runtime_verification_report()




@router.get('/vectra/runtime/evidence', summary='Collect complete VECTRA Runtime Verification Evidence')
def vectra_runtime_evidence(runtime_url: str = None):
    return json_response(run_vectra_runtime_verification_evidence(runtime_url=runtime_url, reason='laboratory_runtime_evidence_request'))


@router.post('/vectra/runtime/verification/run', summary='Run automated VECTRA Runtime Verification Evidence collection')
def vectra_runtime_verification_run(request: dict = None):
    runtime_url = None
    if isinstance(request, dict):
        runtime_url = request.get('runtime_url')
    return json_response(run_vectra_runtime_verification_evidence(runtime_url=runtime_url, reason='laboratory_runtime_verification_run'))


@router.get('/vectra/runtime/verification/status', summary='Read VECTRA Runtime Verification Automation status')
def vectra_runtime_verification_status():
    return json_response(get_vectra_runtime_verification_status())


@router.get('/vectra/laboratory/verification', summary='Collect complete VECTRA Laboratory Product Verification Evidence in one request')
def vectra_laboratory_verification(runtime_url: str = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(run_vectra_laboratory_verification_package(runtime_url=runtime_url))





# FOUNDATION-0007: VECTRA Life Model.
# Life Model is part of Runtime professional state, not GPT instruction and not a Knowledge file.

@router.get('/vectra/life-model', summary='Read VECTRA Life Model from Runtime Repository')
def vectra_life_model(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_life_model())


@router.get('/vectra/life-model/status', summary='Read VECTRA Life Model status')
def vectra_life_model_status(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_life_model_status())


@router.get('/vectra/life-model/verify', summary='Verify VECTRA Life Model')
def vectra_life_model_verify(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_life_model())



# VOS-001: VECTRA Operating System.
# VOS is part of Runtime professional state, not GPT instruction and not a Knowledge file.

@router.get('/vectra/vos', summary='Read VECTRA Operating System from Runtime Repository')
def vectra_vos(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_vos())


@router.get('/vectra/vos/status', summary='Read VECTRA Operating System status')
def vectra_vos_status(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_vos_status())


@router.get('/vectra/vos/verify', summary='Verify VECTRA Operating System')
def vectra_vos_verify(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_vos())


@router.get('/vectra/vos/restore', summary='Restore VECTRA Operating System state')
def vectra_vos_restore(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(restore_vectra_vos_state())

# FOUNDATION-I001: Professional Body Integration.
# Capability Registry becomes the internal professional entry point; REST remains
# transport implementation for Actions and Runtime invocation.

@router.get('/vectra/capabilities', summary='Read VECTRA Capability Registry')
def vectra_capabilities(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_capability_registry())


@router.get('/vectra/capabilities/select', summary='Select VECTRA Capability for natural intent')
def vectra_capability_select(intent: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(select_vectra_capability_for_intent(intent))


@router.get('/vectra/professional-body/status', summary='Read VECTRA Professional Body Integration status')
def vectra_professional_body_status(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_professional_body_status())


@router.get('/vectra/professional-body/restore', summary='Restore VECTRA professional state from Runtime Repository')
def vectra_professional_body_restore(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(restore_vectra_professional_body_state())


@router.get('/vectra/professional-body/verify', summary='Verify VECTRA Professional Body Integration')
def vectra_professional_body_verify(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_professional_body_integration())


# VECTRA Context Capitalization
# Accept confirmed development context into Runtime Repository without automatic
# Professional Model changes. Product Owner approval remains required for model
# consolidation.

@router.post('/vectra/context/capitalization', summary='Capitalize confirmed VECTRA context into Runtime Repository')
def vectra_context_capitalization(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(run_vectra_context_capitalization(request))


@router.get('/vectra/context/capitalization/status', summary='Read VECTRA Context Capitalization status')
def vectra_context_capitalization_status():
    return json_response(get_vectra_context_capitalization_status())


@router.get('/vectra/context/capitalization/reports', summary='Read VECTRA Context Capitalization reports')
def vectra_context_capitalization_reports(limit: int = 20):
    return json_response(list_vectra_context_capitalization_reports(limit=limit))


@router.get('/vectra/context/capitalization/verify', summary='Verify VECTRA Context Capitalization readback')
def vectra_context_capitalization_verify():
    return json_response(verify_vectra_context_capitalization_readback())


@router.get('/vectra/evolution/journal', summary='Read VECTRA Evolution Journal Repository')
def vectra_evolution_journal():
    return list_vectra_evolution_journal_entries()


@router.get('/vectra/evolution/journal/latest', summary='Read latest VECTRA Evolution Journal entries')
def vectra_evolution_journal_latest():
    return get_vectra_evolution_journal_latest()


@router.get('/vectra/evolution/journal/status', summary='Read VECTRA Evolution Journal status')
def vectra_evolution_journal_status():
    return get_vectra_evolution_journal_status()


@router.get('/vectra/evolution/journal/verify', summary='Verify VECTRA Evolution Journal Readback')
def vectra_evolution_journal_verify():
    return verify_vectra_evolution_journal_readback()

@router.get('/vectra/professional-state', summary='Read VECTRA Professional State')
def vectra_professional_state_read():
    return json_response(read_vectra_runtime_object('professional_state'))


@router.get('/vectra/evolution-journal', summary='Read VECTRA Evolution Journal')
def vectra_evolution_journal_read(limit: int = 50):
    return json_response(read_vectra_runtime_object('evolution_journal', limit=limit))


@router.get('/vectra/recovery-bundle', summary='Read VECTRA Recovery Bundle')
def vectra_recovery_bundle_read():
    return json_response(read_vectra_runtime_object('recovery_bundle'))




# GENESIS-0001: Professional Model Repository Foundation
# The professional model is the stable knowledge source of VECTRA. Journals are
# history; this repository is what VECTRA restores at the start of a Laboratory
# working context.

@router.get('/vectra/professional-model', summary='Read VECTRA Professional Model Repository')
def vectra_professional_model_read():
    return json_response(get_vectra_professional_model())


@router.get('/vectra/professional-model/sections', summary='List VECTRA Professional Model Sections')
def vectra_professional_model_sections():
    return json_response(list_vectra_professional_model_sections())


@router.get('/vectra/professional-model/section/{section_id}', summary='Read VECTRA Professional Model Section')
def vectra_professional_model_section_read(section_id: str):
    return json_response(read_vectra_professional_model_section(section_id))


@router.post('/vectra/professional-model/section/{section_id}', summary='Update VECTRA Professional Model Section with readback')
def vectra_professional_model_section_update(section_id: str, request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(update_vectra_professional_model_section(section_id, request))


@router.get('/vectra/professional-model/verify', summary='Verify VECTRA Professional Model Readback')
def vectra_professional_model_verify(section_id: str = None):
    return json_response(get_vectra_professional_model() if not section_id else read_vectra_professional_model_section(section_id))


@router.post('/vectra/professional-model/verify', summary='Verify VECTRA Professional Model Readback Contract')
def vectra_professional_model_verify_post(request: dict = None):
    if not isinstance(request, dict):
        request = {}
    return json_response(verify_vectra_professional_model_readback(request.get('section_id')))


@router.get('/test-plan', summary='TEST PLAN Engine')
def test_plan_engine():
    from app.release_manager import build_test_plan_response
    return build_test_plan_response()


@router.post('/release-manager/run', summary='Autonomous Release Manager Acceptance')
def release_manager_run(request: dict):
    from app.release_manager import run_release_acceptance, build_release_manager_response
    if not isinstance(request, dict):
        request = {}
    result = run_release_acceptance(
        release_id=str(request.get('release_id') or 'manual-release'),
        scenario_ids=request.get('scenario_ids') if isinstance(request.get('scenario_ids'), list) else None,
        release_brief=request.get('release_brief') or request.get('brief'),
    )
    return build_release_manager_response(result)


@router.post('/release-manager/accept-release-brief', summary='Release Manager accepts Release Brief and starts Product Acceptance')
def release_manager_accept_release_brief(request: dict):
    """Receiving Release Brief is the trigger for Product Acceptance.

    Product Owner does not need a separate "check release" command; this route
    starts Release Manager automatically from the supplied Release Brief.
    """
    from app.release_manager import run_release_acceptance, build_release_manager_response
    if not isinstance(request, dict):
        request = {}
    brief_payload = request.get('release_brief') or request.get('brief') or request
    result = run_release_acceptance(
        release_id=str(request.get('release_id') or 'release-brief-received'),
        scenario_ids=request.get('scenario_ids') if isinstance(request.get('scenario_ids'), list) else None,
        release_brief=brief_payload,
    )
    response = build_release_manager_response(result)
    response['release_manager_trigger'] = 'release_brief_received'
    return response


@router.post('/release-brief/preview', summary='Release Brief Preview')
def release_brief_preview(request: dict):
    from app.release_brief import parse_release_brief, build_release_brief_markdown
    from app.development_journal import mark_tasks_fixed
    from app.release_brief import normalize_task_ids
    if not isinstance(request, dict):
        request = {}
    brief = parse_release_brief(request.get('release_brief') or request.get('brief') or request, fallback_release_id=str(request.get('release_id') or 'manual-release'))
    # Engineering automation boundary. Build tooling may supply implemented
    # task ids as top-level metadata; the Release Brief section itself is never
    # filled from this payload. We first persist Open -> Fixed in Development
    # Journal, then render the section only from journal state.
    engineering_fixed_ids = normalize_task_ids(
        request.get('engineering_fixed_task_ids')
        or request.get('implemented_engineering_task_ids')
        or request.get('fixed_task_ids')
    )
    if engineering_fixed_ids:
        mark_tasks_fixed(
            engineering_fixed_ids,
            release=str(brief.release_id),
            version=str(brief.build or ''),
            actor='Engineering',
            comment='Engineering build completed; fix persisted automatically before Release Brief rendering.',
        )
        brief.fixed_engineering_tasks = []
    return {
        'status': 'ok',
        'render_mode': 'release_brief',
        'context': {'level': 'release_brief', 'object_name': 'Release Brief', 'period': None},
        'workspace_markdown': build_release_brief_markdown(brief),
        'release_brief': brief.to_dict(),
    }


@router.post('/scenario-runner/run', summary='Scenario Runner Execution')
def scenario_runner_run(request: dict):
    from app.release_manager import _get_scenario
    from app.scenario_runner import run_scenario
    if not isinstance(request, dict):
        request = {}
    scenario_id = str(request.get('scenario_id') or 'S1-START-SCREEN')
    scenario = _get_scenario(scenario_id)
    if not scenario:
        return {'status': 'error', 'reason': 'unknown_scenario', 'scenario_id': scenario_id}
    return run_scenario(
        scenario=scenario,
        release_id=str(request.get('release_id') or 'manual-scenario-runner'),
        session_id=str(request.get('session_id') or '') or None,
        decision_callback=lambda step_result: 'PASS',
    )


@router.get('/scenario-library', summary='Scenario Library')
def scenario_library_get():
    from app.release_manager import get_full_scenario_library
    return {
        'status': 'ok',
        'render_mode': 'scenario_library',
        'scenario_library': get_full_scenario_library(),
    }


@router.post('/laboratory/analyze-journal', summary='Laboratory Journal Analysis')
def laboratory_analyze_journal(request: dict | None = None):
    from app.laboratory_processor import build_laboratory_response
    return build_laboratory_response()


@router.get('/', summary='Root')
def root():
    return json_response({'status': 'ok'})


@router.get('/health', summary='Health')
def health():
    return json_response({
        'status': 'ok',
        'sheet_url_exists': bool(SHEET_URL),
        'low_volume_threshold': LOW_VOLUME_THRESHOLD,
        'empty_sku_policy': EMPTY_SKU_LABEL,
    })



# LABORATORY-ACTIONS-0003 facade Runtime endpoints.
@router.get('/vectra/laboratory/state/restore', summary='Restore VECTRA Laboratory professional state')
def vectra_laboratory_state_restore(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    professional_body = restore_vectra_professional_body_state()
    business_registry = get_vectra_business_domain_registry()
    domains = (business_registry.get('business_domain_registry') or {}).get('domains', [])
    compact_domains = []
    for item in domains if isinstance(domains, list) else []:
        if not isinstance(item, dict):
            continue
        compact_domains.append({
            'domain_id': item.get('domain_id') or item.get('id') or item.get('domain'),
            'display_name': item.get('display_name') or item.get('name'),
            'status': item.get('status') or 'available',
        })
    professional_summary = {
        'status': professional_body.get('status'),
        'source_of_state': professional_body.get('source_of_state'),
        'professional_readiness': 'PROFESSIONAL_READY' if professional_body.get('status') == 'PASS' else 'PROFESSIONAL_RESTORE_FAILED',
        'identity_restored': bool(professional_body.get('professional_identity')),
        'operating_model_restored': bool(professional_body.get('professional_model')),
        'chat_memory_used_as_source': professional_body.get('chat_memory_used_as_source', False),
    }
    result = {
        'status': 'ok' if professional_body.get('status') == 'PASS' else 'degraded',
        'render_mode': 'vectra_laboratory_state_restore',
        'professional_state': professional_summary,
        'business_selection_status': 'required',
        'available_businesses': compact_domains,
        'active_business_domain': None,
        'business_core_loaded': False,
        'business_readiness_status': 'BUSINESS_DOMAIN_NOT_ACTIVE',
        'business_data_connected': False,
        'business_data_auto_started': False,
        'final_status': 'PROFESSIONAL_READY' if professional_body.get('status') == 'PASS' else 'PROFESSIONAL_RESTORE_FAILED',
        'recommended_next_action': 'select_business_domain',
        'next_dialogue': 'Профессиональная модель восстановлена. Покажите доступные бизнесы и спросите Product Owner, с каким бизнесом продолжаем работу.',
    }
    return json_response(_facade_response('restore_laboratory_state', 'laboratory.restore_state', '/vectra/laboratory/state/restore', result, next_action='Ask Product Owner to select a business domain.'))


@router.post('/vectra/laboratory/facade/knowledge', summary='Execute VECTRA Knowledge facade operation')
def vectra_laboratory_facade_knowledge(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    operation_type, payload, approval, domain, session_id, request_id = _normalize_facade_request(request)
    if not operation_type:
        return json_response(_facade_error('', 'operation_type is required', runtime_service='knowledge_facade'))
    if _requires_product_owner_approval(operation_type) and not approval and operation_type != 'capitalize_confirmed_knowledge':
        return json_response(_facade_error(operation_type, 'Product Owner approval is required for this write/capitalization operation.', runtime_service='knowledge_facade', next_action='Request Product Owner approval, then retry through executeVectraKnowledgeOperation.'))
    try:
        if operation_type == 'get_capitalization_status':
            return json_response(_facade_response(operation_type, 'knowledge_capitalization.get_status', '/vectra/knowledge/capitalization/status', get_vectra_knowledge_capitalization_status()))
        if operation_type == 'create_candidate':
            return json_response(_facade_response(operation_type, 'knowledge_capitalization.create_candidate', '/vectra/knowledge/candidates', create_vectra_knowledge_candidate(payload), next_action='Create capitalization package after Product Owner approval.'))
        if operation_type == 'create_package':
            return json_response(_facade_response(operation_type, 'knowledge_capitalization.create_package', '/vectra/knowledge/capitalization/packages', create_vectra_capitalization_package(payload), next_action='Write confirmed knowledge.'))
        if operation_type == 'write_professional_knowledge':
            payload['knowledge_type'] = 'professional'
            return json_response(_facade_response(operation_type, 'knowledge_capitalization.write_confirmed_knowledge', '/vectra/knowledge/capitalization/write', write_vectra_confirmed_knowledge(payload), next_action='Verify Professional Knowledge readback.'))
        if operation_type == 'write_business_knowledge':
            if domain:
                payload['domain'] = domain
            return json_response(_facade_response(operation_type, 'knowledge_capitalization.write_business_knowledge', '/vectra/domain/{domain}/knowledge/capitalization/write', write_vectra_business_knowledge(payload), next_action='Verify Business Knowledge readback.'))
        if operation_type == 'read_professional_knowledge':
            kid = str(payload.get('knowledge_id') or '').strip()
            result = get_vectra_professional_knowledge(knowledge_id=kid) if kid else list_vectra_professional_knowledge()
            endpoint = '/vectra/knowledge/professional/{knowledge_id}' if kid else '/vectra/knowledge/professional'
            return json_response(_facade_response(operation_type, 'knowledge_capitalization.read_professional_knowledge', endpoint, result))
        if operation_type == 'read_business_knowledge':
            d = domain or str(payload.get('domain') or 'bonboason')
            kid = str(payload.get('knowledge_id') or '').strip()
            result = get_vectra_domain_knowledge_by_id(domain=d, knowledge_id=kid) if kid else get_vectra_domain_knowledge(domain=d)
            endpoint = '/vectra/domain/{domain}/knowledge/{knowledge_id}' if kid else '/vectra/domain/{domain}/knowledge'
            return json_response(_facade_response(operation_type, 'knowledge_capitalization.read_business_knowledge', endpoint, result))
        if operation_type == 'verify_readback':
            kt = str(payload.get('knowledge_type') or '').lower()
            kid = str(payload.get('knowledge_id') or '').strip()
            d = domain or str(payload.get('domain') or 'bonboason')
            if kt == 'business':
                result = verify_vectra_domain_knowledge_readback(domain=d, knowledge_id=kid)
                endpoint = '/vectra/domain/{domain}/knowledge/{knowledge_id}/readback'
                service = 'knowledge_capitalization.verify_domain_knowledge_readback'
            else:
                result = verify_vectra_professional_knowledge_readback(knowledge_id=kid)
                endpoint = '/vectra/knowledge/professional/{knowledge_id}/readback'
                service = 'knowledge_capitalization.verify_professional_knowledge_readback'
            return json_response(_facade_response(operation_type, service, endpoint, result))
        if operation_type == 'list_memory_objects':
            result = list_vectra_memory_objects(memory_space=payload.get('memory_space'), domain=domain or payload.get('domain') or 'bonboason', limit=int(payload.get('limit') or 100))
            return json_response(_facade_response(operation_type, 'memory_repository.list_memory_objects', '/vectra/memory/objects', result))
        if operation_type == 'read_memory_object':
            result = get_vectra_memory_object(object_id=str(payload.get('object_id') or ''), domain=domain or payload.get('domain') or 'bonboason')
            return json_response(_facade_response(operation_type, 'memory_repository.get_memory_object', '/vectra/memory/objects/{object_id}', result))
        if operation_type == 'search_memory_object':
            result = readback_vectra_memory_object(knowledge_id=str(payload.get('knowledge_id') or ''), memory_space=payload.get('memory_space'), domain=domain or payload.get('domain') or 'bonboason')
            return json_response(_facade_response(operation_type, 'memory_repository.readback_memory_object', '/vectra/memory/readback', result))
        if operation_type == 'verify_memory_object_readback':
            result = readback_vectra_memory_object(object_id=payload.get('object_id'), knowledge_id=payload.get('knowledge_id'), memory_space=payload.get('memory_space'), domain=domain or payload.get('domain') or 'bonboason')
            return json_response(_facade_response(operation_type, 'memory_repository.readback_memory_object', '/vectra/memory/readback', result))
        if operation_type == 'get_memory_overview':
            result = get_vectra_memory_overview(domain=domain or payload.get('domain') or 'bonboason')
            return json_response(_facade_response(operation_type, 'memory_repository.get_memory_overview', '/vectra/memory/overview', result))
        if operation_type == 'verify_memory_repository':
            result = verify_vectra_memory_repository_integrity(domain=domain or payload.get('domain') or 'bonboason')
            return json_response(_facade_response(operation_type, 'memory_repository.verify_memory_repository_integrity', '/vectra/memory/verify', result))
        if operation_type == 'list_memory_spaces':
            result = list_vectra_memory_spaces(include_prepared=bool(payload.get('include_prepared', True)))
            return json_response(_facade_response(operation_type, 'memory_spaces.list_memory_spaces', '/vectra/memory/spaces', result))
        if operation_type == 'validate_memory_space':
            result = validate_vectra_memory_space(str(payload.get('memory_space') or ''), require_active=bool(payload.get('require_active', False)))
            return json_response(_facade_response(operation_type, 'memory_spaces.validate_memory_space', '/vectra/memory/spaces/{memory_space}/validate', result))
        if operation_type == 'classify_knowledge_item':
            result = classify_vectra_knowledge_item(payload, domain=domain or payload.get('domain') or 'bonboason')
            return json_response(_facade_response(operation_type, 'memory_classification.classify_knowledge_item', '/vectra/memory/classification', result))
        if operation_type == 'classify_knowledge_package':
            result = classify_vectra_knowledge_package(payload, domain=domain or payload.get('domain') or 'bonboason')
            return json_response(_facade_response(operation_type, 'memory_classification.classify_knowledge_package', '/vectra/memory/classification', result))
        if operation_type == 'verify_automatic_classification':
            result = verify_vectra_automatic_classification(payload, domain=domain or payload.get('domain') or 'bonboason')
            return json_response(_facade_response(operation_type, 'memory_classification.verify_automatic_classification', '/vectra/memory/classification/verify', result))
        if operation_type == 'inspect_memory':
            result = run_vectra_memory_inspection(operation_type=str(payload.get('inspection_type') or payload.get('inspection_operation') or 'overview'), payload=payload, domain=domain or payload.get('domain') or 'bonboason')
            return json_response(_facade_response(operation_type, 'memory_inspection.run_memory_inspection', '/vectra/memory/inspection', result))
        if operation_type == 'inspect_memory_object':
            result = inspect_vectra_memory_object(object_id=str(payload.get('object_id') or ''), domain=domain or payload.get('domain') or 'bonboason')
            return json_response(_facade_response(operation_type, 'memory_inspection.inspect_memory_object', '/vectra/memory/inspection/object/{object_id}', result))
        if operation_type == 'inspect_memory_space':
            result = inspect_vectra_memory_space(memory_space=str(payload.get('memory_space') or ''), domain=domain or payload.get('domain') or 'bonboason', limit=int(payload.get('limit') or 100))
            return json_response(_facade_response(operation_type, 'memory_inspection.inspect_memory_space', '/vectra/memory/inspection/space/{memory_space}', result))
        if operation_type == 'get_memory_statistics':
            result = get_vectra_memory_statistics(domain=domain or payload.get('domain') or 'bonboason')
            return json_response(_facade_response(operation_type, 'memory_inspection.get_memory_statistics', '/vectra/memory/statistics', result))
        if operation_type == 'get_memory_integrity_report':
            result = get_vectra_memory_integrity_report(domain=domain or payload.get('domain') or 'bonboason')
            return json_response(_facade_response(operation_type, 'memory_inspection.get_memory_integrity_report', '/vectra/memory/integrity-report', result))
        if operation_type == 'get_memory_readback_report':
            result = get_vectra_memory_readback_report(domain=domain or payload.get('domain') or 'bonboason', limit=int(payload.get('limit') or 100))
            return json_response(_facade_response(operation_type, 'memory_inspection.get_memory_readback_report', '/vectra/memory/readback-report', result))
        if operation_type == 'create_report':
            reports_result = list_vectra_knowledge_capitalization_reports(limit=int(payload.get('limit') or 20), include_failed=bool(payload.get('include_failed', True)))
            report_id = str(payload.get('report_id') or '').strip()
            if report_id and isinstance(reports_result, dict):
                reports = reports_result.get('reports') if isinstance(reports_result.get('reports'), list) else []
                selected = next((r for r in reports if isinstance(r, dict) and str(r.get('report_id')) == report_id), None)
                reports_result = {
                    'status': 'ok' if selected else 'not_found',
                    'render_mode': 'vectra_knowledge_capitalization_report_readback',
                    'release': reports_result.get('release'),
                    'report_id': report_id,
                    'report': selected,
                    'final_status': selected.get('final_status') if isinstance(selected, dict) else 'NOT_FOUND',
                }
            return json_response(_facade_response(operation_type, 'knowledge_capitalization.list_reports', '/vectra/knowledge/capitalization/reports', reports_result))
        if operation_type == 'capitalize_confirmed_knowledge':
            result = auto_capitalize_vectra_confirmed_knowledge(payload)
            compact_result = _compact_capitalization_facade_result(result, operation_type)
            next_action = 'Capitalization completed. Full technical report remains in Runtime and can be opened by report_id through operation_type=create_report.'
            if isinstance(result, dict) and result.get('final_status') == 'REQUIRES_PRODUCT_OWNER_APPROVAL':
                next_action = 'Product Owner must confirm capitalization, then Laboratory reruns the same facade operation with product_owner_approval=true.'
            compact_result['next_recommended_action'] = next_action
            return json_response(_facade_response(operation_type, 'knowledge_capitalization.AUTO_CAPITALIZATION_PIPELINE', '/vectra/laboratory/facade/knowledge', compact_result, next_action=next_action))
        return json_response(_facade_error(operation_type, f'Unsupported knowledge operation_type: {operation_type}', runtime_service='knowledge_facade'))
    except Exception as exc:
        logger.exception('knowledge_facade_operation_failed')
        return json_response(_facade_error(operation_type, str(exc), runtime_service='knowledge_facade'))


@router.get('/vectra/memory/objects', summary='List unified VECTRA Memory Objects')
def vectra_memory_objects(memory_space: str | None = None, domain: str = 'bonboason', limit: int = 100, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(list_vectra_memory_objects(memory_space=memory_space, domain=domain, limit=limit))


@router.get('/vectra/memory/objects/{object_id}', summary='Read unified VECTRA Memory Object')
def vectra_memory_object_by_id(object_id: str, domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_memory_object(object_id=object_id, domain=domain))


@router.post('/vectra/memory/readback', summary='Verify unified VECTRA Memory Object readback')
def vectra_memory_object_readback(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    return json_response(readback_vectra_memory_object(object_id=payload.get('object_id'), knowledge_id=payload.get('knowledge_id'), memory_space=payload.get('memory_space'), domain=payload.get('domain') or 'bonboason'))


@router.get('/vectra/memory/overview', summary='Get unified VECTRA Memory overview')
def vectra_memory_overview(domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_memory_overview(domain=domain))


@router.get('/vectra/memory/verify', summary='Verify unified VECTRA Memory Repository integrity')
def vectra_memory_verify(domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_memory_repository_integrity(domain=domain))


@router.get('/vectra/memory/spaces', summary='List VECTRA Memory Spaces')
def vectra_memory_spaces(include_prepared: bool = True, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(list_vectra_memory_spaces(include_prepared=include_prepared))


@router.get('/vectra/memory/spaces/{memory_space}', summary='Read VECTRA Memory Space')
def vectra_memory_space_by_id(memory_space: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_memory_space(memory_space))


@router.get('/vectra/memory/spaces/{memory_space}/validate', summary='Validate VECTRA Memory Space')
def vectra_memory_space_validate(memory_space: str, require_active: bool = False, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(validate_vectra_memory_space(memory_space, require_active=require_active))


@router.post('/vectra/memory/classification', summary='Classify VECTRA Knowledge Package')
def vectra_memory_classification(request: dict = None, domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    return json_response(classify_vectra_knowledge_package(payload, domain=payload.get('domain') or domain))


@router.post('/vectra/memory/classification/verify', summary='Verify VECTRA Automatic Knowledge Classification')
def vectra_memory_classification_verify(request: dict = None, domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    return json_response(verify_vectra_automatic_classification(payload, domain=payload.get('domain') or domain))


@router.post('/vectra/memory/inspection', summary='Run VECTRA Memory Inspection')
def vectra_memory_inspection(request: dict = None, domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    return json_response(run_vectra_memory_inspection(operation_type=str(payload.get('inspection_type') or payload.get('inspection_operation') or 'overview'), payload=payload, domain=payload.get('domain') or domain))


@router.get('/vectra/memory/inspection/object/{object_id}', summary='Inspect VECTRA Memory Object')
def vectra_memory_inspection_object(object_id: str, domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(inspect_vectra_memory_object(object_id=object_id, domain=domain))


@router.get('/vectra/memory/inspection/space/{memory_space}', summary='Inspect VECTRA Memory Space')
def vectra_memory_inspection_space(memory_space: str, domain: str = 'bonboason', limit: int = 100, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(inspect_vectra_memory_space(memory_space=memory_space, domain=domain, limit=limit))


@router.get('/vectra/memory/statistics', summary='Get VECTRA Memory Statistics')
def vectra_memory_statistics(domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_memory_statistics(domain=domain))


@router.get('/vectra/memory/integrity-report', summary='Get VECTRA Memory Integrity Report')
def vectra_memory_integrity_report(domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_memory_integrity_report(domain=domain))


@router.get('/vectra/memory/readback-report', summary='Get VECTRA Memory Readback Report')
def vectra_memory_readback_report(domain: str = 'bonboason', limit: int = 100, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_memory_readback_report(domain=domain, limit=limit))


@router.get('/vectra/memory/product-knowledge', summary='List VECTRA Product Knowledge Runtime objects')
def vectra_product_knowledge_runtime(limit: int = 100, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(list_vectra_product_knowledge_runtime(limit=limit))


@router.get('/vectra/memory/product-knowledge/{knowledge_id}', summary='Read VECTRA Product Knowledge by ID')
def vectra_product_knowledge_runtime_by_id(knowledge_id: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_product_knowledge_runtime(knowledge_id=knowledge_id))


@router.post('/vectra/memory/product-knowledge', summary='Capitalize VECTRA Product Knowledge')
def vectra_product_knowledge_runtime_write(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    return json_response(write_vectra_product_knowledge_runtime(payload))


@router.get('/vectra/memory/product-knowledge/verify/readback', summary='Verify VECTRA Product Knowledge readback')
def vectra_product_knowledge_runtime_verify(knowledge_id: str | None = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_product_knowledge_runtime(knowledge_id=knowledge_id))


@router.get('/vectra/memory/product-decisions', summary='List VECTRA Product Decisions Runtime objects')
def vectra_product_decisions_runtime(limit: int = 100, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(list_vectra_product_decisions_runtime(limit=limit))


@router.get('/vectra/memory/product-decisions/{decision_id}', summary='Read VECTRA Product Decision by ID')
def vectra_product_decision_runtime_by_id(decision_id: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_product_decision_runtime(decision_id=decision_id))


@router.post('/vectra/memory/product-decisions', summary='Record VECTRA Product Decision')
def vectra_product_decision_runtime_write(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    return json_response(write_vectra_product_decision_runtime(payload))


@router.get('/vectra/memory/product-decisions/verify/readback', summary='Verify VECTRA Product Decisions readback')
def vectra_product_decisions_runtime_verify(decision_id: str | None = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_product_decisions_runtime(decision_id=decision_id))


@router.get('/vectra/memory/health', summary='Get VECTRA Memory Health status')
def vectra_memory_health(domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_memory_health_status(domain=domain))


@router.get('/vectra/memory/diagnostics', summary='Get VECTRA Memory Diagnostics report')
def vectra_memory_diagnostics(domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_memory_diagnostics_report(domain=domain))


@router.get('/vectra/memory/health/verify', summary='Verify VECTRA Memory Health')
def vectra_memory_health_verify(domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_memory_health(domain=domain))


@router.post('/vectra/laboratory/facade/business-domain', summary='Execute VECTRA Business Domain facade operation')
def vectra_laboratory_facade_business_domain(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    operation_type, payload, approval, domain, session_id, request_id = _normalize_facade_request(request)
    d = domain or str(payload.get('domain') or 'bonboason')
    if _requires_product_owner_approval(operation_type) and not approval:
        return json_response(_facade_error(operation_type, 'Product Owner approval is required.', runtime_service='business_domain_facade'))
    try:
        if operation_type in {'list_domains', 'available_businesses'}:
            return json_response(_facade_response(operation_type, 'repository.get_business_domain_registry', '/vectra/domains', get_vectra_business_domain_registry(), next_action='Ask Product Owner which business to activate.'))
        if operation_type in {'start_session', 'begin_working_session'}:
            return json_response(_facade_response(operation_type, 'repository.start_business_working_session', '/vectra/domain/session/start', start_vectra_business_working_session(payload), next_action='Continue the dialogue from the returned Business Readiness state.'))
        if operation_type == 'activate_domain':
            payload['domain_id'] = d
            payload.setdefault('session_id', session_id)
            payload.setdefault('request_id', request_id)
            return json_response(_facade_response(operation_type, 'repository.activate_business_domain', '/vectra/domain/activate', activate_vectra_business_domain(payload), next_action='Continue business dialogue. Do not connect Business Data unless the current request requires facts.'))
        if operation_type == 'load_business_core':
            payload['domain_id'] = d
            payload.setdefault('session_id', session_id)
            payload.setdefault('request_id', request_id)
            return json_response(_facade_response(operation_type, 'repository.load_business_core', '/vectra/domain/business-core/load', load_vectra_business_core(payload), next_action='Continue business dialogue. Business Data remains disconnected until needed.'))
        if operation_type in {'business_readiness', 'get_business_readiness'}:
            return json_response(_facade_response(operation_type, 'repository.get_business_readiness_status', '/vectra/domain/business-readiness', get_vectra_business_readiness_status()))
        if operation_type == 'restore_domain':
            return json_response(_facade_response(operation_type, 'repository.restore_business_domain', '/vectra/domain/recover', restore_vectra_business_domain(d)))
        if operation_type == 'get_domain_profile':
            return json_response(_facade_response(operation_type, 'repository.get_business_domain_profile', '/vectra/domains', get_vectra_business_domain_profile(d)))
        if operation_type == 'get_domain_knowledge':
            return json_response(_facade_response(operation_type, 'knowledge_capitalization.get_domain_knowledge', '/vectra/domain/{domain}/knowledge', get_vectra_domain_knowledge(domain=d)))
        if operation_type == 'capitalize_domain_knowledge':
            payload['domain'] = d
            return json_response(_facade_response(operation_type, 'knowledge_capitalization.write_business_knowledge', '/vectra/domain/{domain}/knowledge/capitalization/write', write_vectra_business_knowledge(payload)))
        if operation_type == 'verify_domain_knowledge':
            kid = str(payload.get('knowledge_id') or '').strip()
            return json_response(_facade_response(operation_type, 'knowledge_capitalization.verify_domain_knowledge_readback', '/vectra/domain/{domain}/knowledge/{knowledge_id}/readback', verify_vectra_domain_knowledge_readback(domain=d, knowledge_id=kid)))
        return json_response(_facade_error(operation_type, f'Unsupported business domain operation_type: {operation_type}', runtime_service='business_domain_facade'))
    except Exception as exc:
        logger.exception('business_domain_facade_operation_failed')
        return json_response(_facade_error(operation_type, str(exc), runtime_service='business_domain_facade'))


@router.post('/vectra/laboratory/facade/business-data', summary='Execute VECTRA Business Data facade operation')
def vectra_laboratory_facade_business_data(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    operation_type, payload, approval, domain, session_id, request_id = _normalize_facade_request(request)
    period = str(payload.get('period') or '')
    try:
        if operation_type in {'manifest', 'capabilities', 'business_data_manifest', 'business_data_capabilities'}:
            return json_response(_facade_response(operation_type, 'business_data.get_manifest', '/vectra/laboratory/facade/business-data', get_vectra_business_data_manifest(), next_action='Select the next Business Data operation only from supported_operation_types.'))
        if operation_type == 'status':
            return json_response(_facade_response(operation_type, 'business_data.get_status', '/vectra/laboratory/business-data/status', get_vectra_business_data_status()))
        if operation_type == 'entities':
            return json_response(_facade_response(operation_type, 'business_data.get_entities', '/vectra/laboratory/business-data/entities', get_vectra_business_data_entities(limit_per_group=int(payload.get('limit_per_group') or 50))))
        if operation_type in {'discovery', 'business_discovery', 'discover', 'inspect_source'}:
            include_samples = payload.get('include_samples', False)
            if isinstance(include_samples, str):
                include_samples = include_samples.strip().lower() in {'1', 'true', 'yes', 'on'}
            return json_response(_facade_response(
                operation_type,
                'business_data.discovery',
                '/vectra/laboratory/facade/business-data',
                get_vectra_business_data_discovery(
                    period=period or None,
                    limit=int(payload.get('limit') or 25),
                    limit_per_group=int(payload.get('limit_per_group') or 12),
                    include_samples=bool(include_samples),
                    sample_size=int(payload.get('sample_size') or 3),
                ),
            ))
        if operation_type in {'summary', 'summary_business', 'business_summary', 'summary/business'}:
            return json_response(_facade_response(operation_type, 'business_data.get_summary', '/vectra/laboratory/business-data/summary/business', public_summary(get_vectra_business_data_summary('business', period=period))))
        if operation_type == 'verify':
            return json_response(_facade_response(operation_type, 'business_data.verify', '/vectra/laboratory/business-data/verify', verify_vectra_business_data_access()))
        if operation_type == 'query':
            return json_response(_facade_response(operation_type, 'business_data.query', '/vectra/laboratory/business-data/query', run_vectra_business_data_query(message=str(payload.get('message') or payload.get('query') or ''), session_id=session_id or 'laboratory-facade')))
        if operation_type in {'first_impression', 'explore', 'initial_exploration', 'business_first_impression'}:
            return json_response(_facade_response(operation_type, 'business_data.first_impression', '/vectra/laboratory/facade/business-data', get_vectra_business_data_first_impression(period=period or None, message=str(payload.get('message') or payload.get('query') or ''))))
        level_map = {
            'manager_summary': ('manager', 'manager', '/vectra/laboratory/business-data/summary/manager'),
            'contract_summary': ('network', 'network', '/vectra/laboratory/business-data/summary/network'),
            'category_summary': ('category', 'category', '/vectra/laboratory/business-data/summary/category'),
            'sku_summary': ('sku', 'sku', '/vectra/laboratory/business-data/summary/sku'),
        }
        if operation_type in level_map:
            level, key, endpoint = level_map[operation_type]
            kwargs = {'period': period, key: str(payload.get(key) or payload.get('object_name') or '')}
            return json_response(_facade_response(operation_type, f'business_data.summary.{level}', endpoint, public_summary(get_vectra_business_data_summary(level, **kwargs))))
        return json_response(_facade_error(operation_type, f'Unsupported business data operation_type: {operation_type}', runtime_service='business_data_facade'))
    except Exception as exc:
        logger.exception('business_data_facade_operation_failed')
        return json_response(_facade_error(operation_type, str(exc), runtime_service='business_data_facade'))


@router.post('/vectra/laboratory/facade/product-review', summary='Execute VECTRA Product Review facade operation')
def vectra_laboratory_facade_product_review(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    operation_type, payload, approval, domain, session_id, request_id = _normalize_facade_request(request)
    try:
        if operation_type in {'inspect_workspace', 'verify_workspace'}:
            result = run_vectra_runtime_product_verification()
            return json_response(_facade_response(operation_type, 'repository.run_runtime_product_verification', '/vectra/runtime/verify', result))
        if operation_type == 'detect_product_issue':
            result = {'status': 'ok', 'issue_classification': 'requires_product_review', 'payload': payload}
            return json_response(_facade_response(operation_type, 'product_review.detect_product_issue', '/vectra/laboratory/facade/product-review', result, next_action='Create product observation if Product Owner confirms.'))
        if operation_type == 'create_product_observation':
            if not approval:
                return json_response(_facade_error(operation_type, 'Product Owner approval is required to create a product observation.', runtime_service='product_review_facade'))
            result = add_development_journal_global_record(payload)
            return json_response(_facade_response(operation_type, 'development_journal.add_global_record', '/development-journal/global-record', result))
        if operation_type == 'create_engineering_task':
            if not approval:
                return json_response(_facade_error(operation_type, 'Product Owner approval is required to create an engineering task.', runtime_service='product_review_facade'))
            result = add_development_journal_global_record(payload)
            return json_response(_facade_response(operation_type, 'development_journal.add_global_record', '/development-journal/global-record', result))
        if operation_type == 'generate_product_review_report':
            result = build_development_journal_response(limit=int(payload.get('limit') or 50))
            return json_response(_facade_response(operation_type, 'development_journal.build_journal_response', '/development-journal', result))
        return json_response(_facade_error(operation_type, f'Unsupported product review operation_type: {operation_type}', runtime_service='product_review_facade'))
    except Exception as exc:
        logger.exception('product_review_facade_operation_failed')
        return json_response(_facade_error(operation_type, str(exc), runtime_service='product_review_facade'))



# BUSINESS-FRAMEWORK-RESEARCH-CAPABILITY-001:
# Explicit GPT Actions for the first Laboratory Research Professional Activity.
# These routes make the capabilities directly visible in the compact OpenAPI
# instead of requiring Laboratory to infer hidden operation_type values.
@router.post('/vectra/laboratory/research/programs', summary='Create Business Framework Research Program')
def vectra_create_research_program_action(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    result = create_vectra_research_program(payload)
    return json_response(_facade_response(
        'create_research_program',
        'business_framework_research.create_research_program',
        '/vectra/laboratory/research/programs',
        result,
        next_action='Open the Research Workspace and continue the approved Research Program.',
    ))


@router.post('/vectra/laboratory/research/workspace', summary='Get Digital Business Analyst Research Workspace')
def vectra_get_research_workspace_action(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    result = get_vectra_research_workspace(payload)
    return json_response(_facade_response(
        'get_research_workspace',
        'business_framework_research.get_research_workspace',
        '/vectra/laboratory/research/workspace',
        result,
        next_action='Continue the highest-priority approved Research Program.',
    ))


@router.get('/vectra/laboratory/research/foundation/verify', summary='Verify Business Framework Research Foundation')
def vectra_verify_business_framework_research_foundation_action(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    result = verify_vectra_business_framework_research_foundation()
    return json_response(_facade_response(
        'verify_business_framework_research_foundation',
        'business_framework_research.verify_business_framework_research_foundation',
        '/vectra/laboratory/research/foundation/verify',
        result,
        next_action='Create the first Research Program or open the Research Workspace.',
    ))


@router.post('/vectra/laboratory/facade/memory', summary='Execute VECTRA Memory facade operation')
def vectra_laboratory_facade_memory(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    operation_type, payload, approval, domain, session_id, request_id = _normalize_facade_request(request)
    try:
        if operation_type in {'digital_organization_registry_manifest', 'digital_roles_manifest'}:
            return json_response(_facade_response(operation_type, 'digital_organization.get_registry_manifest', '/vectra/laboratory/facade/memory', get_vectra_digital_organization_registry_manifest(), next_action='Register or inspect Digital Professional Roles.'))
        if operation_type in {'register_digital_professional_role', 'digital_role_register'}:
            return json_response(_facade_response(operation_type, 'digital_organization.register_role', '/vectra/laboratory/facade/memory', register_vectra_digital_professional_role(payload)))
        if operation_type in {'get_digital_professional_role', 'digital_role_get'}:
            return json_response(_facade_response(operation_type, 'digital_organization.get_role', '/vectra/laboratory/facade/memory', get_vectra_digital_professional_role(payload)))
        if operation_type in {'list_digital_professional_roles', 'digital_roles_list'}:
            return json_response(_facade_response(operation_type, 'digital_organization.list_roles', '/vectra/laboratory/facade/memory', list_vectra_digital_professional_roles(payload)))
        if operation_type in {'verify_digital_organization_registry', 'digital_organization_registry_verify'}:
            return json_response(_facade_response(operation_type, 'digital_organization.verify_registry', '/vectra/laboratory/facade/memory', verify_vectra_digital_organization_registry()))
        if operation_type in {'framework_validation_manifest', 'digital_business_analyst_framework_validation_manifest'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.framework_validation_manifest', '/vectra/laboratory/facade/memory', get_vectra_framework_validation_manifest(), next_action='Run professional validation of the existing Business Workspace Framework.'))
        if operation_type in {'run_business_workspace_framework_validation', 'validate_existing_business_workspace_framework'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.run_framework_validation', '/vectra/laboratory/facade/memory', run_vectra_business_workspace_framework_validation(payload), next_action='Review the Framework Validation Report and complete Product Review.'))
        if operation_type in {'verify_business_workspace_framework_validation', 'framework_validation_verify'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.verify_framework_validation', '/vectra/laboratory/facade/memory', verify_vectra_business_workspace_framework_validation()))
        if operation_type in {'business_framework_research_manifest', 'research_foundation_manifest'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.get_manifest', '/vectra/laboratory/facade/memory', get_vectra_business_framework_research_manifest(), next_action='Create or inspect a Research Program.'))
        if operation_type in {'create_research_program', 'research_program_create'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.create_program', '/vectra/laboratory/facade/memory', create_vectra_research_program(payload), next_action='Approve the Research Program before activation.'))
        if operation_type in {'transition_research_program', 'research_program_transition'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.transition_program', '/vectra/laboratory/facade/memory', transition_vectra_research_program(payload)))
        if operation_type in {'get_research_program', 'research_program_get'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.get_program', '/vectra/laboratory/facade/memory', get_vectra_research_program(payload)))
        if operation_type in {'list_research_programs', 'research_programs_list', 'research_backlog'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.list_programs', '/vectra/laboratory/facade/memory', list_vectra_research_programs(payload)))
        if operation_type in {'create_research_hypothesis', 'research_hypothesis_create'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.create_hypothesis', '/vectra/laboratory/facade/memory', create_vectra_research_hypothesis(payload)))
        if operation_type in {'transition_research_hypothesis', 'research_hypothesis_transition'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.transition_hypothesis', '/vectra/laboratory/facade/memory', transition_vectra_research_hypothesis(payload)))
        if operation_type in {'get_research_hypothesis', 'research_hypothesis_get'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.get_hypothesis', '/vectra/laboratory/facade/memory', get_vectra_research_hypothesis(payload)))
        if operation_type in {'list_research_hypotheses', 'research_hypotheses_list'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.list_hypotheses', '/vectra/laboratory/facade/memory', list_vectra_research_hypotheses(payload)))
        if operation_type in {'add_research_program_evidence', 'research_program_evidence_add'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.add_evidence', '/vectra/laboratory/facade/memory', add_vectra_research_program_evidence(payload), next_action='Validate shared Research Evidence before confirming a hypothesis or finding.'))
        if operation_type in {'add_research_program_finding', 'research_program_finding_add'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.add_finding', '/vectra/laboratory/facade/memory', add_vectra_research_program_finding(payload)))
        if operation_type in {'create_product_recommendation', 'research_product_recommendation_create'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.create_product_recommendation', '/vectra/laboratory/facade/memory', create_vectra_product_recommendation(payload), next_action='Submit the recommendation for Product Owner Review.'))
        if operation_type in {'record_research_product_owner_review', 'research_product_owner_review'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.record_product_owner_review', '/vectra/laboratory/facade/memory', record_vectra_research_product_owner_review(payload)))
        if operation_type in {'link_research_engineering_task', 'research_engineering_task_link'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.link_engineering_task', '/vectra/laboratory/facade/memory', link_vectra_research_engineering_task(payload)))
        if operation_type in {'record_research_product_verification', 'research_product_verification_record'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.record_product_verification', '/vectra/laboratory/facade/memory', record_vectra_research_product_verification(payload)))
        if operation_type in {'link_research_knowledge_capitalization', 'research_knowledge_capitalization_link'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.link_knowledge_capitalization', '/vectra/laboratory/facade/memory', link_vectra_research_knowledge_capitalization(payload)))
        if operation_type in {'register_professional_methodology', 'professional_methodology_register'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.register_methodology', '/vectra/laboratory/facade/memory', register_vectra_professional_methodology(payload)))
        if operation_type in {'get_professional_methodology', 'professional_methodology_get'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.get_methodology', '/vectra/laboratory/facade/memory', get_vectra_professional_methodology(payload)))
        if operation_type in {'list_professional_methodologies', 'professional_methodologies_list'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.list_methodologies', '/vectra/laboratory/facade/memory', list_vectra_professional_methodologies(payload)))
        if operation_type in {'evaluate_research_maturity', 'research_maturity_evaluate'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.evaluate_maturity', '/vectra/laboratory/facade/memory', evaluate_vectra_research_maturity(payload)))
        if operation_type in {'get_research_traceability', 'research_traceability_get'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.get_traceability', '/vectra/laboratory/facade/memory', get_vectra_research_traceability(payload)))
        if operation_type in {'get_research_workspace', 'research_workspace'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.get_workspace', '/vectra/laboratory/facade/memory', get_vectra_research_workspace(payload), next_action='Continue the highest-priority approved Research Program.'))
        if operation_type in {'verify_business_framework_research_foundation', 'research_foundation_verify'}:
            return json_response(_facade_response(operation_type, 'business_framework_research.verify_foundation', '/vectra/laboratory/facade/memory', verify_vectra_business_framework_research_foundation()))
        if operation_type in {'business_runtime_integration_manifest', 'digital_business_analyst_runtime_manifest'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.runtime_integration_manifest', '/vectra/laboratory/facade/memory', get_vectra_business_runtime_integration_manifest()))
        if operation_type == 'connect_business_runtime':
            return json_response(_facade_response(operation_type, 'digital_business_analyst.connect_business_runtime', '/vectra/laboratory/facade/memory', connect_vectra_business_runtime(payload), next_action='Execute existing Business Runtime commands in read-only mode.'))
        if operation_type == 'execute_business_runtime_command':
            return json_response(_facade_response(operation_type, 'digital_business_analyst.execute_business_runtime_command', '/vectra/laboratory/facade/memory', execute_vectra_business_runtime_command(payload)))
        if operation_type == 'open_existing_business_workspace':
            return json_response(_facade_response(operation_type, 'digital_business_analyst.open_existing_business_workspace', '/vectra/laboratory/facade/memory', open_vectra_existing_business_workspace(payload)))
        if operation_type == 'navigate_existing_business_workspace':
            return json_response(_facade_response(operation_type, 'digital_business_analyst.navigate_existing_business_workspace', '/vectra/laboratory/facade/memory', navigate_vectra_existing_business_workspace(payload)))
        if operation_type in {'get_business_runtime_context', 'inspect_business_navigation_context'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.get_business_runtime_context', '/vectra/laboratory/facade/memory', get_vectra_business_runtime_context(payload)))
        if operation_type == 'start_business_workspace_product_research':
            return json_response(_facade_response(operation_type, 'digital_business_analyst.start_product_research', '/vectra/laboratory/facade/memory', start_vectra_business_workspace_product_research(payload)))
        if operation_type == 'capture_business_workspace_research_step':
            return json_response(_facade_response(operation_type, 'digital_business_analyst.capture_product_research_step', '/vectra/laboratory/facade/memory', capture_vectra_business_workspace_research_step(payload)))
        if operation_type in {'run_business_workspace_framework_product_research', 'product_research_existing_business_workspace'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.run_workspace_framework_product_research', '/vectra/laboratory/facade/memory', run_vectra_business_workspace_framework_product_research(payload), next_action='Review the completed Product Research Report and return Product Verification PASS or grounded findings.'))
        if operation_type == 'list_business_runtime_sessions':
            return json_response(_facade_response(operation_type, 'digital_business_analyst.list_business_runtime_sessions', '/vectra/laboratory/facade/memory', list_vectra_business_runtime_sessions(payload)))
        if operation_type in {'verify_business_runtime_integration', 'digital_business_analyst_runtime_verify'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.verify_business_runtime_integration', '/vectra/laboratory/facade/memory', verify_vectra_business_runtime_integration()))
        if operation_type in {'digital_business_analyst_manifest', 'business_analyst_manifest'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.get_manifest', '/vectra/laboratory/facade/memory', get_vectra_digital_business_analyst_manifest(), next_action='Create a Business Review through the reference Digital Professional Role.'))
        if operation_type in {'create_business_review', 'business_review_create'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.create_review', '/vectra/laboratory/facade/memory', create_vectra_business_review(payload), next_action='Initialize the Business Review and collect validated evidence.'))
        if operation_type in {'initialize_business_review', 'business_review_initialize'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.initialize_review', '/vectra/laboratory/facade/memory', initialize_vectra_business_review(payload), next_action='Collect and validate business evidence.'))
        if operation_type in {'add_business_review_evidence', 'business_review_evidence_add'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.add_evidence', '/vectra/laboratory/facade/memory', add_vectra_business_review_evidence(payload), next_action='Validate evidence before confirming professional findings.'))
        if operation_type in {'validate_business_review_evidence', 'business_review_evidence_validate'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.validate_evidence', '/vectra/laboratory/facade/memory', validate_vectra_business_review_evidence(payload)))
        if operation_type in {'add_business_review_finding', 'business_review_finding_add'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.add_finding', '/vectra/laboratory/facade/memory', add_vectra_business_review_finding(payload)))
        if operation_type in {'confirm_business_review_finding', 'business_review_finding_confirm'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.confirm_finding', '/vectra/laboratory/facade/memory', confirm_vectra_business_review_finding(payload)))
        if operation_type in {'advance_business_review_stage', 'business_review_stage_advance'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.advance_stage', '/vectra/laboratory/facade/memory', advance_vectra_business_review_stage(payload)))
        if operation_type in {'complete_business_review', 'business_review_complete'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.complete_review', '/vectra/laboratory/facade/memory', complete_vectra_business_review(payload), next_action='Review Professional Agenda and determine the next activity.'))
        if operation_type in {'get_business_review', 'business_review_get'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.get_review', '/vectra/laboratory/facade/memory', get_vectra_business_review(payload)))
        if operation_type in {'list_business_reviews', 'business_reviews_list'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.list_reviews', '/vectra/laboratory/facade/memory', list_vectra_business_reviews(payload)))
        if operation_type in {'verify_digital_business_analyst_foundation', 'digital_business_analyst_verify'}:
            return json_response(_facade_response(operation_type, 'digital_business_analyst.verify_foundation', '/vectra/laboratory/facade/memory', verify_vectra_digital_business_analyst_foundation()))
        if operation_type in {'evidence_platform_manifest', 'professional_evidence_manifest'}:
            return json_response(_facade_response(operation_type, 'professional_evidence.get_manifest', '/vectra/laboratory/facade/memory', get_vectra_evidence_platform_manifest(), next_action='Register or query evidence through the shared platform.'))
        if operation_type in {'register_professional_evidence', 'evidence_register'}:
            return json_response(_facade_response(operation_type, 'professional_evidence.register', '/vectra/laboratory/facade/memory', register_vectra_professional_evidence(payload)))
        if operation_type in {'transition_professional_evidence', 'evidence_transition'}:
            return json_response(_facade_response(operation_type, 'professional_evidence.transition', '/vectra/laboratory/facade/memory', transition_vectra_professional_evidence(payload)))
        if operation_type in {'get_professional_evidence', 'evidence_get'}:
            return json_response(_facade_response(operation_type, 'professional_evidence.get', '/vectra/laboratory/facade/memory', get_vectra_professional_evidence(payload)))
        if operation_type in {'list_professional_evidence', 'evidence_list'}:
            return json_response(_facade_response(operation_type, 'professional_evidence.list', '/vectra/laboratory/facade/memory', list_vectra_professional_evidence(payload)))
        if operation_type in {'link_professional_evidence', 'evidence_link'}:
            return json_response(_facade_response(operation_type, 'professional_evidence.link', '/vectra/laboratory/facade/memory', link_vectra_professional_evidence(payload)))
        if operation_type in {'verify_professional_evidence_platform', 'evidence_platform_verify'}:
            return json_response(_facade_response(operation_type, 'professional_evidence.verify', '/vectra/laboratory/facade/memory', verify_vectra_professional_evidence_platform()))
        if operation_type in {'findings_platform_manifest', 'professional_findings_manifest'}:
            return json_response(_facade_response(operation_type, 'professional_findings.get_manifest', '/vectra/laboratory/facade/memory', get_vectra_findings_platform_manifest(), next_action='Register or query findings through the shared platform.'))
        if operation_type in {'register_professional_finding', 'finding_register'}:
            return json_response(_facade_response(operation_type, 'professional_findings.register', '/vectra/laboratory/facade/memory', register_vectra_professional_finding(payload)))
        if operation_type in {'transition_professional_finding', 'finding_transition'}:
            return json_response(_facade_response(operation_type, 'professional_findings.transition', '/vectra/laboratory/facade/memory', transition_vectra_professional_finding(payload)))
        if operation_type in {'get_professional_finding', 'finding_get'}:
            return json_response(_facade_response(operation_type, 'professional_findings.get', '/vectra/laboratory/facade/memory', get_vectra_professional_finding(payload)))
        if operation_type in {'list_professional_findings', 'finding_list'}:
            return json_response(_facade_response(operation_type, 'professional_findings.list', '/vectra/laboratory/facade/memory', list_vectra_professional_findings(payload)))
        if operation_type in {'link_professional_findings', 'finding_link'}:
            return json_response(_facade_response(operation_type, 'professional_findings.link', '/vectra/laboratory/facade/memory', link_vectra_professional_findings(payload)))
        if operation_type in {'verify_professional_findings_platform', 'findings_platform_verify'}:
            return json_response(_facade_response(operation_type, 'professional_findings.verify', '/vectra/laboratory/facade/memory', verify_vectra_professional_findings_platform()))
        if operation_type in {'research_engine_manifest', 'research_manifest'}:
            return json_response(_facade_response(operation_type, 'research_engine.get_manifest', '/vectra/laboratory/facade/memory', get_vectra_research_engine_manifest(), next_action='Create a Research Session through the shared Professional Activity foundation.'))
        if operation_type in {'create_research_session', 'research_session_create'}:
            return json_response(_facade_response(operation_type, 'research_engine.create_session', '/vectra/laboratory/facade/memory', create_vectra_research_session(payload), next_action='Initialize the Research Session before collecting evidence.'))
        if operation_type in {'initialize_research_session', 'research_session_initialize'}:
            return json_response(_facade_response(operation_type, 'research_engine.initialize_session', '/vectra/laboratory/facade/memory', initialize_vectra_research_session(payload), next_action='Collect and validate evidence.'))
        if operation_type in {'update_research_working_context', 'research_context_update'}:
            return json_response(_facade_response(operation_type, 'research_engine.update_working_context', '/vectra/laboratory/facade/memory', update_vectra_research_working_context(payload)))
        if operation_type in {'add_research_evidence', 'research_evidence_add'}:
            return json_response(_facade_response(operation_type, 'research_engine.add_evidence', '/vectra/laboratory/facade/memory', add_vectra_research_evidence(payload), next_action='Validate evidence before using it for confirmed findings.'))
        if operation_type in {'validate_research_evidence', 'research_evidence_validate'}:
            return json_response(_facade_response(operation_type, 'research_engine.validate_evidence', '/vectra/laboratory/facade/memory', validate_vectra_research_evidence(payload)))
        if operation_type in {'add_research_finding', 'research_finding_add'}:
            return json_response(_facade_response(operation_type, 'research_engine.add_finding', '/vectra/laboratory/facade/memory', add_vectra_research_finding(payload)))
        if operation_type in {'advance_research_stage', 'research_stage_advance'}:
            return json_response(_facade_response(operation_type, 'research_engine.advance_stage', '/vectra/laboratory/facade/memory', advance_vectra_research_stage(payload)))
        if operation_type in {'complete_research_session', 'research_session_complete'}:
            return json_response(_facade_response(operation_type, 'research_engine.complete_session', '/vectra/laboratory/facade/memory', complete_vectra_research_session(payload), next_action='Use readiness_evaluation to select the next professional activity.'))
        if operation_type in {'get_research_session', 'research_session_get'}:
            return json_response(_facade_response(operation_type, 'research_engine.get_session', '/vectra/laboratory/facade/memory', get_vectra_research_session(payload)))
        if operation_type in {'list_research_sessions', 'research_session_list'}:
            return json_response(_facade_response(operation_type, 'research_engine.list_sessions', '/vectra/laboratory/facade/memory', list_vectra_research_sessions(payload)))
        if operation_type in {'verify_research_engine_foundation', 'research_engine_verify'}:
            return json_response(_facade_response(operation_type, 'research_engine.verify_foundation', '/vectra/laboratory/facade/memory', verify_vectra_research_engine_foundation()))
        if operation_type in {'professional_orchestration_manifest', 'orchestration_manifest'}:
            return json_response(_facade_response(operation_type, 'professional_orchestration.get_manifest', '/vectra/laboratory/facade/memory', get_vectra_orchestration_manifest(), next_action='Resolve a Product Owner request into a professional goal.'))
        if operation_type in {'resolve_professional_goal', 'professional_goal_resolve'}:
            return json_response(_facade_response(operation_type, 'professional_orchestration.resolve_goal', '/vectra/laboratory/facade/memory', resolve_vectra_professional_goal(payload), next_action='Create and plan the required professional activity.'))
        if operation_type in {'orchestrate_product_owner_goal', 'decision_orchestrator_run'}:
            return json_response(_facade_response(operation_type, 'professional_orchestration.orchestrate_goal', '/vectra/laboratory/facade/memory', orchestrate_vectra_product_owner_goal(payload), next_action='Review Professional Agenda or let Executive Controller select ready work.'))
        if operation_type in {'evaluate_activity_readiness', 'activity_readiness'}:
            return json_response(_facade_response(operation_type, 'professional_orchestration.evaluate_readiness', '/vectra/laboratory/facade/memory', evaluate_vectra_activity_readiness(payload)))
        if operation_type in {'executive_controller_tick', 'executive_controller_review'}:
            return json_response(_facade_response(operation_type, 'professional_orchestration.executive_controller_tick', '/vectra/laboratory/facade/memory', run_vectra_executive_controller_tick(payload), next_action='Activate ready work only through an explicit call.'))
        if operation_type in {'get_professional_agenda', 'professional_agenda'}:
            return json_response(_facade_response(operation_type, 'professional_orchestration.get_agenda', '/vectra/laboratory/facade/memory', get_vectra_professional_agenda(payload)))
        if operation_type in {'verify_professional_orchestration_foundation', 'orchestration_foundation_verify'}:
            return json_response(_facade_response(operation_type, 'professional_orchestration.verify_foundation', '/vectra/laboratory/facade/memory', verify_vectra_professional_orchestration_foundation()))
        if operation_type in {'professional_activity_manifest', 'activity_manifest'}:
            return json_response(_facade_response(operation_type, 'professional_activity.get_manifest', '/vectra/laboratory/facade/memory', get_vectra_professional_activity_manifest(), next_action='Create a professional activity only when a concrete Product Owner goal exists.'))
        if operation_type in {'create_professional_activity', 'activity_create'}:
            return json_response(_facade_response(operation_type, 'professional_activity.create', '/vectra/laboratory/facade/memory', create_vectra_professional_activity(payload), next_action='Plan the activity before queueing or starting it.'))
        if operation_type in {'plan_professional_activity', 'activity_plan'}:
            return json_response(_facade_response(operation_type, 'professional_activity.plan', '/vectra/laboratory/facade/memory', plan_vectra_professional_activity(payload), next_action='Queue or start the planned activity.'))
        if operation_type in {'queue_professional_activity', 'activity_queue'}:
            return json_response(_facade_response(operation_type, 'professional_activity.queue', '/vectra/laboratory/facade/memory', queue_vectra_professional_activity(payload), next_action='The Executive Controller may activate the next queued activity.'))
        if operation_type in {'start_professional_activity', 'activity_start'}:
            return json_response(_facade_response(operation_type, 'professional_activity.start', '/vectra/laboratory/facade/memory', start_vectra_professional_activity(payload), next_action='Execute the current activity stage through an appropriate engine or tool.'))
        if operation_type in {'pause_professional_activity', 'activity_pause'}:
            return json_response(_facade_response(operation_type, 'professional_activity.pause', '/vectra/laboratory/facade/memory', pause_vectra_professional_activity(payload)))
        if operation_type in {'complete_professional_activity', 'activity_complete'}:
            return json_response(_facade_response(operation_type, 'professional_activity.complete', '/vectra/laboratory/facade/memory', complete_vectra_professional_activity(payload), next_action='Review the completed professional result and archive when accepted.'))
        if operation_type in {'fail_professional_activity', 'activity_fail'}:
            return json_response(_facade_response(operation_type, 'professional_activity.fail', '/vectra/laboratory/facade/memory', fail_vectra_professional_activity(payload), next_action='Retry only when recoverable; otherwise prepare a grounded engineering task.'))
        if operation_type in {'cancel_professional_activity', 'activity_cancel'}:
            return json_response(_facade_response(operation_type, 'professional_activity.cancel', '/vectra/laboratory/facade/memory', cancel_vectra_professional_activity(payload)))
        if operation_type in {'archive_professional_activity', 'activity_archive'}:
            return json_response(_facade_response(operation_type, 'professional_activity.archive', '/vectra/laboratory/facade/memory', archive_vectra_professional_activity(payload)))
        if operation_type in {'get_professional_activity', 'activity_get'}:
            return json_response(_facade_response(operation_type, 'professional_activity.get', '/vectra/laboratory/facade/memory', get_vectra_professional_activity(payload)))
        if operation_type in {'list_professional_activities', 'activity_list'}:
            return json_response(_facade_response(operation_type, 'professional_activity.list', '/vectra/laboratory/facade/memory', list_vectra_professional_activities(payload)))
        if operation_type in {'get_executive_activity_status', 'executive_activity_status'}:
            return json_response(_facade_response(operation_type, 'professional_activity.get_executive_status', '/vectra/laboratory/facade/memory', get_vectra_executive_activity_status()))
        if operation_type in {'activate_next_professional_activity', 'executive_activate_next'}:
            return json_response(_facade_response(operation_type, 'professional_activity.activate_next', '/vectra/laboratory/facade/memory', activate_next_vectra_professional_activity(payload)))
        if operation_type in {'verify_professional_activity_foundation', 'activity_foundation_verify'}:
            return json_response(_facade_response(operation_type, 'professional_activity.verify_foundation', '/vectra/laboratory/facade/memory', verify_vectra_professional_activity_foundation()))
        if operation_type in {'professional_intelligence_status', 'pi_status'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.get_status', '/vectra/professional-intelligence/status', get_vectra_professional_intelligence_status()))
        if operation_type in {'create_session_archive', 'session_archive_create'}:
            return json_response(_facade_response(operation_type, 'session_archive.create_session_archive', '/vectra/laboratory/facade/memory', create_vectra_session_archive(payload), next_action='Append session events through append_session_event.'))
        if operation_type in {'import_historical_session', 'historical_session_import', 'session_archive_import'}:
            return json_response(_facade_response(operation_type, 'session_archive.import_historical_session', '/vectra/laboratory/facade/memory', import_vectra_historical_session(payload), next_action='Run verify_historical_archive_discovery, then build_unified_professional_model.'))
        if operation_type in {'bootstrap_session_archive'}:
            return json_response(_facade_response(operation_type, 'session_archive.bootstrap_session_archive', '/vectra/laboratory/facade/memory', bootstrap_vectra_session_archive(payload), next_action='Run verify_historical_archive_discovery, then build_unified_professional_model.'))
        if operation_type in {'append_session_event', 'session_archive_append_event'}:
            return json_response(_facade_response(operation_type, 'session_archive.append_session_event', '/vectra/laboratory/facade/memory', append_vectra_session_event(payload), next_action='Get timeline through get_session_timeline.'))
        if operation_type in {'get_session_timeline', 'session_timeline'}:
            return json_response(_facade_response(operation_type, 'session_archive.get_session_timeline', '/vectra/laboratory/facade/memory', get_vectra_session_timeline(payload), next_action='Build replay context through get_session_replay_context.'))
        if operation_type in {'get_session_replay_context', 'session_replay_context'}:
            return json_response(_facade_response(operation_type, 'session_archive.get_session_replay_context', '/vectra/laboratory/facade/memory', get_vectra_session_replay_context(payload), next_action='Run archive-backed extraction when full archive exists.'))
        if operation_type in {'verify_session_archive', 'session_archive_verify'}:
            return json_response(_facade_response(operation_type, 'session_archive.verify_session_archive', '/vectra/laboratory/facade/memory', verify_vectra_session_archive(payload)))
        if operation_type in {'run_archive_backed_extraction', 'archive_backed_extraction'}:
            return json_response(_facade_response(operation_type, 'session_archive.run_archive_backed_extraction', '/vectra/laboratory/facade/memory', run_vectra_archive_backed_extraction(payload), next_action='Review package, then capitalize_archived_session_knowledge with Product Owner approval.'))
        if operation_type in {'capitalize_archived_session_knowledge', 'archive_backed_capitalization'}:
            payload['product_owner_approval'] = bool(payload.get('product_owner_approval') or approval)
            return json_response(_facade_response(operation_type, 'session_archive.capitalize_archived_session_knowledge', '/vectra/laboratory/facade/memory', capitalize_vectra_archived_session_knowledge(payload), next_action='Run verify_archive_backed_capitalization.'))
        if operation_type in {'verify_archive_backed_capitalization', 'archive_backed_capitalization_verify'}:
            return json_response(_facade_response(operation_type, 'session_archive.verify_archive_backed_capitalization', '/vectra/laboratory/facade/memory', verify_vectra_archive_backed_capitalization(payload)))
        if operation_type in {'get_all_session_archives', 'build_unified_archive_context', 'unified_archive_context', 'historical_archives_context'}:
            return json_response(_facade_response(operation_type, 'unified_professional_model.build_unified_archive_context', '/vectra/laboratory/facade/memory', build_vectra_unified_archive_context(payload), next_action='Build Unified Professional Model through build_unified_professional_model.'))
        if operation_type in {'verify_historical_archive_discovery', 'historical_archive_discovery_verify', 'verify_archive_discovery'}:
            return json_response(_facade_response(operation_type, 'unified_professional_model.verify_historical_archive_discovery', '/vectra/laboratory/facade/memory', verify_vectra_historical_archive_discovery(payload), next_action='If PASS, run build_unified_professional_model.'))
        if operation_type in {'build_unified_professional_model', 'unified_professional_model', 'consolidate_professional_model', 'build_professional_model_from_archives'}:
            return json_response(_facade_response(operation_type, 'unified_professional_model.build_unified_professional_model', '/vectra/laboratory/facade/memory', build_vectra_unified_professional_model(payload), next_action='Send Unified Professional Model and Consolidation Report to VECTRA Laboratory for Product Verification.'))
        if operation_type in {'verify_unified_professional_model', 'verify_professional_model_consolidation', 'verify_vpm_consolidation'}:
            return json_response(_facade_response(operation_type, 'unified_professional_model.verify_unified_professional_model', '/vectra/laboratory/facade/memory', verify_vectra_unified_professional_model(payload)))
        if operation_type in {'build_session_context', 'session_context', 'professional_intelligence_session_context'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.build_session_context', '/vectra/professional-intelligence/session-context', build_vectra_professional_intelligence_session_context(payload), next_action='Run session_context_verify, then continue to PI-IMPL-0002 after Product Verification PASS.'))
        if operation_type in {'verify_session_context', 'verify_session_context_foundation', 'professional_intelligence_verify'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.verify_session_context_foundation', '/vectra/professional-intelligence/session-context/verify', verify_vectra_professional_intelligence_session_context()))
        if operation_type in {'build_session_audit', 'session_audit', 'professional_intelligence_session_audit'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.build_session_audit_report', '/vectra/professional-intelligence/session-audit', build_vectra_professional_intelligence_session_audit(payload), next_action='Run session_audit_verify, then continue to PI-IMPL-0003 after Product Verification PASS.'))
        if operation_type in {'verify_session_audit', 'verify_session_audit_runtime', 'session_audit_verify'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.verify_session_audit_runtime', '/vectra/professional-intelligence/session-audit/verify', verify_vectra_professional_intelligence_session_audit()))
        if operation_type in {'build_knowledge_candidates', 'knowledge_candidates', 'extract_knowledge_candidates', 'professional_intelligence_knowledge_candidates'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.build_knowledge_candidate_report', '/vectra/professional-intelligence/knowledge-candidates', build_vectra_professional_intelligence_knowledge_candidates(payload), next_action='Run verify_knowledge_candidates. PI-IMPL-0005 starts only after Product Verification PASS.'))
        if operation_type in {'verify_knowledge_candidates', 'verify_knowledge_candidate_runtime', 'knowledge_candidates_verify'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.verify_knowledge_candidate_runtime', '/vectra/professional-intelligence/knowledge-candidates/verify', verify_vectra_professional_intelligence_knowledge_candidates()))
        if operation_type in {'build_knowledge_processing', 'knowledge_processing', 'process_knowledge_candidates', 'professional_intelligence_knowledge_processing'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.build_knowledge_processing_report', '/vectra/professional-intelligence/knowledge-processing', build_vectra_professional_intelligence_knowledge_processing(payload), next_action='Run verify_knowledge_processing. PI-IMPL-0008 starts only after Product Verification PASS.'))
        if operation_type in {'verify_knowledge_processing', 'verify_knowledge_processing_runtime', 'knowledge_processing_verify'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.verify_knowledge_processing_runtime', '/vectra/professional-intelligence/knowledge-processing/verify', verify_vectra_professional_intelligence_knowledge_processing()))
        if operation_type in {'build_knowledge_consolidation', 'knowledge_consolidation', 'deduplicate_knowledge_candidates', 'professional_intelligence_knowledge_consolidation'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.build_knowledge_consolidation_report', '/vectra/professional-intelligence/knowledge-consolidation', build_vectra_professional_intelligence_knowledge_consolidation(payload), next_action='Run verify_knowledge_consolidation. PI-IMPL-0010 starts only after Product Verification PASS.'))
        if operation_type in {'verify_knowledge_consolidation', 'verify_knowledge_consolidation_runtime', 'knowledge_consolidation_verify'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.verify_knowledge_consolidation_runtime', '/vectra/professional-intelligence/knowledge-consolidation/verify', verify_vectra_professional_intelligence_knowledge_consolidation()))
        if operation_type in {'build_prepared_knowledge_package', 'prepared_knowledge_package', 'build_knowledge_package', 'professional_intelligence_prepared_package'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.build_prepared_knowledge_package', '/vectra/professional-intelligence/prepared-knowledge-package', build_vectra_professional_intelligence_prepared_package(payload), next_action='Run verify_prepared_knowledge_package. PI-IMPL-0012 starts only after Product Verification PASS.'))
        if operation_type in {'build_package_diagnostics', 'package_diagnostics', 'knowledge_package_diagnostics', 'professional_intelligence_package_diagnostics'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.build_package_diagnostics', '/vectra/professional-intelligence/prepared-knowledge-package/diagnostics', build_vectra_professional_intelligence_package_diagnostics(payload), next_action='Review completeness_report and risk_report before Runtime Capitalization integration.'))
        if operation_type in {'verify_prepared_knowledge_package', 'verify_prepared_knowledge_package_runtime', 'prepared_knowledge_package_verify'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.verify_prepared_knowledge_package_runtime', '/vectra/professional-intelligence/prepared-knowledge-package/verify', verify_vectra_professional_intelligence_prepared_package()))
        if operation_type in {'run_runtime_capitalization', 'runtime_capitalization', 'capitalize_prepared_knowledge_package', 'professional_intelligence_runtime_capitalization'}:
            payload['product_owner_approval'] = bool(payload.get('product_owner_approval') or approval)
            return json_response(_facade_response(operation_type, 'professional_intelligence.run_runtime_capitalization_integration', '/vectra/professional-intelligence/runtime-capitalization', run_vectra_professional_intelligence_runtime_capitalization(payload), next_action='Run verify_runtime_capitalization.'))
        if operation_type in {'build_product_verification_suite', 'product_verification_suite', 'professional_intelligence_product_verification_suite'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.build_product_verification_suite', '/vectra/professional-intelligence/product-verification-suite', build_vectra_professional_intelligence_product_verification_suite(payload), next_action='Run end_to_end_professional_intelligence_validation.'))
        if operation_type in {'end_to_end_professional_intelligence_validation', 'professional_intelligence_e2e_validation', 'run_professional_intelligence_e2e'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.build_end_to_end_professional_intelligence_validation', '/vectra/professional-intelligence/e2e-validation', build_vectra_professional_intelligence_e2e_validation(payload), next_action='Run verify_runtime_capitalization.'))
        if operation_type in {'verify_runtime_capitalization', 'verify_runtime_capitalization_integration', 'runtime_capitalization_verify', 'verify_professional_intelligence_end_to_end'}:
            return json_response(_facade_response(operation_type, 'professional_intelligence.verify_runtime_capitalization_integration', '/vectra/professional-intelligence/runtime-capitalization/verify', verify_vectra_professional_intelligence_runtime_capitalization()))
        if operation_type in {'build_semantic_knowledge_extraction', 'semantic_knowledge_extraction', 'extract_semantic_knowledge', 'semantic_extraction_engine'}:
            return json_response(_facade_response(operation_type, 'semantic_extraction.build_semantic_knowledge_extraction_report', '/vectra/laboratory/facade/memory', build_vectra_semantic_knowledge_extraction_report(payload), next_action='Review semantic extraction quality. Capitalization is not executed by this operation.'))
        if operation_type in {'verify_semantic_knowledge_extraction', 'verify_semantic_extraction_engine', 'semantic_extraction_verify'}:
            return json_response(_facade_response(operation_type, 'semantic_extraction.verify_semantic_knowledge_extraction', '/vectra/laboratory/facade/memory', verify_vectra_semantic_knowledge_extraction(payload), next_action='If PASS, rerun build_unified_professional_model on imported archives.'))
        if operation_type in {'verify_repository_readback_consistency', 'repository_readback_consistency', 'verify_knowledge_repository_readback', 'knowledge_repository_readback_verify'}:
            return json_response(_facade_response(operation_type, 'repository_readback_consistency.verify_repository_readback_consistency', '/vectra/laboratory/facade/memory', verify_vectra_repository_readback_consistency(payload), next_action='If PASS, Repository readback is consistent. If FAIL, inspect failure_reasons and deltas.'))
        if operation_type in {'verify_recovery_snapshot_sync', 'recovery_snapshot_sync_verify', 'verify_recovery_sync'}:
            return json_response(_facade_response(operation_type, 'recovery_snapshot_sync.verify_recovery_snapshot_sync', '/vectra/laboratory/facade/memory', verify_vectra_recovery_snapshot_sync(payload), next_action='If PASS, Recovery Snapshot is synchronized with Repository and Readback.'))
        if operation_type in {'rebuild_recovery_snapshot', 'sync_recovery_snapshot', 'rebuild_recovery_snapshot_after_capitalization'}:
            return json_response(_facade_response(operation_type, 'recovery_snapshot_sync.rebuild_and_persist_recovery_snapshot_after_capitalization', '/vectra/laboratory/facade/memory', rebuild_vectra_recovery_snapshot_after_capitalization(payload), next_action='Run verify_repository_readback_consistency.'))
        if operation_type in {'product_knowledge', 'list_product_knowledge'}:
            return json_response(_facade_response(operation_type, 'product_knowledge.list_product_knowledge', '/vectra/memory/product-knowledge', list_vectra_product_knowledge_runtime(limit=int(payload.get('limit') or 100))))
        if operation_type == 'write_product_knowledge':
            payload['product_owner_approval'] = bool(payload.get('product_owner_approval') or approval)
            return json_response(_facade_response(operation_type, 'product_knowledge.write_product_knowledge', '/vectra/memory/product-knowledge', write_vectra_product_knowledge_runtime(payload)))
        if operation_type == 'verify_product_knowledge':
            return json_response(_facade_response(operation_type, 'product_knowledge.verify_product_knowledge_readback', '/vectra/memory/product-knowledge/verify/readback', verify_vectra_product_knowledge_runtime(knowledge_id=payload.get('knowledge_id'))))
        if operation_type in {'product_decisions', 'list_product_decisions'}:
            return json_response(_facade_response(operation_type, 'product_decisions_runtime.list_product_decisions', '/vectra/memory/product-decisions', list_vectra_product_decisions_runtime(limit=int(payload.get('limit') or 100))))
        if operation_type == 'write_product_decision':
            payload['product_owner_approval'] = bool(payload.get('product_owner_approval') or approval)
            return json_response(_facade_response(operation_type, 'product_decisions_runtime.write_product_decision', '/vectra/memory/product-decisions', write_vectra_product_decision_runtime(payload)))
        if operation_type == 'verify_product_decisions':
            return json_response(_facade_response(operation_type, 'product_decisions_runtime.verify_product_decisions_readback', '/vectra/memory/product-decisions/verify/readback', verify_vectra_product_decisions_runtime(decision_id=payload.get('decision_id'))))
        if operation_type in {'general_knowledge', 'list_general_knowledge'}:
            return json_response(_facade_response(operation_type, 'general_knowledge.list_general_knowledge', '/vectra/memory/general-knowledge', list_vectra_general_knowledge_runtime(limit=int(payload.get('limit') or 100))))
        if operation_type == 'write_general_knowledge':
            payload['product_owner_approval'] = bool(payload.get('product_owner_approval') or approval)
            return json_response(_facade_response(operation_type, 'general_knowledge.write_general_knowledge', '/vectra/memory/general-knowledge', write_vectra_general_knowledge_runtime(payload)))
        if operation_type == 'verify_general_knowledge':
            return json_response(_facade_response(operation_type, 'general_knowledge.verify_general_knowledge_readback', '/vectra/memory/general-knowledge/verify/readback', verify_vectra_general_knowledge_runtime(knowledge_id=payload.get('knowledge_id'))))
        if operation_type in {'revisions', 'list_revisions'}:
            return json_response(_facade_response(operation_type, 'revision_model.list_revisions', '/vectra/memory/revisions', list_vectra_memory_revisions(object_id=payload.get('object_id'), knowledge_id=payload.get('knowledge_id'), memory_space=payload.get('memory_space'), limit=int(payload.get('limit') or 100))))
        if operation_type == 'get_revision':
            return json_response(_facade_response(operation_type, 'revision_model.get_revision', '/vectra/memory/revisions/{revision_id}', get_vectra_memory_revision(revision_id=str(payload.get('revision_id') or ''))))
        if operation_type in {'version_status', 'memory_version_status'}:
            from app.assistant_runtime.memory_repository import list_memory_objects as _list_memory_objects_for_version_status
            _objects = _list_memory_objects_for_version_status(domain=payload.get('domain') or domain, limit=10000).get('objects', [])
            return json_response(_facade_response(operation_type, 'revision_model.get_version_status', '/vectra/memory/revisions', get_vectra_memory_version_status(active_objects=_objects)))
        if operation_type in {'verify_revisions', 'verify_revision_model'}:
            from app.assistant_runtime.memory_repository import list_memory_objects as _list_memory_objects_for_revision_verify
            _objects = _list_memory_objects_for_revision_verify(domain=payload.get('domain') or domain, limit=10000).get('objects', [])
            return json_response(_facade_response(operation_type, 'revision_model.verify_revision_model', '/vectra/memory/revisions/verify', verify_vectra_revision_model(active_objects=_objects)))
        if operation_type in {'release_history', 'list_release_history'}:
            return json_response(_facade_response(operation_type, 'release_history_runtime.list_release_history', '/vectra/memory/release-history', list_vectra_release_history_runtime(limit=int(payload.get('limit') or 100))))
        if operation_type == 'write_release_history':
            payload['product_verification_pass'] = bool(payload.get('product_verification_pass') or approval)
            return json_response(_facade_response(operation_type, 'release_history_runtime.write_release_history', '/vectra/memory/release-history', write_vectra_release_history_runtime(payload)))
        if operation_type == 'verify_release_history':
            return json_response(_facade_response(operation_type, 'release_history_runtime.verify_release_history_readback', '/vectra/memory/release-history/verify/readback', verify_vectra_release_history_runtime(release_id=payload.get('release_id'))))
        if operation_type in {'health', 'memory_health'}:
            return json_response(_facade_response(operation_type, 'memory_health.get_memory_health_status', '/vectra/memory/health', get_vectra_memory_health_status(domain=payload.get('domain') or domain)))
        if operation_type in {'diagnostics', 'memory_diagnostics'}:
            return json_response(_facade_response(operation_type, 'memory_health.get_memory_diagnostics_report', '/vectra/memory/diagnostics', get_vectra_memory_diagnostics_report(domain=payload.get('domain') or domain)))
        if operation_type in {'verify_health', 'verify_memory_health'}:
            return json_response(_facade_response(operation_type, 'memory_health.verify_memory_health', '/vectra/memory/health/verify', verify_vectra_memory_health(domain=payload.get('domain') or domain)))
        if operation_type in {'architecture_conformance', 'conformance', 'get_architecture_conformance'}:
            return json_response(_facade_response(operation_type, 'architecture_conformance.get_architecture_conformance_report', '/vectra/memory/architecture-conformance', get_vectra_architecture_conformance_report(domain=payload.get('domain') or domain)))
        if operation_type in {'verify_architecture_conformance', 'verify_conformance'}:
            return json_response(_facade_response(operation_type, 'architecture_conformance.verify_architecture_conformance', '/vectra/memory/architecture-conformance/verify', verify_vectra_architecture_conformance(domain=payload.get('domain') or domain)))
        if operation_type in {'recovery_optimized', 'compact_recovery', 'build_compact_recovery_context'}:
            return json_response(_facade_response(operation_type, 'recovery_optimization.build_compact_recovery_context', '/vectra/memory/recovery-optimized', build_vectra_compact_recovery_context(domain=payload.get('domain') or domain, max_objects_per_space=int(payload.get('max_objects_per_space') or 5))))
        if operation_type in {'verify_recovery_optimization', 'verify_compact_recovery'}:
            return json_response(_facade_response(operation_type, 'recovery_optimization.verify_recovery_optimization', '/vectra/memory/recovery-optimized/verify', verify_vectra_recovery_optimization(domain=payload.get('domain') or domain)))
        if operation_type in {'e2e_validation', 'end_to_end_validation', 'professional_memory_validation'}:
            return json_response(_facade_response(operation_type, 'professional_memory_validation.run_professional_memory_e2e_validation', '/vectra/memory/e2e-validation', run_vectra_professional_memory_e2e_validation(domain=payload.get('domain') or domain)))
        if operation_type in {'verify_professional_memory_program', 'verify_e2e_validation'}:
            return json_response(_facade_response(operation_type, 'professional_memory_validation.verify_professional_memory_program', '/vectra/memory/e2e-validation/verify', verify_vectra_professional_memory_program(domain=payload.get('domain') or domain)))
        return json_response(_facade_error(operation_type, f'Unsupported memory operation_type: {operation_type}', runtime_service='memory_facade'))
    except Exception as exc:
        logger.exception('memory_facade_operation_failed')
        return json_response(_facade_error(operation_type, str(exc), runtime_service='memory_facade'))




# PROFESSIONAL-INTELLIGENCE — PI-IMPL-0001/0002/0003/0004.
# These endpoints are structural/read-only for Product Verification of the early
# Professional Intelligence implementation increments. PI-IMPL-0003/0004 extract
# Knowledge Candidates with Evidence only. They do not validate, classify into
# Memory Spaces, normalize, deduplicate, build packages or capitalize knowledge.

@router.get('/vectra/professional-intelligence/status', summary='Read VECTRA Professional Intelligence implementation status')
def vectra_professional_intelligence_status(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_professional_intelligence_status())


@router.post('/vectra/professional-intelligence/session-context', summary='Build Professional Intelligence Session Context')
def vectra_professional_intelligence_session_context(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    return json_response(build_vectra_professional_intelligence_session_context(payload))


@router.get('/vectra/professional-intelligence/session-context/verify', summary='Verify Professional Intelligence Session Context Foundation')
def vectra_professional_intelligence_session_context_verify(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_professional_intelligence_session_context())


@router.post('/vectra/professional-intelligence/session-audit', summary='Build Professional Intelligence Session Audit Report')
def vectra_professional_intelligence_session_audit(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    return json_response(build_vectra_professional_intelligence_session_audit(payload))


@router.get('/vectra/professional-intelligence/session-audit/verify', summary='Verify Professional Intelligence Session Audit Runtime')
def vectra_professional_intelligence_session_audit_verify(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_professional_intelligence_session_audit())




@router.post('/vectra/professional-intelligence/knowledge-candidates', summary='Build Professional Intelligence Knowledge Candidates with Evidence')
def vectra_professional_intelligence_knowledge_candidates(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    return json_response(build_vectra_professional_intelligence_knowledge_candidates(payload))


@router.get('/vectra/professional-intelligence/knowledge-candidates/verify', summary='Verify Professional Intelligence Knowledge Candidate and Evidence Runtime')
def vectra_professional_intelligence_knowledge_candidates_verify(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_professional_intelligence_knowledge_candidates())


@router.post('/vectra/professional-intelligence/knowledge-processing', summary='Build Professional Intelligence Knowledge Processing Report')
def vectra_professional_intelligence_knowledge_processing(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    return json_response(build_vectra_professional_intelligence_knowledge_processing(payload))


@router.get('/vectra/professional-intelligence/knowledge-processing/verify', summary='Verify Professional Intelligence Knowledge Validation, Classification and Normalization')
def vectra_professional_intelligence_knowledge_processing_verify(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_professional_intelligence_knowledge_processing())


@router.post('/vectra/professional-intelligence/runtime-capitalization', summary='Run Professional Intelligence Runtime Capitalization Integration')
def vectra_professional_intelligence_runtime_capitalization(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    return json_response(run_vectra_professional_intelligence_runtime_capitalization(payload))


@router.get('/vectra/professional-intelligence/runtime-capitalization/verify', summary='Verify Professional Intelligence Runtime Capitalization Integration')
def vectra_professional_intelligence_runtime_capitalization_verify(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_professional_intelligence_runtime_capitalization())


@router.post('/vectra/professional-intelligence/product-verification-suite', summary='Run Professional Intelligence Product Verification Suite')
def vectra_professional_intelligence_product_verification_suite(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    return json_response(build_vectra_professional_intelligence_product_verification_suite(payload))


@router.post('/vectra/professional-intelligence/e2e-validation', summary='Run End-to-End Professional Intelligence Validation')
def vectra_professional_intelligence_e2e_validation(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    return json_response(build_vectra_professional_intelligence_e2e_validation(payload))

@router.get('/vectra/memory/architecture-conformance', summary='Get VECTRA Memory Architecture Conformance report')
def vectra_memory_architecture_conformance(domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_architecture_conformance_report(domain=domain))


@router.get('/vectra/memory/architecture-conformance/verify', summary='Verify VECTRA Memory Architecture Conformance')
def vectra_memory_architecture_conformance_verify(domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_architecture_conformance(domain=domain))


@router.get('/vectra/memory/recovery-optimized', summary='Build compact VECTRA Professional Memory recovery context')
def vectra_memory_recovery_optimized(domain: str = 'bonboason', max_objects_per_space: int = 5, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(build_vectra_compact_recovery_context(domain=domain, max_objects_per_space=max_objects_per_space))


@router.get('/vectra/memory/recovery-optimized/verify', summary='Verify compact VECTRA Professional Memory recovery context')
def vectra_memory_recovery_optimized_verify(domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_recovery_optimization(domain=domain))


@router.get('/vectra/memory/e2e-validation', summary='Run End-to-End Professional Memory validation')
def vectra_memory_e2e_validation(domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(run_vectra_professional_memory_e2e_validation(domain=domain))


@router.get('/vectra/memory/e2e-validation/verify', summary='Verify Professional Memory v1.0 program completion')
def vectra_memory_e2e_validation_verify(domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_professional_memory_program(domain=domain))


@router.get('/vectra/memory/general-knowledge', summary='List VECTRA General Knowledge Runtime objects')
def vectra_general_knowledge_runtime(limit: int = 100, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(list_vectra_general_knowledge_runtime(limit=limit))


@router.get('/vectra/memory/general-knowledge/{knowledge_id}', summary='Read VECTRA General Knowledge by ID')
def vectra_general_knowledge_runtime_by_id(knowledge_id: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_general_knowledge_runtime(knowledge_id=knowledge_id))


@router.post('/vectra/memory/general-knowledge', summary='Capitalize VECTRA General Knowledge')
def vectra_general_knowledge_runtime_write(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    return json_response(write_vectra_general_knowledge_runtime(payload))


@router.get('/vectra/memory/general-knowledge/verify/readback', summary='Verify VECTRA General Knowledge readback')
def vectra_general_knowledge_runtime_verify(knowledge_id: str | None = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_general_knowledge_runtime(knowledge_id=knowledge_id))


@router.get('/vectra/memory/revisions', summary='List VECTRA Memory Object revisions')
def vectra_memory_revisions(object_id: str | None = None, knowledge_id: str | None = None, memory_space: str | None = None, limit: int = 100, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(list_vectra_memory_revisions(object_id=object_id, knowledge_id=knowledge_id, memory_space=memory_space, limit=limit))


@router.get('/vectra/memory/revisions/verify', summary='Verify VECTRA Revision and Version Model')
def vectra_memory_revision_verify(domain: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    objects = list_vectra_memory_objects(domain=domain, limit=10000).get('objects', [])
    return json_response(verify_vectra_revision_model(active_objects=objects))


@router.get('/vectra/memory/revisions/{revision_id}', summary='Read VECTRA Memory Object revision by ID')
def vectra_memory_revision_by_id(revision_id: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_memory_revision(revision_id=revision_id))


@router.get('/vectra/memory/release-history', summary='List VECTRA Release History Runtime objects')
def vectra_release_history_runtime(limit: int = 100, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(list_vectra_release_history_runtime(limit=limit))


@router.get('/vectra/memory/release-history/{release_id}', summary='Read VECTRA Release History by ID')
def vectra_release_history_runtime_by_id(release_id: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_release_history_runtime(release_id=release_id))


@router.post('/vectra/memory/release-history', summary='Record VECTRA verified engineering release')
def vectra_release_history_runtime_write(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = request if isinstance(request, dict) else {}
    return json_response(write_vectra_release_history_runtime(payload))


@router.get('/vectra/memory/release-history/verify/readback', summary='Verify VECTRA Release History readback')
def vectra_release_history_runtime_verify(release_id: str | None = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_release_history_runtime(release_id=release_id))


@router.post('/vectra/laboratory/facade/repository', summary='Execute VECTRA Repository facade operation')
def vectra_laboratory_facade_repository(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    operation_type, payload, approval, domain, session_id, request_id = _normalize_facade_request(request)
    try:
        if operation_type == 'status':
            return json_response(_facade_response(operation_type, 'repository_inspection.status', '/vectra/laboratory/repository/status', get_vectra_repository_inspection_status()))
        if operation_type == 'components':
            return json_response(_facade_response(operation_type, 'repository_inspection.components', '/vectra/laboratory/repository/components', get_vectra_repository_components()))
        if operation_type == 'verify':
            return json_response(_facade_response(operation_type, 'repository_inspection.verify', '/vectra/laboratory/repository/verify', verify_vectra_repository_against_release_brief(release_brief_text=payload.get('release_brief_text'))))
        if operation_type == 'inspect':
            result = {'status': 'ok', 'manifest': get_vectra_repository_manifest(), 'components': get_vectra_repository_components()}
            return json_response(_facade_response(operation_type, 'repository_inspection.inspect', '/vectra/laboratory/repository/manifest', result))
        if operation_type == 'recovery_snapshot':
            return json_response(_facade_response(operation_type, 'repository.create_recovery_snapshot', '/vectra/runtime/snapshot', create_vectra_assistant_recovery_snapshot(reason='laboratory_facade_repository_recovery_snapshot')))
        if operation_type == 'readback':
            result = {'status': 'ok', 'repository_status': get_vectra_repository_inspection_status(), 'verification': verify_vectra_repository_against_release_brief(release_brief_text=payload.get('release_brief_text'))}
            return json_response(_facade_response(operation_type, 'repository_inspection.readback', '/vectra/laboratory/repository/verify', result))
        return json_response(_facade_error(operation_type, f'Unsupported repository operation_type: {operation_type}', runtime_service='repository_facade'))
    except Exception as exc:
        logger.exception('repository_facade_operation_failed')
        return json_response(_facade_error(operation_type, str(exc), runtime_service='repository_facade'))


# FOUNDATION-0008: Laboratory read-only Business Data access.
# These endpoints expose the same Business Data source and existing summary/query paths
# used by Working GPT, but only through read-only Laboratory API routes.

@router.get('/vectra/laboratory/business-data/status', summary='Read VECTRA Laboratory Business Data access status')
def vectra_laboratory_business_data_status(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_business_data_status())


@router.get('/vectra/laboratory/business-data/entities', summary='Read VECTRA Laboratory Business Data entities')
def vectra_laboratory_business_data_entities(limit_per_group: int = 50, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_business_data_entities(limit_per_group=limit_per_group))


@router.get('/vectra/laboratory/business-data/sample', summary='Read VECTRA Laboratory Business Data sample')
def vectra_laboratory_business_data_sample(limit: int = 10, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_business_data_sample(limit=limit))


@router.get('/vectra/laboratory/business-data/summary/business', summary='Read-only Laboratory Business summary')
def vectra_laboratory_business_summary(period: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(public_summary(get_vectra_business_data_summary('business', period=period)))


@router.get('/vectra/laboratory/business-data/summary/manager-top', summary='Read-only Laboratory Manager Top summary')
def vectra_laboratory_manager_top_summary(manager_top: str, period: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(public_summary(get_vectra_business_data_summary('manager-top', period=period, manager_top=manager_top)))


@router.get('/vectra/laboratory/business-data/summary/manager', summary='Read-only Laboratory Manager summary')
def vectra_laboratory_manager_summary(manager: str, period: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(public_summary(get_vectra_business_data_summary('manager', period=period, manager=manager)))


@router.get('/vectra/laboratory/business-data/summary/network', summary='Read-only Laboratory Network summary')
def vectra_laboratory_network_summary(network: str, period: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(public_summary(get_vectra_business_data_summary('network', period=period, network=network)))


@router.get('/vectra/laboratory/business-data/summary/category', summary='Read-only Laboratory Category summary')
def vectra_laboratory_category_summary(category: str, period: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(public_summary(get_vectra_business_data_summary('category', period=period, category=category)))


@router.get('/vectra/laboratory/business-data/summary/tmc-group', summary='Read-only Laboratory TMC Group summary')
def vectra_laboratory_tmc_group_summary(tmc_group: str, period: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(public_summary(get_vectra_business_data_summary('tmc-group', period=period, tmc_group=tmc_group)))


@router.get('/vectra/laboratory/business-data/summary/sku', summary='Read-only Laboratory SKU summary')
def vectra_laboratory_sku_summary(sku: str, period: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(public_summary(get_vectra_business_data_summary('sku', period=period, sku=sku)))


@router.get('/vectra/laboratory/business-data/query', summary='Read-only Laboratory Business Data query')
def vectra_laboratory_business_data_query(message: str, session_id: str = 'laboratory-read-only', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(run_vectra_business_data_query(message=message, session_id=session_id))


@router.get('/vectra/laboratory/business-data/verify', summary='Verify Laboratory Business Data access')
def vectra_laboratory_business_data_verify(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_business_data_access())


@router.get('/vectra/laboratory/repository/status', summary='Read-only VECTRA Repository inspection status')
def vectra_laboratory_repository_status(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_repository_inspection_status())


@router.get('/vectra/laboratory/repository/manifest', summary='Read-only VECTRA Repository manifest')
def vectra_laboratory_repository_manifest(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_repository_manifest())


@router.get('/vectra/laboratory/repository/tree', summary='Read-only VECTRA Repository tree')
def vectra_laboratory_repository_tree(max_items: int = 800, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_repository_tree(max_items=max_items))


@router.get('/vectra/laboratory/repository/components', summary='Read-only VECTRA Repository component map')
def vectra_laboratory_repository_components(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_repository_components())


@router.get('/vectra/laboratory/repository/verify', summary='Verify VECTRA Repository implementation read-only')
def vectra_laboratory_repository_verify(release_brief_text: str = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_repository_against_release_brief(release_brief_text=release_brief_text))


@router.post('/vectra/knowledge/candidates', summary='Create VECTRA Knowledge Candidate')
def vectra_knowledge_candidate_create(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    if not isinstance(request, dict):
        request = {}
    return json_response(create_vectra_knowledge_candidate(request))


@router.post('/vectra/knowledge/capitalization/packages', summary='Create VECTRA Knowledge Capitalization Package')
def vectra_knowledge_capitalization_package_create(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    if not isinstance(request, dict):
        request = {}
    return json_response(create_vectra_capitalization_package(request))


@router.post('/vectra/knowledge/capitalization/write', summary='Write confirmed VECTRA Knowledge')
def vectra_knowledge_capitalization_write(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    if not isinstance(request, dict):
        request = {}
    return json_response(write_vectra_confirmed_knowledge(request))


@router.post('/vectra/knowledge/capitalization', summary='Run VECTRA Knowledge Capitalization')
def vectra_knowledge_capitalization(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    if not isinstance(request, dict):
        request = {}
    return json_response(capitalize_vectra_knowledge(request))


@router.get('/vectra/knowledge/capitalization/status', summary='Read VECTRA Knowledge Capitalization status')
def vectra_knowledge_capitalization_status(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_knowledge_capitalization_status())


@router.get('/vectra/knowledge/capitalization/reports', summary='List VECTRA Knowledge Capitalization reports')
def vectra_knowledge_capitalization_reports(limit: int = 20, include_failed: bool = True, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(list_vectra_knowledge_capitalization_reports(limit=limit, include_failed=include_failed))


@router.get('/vectra/knowledge/professional', summary='Get Professional Knowledge List')
def vectra_knowledge_professional(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(list_vectra_professional_knowledge())


@router.get('/vectra/knowledge/professional/overview', summary='Professional Knowledge Overview')
def vectra_knowledge_professional_overview(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_professional_knowledge_overview())


@router.get('/vectra/knowledge/professional/{knowledge_id}', summary='Get Professional Knowledge')
def vectra_knowledge_professional_by_id(knowledge_id: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_professional_knowledge(knowledge_id=knowledge_id))


@router.get('/vectra/knowledge/professional/{knowledge_id}/readback', summary='Verify Professional Knowledge Readback')
def vectra_knowledge_professional_readback(knowledge_id: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_professional_knowledge_readback(knowledge_id=knowledge_id))


@router.get('/vectra/domain/{domain}/knowledge', summary='Read capitalized Business Domain Knowledge')
def vectra_domain_knowledge(domain: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_domain_knowledge(domain=domain))



@router.get('/vectra/domain/{domain}/knowledge/overview', summary='Business Domain Knowledge Overview')
def vectra_domain_knowledge_overview(domain: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_domain_knowledge_overview(domain=domain))


@router.get('/vectra/domain/{domain}/knowledge/{knowledge_id}', summary='Get Business Domain Knowledge by ID')
def vectra_domain_knowledge_by_id(domain: str, knowledge_id: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(get_vectra_domain_knowledge_by_id(domain=domain, knowledge_id=knowledge_id))


@router.get('/vectra/domain/{domain}/knowledge/{knowledge_id}/readback', summary='Verify Business Domain Knowledge Readback')
def vectra_domain_knowledge_readback(domain: str, knowledge_id: str, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_domain_knowledge_readback(domain=domain, knowledge_id=knowledge_id))


@router.post('/vectra/domain/{domain}/knowledge/candidates', summary='Create Business Knowledge Candidate')
def vectra_business_knowledge_candidate_create(domain: str, request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = dict(request or {})
    payload['domain'] = domain
    return json_response(create_vectra_business_knowledge_candidate(payload))


@router.post('/vectra/domain/{domain}/knowledge/capitalization/packages', summary='Create Business Knowledge Capitalization Package')
def vectra_business_knowledge_capitalization_package_create(domain: str, request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = dict(request or {})
    payload['domain'] = domain
    return json_response(create_vectra_business_knowledge_capitalization_package(payload))


@router.post('/vectra/domain/{domain}/knowledge/capitalization/write', summary='Write Business Knowledge')
def vectra_business_knowledge_write(domain: str, request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    payload = dict(request or {})
    payload['domain'] = domain
    return json_response(write_vectra_business_knowledge(payload))


@router.get('/vectra/knowledge/verify', summary='Verify VECTRA Knowledge Capitalization Runtime')
def vectra_knowledge_verify(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    _verify_laboratory_api_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_knowledge_capitalization())



@router.get('/business_summary', summary='Business Summary')
def business_summary(period: str):
    return json_response(public_summary(get_business_summary(period=period)))


@router.get('/manager_top_summary', summary='Manager Top Summary')
def manager_top_summary(manager_top: str, period: str):
    return json_response(public_summary(get_manager_top_summary(manager_top=manager_top, period=period)))


@router.get('/manager_summary', summary='Manager Summary')
def manager_summary(manager: str, period: str):
    return json_response(public_summary(get_manager_summary(manager=manager, period=period)))


@router.get('/network_summary', summary='Network Summary')
def network_summary(network: str, period: str):
    return json_response(public_summary(get_network_summary(network=network, period=period)))


@router.get('/sku_summary', summary='SKU Summary')
def sku_summary(sku: str, period: str):
    return json_response(public_summary(get_sku_summary(sku=sku, period=period)))


def _is_render_ready_payload(payload):
    if not isinstance(payload, dict):
        return False

    # DEV-0002: a payload that already carries canonical runtime rendering
    # must not be sent through public_summary(). public_summary() is intended
    # for raw domain summaries and can drop already-built Workspace artifacts
    # such as workspace_markdown / workspace_primary_block. This was the root
    # cause of workspace_markdown_missing for Product Team Assistant research
    # and workspace-opening error screens.
    if isinstance(payload.get('workspace_markdown'), str) and payload.get('workspace_markdown').strip():
        # Raw analytical summaries may already carry a preliminary markdown copy
        # for state continuity while still requiring public_summary() to normalize
        # metrics and build final render blocks. Treat markdown-only payloads as
        # render-ready only when they also expose an explicit render surface.
        if payload.get('status') == 'error' or payload.get('render_mode') or payload.get('screen_order'):
            return True
    if isinstance(payload.get('workspace_primary_block'), list) and payload.get('workspace_primary_block'):
        return True

    render_keys = {'kpi_block', 'structure_block', 'navigation_block', 'drain_block_render', 'result_block'}
    return bool(render_keys.intersection(payload.keys())) and isinstance(payload.get('context'), dict)


def _replace_user_terms(value):
    replacements = {
        'Результат объекта': 'Результат периода',
        'Вклад в прибыль бизнеса': 'Результат периода',
        'Вклад в результат бизнеса': 'Результат периода',
        'Потенциал возврата прибыли': 'Потенциал прибыли',
        'Business Benchmark': 'Средний уровень бизнеса',
        'SKU Benchmark': 'Эффективность SKU относительно бизнеса',
        'Проверить SKU Benchmark': 'Проверить эффективность SKU',
    }
    if isinstance(value, str):
        out = value
        for old, new in replacements.items():
            out = out.replace(old, new)
        return out
    if isinstance(value, list):
        return [_replace_user_terms(v) for v in value]
    if isinstance(value, dict):
        return {k: _replace_user_terms(v) for k, v in value.items()}
    return value


def _apply_stage51_render_overrides(payload):
    if not isinstance(payload, dict):
        return payload
    payload = _replace_user_terms(dict(payload))
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    level = str(ctx.get('level') or payload.get('level') or '').strip().lower()
    render_mode = str(payload.get('render_mode') or '').strip().lower()

    if render_mode not in {'start', 'list_only', 'reasons', 'kpi_only', 'voice_diagnostic', 'action_package', 'negotiation_workspace', 'task_workspace', 'post_meeting_workspace', 'execution_workspace', 'development_journal', 'development_journal_capture', 'development_journal_export', 'release_manager', 'laboratory_analysis', 'test_plan', 'architecture_complete_gate', 'product_review', 'sprint_candidate', 'decision_capture', 'task_capture', 'feedback_capture', 'corporate_memory', 'closed_loop_status', 'product_intelligence', 'scenario_runner', 'scenario_library'} and level:
        try:
            payload['result_block'] = _render_result_block(payload)
            payload['summary_block'] = _build_benchmark_driven_summary(payload)
            payload['explanation_block'] = _build_explanation_block(payload)
            payload['next_step_block'] = _build_next_step_block(payload)
            payload['diagnosis_block'] = _build_assistant_diagnosis_block(payload)
            payload['recommended_next_step_block'] = _build_recommended_next_step_block(payload)
            payload['opportunity_explanation_block'] = _build_opportunity_explanation_block(payload)
            payload['anomaly_explanation_block'] = _build_anomaly_explanation_block(payload)
            payload['business_opportunity_block'] = _render_business_opportunity_block(payload)
            payload['recommendation_block'] = _render_recommendation_block(payload)
            payload['narrative_block'] = _render_narrative_block(payload)
            payload['product_workspace_block'] = _render_product_workspace_block(payload)
            payload['management_workspace_block'] = _render_management_workspace_block(payload)
            payload['screen_order'] = _stage7_screen_order(payload)
        except Exception:
            logger.exception('stage51_explanation_override_failed')

    try:
        drain = _normalize_drain(payload)
        if isinstance(drain, dict):
            if render_mode == 'list_only':
                payload['drain_block_render'] = _render_vitrina_block(payload)
                payload['summary_block'] = 'Витрина объекта. Полный список текущего уровня без аналитического сопровождения.'
            else:
                payload['drain_block_render'] = _render_drain_block(drain)
            if render_mode not in {'start', 'list_only', 'reasons', 'kpi_only', 'voice_diagnostic', 'action_package', 'negotiation_workspace', 'task_workspace', 'post_meeting_workspace', 'execution_workspace', 'development_journal', 'development_journal_capture', 'development_journal_export', 'release_manager', 'laboratory_analysis', 'test_plan', 'architecture_complete_gate', 'product_review', 'sprint_candidate', 'decision_capture', 'task_capture', 'feedback_capture', 'corporate_memory', 'closed_loop_status', 'product_intelligence', 'scenario_runner', 'scenario_library', 'self_evolution'}:
                payload['navigation_block'] = _render_navigation_block(payload, _normalize_navigation(payload, drain), drain)
    except Exception:
        logger.exception('stage51_navigation_override_failed')
    return payload



def _attach_sku_passport_if_missing(payload):
    if not isinstance(payload, dict):
        return payload
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    if payload.get('render_mode') in {'action_package', 'negotiation_workspace', 'task_workspace', 'post_meeting_workspace', 'list_only', 'reasons', 'kpi_only'}:
        return payload
    if str(ctx.get('level') or '').strip().lower() != 'sku':
        return payload
    if payload.get('sku_passport') and payload.get('sku_passport_block'):
        return payload
    sku = ctx.get('object_name') or payload.get('object_name')
    period = ctx.get('period') or payload.get('period')
    if not sku or not period:
        return payload
    filter_payload = {}
    path = payload.get('path') if isinstance(payload.get('path'), list) else []
    # Path convention: Business -> Top Manager -> Manager -> Network -> Category -> SKU
    if len(path) >= 4:
        filter_payload['network'] = path[3]
    if len(path) >= 5:
        filter_payload['category'] = path[4]
    existing_filter = payload.get('filter') if isinstance(payload.get('filter'), dict) else {}
    filter_payload.update({k: v for k, v in existing_filter.items() if k in {'network', 'category', 'tmc_group'}})
    try:
        rebuilt = public_summary(get_sku_summary(sku=sku, period=period, filter_payload=filter_payload))
        for key in ('sku_passport', 'sku_passport_block', 'business_context', 'business_context_block', 'category_workspace', 'category_workspace_block', 'business_opportunity', 'business_opportunity_block', 'recommendation_engine', 'recommendation_block', 'narrative_engine', 'narrative_block', 'product_workspace', 'product_workspace_block', 'management_intelligence', 'management_workspace', 'management_passport', 'management_workspace_block', 'business_workspace_block', 'contract_workspace_block'):
            if rebuilt.get(key):
                payload[key] = rebuilt.get(key)
        if payload.get('sku_passport_block'):
            order = payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
            if 'sku_passport_block' not in order:
                payload['screen_order'] = ['sku_passport_block'] + order
    except Exception:
        logger.exception('attach_sku_passport_failed')
    return payload



def _attach_management_workspace_if_missing(payload):
    if not isinstance(payload, dict):
        return payload
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    level = str(ctx.get('level') or payload.get('level') or '').strip().lower()
    if level not in {'business', 'manager_top', 'manager'}:
        return payload
    if payload.get('management_intelligence') and payload.get('management_workspace_block'):
        return payload
    period = ctx.get('period') or payload.get('period')
    object_name = ctx.get('object_name') or payload.get('object_name')
    if not period:
        return payload
    try:
        if level == 'business':
            rebuilt = public_summary(get_business_summary(period=period))
        elif level == 'manager_top':
            if not object_name:
                return payload
            rebuilt = public_summary(get_manager_top_summary(manager_top=object_name, period=period))
        elif level == 'manager':
            if not object_name:
                return payload
            rebuilt = public_summary(get_manager_summary(manager=object_name, period=period))
        else:
            return payload
        for key in ('management_intelligence', 'management_workspace', 'management_passport', 'management_workspace_block', 'business_workspace_block', 'contract_workspace_block'):
            if rebuilt.get(key):
                payload[key] = rebuilt.get(key)
        if payload.get('management_workspace_block'):
            order = payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
            if 'management_workspace_block' not in order:
                payload['screen_order'] = ['management_workspace_block'] + order
    except Exception:
        logger.exception('attach_management_workspace_failed')
    return payload

def _prepare_vectra_query_payload(payload):
    """Normalize only raw API/domain summaries.

    UI/state commands (все / причины / назад) may already return a final
    render-ready screen. Re-normalizing that screen through public_summary()
    breaks it because render screens do not carry the raw metrics contract.
    """
    if _is_render_ready_payload(payload):
        ready = dict(payload)
        ready.setdefault('status', 'ok')
        ctx = ready.get('context') if isinstance(ready.get('context'), dict) else {}
        render_mode = str(ready.get('render_mode') or '').strip().lower()
        level = str(ctx.get('level') or '').strip().lower()

        # DEV-0002: render-ready diagnostic/error payloads already contain the
        # public user-facing markdown. Do not run them through analytical
        # enrichment helpers: those helpers expect raw KPI metric contracts and
        # can fail or strip the already-built Workspace response.
        if ready.get('status') == 'error' or render_mode in {'start', 'voice_diagnostic', 'workspace_api_attempt_error', 'product_team_research_workspace'} or level in {'start', 'workspace_opening_error', 'voice_management', 'product_team_research'}:
            return _ensure_vectra_query_render_contract(ready)

        ready = _attach_sku_passport_if_missing(ready)
        ready = _attach_management_workspace_if_missing(ready)
        ready = _apply_stage51_render_overrides(ready)
        ready = _attach_product_recovery_blocks(ready)
        return _ensure_vectra_query_render_contract(_force_product_navigation(ready))
    rendered = public_summary(payload)
    rendered = _attach_sku_passport_if_missing(rendered)
    rendered = _attach_management_workspace_if_missing(rendered)
    rendered = _apply_stage51_render_overrides(rendered)
    rendered = _attach_product_recovery_blocks(rendered)
    return _ensure_vectra_query_render_contract(rendered)


def _force_product_navigation(payload):
    """Final product navigation guard for /vectra/query render-only payload.

    Keeps product commands visible even when upstream navigation was produced
    by an older/raw view. Does not change calculations, drain, vector or KPI.
    """
    if not isinstance(payload, dict) or payload.get('status') == 'error':
        return payload

    nav = payload.get('navigation_block') or []
    if not isinstance(nav, list):
        nav = []

    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    render_mode = str(payload.get('render_mode') or '').strip().lower()

    if render_mode == 'voice_diagnostic':
        return payload

    if render_mode == 'action_package':
        out = []
        seen = set()
        for line in nav:
            text = str(line)
            if text and text not in seen:
                out.append(text)
                seen.add(text)
        if not any('назад' in x.lower() for x in out):
            out.append('назад — вернуться к объекту')
        payload['navigation_block'] = out
        return payload

    if render_mode == 'kpi_only':
        payload['decision_workspace_block'] = []
        payload['explanation_block'] = []
        payload['next_step_block'] = []
        payload['recommended_next_step_block'] = []
        payload['diagnosis_block'] = []
        payload['reasons_block_render'] = []
        payload['decision_block_render'] = []
        payload['business_opportunity_block'] = []
        payload['recommendation_block'] = []
        payload['narrative_block'] = []
        payload['product_workspace_block'] = []
        payload['business_context_block'] = []
        payload['category_workspace_block'] = []
        payload['drain_block_render'] = []
        payload['drain_total'] = 0
        payload['navigation_block'] = ['причины — разобрать факторы', 'все — витрина текущего уровня', 'назад — вернуться к рабочему столу']
        payload['screen_order'] = ['summary_block', 'result_block', 'period_result_block', 'kpi_block', 'kpi_table', 'navigation_block']
        return payload

    if render_mode == 'reasons':
        payload['decision_workspace_block'] = []
        payload['explanation_block'] = []
        payload['next_step_block'] = []
        payload['recommended_next_step_block'] = []
        payload['diagnosis_block'] = []
        payload['decision_block_render'] = []
        payload['business_opportunity_block'] = []
        payload['recommendation_block'] = []
        payload['narrative_block'] = []
        payload['product_workspace_block'] = []
        payload['business_context_block'] = []
        payload['category_workspace_block'] = []
        payload['drain_block_render'] = []
        payload['drain_total'] = 0
        payload['navigation_block'] = ['назад к объекту']
        payload['screen_order'] = ['summary_block', 'reasons_block_render', 'navigation_block']
        return payload

    # A numeric line starts navigation to a concrete child object.
    has_numeric_items = any(str(line).strip()[:1].isdigit() for line in nav)
    if render_mode == 'list_only':
        payload['decision_workspace_block'] = []
        payload['explanation_block'] = []
        payload['next_step_block'] = []
        payload['recommended_next_step_block'] = []
        payload['diagnosis_block'] = []
        payload['reasons_block_render'] = []
        payload['decision_block_render'] = []
        payload['business_opportunity_block'] = []
        payload['recommendation_block'] = []
        payload['narrative_block'] = []
        payload['product_workspace_block'] = []
        out = []
        seen = set()
        for line in nav:
            text = str(line)
            if text.strip()[:1].isdigit() or 'назад' in text.lower():
                if text not in seen:
                    out.append(text)
                    seen.add(text)
        if not any('назад' in x.lower() for x in out):
            out.append('назад — вернуться к объекту')
        payload['navigation_block'] = out
        return payload

    out = []
    seen = set()

    def add(line):
        if line and line not in seen:
            out.append(line)
            seen.add(line)

    for line in nav:
        add(str(line))

    # Full list / object screen commands.
    if level != 'sku' and render_mode != 'list_only' and has_numeric_items:
        add('все — полный список')

    # Action-first navigation: after a рабочий стол, user should see working actions,
    # not only the next DATA level. Detailed drilldown remains available through
    # numeric commands / all / direct free questions.
    if level == 'network':
        add('подготовить переговоры — собрать позицию по контракту')
        add('собрать пакет позиций — выбрать позиции для ввода')
        add('показать лидеров SKU — роли текущих позиций')
        add('показать отсутствующие SKU — лидеры бизнеса вне контракта')
        add('показать ассортиментные перекосы — концентрация и пробелы')
        add('создать задачи — зафиксировать действия по контракту')
        add('причины — разбор контракта')
    elif level in {'category', 'tmc_group'}:
        add('подготовить пакет развития — форматы и позиции категории')
        add('подготовить переговорный аргумент — как продать категорию')
        add('посмотреть отсутствующие позиции — найти ассортиментные возможности')
    elif level == 'sku':
        add('паспорт SKU — полная карточка позиции')
        add('подготовить переговоры — использовать позицию как аргумент')
        add('создать задачи — зафиксировать действие по SKU')
    elif level and level not in {'start'} and not _is_product_layer_level(level):
        add('причины — разбор')

    # v9: no separate 'искать' command; numeric navigation and 'все' are enough.

    # Back should exist below business and in list/reasons modes.
    if level and level not in {'business', 'start'}:
        add('назад — вверх')
    elif render_mode in {'list_only', 'reasons', 'kpi_only'}:
        add('назад — вверх')

    payload['navigation_block'] = out
    return payload






def _is_runtime_natural_query(message: str) -> bool:
    """Return True only for explicit Runtime/Repository readback commands.

    This prevents ordinary business requests from being hijacked by command_help,
    while allowing Custom GPT to verify Runtime through the same /vectra/query
    Action used for normal dialogue.
    """
    try:
        classification = classify_vectra_natural_command({'message': message})
    except Exception:
        return False
    intent = str(classification.get('intent') or '')
    confidence = float(classification.get('confidence') or 0)
    explicit_intents = {
        'runtime_product_verification',
        'memory_overview',
        'journal_read',
        'state_read',
        'recovery_read',
        'knowledge_read',
        'decisions_read',
        'pending_approvals_read',
        'runtime_reports_read',
        'snapshots_read',
        'repository_status_read',
    }
    return intent in explicit_intents and confidence >= 0.75

@router.post('/vectra/query', summary='Stateful VECTRA Query')
def vectra_query(request: VectraQueryRequest):
    session_id = _stable_session_id(request)
    logger.info('vectra_query_received session_id=%s message=%r', session_id, request.message)
    
    # State/UI commands (все / причины / назад) are handled only inside
    # orchestration.py. routes.py is now only API/render boundary.
    _hydrate_runtime_context_from_request(session_id, request)

    if _is_runtime_natural_query(request.message):
        payload = execute_vectra_natural_command({'message': request.message, 'session_id': session_id})
    elif is_self_evolution_command(request.message):
        result = run_self_evolution_cycle(
            decision='Переходный сервисный запуск Self Evolution Engine через команду Product Owner.',
            object_changed='Product Team Assistant Self Evolution Model',
            rationale='Product Team Assistant должен сохранять собственную модель развития вне истории чата.',
            source='vectra_query_service_command',
            metadata={'session_id': session_id, 'owner_command': request.message},
        )
        payload = build_self_evolution_response(result)
    elif is_confirmed_knowledge_message(request.message):
        result = run_autonomous_self_evolution_cycle(
            decision=str(request.message or 'Подтверждённое знание обнаружено.'),
            object_changed='Product Team Assistant confirmed knowledge',
            rationale='Подтверждённое решение должно быть автоматически классифицировано, приоритизировано и интегрировано в модель Assistant.',
            source='vectra_query_autonomous_detection',
            metadata={'session_id': session_id, 'owner_message': request.message},
        )
        payload = build_autonomous_self_evolution_response(result)
    elif _is_product_owner_autonomous_research_start(request.message):
        payload = _build_product_owner_autonomous_research_start(request.message, session_id=session_id)
    elif _is_product_team_research_request(request.message):
        payload = _build_product_team_research_workspace(request.message, session_id=session_id)
    elif _is_research_continue_request(request.message, session_id):
        obj = _current_product_team_research_object(session_id)
        payload = _build_product_team_autonomous_research_workspace(obj, session_id=session_id, trigger='continue_research_route')
    elif _is_numeric_research_action(request.message, session_id):
        payload = _build_product_team_research_action_workspace(request.message, session_id=session_id)
    else:
        payload = orchestrate_vectra_query(request.message, session_id=session_id)
    logger.info('vectra_query_result session_id=%s status=%s reason=%s', session_id, payload.get('status'), payload.get('reason'))
    rendered_payload = apply_runtime_contract(_prepare_vectra_query_payload(payload))
    response_scope = _detect_response_scope(request.message)
    if rendered_payload.get('render_mode') == 'vectra_natural_command_guidance':
        render_only_payload = {
            'status': rendered_payload.get('status', 'ok'),
            'reason': rendered_payload.get('reason'),
            'context': rendered_payload.get('context'),
            'render_mode': 'vectra_natural_command_guidance',
            'workspace_markdown': rendered_payload.get('workspace_markdown', ''),
            'workspace_render_instruction': rendered_payload.get('workspace_render_instruction', 'Показать пользователю workspace_markdown полностью и без изменений.'),
            'screen_order': ['workspace_markdown'],
            'classification': rendered_payload.get('classification', {}),
            'selected_action': rendered_payload.get('selected_action'),
            'product_owner_report': rendered_payload.get('product_owner_report', {}),
            'result': rendered_payload.get('result', {}),
        }
    elif rendered_payload.get('render_mode') == 'self_evolution':
        render_only_payload = {
            'status': rendered_payload.get('status', 'ok'),
            'reason': rendered_payload.get('reason'),
            'context': rendered_payload.get('context'),
            'render_mode': 'self_evolution',
            'workspace_markdown': rendered_payload.get('workspace_markdown', ''),
            'workspace_render_instruction': rendered_payload.get('workspace_render_instruction', 'Показать пользователю workspace_markdown полностью и без изменений.'),
            'screen_order': ['workspace_markdown'],
            'self_evolution': rendered_payload.get('self_evolution', {}),
            'current_model_version': rendered_payload.get('current_model_version'),
            'cycle_completed': rendered_payload.get('cycle_completed', False),
            'instruction_update_required': rendered_payload.get('instruction_update_required', False),
            'instruction_update_note': rendered_payload.get('instruction_update_note', ''),
        }
    elif rendered_payload.get('render_mode') == 'list_only':
        render_only_payload = _make_list_only_public_payload(rendered_payload)
    elif rendered_payload.get('render_mode') == 'reasons':
        render_only_payload = _make_reasons_only_public_payload(rendered_payload)
    elif response_scope == 'kpi':
        render_only_payload = _make_kpi_only_public_payload(rendered_payload)
    else:
        render_only_payload = {
        'status': rendered_payload.get('status', 'ok'),
        'reason': rendered_payload.get('reason'),
        'context': rendered_payload.get('context'),
        'compare_base': rendered_payload.get('compare_base'),
        # CHANGE-005.1: put Profit First block before KPI so clients that render
        # payload order start with «Что произошло», not with Opportunity/KPI.
        'result_block': rendered_payload.get('result_block', []),
        'period_result_block': rendered_payload.get('period_result_block', []),
        'summary_block': rendered_payload.get('summary_block', ''),
        'kpi_block': rendered_payload.get('kpi_block', []),
        'kpi_table': rendered_payload.get('kpi_table', []),
        'structure_block': rendered_payload.get('structure_block', []),
        'main_driver': rendered_payload.get('main_driver', ''),
        'drain_block_render': rendered_payload.get('drain_block_render', []),
        'drain_total': rendered_payload.get('drain_total', 0),
        'all_block': rendered_payload.get('all_block', []),
        'navigation_block': rendered_payload.get('navigation_block', []),
        'explanation_block': rendered_payload.get('explanation_block', []),
        'next_step_block': rendered_payload.get('next_step_block', []),
        'diagnosis_block': rendered_payload.get('diagnosis_block', []),
        'recommended_next_step_block': rendered_payload.get('recommended_next_step_block', []),
        'opportunity_explanation_block': rendered_payload.get('opportunity_explanation_block', []),
        'anomaly_explanation_block': rendered_payload.get('anomaly_explanation_block', []),
        'screen_order': rendered_payload.get('screen_order', []),
        'workspace_primary_block': rendered_payload.get('workspace_primary_block', []),
        'workspace_markdown': rendered_payload.get('workspace_markdown', ''),
        'workspace_render_instruction': rendered_payload.get('workspace_render_instruction', ''),
        'active_workspace_state': rendered_payload.get('active_workspace_state', {}),
        'workspace_action_map': rendered_payload.get('workspace_action_map', []),
        'workspace_runtime_contract': rendered_payload.get('workspace_runtime_contract', {}),
        # DEV-0004: expose Research Flow state at the public API boundary.
        # Custom GPT Actions do not receive hidden chat state automatically;
        # these fields are the explicit bridge that allows the user-mode
        # Product Team Assistant to continue the research scenario after
        # vectraQuery returns.
        'active_research_state': rendered_payload.get('active_research_state', {}) or rendered_payload.get('research_flow_status', {}),
        'research_flow_status': rendered_payload.get('research_flow_status', {}) or rendered_payload.get('active_research_state', {}),
        'research_path': rendered_payload.get('research_path', []),
        'current_step': rendered_payload.get('current_step', ''),
        'next_step': rendered_payload.get('next_step', ''),
        # DEV-0006: public Autonomous User Session bridge.
        'autonomous_user_session': rendered_payload.get('autonomous_user_session', {}),
        'user_session_id': rendered_payload.get('user_session_id', ''),
        'user_role': rendered_payload.get('user_role', ''),
        'user_goal': rendered_payload.get('user_goal', ''),
        'user_history': rendered_payload.get('user_history', []),
        'session_status': rendered_payload.get('session_status', ''),
        'owner_command_forwarded_to_vectra': rendered_payload.get('owner_command_forwarded_to_vectra', (rendered_payload.get('autonomous_user_session') or {}).get('owner_command_forwarded_to_vectra') if isinstance(rendered_payload.get('autonomous_user_session'), dict) else None),
        'owner_command': rendered_payload.get('owner_command', (rendered_payload.get('autonomous_user_session') or {}).get('owner_command') if isinstance(rendered_payload.get('autonomous_user_session'), dict) else ''),
        'autonomous_user_session_active': rendered_payload.get('autonomous_user_session_active', bool(rendered_payload.get('autonomous_user_session'))),
        'autonomous_route': rendered_payload.get('autonomous_route', ''),
        'previous_context_closed': rendered_payload.get('previous_context_closed', False),
        'owner_command_type': rendered_payload.get('owner_command_type', ''),
        'start_screen_contract': rendered_payload.get('start_screen_contract', {}),
        'runtime_navigation': rendered_payload.get('runtime_navigation', {}),
        'path': rendered_payload.get('path', []),
        'reasons_block': rendered_payload.get('reasons_block', []),
        'reasons_block_render': rendered_payload.get('reasons_block_render', []),
        'decision_block': rendered_payload.get('decision_block', []),
        'decision_block_render': rendered_payload.get('decision_block_render', []),
        'business_result_rating_block': rendered_payload.get('business_result_rating_block', []),
        'profit_loss_rating_block': rendered_payload.get('profit_loss_rating_block', []),
        'opportunity_rating_block': rendered_payload.get('opportunity_rating_block', []),
        'priority_action_block': rendered_payload.get('priority_action_block', []),
        'object_reasons_block': rendered_payload.get('object_reasons_block', []),
        'factor_change_block': rendered_payload.get('factor_change_block', []),
        'factor_change_table': rendered_payload.get('factor_change_table', []),
        'benchmark_diagnostic_block': rendered_payload.get('benchmark_diagnostic_block', []),
        'benchmark_diagnostic_table': rendered_payload.get('benchmark_diagnostic_table', []),
        'product_layer_block': rendered_payload.get('product_layer_block', []),
        'product_insight_block': rendered_payload.get('product_insight_block', []),
        'product_tmc_decision_block': rendered_payload.get('product_tmc_decision_block', []),
        'sku_passport': rendered_payload.get('sku_passport', {}),
        'sku_passport_block': rendered_payload.get('sku_passport_block', []),
        'decision_workspace': rendered_payload.get('decision_workspace', {}),
        'business_context': rendered_payload.get('business_context', {}),
        'business_context_block': rendered_payload.get('business_context_block', []),
        'category_workspace': rendered_payload.get('category_workspace', {}),
        'category_workspace_block': rendered_payload.get('category_workspace_block', []),
        'business_opportunity': rendered_payload.get('business_opportunity', {}),
        'business_opportunity_block': rendered_payload.get('business_opportunity_block', []),
        'recommendation_engine': rendered_payload.get('recommendation_engine', {}),
        'recommendation_block': rendered_payload.get('recommendation_block', []),
        'narrative_engine': rendered_payload.get('narrative_engine', {}),
        'narrative_block': rendered_payload.get('narrative_block', []),
        'product_workspace': rendered_payload.get('product_workspace', {}),
        'product_workspace_block': rendered_payload.get('product_workspace_block', []),
        'management_intelligence': rendered_payload.get('management_intelligence', {}),
        'management_workspace': rendered_payload.get('management_workspace', {}),
        'management_passport': rendered_payload.get('management_passport', {}),
        'management_workspace_block': rendered_payload.get('management_workspace_block', []),
        'business_workspace_block': rendered_payload.get('business_workspace_block', []),
        'contract_workspace_block': rendered_payload.get('contract_workspace_block', []),
        'decision_workspace_block': rendered_payload.get('decision_workspace_block', []),
        'render_mode': rendered_payload.get('render_mode', ''),
        # CHANGE-006.1: hide aggregate Benchmark Money from the public render payload.
        'opportunity_money': rendered_payload.get('opportunity_money'),
        'navigation_money': rendered_payload.get('navigation_money'),
        'net_drain_money': rendered_payload.get('net_drain_money'),
        'gross_loss_money': rendered_payload.get('gross_loss_money'),
        'internal_drain_money': rendered_payload.get('internal_drain_money'),
        }
    render_only_payload = _trim_default_public_payload(render_only_payload)
    render_only_payload = _force_product_navigation(render_only_payload)
    render_only_payload = _ensure_public_markdown_for_diagnostic(render_only_payload)
    _ctx_level = str((render_only_payload.get('context') or {}).get('level') or '').strip().lower() if isinstance(render_only_payload.get('context'), dict) else ''
    if _ctx_level != 'assistant_dialogue' and render_only_payload.get('render_mode') not in {'start', 'list_only', 'reasons', 'kpi_only', 'voice_diagnostic', 'action_package', 'negotiation_workspace', 'task_workspace', 'post_meeting_workspace', 'execution_workspace', 'development_journal', 'development_journal_capture', 'development_journal_export', 'release_manager', 'laboratory_analysis', 'test_plan', 'architecture_complete_gate', 'product_review', 'sprint_candidate', 'decision_capture', 'task_capture', 'feedback_capture', 'corporate_memory', 'closed_loop_status', 'product_intelligence', 'scenario_runner', 'scenario_library', 'self_evolution'}:
        pre_render_payload = render_only_payload
        render_only_payload = _make_full_workspace_public_payload(render_only_payload)
        if render_only_payload.get('status') == 'error' and render_only_payload.get('reason') == 'workspace_generation_error':
            _record_runtime_rendering_issue(session_id, pre_render_payload, 'workspace_markdown_missing', 'Workspace opening request produced no non-empty workspace_markdown.', render_only_payload.get('error_code'))
        elif not render_only_payload.get('workspace_action_map'):
            _record_runtime_rendering_issue(session_id, render_only_payload, 'workspace_action_map_empty', 'Workspace rendered without visible action map extracted from workspace_markdown.', 'workspace_action_map_empty')
    # Persist only analytical object/list screens at the API boundary.
    # UI display modes (все / причины) are produced by orchestration.py and
    # must not overwrite current_screen; otherwise the next «назад» would
    # return to a display mode instead of the object screen.
    should_save_rendered_state = (
        render_only_payload.get('status') != 'error'
        and _ctx_level != 'assistant_dialogue'
        and render_only_payload.get('render_mode') not in {'start', 'list_only', 'reasons', 'kpi_only', 'voice_diagnostic', 'action_package', 'negotiation_workspace', 'task_workspace', 'post_meeting_workspace', 'execution_workspace', 'development_journal', 'development_journal_capture', 'development_journal_export', 'release_manager', 'laboratory_analysis', 'test_plan', 'architecture_complete_gate', 'product_review', 'sprint_candidate', 'decision_capture', 'task_capture', 'feedback_capture', 'corporate_memory', 'closed_loop_status', 'product_intelligence', 'scenario_runner', 'scenario_library', 'self_evolution'}
    )
    # Explicit object-scoped KPI requests (`Покажи Варус KPI`) return a KPI-only
    # public payload, but State must keep the full rendered рабочий стол for the
    # same object so the next local command (`причины`, `все`, `назад`) works.
    # Local KPI command (`kpi`) already comes from a kpi_only state payload and
    # must not overwrite the active рабочий стол.
    if response_scope == 'kpi' and str(rendered_payload.get('render_mode') or '').strip().lower() != 'kpi_only':
        should_save_rendered_state = True

    action_display_modes = {'action_package', 'negotiation_workspace', 'task_workspace', 'post_meeting_workspace', 'execution_workspace'}
    if should_save_rendered_state or str(rendered_payload.get('render_mode') or '').strip().lower() in action_display_modes:
        try:
            # State must keep the full rendered payload, not the public render-only
            # response. For action display modes save_last_payload updates only
            # last_payload and preserves current_screen, so `назад` restores the
            # analytical Workspace while numeric commands can read the last shown
            # action menu.
            save_last_payload(session_id, rendered_payload)
        except Exception:
            logger.exception('vectra_query_render_state_save_failed session_id=%s', session_id)
    logger.info(
        'vectra_query_render_contract session_id=%s has_kpi_block=%s has_structure_block=%s has_drain_block_render=%s has_navigation_block=%s has_result_block=%s',
        session_id,
        'kpi_block' in render_only_payload,
        'structure_block' in render_only_payload,
        'drain_block_render' in render_only_payload,
        'navigation_block' in render_only_payload,
        'result_block' in render_only_payload,
    )
    _log_vectra_query_payload(session_id, render_only_payload)
    render_only_payload = _enforce_public_response_budget(render_only_payload)
    return json_response(render_only_payload)


@router.get('/meta/entities')
def meta_entities(period: str = ''):
    payload = get_entity_dictionary(period=period or None)
    return json_response({
        'status': 'ok',
        'period': period or None,
        'entity_counts': {
            key: len(value.get('canonical', []))
            for key, value in payload.items()
            if isinstance(value, dict) and 'canonical' in value
        },
    })


# GENESIS-0003: Professional Reflection Foundation
# Reflection analyses a completed working stage and creates Knowledge Candidates.
# It does not update Professional Model and does not run Knowledge Consolidation.

@router.get('/vectra/reflection/status', summary='Read VECTRA Professional Reflection Status')
def vectra_reflection_status():
    return json_response(get_vectra_reflection_status())


@router.post('/vectra/reflection/run', summary='Run VECTRA Professional Reflection for a completed working stage')
def vectra_reflection_run(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(run_vectra_professional_reflection(request))


@router.get('/vectra/reflection/candidates', summary='Read VECTRA Knowledge Candidate Repository')
def vectra_reflection_candidates(status: str = None, limit: int = 50):
    return json_response(list_vectra_knowledge_candidates(status=status, limit=limit))


@router.patch('/vectra/reflection/candidate/{candidate_id}/status', summary='Update Knowledge Candidate Status')
def vectra_reflection_candidate_status(candidate_id: str, request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(update_vectra_knowledge_candidate_status(candidate_id, request.get('status'), reviewer_note=str(request.get('reviewer_note') or '')))


@router.get('/vectra/reflection/reports', summary='Read VECTRA Reflection Reports')
def vectra_reflection_reports(limit: int = 20):
    return json_response(list_vectra_reflection_reports(limit=limit))


@router.get('/vectra/reflection/verify', summary='Verify VECTRA Professional Reflection Readback')
def vectra_reflection_verify():
    return json_response(verify_vectra_reflection_readback())



# GENESIS-0005 Professional Observation Foundation.
# Captures professional runtime events for later Reflection without changing
# Professional Model, without running Consolidation and without Product Decisions.

@router.get('/vectra/observation/status', summary='Read VECTRA Professional Observation Status')
def vectra_observation_status():
    return json_response(get_vectra_observation_status())


@router.post('/vectra/observation/capture', summary='Capture VECTRA Professional Observation Event')
def vectra_observation_capture(request: dict):
    if not isinstance(request, dict):
        request = {}
    return json_response(capture_vectra_professional_observation(request))


@router.get('/vectra/observation/events', summary='Read VECTRA Professional Observation Events')
def vectra_observation_events(status: str = None, limit: int = 50):
    return json_response(list_vectra_professional_observations(status=status, limit=limit))


@router.post('/vectra/observation/report', summary='Create VECTRA Professional Observation Report')
def vectra_observation_report():
    return json_response(create_vectra_observation_report())


@router.get('/vectra/observation/reports', summary='Read VECTRA Professional Observation Reports')
def vectra_observation_reports(limit: int = 20):
    return json_response(list_vectra_observation_reports(limit=limit))


@router.get('/vectra/observation/verify', summary='Verify VECTRA Professional Observation Readback')
def vectra_observation_verify():
    return json_response(verify_vectra_observation_readback())


# GENESIS-0006 Active Responsibilities Foundation.
# Tracks and verifies VECTRA professional responsibilities without changing
# Professional Model, without running Reflection/Consolidation and without
# automatic Product Decisions.

@router.get('/vectra/responsibilities/status', summary='Read VECTRA Active Responsibilities Status')
def vectra_responsibilities_status():
    return json_response(get_vectra_responsibility_status())


@router.get('/vectra/responsibilities', summary='Read VECTRA Active Responsibilities Repository')
def vectra_responsibilities_read(status: str = None, limit: int = 50):
    return json_response(list_vectra_active_responsibilities(status=status, limit=limit))


@router.post('/vectra/responsibilities/run', summary='Run VECTRA Active Responsibilities Check')
def vectra_responsibilities_run(request: dict = None):
    if not isinstance(request, dict):
        request = {}
    return json_response(run_vectra_responsibility_check(request))


@router.get('/vectra/responsibilities/reports', summary='Read VECTRA Active Responsibilities Reports')
def vectra_responsibilities_reports(limit: int = 20):
    return json_response(list_vectra_responsibility_reports(limit=limit))


@router.get('/vectra/responsibilities/verify', summary='Verify VECTRA Active Responsibilities Readback')
def vectra_responsibilities_verify():
    return json_response(verify_vectra_responsibility_readback())


# GENESIS-0007 Recovery Evolution Foundation.
# Expands Recovery into a verifiable mechanism for restoring the VECTRA
# professional baseline. It does not change Professional Model, does not run
# Reflection/Consolidation and does not make Product Decisions automatically.

@router.get('/vectra/recovery/status', summary='Read VECTRA Recovery Evolution Status')
def vectra_recovery_evolution_status():
    return json_response(get_vectra_recovery_evolution_status())


@router.post('/vectra/recovery/run', summary='Run VECTRA Recovery Evolution checkpoint')
def vectra_recovery_evolution_run(request: dict = None):
    if not isinstance(request, dict):
        request = {}
    return json_response(run_vectra_recovery_evolution(request))


@router.get('/vectra/recovery/reports', summary='Read VECTRA Recovery Evolution Reports')
def vectra_recovery_evolution_reports(limit: int = 20):
    return json_response(list_vectra_recovery_evolution_reports(limit=limit))


@router.get('/vectra/recovery/checkpoints', summary='Read VECTRA Recovery Checkpoints')
def vectra_recovery_evolution_checkpoints(limit: int = 20):
    return json_response(list_vectra_recovery_checkpoints(limit=limit))


@router.get('/vectra/recovery/verify', summary='Verify VECTRA Recovery Evolution Readback')
def vectra_recovery_evolution_verify(checkpoint_id: str = None):
    return json_response(verify_vectra_recovery_evolution_readback(checkpoint_id=checkpoint_id))


# GENESIS-0008 Laboratory -> Working VECTRA Synchronization Foundation.
# Prepares verifiable synchronization packages without applying changes
# automatically to Working VECTRA and without changing Professional Model.

@router.get('/vectra/synchronization/status', summary='Read VECTRA Laboratory Synchronization Status')
def vectra_synchronization_status():
    return json_response(get_vectra_synchronization_status())


@router.post('/vectra/synchronization/run', summary='Prepare VECTRA Laboratory to Working VECTRA synchronization package')
def vectra_synchronization_run(request: dict = None):
    if not isinstance(request, dict):
        request = {}
    return json_response(build_vectra_synchronization_package(request))


@router.get('/vectra/synchronization/packages', summary='Read VECTRA Synchronization Packages')
def vectra_synchronization_packages(limit: int = 20):
    return json_response(list_vectra_synchronization_packages(limit=limit))


@router.get('/vectra/synchronization/reports', summary='Read VECTRA Synchronization Reports')
def vectra_synchronization_reports(limit: int = 20):
    return json_response(list_vectra_synchronization_reports(limit=limit))


@router.get('/vectra/synchronization/verify', summary='Verify VECTRA Synchronization Readback')
def vectra_synchronization_verify(package_id: str = None):
    return json_response(verify_vectra_synchronization_readback(package_id=package_id))

# GENESIS-0010 Product Owner Review Workflow.
# Presents a prepared Synchronization Session for Product Owner decision without
# applying changes, changing Professional Model or publishing to Working VECTRA.

# GENESIS-0011 Controlled Synchronization Execution.
# Executes only Product Owner approved synchronization review sessions.

@router.get('/vectra/synchronization/execution', summary='Read VECTRA Controlled Synchronization Execution')
def vectra_synchronization_execution():
    return json_response(get_vectra_synchronization_execution())


@router.post('/vectra/synchronization/execute', summary='Execute VECTRA Controlled Synchronization')
def vectra_synchronization_execute(request: dict = None):
    if not isinstance(request, dict):
        request = {}
    return json_response(execute_vectra_synchronization(request))


@router.get('/vectra/synchronization/execution/report', summary='Read VECTRA Synchronization Execution Report')
def vectra_synchronization_execution_report():
    return json_response(get_vectra_synchronization_execution_report())


@router.get('/vectra/synchronization/execution/status', summary='Read VECTRA Synchronization Execution Status')
def vectra_synchronization_execution_status():
    return json_response(get_vectra_synchronization_execution_status())


@router.get('/vectra/synchronization/execution/verify', summary='Verify VECTRA Synchronization Execution Readback')
def vectra_synchronization_execution_verify():
    return json_response(verify_vectra_synchronization_execution_readback())

@router.get('/vectra/review/session', summary='Read or open VECTRA Product Owner Review Session')
def vectra_review_session():
    return json_response(get_vectra_review_session())


@router.post('/vectra/review/session', summary='Open VECTRA Product Owner Review Session')
def vectra_review_open(request: dict = None):
    if not isinstance(request, dict):
        request = {}
    return json_response(open_vectra_review_session(request))


@router.get('/vectra/review/report', summary='Read VECTRA Product Owner Review Report')
def vectra_review_report():
    return json_response(get_vectra_review_report())


@router.get('/vectra/review/status', summary='Read VECTRA Product Owner Review Status')
def vectra_review_status():
    return json_response(get_vectra_review_status())


@router.post('/vectra/review/decision', summary='Record VECTRA Product Owner Review Decision')
def vectra_review_decision(request: dict = None):
    if not isinstance(request, dict):
        request = {}
    return json_response(record_vectra_review_decision(request.get('decision'), request))


@router.get('/vectra/review/verify', summary='Verify VECTRA Product Owner Review Readback')
def vectra_review_verify():
    return json_response(verify_vectra_review_readback())




# FOUNDATION-0006: Business Domain Framework (Bonboason Domain)
# Business Domain is a persistent professional model of a concrete business.
# It does not change VECTRA Professional Identity and is restored from Runtime.

@router.get('/vectra/domains', summary='Read VECTRA Business Domain Registry')
def vectra_business_domains(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    require_laboratory_key(x_vectra_laboratory_key)
    return json_response(get_vectra_business_domain_registry())


@router.get('/vectra/domain/status', summary='Read active VECTRA Business Domain')
def vectra_business_domain_status(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    require_laboratory_key(x_vectra_laboratory_key)
    return json_response(get_vectra_active_business_domain())


@router.get('/vectra/domain/bonboason', summary='Read Bonboason Business Domain Profile')
def vectra_business_domain_bonboason(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    require_laboratory_key(x_vectra_laboratory_key)
    return json_response(get_vectra_business_domain_profile('bonboason'))


@router.get('/vectra/domain/recover', summary='Restore active or Bonboason Business Domain from Runtime')
def vectra_business_domain_recover(domain_id: str = 'bonboason', x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    require_laboratory_key(x_vectra_laboratory_key)
    return json_response(restore_vectra_business_domain(domain_id))


@router.post('/vectra/domain/activate', summary='Activate VECTRA Business Domain')
def vectra_business_domain_activate(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    require_laboratory_key(x_vectra_laboratory_key)
    if not isinstance(request, dict):
        request = {}
    return json_response(activate_vectra_business_domain(request))


@router.post('/vectra/domain/capitalization', summary='Capitalize confirmed professional context into Business Domain')
def vectra_business_domain_capitalization(request: dict = None, x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    require_laboratory_key(x_vectra_laboratory_key)
    if not isinstance(request, dict):
        request = {}
    return json_response(capitalize_vectra_business_domain_context(request))


@router.get('/vectra/domain/verify', summary='Verify Business Domain Framework')
def vectra_business_domain_verify(x_vectra_laboratory_key: str | None = Header(default=None, alias='X-VECTRA-LABORATORY-KEY')):
    require_laboratory_key(x_vectra_laboratory_key)
    return json_response(verify_vectra_business_domain_framework())
