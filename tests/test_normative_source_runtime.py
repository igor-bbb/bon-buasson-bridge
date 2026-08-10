from __future__ import annotations

from app.api import routes
from app.assistant_runtime.architecture_registry_runtime import execute_architecture_registry_operation
from app.assistant_runtime.laboratory_self_audit_integrity import run_laboratory_self_audit
from app.assistant_runtime.normative_source_runtime import (
    get_normative_source,
    trace_normative_usage,
    verify_normative_sources,
)


def test_all_three_canonical_sources_have_verified_exact_content():
    result = verify_normative_sources()

    assert result["status"] == "PASS"
    assert result["verification_status"] == "PASS"
    assert result["expected_sources_count"] == 3
    assert result["verified_sources_count"] == 3
    assert result["canonical_content_verified"] is True
    assert all(item["content_integrity_verified"] for item in result["sources"])


def test_full_content_and_declared_metadata_are_readable():
    result = get_normative_source({"source_id": "VECTRA-MASTER-ARCHITECTURE"})

    assert result["status"] == "PASS"
    assert result["declared_version"] == "Master.md v0.1"
    assert result["declared_status"] == "Рабочая сборка"
    assert result["filename_version"] == "v29"
    assert result["metadata_conflict"]
    assert result["content"].startswith("# VECTRA MASTER ARCHITECTURE")
    assert result["returned_content_length"] == 12000
    assert result["next_offset"] == 12000
    assert result["has_more"] is True
    assert result["complete_content_returned"] is False


def test_large_source_can_be_read_to_completion_in_bounded_chunks():
    offset = 0
    chunks = []
    expected_sha256 = None

    while True:
        result = get_normative_source({
            "source_id": "VECTRA-MASTER-ARCHITECTURE",
            "offset": offset,
            "max_chars": 16000,
        })
        assert result["status"] == "PASS"
        assert result["content_offset"] == offset
        assert result["returned_content_length"] <= 16000
        assert result["content_integrity_verified"] is True
        expected_sha256 = expected_sha256 or result["sha256"]
        assert result["sha256"] == expected_sha256
        chunks.append(result["content"])
        if not result["has_more"]:
            assert result["next_offset"] is None
            break
        offset = result["next_offset"]

    content = "".join(chunks)
    import hashlib
    assert len(content) == result["content_length"]
    assert hashlib.sha256(content.encode("utf-8")).hexdigest() == expected_sha256


def test_normative_source_chunk_range_is_validated():
    invalid = get_normative_source({
        "source_id": "VECTRA-MASTER-ARCHITECTURE",
        "offset": 0,
        "max_chars": 16001,
    })

    assert invalid["status"] == "FAIL"
    assert invalid["failure_reason"] == "normative_source_read_range_invalid"


def test_usage_trace_requires_an_actual_section_in_canonical_content():
    passed = trace_normative_usage({
        "source_id": "VECTRA-ARCHITECTURAL-CONSTITUTION",
        "reasoning_operation": "architecture_review",
        "section": "Статья 7. Прослеживаемость",
    })
    failed = trace_normative_usage({
        "source_id": "VECTRA-ARCHITECTURAL-CONSTITUTION",
        "reasoning_operation": "architecture_review",
        "section": "Несуществующая статья",
    })

    assert passed["status"] == "PASS"
    assert passed["usage_confirmed"] is True
    assert failed["status"] == "FAIL"
    assert failed["failure_reason"] == "normative_section_not_found"


def test_existing_architecture_registry_facade_publishes_normative_reads():
    result = execute_architecture_registry_operation("list_normative_sources", {})
    schema = routes._memory_facade_operation_request_schema()
    operations = schema["properties"]["operation_type"]["enum"]

    assert result["status"] == "PASS"
    assert "list_normative_sources" in operations
    assert "get_normative_source" in operations
    assert "verify_normative_sources" in operations
    assert "trace_normative_usage" in operations


def test_self_audit_closes_normative_content_gap_without_mutation():
    result = run_laboratory_self_audit()

    assert result["status"] == "PASS"
    assert result["normative_documents"]["canonical_content_verified"] is True
    assert result["normative_documents"]["architecture_registry_linked"] is True
    assert result["normative_documents"]["evidence_status"] == "CANONICAL_CONTENT_VERIFIED"
    assert "SELF-AUDIT-NORMATIVE-CONTENT-001" not in {
        item["gap_id"] for item in result["unconfirmed_gaps"]
    }
