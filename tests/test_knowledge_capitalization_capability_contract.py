import json
from pathlib import Path


REGISTRY_PATH = (
    Path(__file__).resolve().parents[1]
    / "assistant_repository"
    / "runtime"
    / "capabilities"
    / "capability_registry.json"
)

EXPECTED_CAPABILITIES = {
    "knowledge_candidate_creation": {
        "runtime_service": "knowledge_capitalization.create_knowledge_candidate",
        "transport_endpoint": "/vectra/knowledge/candidates",
        "product_owner_approval_required": False,
    },
    "knowledge_capitalization_package": {
        "runtime_service": "knowledge_capitalization.create_capitalization_package",
        "transport_endpoint": "/vectra/knowledge/capitalization/packages",
        "product_owner_approval_required": True,
    },
    "knowledge_capitalization_write": {
        "runtime_service": "knowledge_capitalization.write_confirmed_knowledge",
        "transport_endpoint": "/vectra/knowledge/capitalization/write",
        "product_owner_approval_required": True,
    },
    "knowledge_capitalization_status": {
        "runtime_service": "knowledge_capitalization.get_knowledge_capitalization_status",
        "transport_endpoint": "/vectra/knowledge/capitalization/status",
        "product_owner_approval_required": False,
    },
}


def _load_capabilities() -> dict[str, dict]:
    registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    return {
        capability["capability_id"]: capability
        for capability in registry["capabilities"]
    }


def test_complete_knowledge_capitalization_contract_is_published() -> None:
    capabilities = _load_capabilities()

    missing = EXPECTED_CAPABILITIES.keys() - capabilities.keys()
    assert not missing, f"Missing Knowledge Capitalization capabilities: {sorted(missing)}"

    for capability_id, expected in EXPECTED_CAPABILITIES.items():
        capability = capabilities[capability_id]
        assert capability["status"] == "active"
        assert capability["maturity_level"] == "Production"
        assert capability["runtime_service"] == expected["runtime_service"]
        assert capability["transport_endpoint"] == expected["transport_endpoint"]
        assert (
            capability["product_owner_approval_required"]
            is expected["product_owner_approval_required"]
        )


def test_capability_ids_are_unique() -> None:
    registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    capability_ids = [item["capability_id"] for item in registry["capabilities"]]

    assert len(capability_ids) == len(set(capability_ids)), (
        "Capability Registry contains duplicate capability_id values"
    )
