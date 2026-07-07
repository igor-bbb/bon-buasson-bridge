# VECTRA Deploy Package

Release: LABORATORY-ACTIONS-0003 Facade Laboratory API

This package introduces a compact professional facade API for VECTRA Laboratory.

## Official GPT Actions OpenAPI

Product Owner imports only one URL after deploy:

`/vectra/laboratory/openapi.json`

The official schema contains 12 public GPT Actions, below the GPT Actions 30-operation limit.

## Facade Actions

- `getVectraRuntimeStatus`
- `restoreVectraLaboratoryState`
- `verifyVectraRuntime`
- `getVectraCapabilities`
- `getVectraActionManifest`
- `verifyVectraActionCompleteness`
- `executeVectraKnowledgeOperation`
- `executeVectraBusinessDomainOperation`
- `executeVectraBusinessDataOperation`
- `executeVectraProductReviewOperation`
- `executeVectraRepositoryOperation`
- `determineVectraLaboratoryNextAction`

## Diagnostic / Legacy OpenAPI exports

These URLs remain available for diagnostics and backward compatibility only:

- `/vectra/laboratory/openapi/core.json`
- `/vectra/laboratory/openapi/business-data.json`
- `/vectra/laboratory/openapi/knowledge.json`

They are not the main GPT import path.

## Action Manifest

`GET /vectra/laboratory/actions/manifest` returns:

- public facade actions;
- internal Runtime operations;
- capability mapping;
- Runtime service;
- internal endpoint;
- export status;
- approval requirement;
- access mode;
- release version.

## Completeness Verification

`GET /vectra/laboratory/actions/verify` checks:

Runtime Capability Registry ↔ Facade Actions ↔ Internal Runtime Services.

Expected result:

`✅ Комплектация полная.`

## Safety

Write/capitalization operations through facade require `product_owner_approval = true`.

The facade preserves Action First Policy:

1. Runtime Action first.
2. Conclusion after Runtime response.
3. No Product Owner endpoint selection required.
