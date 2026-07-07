# VECTRA Deploy Package

Deploy package for VECTRA Runtime.

This archive contains only application code and runtime repository files required for deployment.
Release Brief and verification evidence are distributed separately and must not be deployed to Render as application files.

Current increment: FOUNDATION-0011 — Split Laboratory Actions schemas for GPT 30-operation limit.
Base increment: FOUNDATION-0010 — Knowledge Write Access.

## FOUNDATION-0011 — GPT Actions import URLs

Product Owner should import three separate OpenAPI schemas into VECTRA Laboratory GPT Actions:

1. Core
   - `GET /vectra/laboratory/openapi/core.json`
   - version: `FOUNDATION-0011-LABORATORY-CORE`

2. Business Data
   - `GET /vectra/laboratory/openapi/business-data.json`
   - version: `FOUNDATION-0011-BUSINESS-DATA`

3. Knowledge / Self Evolution
   - `GET /vectra/laboratory/openapi/knowledge.json`
   - version: `FOUNDATION-0011-KNOWLEDGE-SELF-EVOLUTION`

Legacy compatibility endpoint:

- `GET /vectra/laboratory/openapi.json`

The legacy endpoint now returns the Core schema only, so it stays below the GPT Actions 30-operation limit.

## Security

Laboratory endpoints remain protected by optional header `X-VECTRA-LABORATORY-KEY` when `VECTRA_LABORATORY_API_KEY` is configured in Runtime.

Business Data and Repository Inspection endpoints are read-only.
Knowledge Capitalization write endpoints require Product Owner approval in the request payload and perform Write → Readback → Verification.
