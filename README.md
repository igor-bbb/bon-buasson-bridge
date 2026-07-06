# VECTRA GENESIS Deploy Package

Deploy package for VECTRA Runtime.

This archive contains the application code and runtime repository required for deployment.
Release Brief documents are intentionally distributed separately and must not be deployed to Render as application files.

Current increment: GENESIS-0010 — Product Owner Review Workflow.
Base increment: GENESIS-0009 — Controlled Synchronization Session.

## FOUNDATION-0002 — Direct Runtime Verification Access

VECTRA Laboratory can be connected through a Custom GPT Action using `openapi_laboratory_actions.yaml`.

Required runtime endpoints:

- `GET /vectra/runtime/status`
- `GET /vectra/runtime/snapshot`
- `GET /vectra/laboratory/verification`
- `GET /vectra/professional/model`
- `GET /vectra/evolution/status`

Optional protection: set `VECTRA_LABORATORY_API_KEY` in Runtime environment and configure the same value in the Custom GPT Action as header `X-VECTRA-LABORATORY-KEY`.
