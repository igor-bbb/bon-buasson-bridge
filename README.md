# VECTRA Runtime 0001 — Assistant Runtime Repository Foundation

This deploy package is based on DOP-0005R and adds the first operational VECTRA internal runtime repository for Product Team Assistant.

## What changed

VECTRA now contains an `assistant_repository/` workspace and API endpoints for:

- assistant recovery;
- assistant state;
- evolution journal;
- knowledge documents;
- product decisions;
- recovery snapshots;
- runtime repository status.

## Main principle

VECTRA is the digital organization. The Assistant Runtime Repository is an internal VECTRA service, not a separate platform.

ChatGPT remains the intelligent interface. VECTRA stores the professional memory.

## Key endpoints

- `GET /assistant/repository`
- `GET /assistant/recovery`
- `GET /assistant/state`
- `POST /assistant/state`
- `GET /assistant/runtime`
- `GET /assistant/evolution-journal`
- `POST /assistant/journal`
- `POST /assistant/evolution`
- `GET /assistant/knowledge`
- `POST /assistant/knowledge`
- `PATCH /assistant/knowledge/{document_id}`
- `POST /assistant/decision`
- `POST /assistant/snapshot`

## Storage note

The default implementation is file-based. For durable persistence across redeploys, configure `VECTRA_ASSISTANT_REPOSITORY_PATH` to a persistent disk or replace the storage adapter with Git-backed/database persistence.

## VECTRA-RUNTIME-0002 — Runtime Execution & Transparent Control

This release turns Assistant Runtime Repository into an active execution layer.
After a confirmed Product Acceptance or work-shift closure, VECTRA can now:

- classify the event;
- analyze which internal objects are affected;
- safely update journals, working state, event queue and recovery snapshots;
- prepare Product Owner approvals for standards, methodology and product-model changes;
- generate a human-readable control report for Product Owner.

New endpoints:

- `GET /assistant/runtime-execution/model`
- `POST /assistant/runtime-execution/run`
- `GET /assistant/runtime-execution/reports`
- `GET /assistant/runtime-execution/pending-approvals`
- `POST /assistant/work-shift/start`
- `POST /assistant/work-shift/close`

Core operating principle:

> Automation removes manual execution, not Product Owner control.

Product Owner receives human-language reports; internal services keep technical details inside VECTRA.
