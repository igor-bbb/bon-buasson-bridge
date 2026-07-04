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
