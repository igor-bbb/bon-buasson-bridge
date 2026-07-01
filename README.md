# VECTRA API — Sprint W15.1

Deploy package for Sprint W15.1 — Runtime Navigation & Workspace Transition.

## Included

- FastAPI application code in `app/`
- Runtime Navigation corrections
- Workspace transition fixes for Contract → Negotiation → SKU Package → Task → Execution
- Development Journal independence preserved
- `requirements.txt`
- `run.sh`

## W15.1 scope

- Numeric commands now resolve against the last displayed Workspace menu, including action Workspaces.
- Action Workspaces are saved as `last_payload` while preserving the analytical `current_screen` for `назад` and data-dependent actions.
- Added explicit Execution Workspace.
- Negotiation, SKU Package, Task, Post Meeting and Execution actions return complete `workspace_markdown`.
- Action Workspace menus are exposed through a visible `Action Zone` and rebuilt into `workspace_action_map`.
- `назад` restores the analytical Workspace snapshot without recalculation or KPI corruption.
- Development Journal routes remain independent from Workspace Runtime.
- `/vectra/query` can hydrate runtime context from explicit Custom GPT fields: `active_workspace_state`, `workspace_action_map`, `runtime_context`.

## Deploy

Use the existing Render/FastAPI deployment process.

Start command example:

```bash
./run.sh
```
