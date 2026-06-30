# VECTRA Sprint W15.0 — Product Acceptance Corrections

Deploy build.

## Main changes

- Context Engine now treats `active_workspace_state` as the primary runtime context when it is explicitly supplied by Custom GPT Actions.
- Free follow-up questions stay inside the opened Workspace until the user explicitly opens another object.
- Runtime Navigation keeps `workspace_action_map` synchronized with the final visible `workspace_markdown` menu.
- `назад` continues to restore saved state without re-running analytical Workspace calculation.
- Historical dynamics are standardized to a 6-month horizon for primary Workspace blocks when history is available.
- Start Screen remains a local entry point and does not call analytical Runtime.
- Negotiation Workspace is guarded against partial responses: it must return complete `workspace_markdown` or an explicit error.

## Production Acceptance focus

Recommended scenarios after deploy:

1. `Начать Анализ` → local start screen.
2. `Покажи Варус 2026-02` → free follow-up: `Что бы ты сделал?`.
3. Contract Workspace → numeric command from visible menu.
4. Contract Workspace → `подготовить переговоры`.
5. Any Workspace → `назад`.
6. Any Workspace → check historical dynamics block and short conclusion.

