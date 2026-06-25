# VECTRA Stage 8.2 — Network Decision Workspace Render

Status: COMPLETED

Included changes:
- Stage 8.1 `decision_workspace` contract preserved.
- Network level is now rendered as Decision Workspace, not as a normal Object Screen.
- `screen_order` for Network with `decision_workspace` is now `['decision_workspace_block']`.
- Old KPI, factors, benchmark, opportunity and navigation data remain available in API payload for compatibility and evidence mode.
- `decision_workspace_block` expanded into a full user-facing workspace:
  - contract header
  - main question
  - contract diagnosis
  - Contract Diagnostics
  - Product Diagnostics
  - Category Intelligence
  - potential profit
  - recommended actions
  - evidence
- Domain contract stage updated from `8.1` to `8.2`.

Validation:
- `python -m compileall -q app`
- `import app.main`
- FastAPI `/health` via TestClient
- `/network_summary?network=Варус&period=2026-02` returns `screen_order = ['decision_workspace_block']`
