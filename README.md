# VECTRA Stage 8.1 — Decision Workspace Contract

Status: COMPLETED

Included changes:
- Added `decision_workspace` contract for Network level.
- Network now exposes Stage 8 Decision Engine structure without removing current Object Screen compatibility.
- Added API/render blocks:
  - `decision_workspace`
  - `decision_workspace_block`
- Decision Workspace currently includes:
  - `contract_diagnostics`
  - `product_diagnostics`
  - `category_intelligence`
  - `assortment_analysis` placeholder
  - `recommended_actions`
  - `evidence`
- Product Layer remains available as evidence/detailing mode.

Validation:
- `python -m compileall -q app`
- `import app.main`
- FastAPI `/health` via TestClient
