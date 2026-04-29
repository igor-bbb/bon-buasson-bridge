# VECTRA v5 — State Navigation / Product Flow Fix

Scope: technical mode only. No business formulas changed.

Fixed:
- UI commands (`все`, `причины`, `назад`) no longer re-enter raw summary normalization when the active state already contains a rendered screen.
- `все` is treated as navigation-only: full list from `all_block`, no KPI/structure/decision screen.
- `причины` is treated as explanation mode for the current object. It uses `reasons_block_render` / `reasons_block`; fallback to current structure lines prevents fake zero screens.
- Full-list selection updates `last_list_items`, so choosing a number after `все` continues the same vertical path.
- `/vectra/query` now detects render-ready payloads and returns them as-is instead of forcing `public_summary()` again.

Product logic locked:
- manager_top / manager: управленческий выбор направления.
- network: развилка contract reasons vs price drilldown.
- category / tmc_group / sku: price/assortment mode only.
