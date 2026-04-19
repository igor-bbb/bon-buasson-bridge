# TZ3 package notes

Base package: `bon-buasson-bridge-main (4).zip`

Applied in this package:
- KPI contract moved to previous year base in `app/domain/summary.py`
- Structure contract unified with `value_money`, `value_percent`, `base_percent`, `delta_percent`, `effect_money`
- Expense signs normalized negative for retro/logistics/personnel/other
- Goal contract now returns `value_money`, `type`, `goal_label`
- Drain block sorted by `effect_money` ascending and limited to top-3
- Network summary now exposes `focus_block` and `decision_block`
- Public summary routes now expose `focus_block` and `decision_block`

Smoke check completed:
- Python compilation passed for patched modules
- Mock data smoke run passed for business / manager / network summaries

Not fully verified in this container:
- live Google Sheet backed runtime flow
- full stateful `/vectra/query` navigation under production data
