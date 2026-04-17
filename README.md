# VECTRA FINAL LOCK v2

Единый backend contract:

{
  "context": {},
  "metrics": {},
  "drain_block": [],
  "goal": {},
  "focus_block": {},
  "navigation": {}
}

Правила:
- business -> к прошлому году
- manager_top/manager/network/sku -> к business
- delta_prev_year только справочно
- analysis_block/signal/summary/diagnosis/action/impact/priority удалены из response
- полный P&L в metrics
- finrez_final только на business
- retro_bonus всегда расход
- drain сортируется по potential_money DESC
- goal = delta finrez_pre к прошлому году
