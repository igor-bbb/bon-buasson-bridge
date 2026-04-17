
# VECTRA FULL PROJECT — GLOBAL BACKEND LOCK v2

Core response contract for summary endpoints:

```json
{
  "context": {},
  "metrics": {},
  "drain_block": [],
  "goal": {},
  "focus_block": {},
  "navigation": {}
}
```

Rules:
- business compares to previous year
- manager_top / manager / network / sku compare to business
- previous year below business is informational only
- finrez_final only on business
- retro_bonus is always a cost
- gap = markup - margin_pre
- drain sorted by potential_money DESC
- no signal / summary / headline / diagnosis blocks in summary response
- backend returns numbers only
