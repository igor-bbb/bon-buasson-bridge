# VECTRA — GLOBAL BACKEND LOCK v2

## Основа
Summary endpoints работают независимо от comparison.

## Summary contract
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

## Правила
- business -> к ПГ
- manager_top / manager / network / sku -> к business
- finrez_final только для business
- retro_bonus = расход
- gap = markup - margin_pre
- drain_block сортируется по potential_money DESC
- delta_prev_year только как справочный слой
- backend не отдает signal, summary, diagnosis, action и любые текстовые выводы

VECTRA package cleaned: removed __pycache__ and .pyc files.
