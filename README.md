
# VECTRA FULL PROJECT — TZ LOCK

Core flow:
DATA → FILTER → AGGREGATION → METRICS → DRAIN → GOAL → NAVIGATION

Rules:
- DATA is source of truth
- finrez_final = sum(finrez from DATA)
- business compares to previous year
- manager/network/sku compare to business
- delta_prev_year is display-only
- drain sorted by potential_money DESC
- backend returns numbers and contract only
- no signal/headline/summary/diagnosis blocks in API response
