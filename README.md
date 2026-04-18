# VECTRA Backend (TZ Lock Rebuilt)

Vectra — система управления прибыльностью.

Архитектура:
DATA → FILTER → AGG → Δ → DRAIN → REASONS → DECISION

Ключевые правила:
- DATA — единственный источник истины
- Финансы не пересчитываются
- business сравнивается с ПГ
- manager/network/sku сравниваются с business
- delta_prev_year возвращается отдельным полем и используется только для отображения
- drain сортируется по potential_money DESC
- signal/kpi_zone не отдаются наружу
- решения обязательны с уровня network
