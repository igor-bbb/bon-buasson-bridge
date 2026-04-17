# VECTRA — система управления прибыльностью

VECTRA показывает, где теряются деньги и как их вернуть.

## Ядро

DATA → FILTER → AGGREGATION → COMPARISON → DIAGNOSTICS → DECISION → RESPONSE

## Главные правила

- DATA — единственный источник истины
- финансовые показатели не пересчитываются
- `finrez_final` = сумма поля `finrez` из DATA
- business сравнивается с прошлым годом
- все уровни ниже business сравниваются с business
- `delta_prev_year` возвращается отдельным полем и используется только для отображения
- разрыв = `markup - margin_pre`
- дренаж сортируется по `lost_money DESC`
- сигнал квартилей используется только как сигнал, не в расчетах
- решения обязательны с уровня network
- решения считаются по модели v1: `effect = Δ(п.п.) × revenue`

## Предпочтительные endpoint'ы

- `/business_summary`
- `/manager_top_summary`
- `/manager_summary`
- `/network_summary`
- `/sku_summary`
- `/business_reasons`
- `/manager_top_reasons`
- `/manager_reasons`
- `/network_reasons`
- `/sku_reasons`
- `/vectra/query`

## Цель продукта

Путь пользователя:

факт → отклонение → причина → где → решение → эффект

Если экран не отвечает, где теряются деньги и что делать дальше, он не готов.
