# FIX CORE — ВЕТКИ "ВСЕ" + "ПРИЧИНЫ"

Статус: готово.

Что изменено:
- в summary response добавлен `all_block` как полный список объектов уровня, сортировка по `effect_money ASC`
- добавлен `cause_block` как список статей из `structure`, сортировка по `effect_money ASC`
- в `navigation` добавлены флаги `has_all`, `has_causes`, `has_back`
- команды `все` и `причины` больше не уходят в legacy views, а возвращают тот же summary-контракт текущего объекта

Что не изменялось:
- расчеты KPI
- effect
- base_money
- structure
- основной flow

Проверка:
- pytest: 13 passed
