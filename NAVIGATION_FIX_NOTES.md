FIX-PACK — NAVIGATION (ALL + REASONS)

Изменено:
- app/query/orchestration.py

Что сделано:
- команда "все" возвращает текущий contract с drain_block = all_block
- команда "причины" возвращает текущий contract с drain_block по cause_block
- числовой выбор в ветке "причины" фильтрует объекты следующего уровня по выбранной статье
- back восстанавливает предыдущий экран через stack/session state
- нумерация не смешана с текстовыми командами

Что не менялось:
- KPI
- structure
- effect
- base_money
- расчеты
