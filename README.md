# VECTRA Stage 8.4 — Network Contract Workspace MVP

Status: COMPLETED

Главное изменение:
- Network (`Покажи Варус`, `Покажи АТБ`) снова является полным рабочим столом контракта, а не коротким Decision-only экраном.
- Сеть трактуется как контракт/клиент, то есть объект бизнеса с контрагентом.
- Пользователь получает полный разбор объекта и свободно выбирает направление дальнейшей работы.
- Ассистент может подсказать приоритетное направление, но не навязывает один обязательный следующий шаг.

Что изменено:
- `screen_order` для уровня `network` теперь возвращает полный порядок блоков:
  - result_block
  - diagnosis_block
  - anomaly_explanation_block
  - factor_change_block
  - benchmark_diagnostic_block
  - opportunity_rating_block
  - opportunity_explanation_block
  - decision_workspace_block
  - decision_block_render
  - navigation_block
- `decision_workspace` переименован по смыслу в `network_contract_workspace` внутри payload.
- `decision_workspace_block` теперь рендерит блок «Рабочий стол контракта»:
  - доступные направления работы;
  - категории в контракте;
  - приоритетное направление ассистента;
  - свободный диалог с ассистентом.
- Старые KPI, факторы, benchmark, opportunity и navigation остаются в полном экране.

Validation:
- `python -m compileall -q app`
- `import app.main`
- FastAPI `/health` via TestClient
- `/network_summary?network=Варус&period=2026-02` returns full Network screen order and `decision_workspace.type = network_contract_workspace`.
