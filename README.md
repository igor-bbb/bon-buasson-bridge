# VECTRA Sprint 11 — Scoped Component Response Guard

Status: COMPLETED

Главное изменение:
- Усилен scoped rendering для `kpi`, `все`, `причины`.
- Добавлен локальный KPI-only режим поверх текущего Workspace.
- Исправлено состояние после `Покажи <объект> KPI`: следующий запрос `причины` снова работает по тому же объекту.
- Showcase получил компактный `priority_signal`.
- Добавлен API response budget guard против oversized payload / ResponseTooLargeError.

Validation:
- `python -m compileall -q app`
- `import app.main`
- FastAPI TestClient:
  - `Бизнес 2026-02 → все → 1 → все → 1 → все → 1`
  - `Покажи Варус 2026-02 → Покажи Варус KPI → причины → назад → kpi → назад`
  - `Покажи АТБ 2026-02 → Покажи АТБ KPI → все`

Deploy notes:
- Deploy-ready ZIP не содержит CHANGE LOG и TEST PLAN внутри проекта.
- DATA файл не включён в deploy package; приложение продолжает использовать `VECTRA_GOOGLE_SHEET_URL`.
