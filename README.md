# VECTRA Sprint 8.3 — Product Intelligence Engine

Status: COMPLETED

Главное изменение:
- Добавлен Product Intelligence Engine поверх существующей DATA.
- Реализованы DATA-only слои: Business Opportunity Engine, Recommendation Engine, Narrative Engine v2 и Product Workspace v2.
- /vectra/query теперь сохраняет Product Intelligence блоки при открытии объектов свободным запросом.
- Reasons и list-only режимы не смешиваются с ассистентским Product Intelligence экраном.

Validation:
- `python -m compileall -q app`
- `import app.main`
- FastAPI `/health` via TestClient
- `/business_summary?period=2026-02`
- `/network_summary?network=Варус&period=2026-02`
- `/network_summary?network=АТБ&period=2026-02`
- `/sku_summary?sku=Напій Бон Буассон «Лимонад» 2л&period=2026-02`
- `/vectra/query`: `Покажи Варус 2026-02`, `причины`, `назад`, `все`

Deploy notes:
- Deploy-ready ZIP не содержит CHANGE LOG и TEST PLAN внутри проекта.
- DATA файл не включён в deploy package; приложение продолжает использовать `VECTRA_GOOGLE_SHEET_URL`.
