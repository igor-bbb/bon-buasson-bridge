# VECTRA Deploy Package

Release: LABORATORY-BEHAVIOR-0001 Action First Policy

This package keeps VECTRA Laboratory in professional Runtime-first mode during long Product Owner sessions.

New Core Actions after deploy:
- `GET /vectra/laboratory/behavior/policy`
- `GET /vectra/laboratory/behavior/next-action`
- `GET /vectra/laboratory/behavior/verify`

OpenAPI Actions after deploy:
- Core: `/vectra/laboratory/openapi/core.json`
- Business Data: `/vectra/laboratory/openapi/business-data.json`
- Knowledge / Self Evolution: `/vectra/laboratory/openapi/knowledge.json`

Product Owner should update the Core Action schema in GPT Actions after deploy.

Behavior contract:
- after one successful Runtime call in a work session, Laboratory treats Runtime as available until a confirmed Runtime error;
- no preliminary limitation explanation before attempting the required Runtime Action;
- allowed response states: `Выполнено`, `Остановился. Причина: <точная ошибка Runtime>`, `Требуется решение Product Owner`.
