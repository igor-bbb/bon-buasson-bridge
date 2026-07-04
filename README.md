# VECTRA-RUNTIME-0003

## Natural Command Guidance & Readback Verification

Этот релиз добавляет два ключевых эксплуатационных правила:

1. Product Owner не обязан помнить технические команды.
2. Любая память VECTRA должна быть доступна для чтения обратно.

VECTRA принимает естественный запрос, определяет намерение, открывает нужный раздел памяти и показывает результат человеческим языком.

## Главные маршруты

- GET /vectra/memory
- GET /vectra/state
- GET /vectra/evolution-journal
- GET /vectra/decisions
- GET /vectra/knowledge
- GET /vectra/recovery
- GET /vectra/snapshots
- GET /vectra/runtime-reports
- GET /vectra/pending-approvals
- POST /vectra/command-guidance/resolve
- POST /vectra/read
- POST /vectra/verify-write-readback

## Identity Migration

Runtime теперь восстанавливает VECTRA как центральную сущность.
Product Team Assistant является внутренней профессиональной моделью VECTRA, а не отдельным продуктом.
