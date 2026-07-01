# VECTRA Stabilization S3 — Release Brief & Release Manager Integration

## Назначение

Релиз переводит инженерные артефакты VECTRA на новый формат сопровождения релиза.

Теперь вместо отдельных документов CHANGE LOG, TEST PLAN и Implementation Report используется единый **Release Brief**.

## Что изменено

- Добавлен `app/release_brief.py`.
- Release Manager принимает Release Brief и выбирает проверочные сценарии.
- Release Manager возвращает Product Owner Report простым языком.
- Scenario Runner возвращает явное подтверждение запуска и завершения.
- Добавлен endpoint `/release-brief/preview`.
- Команда «Проверь последний релиз» запускает Release Manager.

## Основные endpoints

- `POST /release-manager/run`
- `POST /scenario-runner/run`
- `POST /release-brief/preview`
- `GET /test-plan`
- `GET /scenario-library`
- `POST /laboratory/analyze-journal`

## Артефакты релиза

После этого релиза инженерный комплект состоит только из:

1. Deploy ZIP.
2. Release Brief.
3. Instruction — только если изменилась.

Instruction в этом релизе не менялась.


## S2-FIX-002

Release Manager Product Owner Report now includes a mandatory Development Journal Status block. The block is computed automatically from Product Acceptance results and current Development Journal state after the check.
