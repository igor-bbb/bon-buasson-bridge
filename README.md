# VECTRA DEV-0011B — Value & Priority Engine

Deploy-ready FastAPI package.

## Release goal

DEV-0011B extends Professional Activity Engine. Product Team Assistant now evaluates professional work blocks by expected product value, not only by queue order or status priority.

## Added

- `app/self_evolution/value_priority_engine.py`
- value-aware enrichment of Professional Activity Plan
- persistent `value_priority_engine` state inside Assistant State Manager
- `GET /self-evolution/activity/value-priority`
- `POST /self-evolution/activity/value-priority`

## Principle

Product Team Assistant must choose the next professional work block by expected value for product development, architectural risk reduction, Product Owner value, continuity of the Assistant model, dependency impact and digital organization maturity.

## Release cleanup

Deploy ZIP excludes temporary folders, caches and test directories.
