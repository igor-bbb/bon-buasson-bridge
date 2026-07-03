# VECTRA DEV-0011C — Dependency Manager

Deploy-ready package for VECTRA Professional Activity Engine.

## Release purpose

DEV-0011C adds Dependency Manager on top of DEV-0011B Value & Priority Engine.

Product Team Assistant now evaluates professional work not only by value and priority, but also by dependencies, blockers, consolidation opportunities and dynamic replanning needs.

## Added components

- `app/self_evolution/dependency_manager.py`
- `GET /self-evolution/activity/dependencies`
- `POST /self-evolution/activity/dependencies`

## Updated components

- `app/self_evolution/work_planner.py`
- `app/self_evolution/state_manager.py`
- `app/self_evolution/repository.py`
- `app/api/routes.py`

## Engineering note

Repository remains infrastructure. Product Team Assistant professional state remains the center of Self Evolution and Professional Activity planning.
