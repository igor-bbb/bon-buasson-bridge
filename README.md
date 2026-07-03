# VECTRA DEV-0011A — Professional Activity Work Planner

This deploy package adds the first layer of the Professional Activity Engine.

## Main addition

- `app/self_evolution/work_planner.py`

## New endpoints

- `GET /self-evolution/activity/plan`
- `POST /self-evolution/activity/plan`

## Updated endpoint

- `GET /self-evolution/recover` now includes `professional_activity_plan`.

## Release cleanup

Tests, temporary files and Python caches are not included in this deploy package.
