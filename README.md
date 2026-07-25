# VECTRA Runtime Repository

This repository contains the VECTRA Runtime, persistent Runtime data, automated
engineering tests, and the compact OpenAPI contract used by GPT Actions.

## Runtime entry points

- Application: `app/main.py`
- Runtime API and existing facades: `app/api/routes.py`
- Official GPT Actions schema: `/openapi.json`
- Production Runtime: `https://bon-buasson-api.onrender.com`

## Engineering verification

Run the complete regression suite before every release:

```bash
python -m pytest -q
```

The `tests/` directory is part of the required repository structure and provides
regression protection. The `docs/` directory contains retained verification
reports and other release evidence; it is not an alternative normative source.

## Architecture Registry

`VECTRA-ARCHITECTURE-REGISTRY-001` provides the permanent Architecture Registry
Runtime through the existing Memory facade. It uses AOT v1.0 and AOMM v1.0
without introducing a new public GPT Action.

The public GPT Actions count remains limited to 30 operations.
