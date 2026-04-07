# VECTRA CORE v2 MVP

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Env

Copy `.env.example` and set:

- `VECTRA_GOOGLE_SHEET_URL`
- `VECTRA_LOW_VOLUME_THRESHOLD`
- `VECTRA_EMPTY_SKU_LABEL`

## Run

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Tests

```bash
pytest -q
```
