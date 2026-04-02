import requests

from app.config import SHEET_URL


GOOGLE_SHEETS_EDIT_TOKEN = '/edit'
GOOGLE_SHEETS_EXPORT_TOKEN = '/export?format=csv'


def normalize_sheet_url(url: str) -> str:
    if GOOGLE_SHEETS_EDIT_TOKEN in url and 'format=csv' not in url:
        return url.replace(GOOGLE_SHEETS_EDIT_TOKEN, GOOGLE_SHEETS_EXPORT_TOKEN)
    return url


def get_csv_text() -> str:
    if not SHEET_URL:
        raise ValueError('VECTRA_GOOGLE_SHEET_URL is empty')

    url = normalize_sheet_url(SHEET_URL)
    response = requests.get(
        url,
        timeout=60,
        headers={'Accept': 'text/csv, text/plain;q=0.9, */*;q=0.8'},
    )
    response.raise_for_status()

    for encoding in ('utf-8-sig', response.encoding, 'utf-8', 'cp1251'):
        if not encoding:
            continue
        try:
            return response.content.decode(encoding, errors='replace')
        except Exception:
            continue

    return response.text
