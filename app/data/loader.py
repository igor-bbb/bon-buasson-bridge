import requests
from app.config import SHEET_URL

GOOGLE_SHEETS_EDIT_TOKEN = '/edit'
GOOGLE_SHEETS_EXPORT_TOKEN = '/export?format=csv'

# 🔴 CACHE
CSV_TEXT_CACHE = None


def normalize_sheet_url(url: str) -> str:
    if GOOGLE_SHEETS_EDIT_TOKEN in url and 'format=csv' not in url:
        return url.replace(GOOGLE_SHEETS_EDIT_TOKEN, GOOGLE_SHEETS_EXPORT_TOKEN)
    return url


def get_csv_text() -> str:
    global CSV_TEXT_CACHE

    # 🔴 ЕСЛИ УЖЕ ЗАГРУЖАЛИ — ВЕРНУТЬ ИЗ CACHE
    if CSV_TEXT_CACHE is not None:
        return CSV_TEXT_CACHE

    if not SHEET_URL:
        raise ValueError('VECTRA_GOOGLE_SHEET_URL is empty')

    url = normalize_sheet_url(SHEET_URL)

    response = requests.get(url)
    response.raise_for_status()

    CSV_TEXT_CACHE = response.content.decode("utf-8")

    return CSV_TEXT_CACHE
