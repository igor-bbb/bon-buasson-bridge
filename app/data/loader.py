import os

import requests
from app.config import SHEET_URL

GOOGLE_SHEETS_EDIT_TOKEN = '/edit'
GOOGLE_SHEETS_EXPORT_TOKEN = '/export?format=csv'

# 🔴 CACHE
CSV_TEXT_CACHE = None
CSV_SOURCE_URL = None


def normalize_sheet_url(url: str) -> str:
    if GOOGLE_SHEETS_EDIT_TOKEN in url and 'format=csv' not in url:
        return url.replace(GOOGLE_SHEETS_EDIT_TOKEN, GOOGLE_SHEETS_EXPORT_TOKEN)
    return url


def get_sheet_url() -> str:
    return os.getenv('VECTRA_GOOGLE_SHEET_URL') or SHEET_URL


def get_csv_text() -> str:
    global CSV_TEXT_CACHE, CSV_SOURCE_URL

    sheet_url = get_sheet_url()

    # 🔴 ЕСЛИ УЖЕ ЗАГРУЖАЛИ И URL НЕ ИЗМЕНИЛСЯ — ВЕРНУТЬ ИЗ CACHE
    if CSV_TEXT_CACHE is not None and CSV_SOURCE_URL == sheet_url:
        return CSV_TEXT_CACHE

    if not sheet_url:
        raise ValueError('VECTRA_GOOGLE_SHEET_URL is empty')

    url = normalize_sheet_url(sheet_url)

    response = requests.get(url)
    response.raise_for_status()

    CSV_TEXT_CACHE = response.content.decode("utf-8-sig")
    CSV_SOURCE_URL = sheet_url

    return CSV_TEXT_CACHE
