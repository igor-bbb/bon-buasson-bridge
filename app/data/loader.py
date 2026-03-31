import requests

from app.config import SHEET_URL


def get_csv_text() -> str:
    if not SHEET_URL:
        raise ValueError("VECTRA_GOOGLE_SHEET_URL is empty")

    response = requests.get(SHEET_URL, timeout=60)
    response.raise_for_status()

    return response.content.decode("utf-8-sig", errors="replace")
