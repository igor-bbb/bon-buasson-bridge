import requests
from app.config import SHEET_URL

CSV_TEXT_CACHE = None


def get_csv_text() -> str:
    global CSV_TEXT_CACHE

    if CSV_TEXT_CACHE is not None:
        return CSV_TEXT_CACHE

    if not SHEET_URL:
        raise ValueError("SHEET_URL is empty")

    url = SHEET_URL

    # 🔴 ГАРАНТИЯ CSV
    if "/edit" in url:
        url = url.replace("/edit", "/export?format=csv")

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Failed to load sheet: {response.status_code}")

    text = response.text

    # 🔴 ПРОВЕРКА: НЕ HTML ЛИ ЭТО
    if "<html" in text.lower():
        raise Exception("Google Sheet is not публичный или ссылка неверная")

    CSV_TEXT_CACHE = text

    return CSV_TEXT_CACHE
