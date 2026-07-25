import csv
from io import StringIO
from typing import Any, Dict, Iterator, Optional, List

from app.data.loader import get_csv_text
from app.domain.normalization import clean_row_keys

# 🔴 CACHE RAW ROWS
RAW_ROWS_CACHE: Optional[List[Dict[str, Any]]] = None


def build_reader(csv_text: str) -> csv.DictReader:
    sample = csv_text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,")
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ";" if sample.count(";") >= sample.count(",") else ","

    return csv.DictReader(StringIO(csv_text), delimiter=delimiter)


def load_raw_rows() -> List[Dict[str, Any]]:
    global RAW_ROWS_CACHE

    if RAW_ROWS_CACHE is None:
        csv_text = get_csv_text()
        reader = build_reader(csv_text)

        rows: List[Dict[str, Any]] = []
        for row in reader:
            rows.append(clean_row_keys(row))

        RAW_ROWS_CACHE = rows

    return RAW_ROWS_CACHE


def iter_raw_rows(limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    rows = load_raw_rows()

    if limit is None:
        for row in rows:
            yield row
    else:
        for row in rows[:limit]:
            yield row
