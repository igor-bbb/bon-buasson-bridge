import csv
from io import StringIO
from typing import Any, Dict, Iterator, Optional

from app.data.loader import get_csv_text
from app.domain.normalization import clean_row_keys


def build_reader(csv_text: str) -> csv.DictReader:
    reader_semicolon = csv.DictReader(StringIO(csv_text), delimiter=";")
    first_row = next(reader_semicolon, None)

    if first_row is not None and len(list(first_row.keys())) > 1:
        return csv.DictReader(StringIO(csv_text), delimiter=";")

    return csv.DictReader(StringIO(csv_text), delimiter=",")


def iter_raw_rows(limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    csv_text = get_csv_text()
    reader = build_reader(csv_text)

    for i, row in enumerate(reader):
        yield clean_row_keys(row)

        if limit is not None and i + 1 >= limit:
            break
