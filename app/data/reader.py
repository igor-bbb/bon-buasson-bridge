import csv
from io import StringIO
from typing import Any, Dict, Iterator, Optional

from app.data.loader import get_csv_text
from app.domain.normalization import clean_row_keys


def build_reader(csv_text: str) -> csv.DictReader:
    sample = csv_text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=';,	')
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ';' if sample.count(';') >= sample.count(',') else ','

    return csv.DictReader(StringIO(csv_text), delimiter=delimiter)


def iter_raw_rows(limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    csv_text = get_csv_text()
    reader = build_reader(csv_text)

    for i, row in enumerate(reader):
        yield clean_row_keys(row)

        if limit is not None and i + 1 >= limit:
            break
