"""Historical Book Equity data from Ken French's library.

Source: hand-collected book equity values from the Moody's Industrial,
Public Utility, Transportation, and Bank and Finance Manuals (Davis,
Fama & French 2000, J. Finance 55:389).
see: https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/Data_Library/det_historical_be_data.html

Book_Equity(t) is from the year-t Moody's edition, publicly available
by June 30 of year t. Canonical downstream join:
    BE(t) ↔ ME(Dec t-1) ↔ returns(Jul t – Jun t+1)

i.e. do not match BE(1990) to calendar-year-1990 returns.

The wide table is returned in exactly the shape Ken French publishes it
(one row per permno, years as columns). No unpivoting, no alignment, no 
end of period offset.

Returns:
    pa.Table of: permno, first_moody_year, last_moody_year, 1926..2001
"""
from __future__ import annotations
import io
import zipfile
import httpx
import pyarrow as pa

## Just a function, not a child of Base. Print preview won't work for this.

URL = (
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"
    "Historical_BE_Data.zip"
)

YEARS = tuple(range(1926, 2002))
SENTINEL = -99.99
SENTINEL_STR = f"{SENTINEL:.3f}"  # "-99.990", as it appears in the file
YEAR_COLS = tuple(str(y) for y in YEARS)
N_COLS = 3 + len(YEAR_COLS)  # permno + 2 coverage years + 76 BE columns

SCHEMA = pa.schema(
    [
        ("permno", pa.int32()),
        ("first_moody_year", pa.int16()),
        ("last_moody_year", pa.int16()),
    ]
    + [(y, pa.float64()) for y in YEAR_COLS]
)


def _parse_rows(text: str) -> list[list[str]]:
    """Return the file's data rows, right-padded with the sentinel."""
    rows: list[list[str]] = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 3 or not all(p.isdigit() for p in parts[:3]):
            continue  # header / footer / blank
        if len(parts) > N_COLS:
            raise ValueError(f"row overflow: {len(parts)} > {N_COLS} tokens")
        rows.append(parts + [SENTINEL_STR] * (N_COLS - len(parts)))
    return rows


def load_historical_be(url: str = URL) -> pa.Table:
    """Download and parse the Historical BE file into a wide Arrow table."""
    with httpx.Client(follow_redirects=True, timeout=60.0) as client:
        resp = client.get(url)
        resp.raise_for_status()
        archive_bytes = resp.content

    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as z:
        text = z.read(z.namelist()[0]).decode("latin-1")

    cols = list(zip(*_parse_rows(text)))

    fields: dict[str, pa.Array] = {
        "permno": pa.array(cols[0]).cast(pa.int32()),
        "first_moody_year": pa.array(cols[1]).cast(pa.int16()),
        "last_moody_year": pa.array(cols[2]).cast(pa.int16()),
    }
    for name, col in zip(YEAR_COLS, cols[3:]):
        fields[name] = pa.array(
            [None if v == SENTINEL_STR else float(v) for v in col],
            pa.float64(),
        )

    return pa.table(fields, schema=SCHEMA)


if __name__ == "__main__":
    table = load_historical_be()
    print(f"{table.num_rows:,} rows × {table.num_columns} columns")
    assert table.schema == SCHEMA, f"schema drift:\n{table.schema}"
    assert table.num_rows == 1_794, f"unexpected row count: {table.num_rows}"
    print(table.schema)
