#!/usr/bin/env python3
# getfactormodels: A Python package to retrieve financial factor model data.
# Copyright (C) 2025 S. Martin <x512@pm.me>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import pyarrow as pa
import pyarrow.compute as pc
import math
import sys
"""Utilities for working with pyarrow."""


def scale_to_decimal(table: pa.Table) -> pa.Table:
    """Standardize float cols to decimal (5.2 -> 0.052)."""
    for i, field in enumerate(table.schema):
        if pa.types.is_floating(field.type):
            scaled_col = pc.divide(table.column(i), 100.0)
            table = table.set_column(i, field.name, scaled_col)
    return table


def round_to_precision(table: pa.Table, precision: int) -> pa.Table:
    """Rounds all float cols in a pa.Table to precision, and RF to 4.
    - Attempts to get the _precision from the model
    - Every model uses this
    """
    # auto resolve to: precision arg, else model's ._precision or '6'.
    prec_val = precision or getattr(table, '_precision', 6)
    rf_cols = {'RF', 'R_F', 'AQR_RF', 'RF_AQR'} # TODO: fix this up.

    new_cols = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_floating(field.type):
            p = 4 if field.name.upper() in rf_cols else prec_val
            col = pc.round(col, p)
        new_cols.append(col)

    return pa.Table.from_arrays(new_cols, schema=table.schema)


def rearrange_columns(table: pa.Table) -> pa.Table:
    """Internal helper: Standardize column orders.
    * Always returns: 'date', [FACTORS...], 'RF'
    """
    cols = table.column_names
    front = [c for c in ['date', 'Mkt-RF'] if c in cols]
    back = [c for c in ['RF', 'AQR_RF'] if c in cols]
    fixed = set(front + back) #fix: don't define set in the loop! Now list comp is O(1)
    mid = [c for c in cols if c not in fixed]
    return table.select(front + mid + back)


def filter_table_by_date(table: pa.Table,
                         start: str | None,
                         end: str | None) -> pa.Table:
    """Slices a table to a start-end range."""
    if start is None and end is None:
        return table

    # target the date col and its type
    date_col = 'date' if 'date' in table.column_names else table.column_names[0]
    target_type = table.schema.field(date_col).type
    expr = pc.field(date_col)

    mask = None
    if start:
        mask = (expr >= pc.scalar(start).cast(target_type))

    if end:
        end_mask = (expr <= pc.scalar(end).cast(target_type))
        mask = (mask & end_mask) if mask is not None else end_mask

    return table.filter(mask)


def verify_cols_exist(table: pa.Table, names: str | list[str] | None) -> list[str]:
    """
    Private helper: Checks if columns exist in a pyarrow Table by name.
    Returns the list of valid factors or raises if missing.
    """
    if table.num_rows == 0:
        raise RuntimeError("Table is empty.")

    if not names: return []

    input_list = [names] if isinstance(names, str) else list(names)

    cols = set(table.column_names)
    missing = [f for f in input_list if f not in cols]

    if missing:
        raise ValueError(f"Columns not found in Table: {missing}")

    return input_list




def offset_period_eom(table: pa.Table, frequency: str) -> pa.Table:
    """
    Private helper to offset a pa.Table's col 0 to EOM.
    Standardizes col 0 to EOM for m/q freqs.
    """
    if table.num_columns == 0:
        raise ValueError("Table has no columns.")

    first_col = table.column(0).cast(pa.string())

    # fix(hmld, type): trims everything after yyyy-mm-dd (10 chars). 
    # HML_Devil is the only model that needs/needed this.
    d_str = pc.utf8_slice_codeunits(first_col.cast(pa.string()), 
                                    start=0, stop=10)

    clean = pc.replace_substring_regex(d_str, pattern=r"(\.0$|[-/ ])", replacement="")

    lengths = pc.utf8_length(clean)
    #is_year = pc.equal(lengths, 4)   # YYYY
    #is_month = pc.equal(lengths, 6)  # YYYYMM

    # use pc.if_else to build a YYYYMMDD string
    # chain logic: year adds '1231', month adds '01'
    date_str = pc.if_else(
        pc.equal(lengths, 4),
        pc.binary_join_element_wise(clean, pa.scalar("1231"), ""),
        pc.if_else(
            pc.equal(lengths, 6),
            pc.binary_join_element_wise(clean, pa.scalar("01"), ""),
            clean
        )
    )
    dates = pc.strptime(date_str, format="%Y%m%d", unit="ms")

    if frequency in ['m', 'q']:
        _next_mth = pc.ceil_temporal(dates, 1, unit='month')
        _one_day_ms = pa.scalar(86400000, type=pa.duration('ms'))
        eom_dates = pc.subtract(_next_mth, _one_day_ms)
    else:
        eom_dates = dates

    return table.set_column(0, table.column_names[0], dates.cast(pa.date32()))


def parse_quarterly_dates(table: pa.Table) -> pa.Table:
    """Internal: converts a single 'yyyyq' col, or 
    'year' and 'period' cols to a iso datestr (yyyymmdd).
    Moved out of icr and q models and combined here. Needs rework.
    ---
    Note
    - Private helper, used by ICRFactors() and QFactors().
    """
    if table.num_rows == 0:
        return table

    # q-factors, 2 col, year and period:
    if "year" in table.column_names and "period" in table.column_names:
        year = table.column("year").cast(pa.string())
        qtr  = table.column("period").cast(pa.int32())
        table = table.drop(["year", "period"])
    else:
        # ICR factors: 20231
        dates = table.column(0).cast(pa.string())
        year  = pc.utf8_slice_codeunits(dates, 0, 4)
        qtr   = pc.utf8_slice_codeunits(dates, 4, 5).cast(pa.int32())
        table = table.remove_column(0)
    # multiply 'q' to it's month.
    m_ints = pc.multiply(qtr, 3).cast(pa.string())
    # pad month if needed
    months = pc.utf8_lpad(m_ints, width=2, padding="0")
    # add '01' for yyyymmdd (relies on offset_period_eom to shift to eom)
    days   = pa.array(["01"] * table.num_rows, type=pa.string())

    # make a yyyymmdd str
    datestr = pc.binary_join_element_wise(year, months, days, "-")

    try:
        # cast to date32
        date_col = pc.cast(datestr, pa.date32())
        # Replaces first col, 0, date:
        table = table.add_column(0, "date", date_col)
        #offset
        return offset_period_eom(table, "q")
    except pa.ArrowInvalid as e:
        raise ValueError(f"Failed to parse quarterly dates: {e}")


def _format_for_preview(val, col_name, precision=6):
    """Private helper for print_table_preview output. Rounds for display converts None to NaNs."""
    if val is None or (isinstance(val, float) and math.isnan(val)):   #math over pycompute just for this
        return "NaN"

    if isinstance(val, (float, int)) and col_name != "date":
        prec = 4 if col_name.upper() in {'RF', 'RF_AQR'} else precision
        return f"{val:.{prec}f}"

    return str(val)

def print_table_preview(table, n_rows=4):
    """Prints a pa.Table like a (simplified) pd.DataFrame.

    Used by the cli, and base class __repr__ (TODO).

    Notes: 
        - Hardcoded 'date' and '[...]' for the index col and gap. 
        - Head and tails (n_rows) 4 by default. 
        - Table size is included in the footer (might change to a nice header)
        - Will print whole table if total is less than (n_rows * 2) + 5. 
    """
    total_rows = table.num_rows
    if total_rows == 0:
        print("Empty Table", file=sys.stderr)
        return

    # Get the model's own _precision property!
    precision = getattr(table, '_precision', 6)

    columns = table.column_names
    # if there's a gap, then the row is None. (Below, if None, prints "[...]")
    is_gap = total_rows <= (n_rows * 2) + 5   # +5 to prevent hiding just a few cols
    # slice a head and tail
    head = table.slice(0, n_rows).to_pylist() if not is_gap else table.to_pylist()
    tail = table.slice(total_rows - n_rows, n_rows).to_pylist() if not is_gap else []

    # calc width of cols
    col_widths = {
        col: max(
            len(col),
            max((len(_format_for_preview(r[col], col, precision)) for r in head + tail), default=0)
        ) + 2 
        for col in columns
    }

    # construct header
    header_row = "".join(c.rjust(col_widths[c]) for c in columns[1:])
    print(f"\t    {header_row}", file=sys.stderr)
    print("date", file=sys.stderr) 

    # print (head, gap, tail) rows. TODO: nice header, __str__ in base
    display_rows = head + ([None] if tail else []) + tail
    for row in display_rows:
        if row is None:    # from above, row's None, because is_gap
            print("  [...]", file=sys.stderr)
            continue

        # left align dates, other cols right
        date_str = _format_for_preview(row['date'], 'date', precision).ljust(col_widths['date'])
        factors_str = "".join(_format_for_preview(row[c], c, precision).rjust(col_widths[c]) for c in columns[1:])

        print(f"{date_str}{factors_str}", file=sys.stderr)

    # footer, like pandas, with size: "[rows x cols, size]"
    buf_size = table.get_total_buffer_size()
    size_kb = buf_size / 1024
    print(f"\n[{total_rows} rows x {len(columns)} columns, {size_kb:.1f} kb]", file=sys.stderr)

