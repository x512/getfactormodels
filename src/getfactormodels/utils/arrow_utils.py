# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import logging
import pyarrow as pa
import pyarrow.compute as pc

"""Utilities for working with pyarrow."""

log = logging.getLogger(__name__)


def scale_to_decimal(table: pa.Table) -> pa.Table:
    """Standardize float cols to decimal (5.2 -> 0.052)."""
    for i, field in enumerate(table.schema):
        if pa.types.is_floating(field.type):
            scaled_col = pc.divide(table.column(i), 100.0)
            table = table.set_column(i, field.name, scaled_col)
    return table


def round_to_precision(table: pa.Table, precision: int) -> pa.Table:
    """Rounds all float cols in a pa.Table to precision, and RF to 4.

    Note:
    Used by all models.
    """
    # auto resolve to: precision arg, else model's ._precision or '6'.
    prec_val = precision or getattr(table, '_precision', 6)
    rf_cols = {'RF', 'R_F'} # TODO: fix this up.

    new_cols = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_floating(field.type):
            p = 4 if field.name.upper() in rf_cols else 6 if field.name.upper() == "RF_AQR" else prec_val
            col = pc.round(col, ndigits=p, round_mode='half_to_even')
        new_cols.append(col)

    return pa.Table.from_arrays(new_cols, schema=table.schema)


def rearrange_columns(table: pa.Table) -> pa.Table:
    """Standardize column orders: 'date', [FACTORS], 'RF'"""
    cols = table.column_names
    front = [c for c in ['date', 'Mkt-RF'] if c in cols]
    back = [c for c in ['RF_AQR', 'RF_Q', 'RF'] if c in cols]
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


def _validate_columns(table: pa.Table, names: str | list[str] | None) -> list[str]:
    """Private helper: Checks if columns exist in a pyarrow Table by name.

    Returns the list of validated factors or raises if missing.
    """
    if table.num_rows == 0:
        raise RuntimeError("Table is empty.")

    if not names:
        return []

    input_list = [names] if isinstance(names, str) else list(names)

    cols = set(table.column_names)

    missing = [f for f in input_list if f not in cols]
    if missing:
        raise ValueError(f"Columns not found in Table: {missing}")

    return input_list


def select_table_columns(table: pa.Table, factors: str | list[str]) -> pa.Table:
    """Helper: constructs a table from a pa.Table by selecting cols by name.

    'date' is col 0, and if passed in 'factors', is depduplicated.
    Used by base FactorModel's __getitem__, _extract_as_table and .extract()

    Case insensitive.
    """
    req_cols = [factors] if isinstance(factors, str) else list(factors)

    name_map = {col.lower(): col for col in table.column_names}

    valid_names = [name_map.get(f.lower(), f) for f in req_cols]
    valid_cols = _validate_columns(table, valid_names)

    factor_cols = [f for f in valid_cols if f.lower() != 'date']
    if not factor_cols:
        raise ValueError("Extraction must include at least one factor.")   ### TEST THIS

    # ['date'] + [verified factors minus date]
    selection = list(dict.fromkeys(['date'] + factor_cols))

    return table.select(selection).combine_chunks()


def _format_for_preview(val, col_name, precision=6):
    """Private helper for print_table_preview output. Rounds for display converts None to NaNs."""
    if val is None or (isinstance(val, float) and val != val): 
        return "NaN"

    if isinstance(val, (float, int)) and col_name != "date":
        prec = 4 if col_name.upper() in {'RF', 'RF_AQR'} else precision
        return f"{val:.{prec}f}"

    return str(val)


def print_table_preview(table, n_rows=4) -> str | None:
    """Prints a pa.Table like a (simplified) pd.DataFrame.

    Used by the cli, and base class __repr__ (TODO).

    Notes:
        - Hardcoded 'date' and '[...]' for the index col and gap. 
        - Head and tails (n_rows) 4 by default. 
        - Table size is included in the footer (might change to a nice header)
        - Will print whole table if total is less than (n_rows * 2) + 5. 
    """
    total_rows = table.num_rows
    if table.num_rows == 0:
        log.warning("Empty Table")  # no sys
        return None

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
            max((len(_format_for_preview(r[col], col, precision)) for r in head + tail), default=0),
        ) + 2 
        for col in columns
    }

    # construct header (collect into list, no printing to stderr)
    output = []
    header_row = "".join(c.rjust(col_widths[c]) for c in columns[1:])
    output.append(f"\t    {header_row}")
    output.append("date")

    # print (head, gap, tail) rows. TODO: nice header, __str__ in base
    display_rows = head + ([None] if tail else []) + tail
    for row in display_rows:
        if row is None:    # from above, row's None, because is_gap
            output.append("  [...] ")   ### TEST THIS
            continue

        # left align dates, other cols right
        date_str = _format_for_preview(row['date'], 'date', precision).ljust(col_widths['date'])
        factors_str = "".join(_format_for_preview(row[c], c, precision).rjust(col_widths[c]) for c in columns[1:])
        output.append(f"{date_str}{factors_str}")

    buf_size = table.get_total_buffer_size()
    output.append(f"\n[{total_rows} rows x {len(columns)} columns, {buf_size / 1024:.1f} kb]")
    # change: return the str, not print
    return "\n".join(output)
