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


def filter_table_by_date(table: pa.Table, start: str | None, end: str | None) -> pa.Table:
    """Slices a table to a start-end range."""
    if start is None and end is None:
        return table

    # target the date col and its type
    date_col = 'date' if 'date' in table.column_names else table.column_names[0]
    target_type = table.schema.field(date_col).type
    expr = pc.field(date_col)

    # Build the combined expression
    mask = None
    if start:
        mask = (expr >= pc.scalar(start).cast(target_type))
    
    if end:
        end_mask = (expr <= pc.scalar(end).cast(target_type))
        mask = (mask & end_mask) if mask is not None else end_mask

    return table.filter(mask) if mask is not None else table


def rearrange_columns(table: pa.Table) -> pa.Table:
    """Internal helper: Standardize column orders.
    
    The order should always be: 
      date, Mkt-RF, [FACTORS...], RF.
    """
    cols = table.column_names

    if 'date' not in cols:
        # if 'date' isn't col 0 by now the subclass/model is bad. It's not the user's input.
        raise RuntimeError(
            f"Error: model does not have a 'date' column! (cols: {cols})",
        )
    front = [c for c in ['date', 'Mkt-RF'] if c in cols]
    back = [c for c in ['RF', 'AQR_RF'] if c in cols]
    mid = [c for c in cols if c not in set(front + back)]
    return table.select(front + mid + back)


def verify_cols_exist(table: pa.Table, names: str | list[str] | None) -> list[str]:
    """Private helper: Checks if columns exist in a pyarrow Table by name.
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


def scale_to_decimal(table: pa.Table) -> pa.Table:
    """Standardize float cols to decimal (5.2 -> 0.052)."""
    for i, field in enumerate(table.schema):
        if pa.types.is_floating(field.type):
            scaled_col = pc.divide(table.column(i), 100.0)
            table = table.set_column(i, field.name, scaled_col)
    return table


def round_to_precision(table: pa.Table, precision: int) -> pa.Table:
    """Apply a model's _precision to all float cols 
    and a precision of 4 to all RF columns.
    """
    new_arrays = []
    for field in table.schema:
        col = table.column(field.name)
        if pa.types.is_floating(field.type):
            prec = 4 if field.name in ['RF', 'AQR_RF', 'RF_AQR'] else precision
            new_arrays.append(pc.round(col, prec))
        else:
            new_arrays.append(col)
    return pa.Table.from_arrays(new_arrays, schema=table.schema)


def aqr_dt_fix(d) -> str:
    """Fixes AQR's 'MM/DD/YYYY' format."""
    if isinstance(d, str) and '/' in d:
        m, day, y = d.split('/')
        return f"{y}{m.zfill(2)}{day.zfill(2)}"
    if hasattr(d, 'strftime'):
        # for the dt objects from Calamine
        return d.strftime("%Y%m%d")
    return str(d)

def offset_period_eom(table: pa.Table, frequency: str) -> pa.Table:
    """
    Private helper to offset a pa.Table's col 0 to EOM.
    Standardizes col 0 to EOM for m/q freqs.
    """
    if table.num_columns == 0:
        raise ValueError("Table has no columns.")
    
    orig_name = table.column_names[0]
    first_col = table.column(0)

    # fix(hmld, type): trims everything after yyyy-mm-dd (10 chars). 
    # HML_Devil is the only model that needs/needed this.
    d_str = pc.utf8_slice_codeunits(first_col.cast(pa.string()), 
                                                    start=0, stop=10)
    clean_str = pc.replace_substring_regex(d_str, pattern=r"(\.0$|[-/ ])", replacement="")

    lengths = pc.utf8_length(clean_str)
    is_year = pc.equal(lengths, 4)   # YYYY
    is_month = pc.equal(lengths, 6)  # YYYYMM

    # chain logic: year adds '0101', month adds '01'
    date_str = pc.if_else(
        is_year,
        pc.binary_join_element_wise(clean_str, pa.array(["1231"] * len(table)), ""),
        pc.if_else(
            is_month,
            pc.binary_join_element_wise(clean_str, pa.array(["01"] * len(table)), ""),
            clean_str,
        ),
    )
    dates = pc.strptime(date_str, format="%Y%m%d", unit="ms")

    if frequency in ['m', 'q']:
        _next_mth = pc.ceil_temporal(dates, 1, unit='month')
        _one_day_ms = pa.scalar(86400000, type=pa.duration('ms'))
        processed_dates = pc.subtract(_next_mth, _one_day_ms)
    else:
        processed_dates = dates

    # fix: to date32, to 'ns'
    eom_dates = processed_dates.cast(pa.date32())
    _dt = eom_dates.cast(pa.timestamp('ns'))
    table.validate()

    return table.set_column(0, orig_name, _dt)

