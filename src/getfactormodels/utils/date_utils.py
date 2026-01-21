# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import calendar
import logging
import re
import sys
from datetime import date, datetime
from typing import Any
import pyarrow as pa
import pyarrow.compute as pc

log = logging.getLogger(__name__)

def offset_period_eom(table: pa.Table, frequency: str) -> pa.Table:
    """Utility to offset a pa.Table's date column to the last day of its period."""
    if table.num_columns == 0:
        return table
        
    orig_name = table.column_names[0]
    freq = frequency.lower()

    # remove non digits 
    raw = pc.replace_substring_regex(table.column(0).cast(pa.string()), r"\D", "")
    lengths = pc.utf8_length(raw)

    # Constructs a valid date str. If 8 chars, keep. If 4 or 6 append 0101 or 01. 
    clean = pc.case_when(
        pc.make_struct(pc.equal(lengths, 8), pc.equal(lengths, 6), pc.equal(lengths, 4)),
        raw,                                                    # 8
        pc.binary_join_element_wise(raw, pa.scalar("01"), ""),  # 6
        pc.binary_join_element_wise(raw, pa.scalar("0101"), ""), # 4
    )
    dates = pc.strptime(clean, format="%Y%m%d", unit="s")

    if freq in ['m', 'q', 'y']:
        # Map the frequency to ceil_temporal's unit. 
        unit_map = {'m': 'month', 'q': 'quarter', 'y': 'year'}
        unit = unit_map[freq]

        # Add a day to make sure we're strictly inside the period.
        _one_day = pa.scalar(86400000, type=pa.duration('ms'))
        extra_day = pc.add(dates, _one_day)

        # From the start of the next period, subtract one day to get EOM/Q/Y
        next_period_start = pc.ceil_temporal(extra_day, unit=unit)
        final_dates = pc.subtract(next_period_start, _one_day)
    else: # 'd' or unknown, just return the parsed dates.
        final_dates = dates

    return table.set_column(0, orig_name, final_dates.cast(pa.date32()))


# ALL retreived quarterly dates: Q factors, ABS, ICR, fred
def parse_quarterly_dates(table: pa.Table) -> pa.Table:
    """Converts quarterly dates to a datetime str.

    Converts 1 column, "yyyyq", or two columns, "year" and "period" to a single 
    'date' column, of iso datestr yyyy-mm-dd, set to the end of the period.

    - Used by ICRFactors() and QFactors().
    """
    if table.num_rows == 0:
        return table

    # q-factors: 2 cols - 'year' and 'period'
    if "year" in table.column_names and "period" in table.column_names:
        year = table.column("year").cast(pa.string())
        qtr = table.column("period").cast(pa.int32())
        table = table.drop(["year", "period"])
    else: 
        dates = table.column(0).cast(pa.string())
        year = pc.utf8_slice_codeunits(dates, 0, 4)
        qtr = pc.utf8_slice_codeunits(dates, start=-1).cast(pa.int32())
        table = table.remove_column(0)
    m_int = pc.multiply(qtr, 3).cast(pa.string())
    month = pc.utf8_lpad(m_int, width=2, padding="0")

    days = pa.array(["01"] * table.num_rows, type=pa.string())
    datestr = pc.binary_join_element_wise(year, month, days, "-")
    
    try:
        date_col = datestr.cast(pa.date32())
        # Re-insert as the first column
        return table.add_column(0, "date", date_col).combine_chunks()
    except pa.ArrowInvalid as e:
        raise ValueError(f"Failed to parse quarterly dates: {e}")


def validate_date_range(start: str | None, end: str | None) -> tuple[str | None, str | None]:
    """Validates start and end date range. Sets end to today if in the future.
    
    If start is in the future, err.
    """
    today = date.today().isoformat()

    if start and end and start > end:
        raise ValueError(f"start_date ({start}) is after end_date ({end}).")

    if start and start > today:
        raise ValueError(f"Start date is in the future. {start} > {today}.") 
    
    if end and end > today:
        print(f"WARNING: End date is in the future: {end}. Setting to today: {today}.", file=sys.stderr)
        end = today

    return start, end


def _roll_to_eom(dt: datetime) -> str:
    """Roll a datetime to the last day of its month.
    
    Used for user input only.
    """
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    return dt.replace(day=last_day).strftime("%Y-%m-%d")


def _validate_input_date(date_input: Any, *, is_end: bool = False) -> str | None:
    if date_input is None:
        return None

    raw_str = str(date_input).strip()

    if '-' in raw_str:
        parts = raw_str.split('-')
        year = parts[0]
        month = parts[1].zfill(2) # don't assume a day exists
        
        if len(parts) >= 3:
            day = parts[2].zfill(2)
            raw_str = f"{year}-{month}-{day}"
        else:
            raw_str = f"{year}-{month}"

    clean_str = re.sub(r'\D', '', raw_str) 
    try:
        if len(clean_str) == 4:  # YYYY
            dt = datetime(int(clean_str), 12, 31) if is_end else datetime(int(clean_str), 1, 1)
        elif len(clean_str) == 6: # YYYYMM
            dt = datetime.strptime(clean_str, "%Y%m")
            if is_end:
                dt = dt.replace(day=calendar.monthrange(dt.year, dt.month)[1])
        elif len(clean_str) == 8: # YYYYMMDD
            dt = datetime.strptime(clean_str, "%Y%m%d")
        else:
            try:
                dt = datetime.fromisoformat(raw_str.replace('/', '-'))
            except ValueError:
                dt = datetime.strptime(raw_str, "%Y-%m")
                if is_end:
                    dt = dt.replace(day=calendar.monthrange(dt.year, dt.month)[1])

        return dt.strftime("%Y-%m-%d")

    except Exception:
        raise ValueError(f"Invalid date: '{date_input}'. Use YYYY, YYYY-MM, or YYYY-MM-DD.")

# test:
# yyyy, yyyy-mm, yyyymm, yyyymmdd end dates 
# That they return as intended, and yyyy-mm isn't filling in today's day.
# isn't reading YYYYMM as YYMMDD (201204 becoming 2020-12-04)
# isn't filling in today's month or current year.
# IS TESTED WITH ALL RETURNED VALUES IN SOURCE DATA.
#TODO: style warnings! Consistent logging!
