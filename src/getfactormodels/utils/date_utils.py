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
    """Private helper to offset a pa.Table's col 0 to EOM.

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
            clean,
        ),
    )
    dates = pc.strptime(date_str, format="%Y%m%d", unit="ms")

    if frequency in ['m', 'q']:
        _next_mth = pc.ceil_temporal(dates, 1, unit='month')
        _one_day_ms = pa.scalar(86400000, type=pa.duration('ms'))
        eom_dates = pc.subtract(_next_mth, _one_day_ms)
    else:
        eom_dates = dates

    return table.set_column(0, table.column_names[0], eom_dates.cast(pa.date32()))


def parse_quarterly_dates(table: pa.Table) -> pa.Table:
    """Internal: converts quarterly dates in source data to iso date str.

    Converts 1 column, "yyyyq", or two columns, "year" and "period" to a single 
    'date' column, of iso datestr yyyy-mm-dd, set to the end of the period.

    Note:
    - Used by ICRFactors() and QFactors(). Functions removed and combined
      here, needs rework.
    """
    if table.num_rows == 0:
        return table

    # q-factors: 2 cols - 'year' and 'period'
    if "year" in table.column_names and "period" in table.column_names:
        year = table.column("year").cast(pa.string())
        qtr = table.column("period").cast(pa.int32())
        table = table.drop(["year", "period"])
    else: 
        # ICR factors: YYYYQ
        dates = table.column(0).cast(pa.string())
        year = pc.utf8_slice_codeunits(dates, 0, 4)
        qtr = pc.utf8_slice_codeunits(dates, 4, 5).cast(pa.int32())
        table = table.remove_column(0)

    # Find month (q*3) and make sure it's padded (9 to 09)
    m_int = pc.multiply(qtr, 3).cast(pa.string())
    month = pc.utf8_lpad(m_int, width=2, padding="0")

    # Add '01' for days (offset will set to EOM)
    days = pa.array(["01"] * table.num_rows, type=pa.string())

    # Create a YYYYMMDD date str
    datestr = pc.binary_join_element_wise(year, month, days, "-")
    try:
        # cast it to date32
        date_col = pc.cast(datestr, pa.date32())

        # Set first col to 'date' (replaces col 0)
        table = table.add_column(0, "date", date_col)
        
        # NOTE: don't offset_period_eom here. Causes models to offset twice.
        return table.combine_chunks()

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
    
    # Standardize dashed inputs without assuming a day exists
    if '-' in raw_str:
        parts = raw_str.split('-')
        year = parts[0]
        month = parts[1].zfill(2)
        
        if len(parts) >= 3:
            # Full YYYY-MM-DD
            day = parts[2].zfill(2)
            raw_str = f"{year}-{month}-{day}"
        else:
            # Short YYYY-MM (subscript safe)
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
