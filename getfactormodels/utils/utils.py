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
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
import pyarrow as pa
import pyarrow.compute as pc
from dateutil import parser

#from pyarrow.compute import ceil_temporal, subtract
# pyright: reportUnusedFunction=false

log = logging.getLogger(__name__) #TODO: consistent logging.

# TODO: redo this ....
__model_input_map = MappingProxyType({
    "3": r"\b((f?)f)?3\b|(ff)?1993",
    "5": r"\b(ff)?5|ff2015\b",
    "4": r"\b(c(ar(hart)?)?4?|ff4|carhart1997|4)\b",
    "6": r"\b(ff)?6|ff2018\b",
    "Q": r"\b(q(5)?|hmxz)\b",
    "Qclassic": r"q4|q_?classic|classic_q", # removed \b from inside
    "Mispricing": r"\b(sy4?|mispricing)|misp|yuan$|m4|mis|sy\b",
    "Liquidity": r"^(il)?liq(uidity)?|(pastor|ps|sp)$",
    "ICR": r"icr|hkm",
    "DHS": r"^(\bdhs\b|behav.*)$",
    "HMLDevil": r"\bhml(_)?d(evil)?|hmld\b",
    "BarillasShanken": r"\b(bs|bs6|barillas|shanken)\b" })

def _get_model_key(model: str | int) -> str:
    """Private helper: Convert a model name to a model key.

    >>> _get_model_key('ff3')
    '3'
    >>> _get_model_key('liQ')
    'Liquidity'
    >>> _get_model_key('q4_factors')
    'Qclassic'
    """
    model_str = str(model).lower().strip()

    for key, pattern in __model_input_map.items():
        wrapped_pattern = rf"({pattern})"
        
        if re.search(wrapped_pattern, model_str, re.IGNORECASE):
            return key

    return model_str


def _prepare_filepath(filepath: str | Path | None, filename: str) -> Path:
    if filepath is None:
        return Path.cwd() / filename

    user_path = Path(filepath).expanduser()

    if user_path.is_dir():
        # directory, append filename
        final_path = user_path / filename
    else:
        # file path: add ext if missing (defaulting to .csv) 
        if not user_path.suffix:
            user_path = user_path.with_suffix(".csv")

        user_path.parent.mkdir(parents=True, exist_ok=True)
        final_path = user_path

    return final_path


def _generate_filename(model: 'FactorModel') -> str: # type: ignore [reportUndefinedVariable]   TODO: FIXME
    """Private helper: create filename using metadata from a model instance."""
    # TODO: one day add a name property to models...
    # TODO: if user used 'carhart' use carhart, if they used (ff)4, use ff.
    _name = getattr(model, 'model', model.__class__.__name__.replace('Factors', ''))

    # 3 to "ff3", FF models only ones that accept int.
    model_name = f"ff{_name}" if str(_name).isdigit() else _name

    freq = getattr(model, 'frequency', 'd').lower()
    _ff_region = getattr(model, 'region', None)

    if hasattr(model, 'data') and not model.data.empty:
        # get actual start/end dates (ValueError if data empty)
        start = model.data.index.min().strftime('%Y%m%d')
        end = model.data.index.max().strftime('%Y%m%d')

        date_str = f"{start}-{end}"
    else:
        # something might be messed up, or data is empty
        from datetime import datetime
        log.warning("No data. Used timestamp for filename.")
        date_str = f"no_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # filter out the Nones...
    parts = [model_name, freq, _ff_region]
    #join it together...
    base = "_".join(str(p).lower() for p in parts if p)
    #what a beautiful filename!!
    return f"{base}_{date_str}.csv"


def _save_to_file(data, filepath, model_instance=None):
    _name = _generate_filename(model_instance) if model_instance else "factors.csv"
    full_path = _prepare_filepath(filepath, _name)
    print(f"DEBUG: Attempting to save to: {full_path.absolute()}")
    try:
        extension = full_path.suffix.lower()

        if extension == '.txt':
            data.to_csv(str(full_path), sep='\t')
        elif extension == '.csv':
            data.to_csv(str(full_path))
        elif extension == '.pkl':
            data.to_pickle(str(full_path))
        else:
            supported = ['.txt', '.csv', '.pkl']  # to add: feather, parquet, json
            raise ValueError(f'Unsupported file extension: {extension}. Must be one of: {supported}')
        print(f"File saved to: {full_path}")
    except Exception as e:
        # Fix UP024: Use OSError instead of IOError
        # Fix B904: Use from e to chain the exception
        raise OSError(f"Failed to save file to {full_path}: {str(e)}") from e

# TODO: clean
def _offset_period_eom(table: pa.Table, frequency: str) -> pa.Table:
    """Private helper to offset a pa.Table's col 0 to EOM. 

    Standardizes col 0 to EOM for m/q freqs.
    """
    if table.num_columns == 0:
        raise ValueError("Table has no columns.")
    
    orig_name = table.column_names[0]
    first_col = table.column(0)

    # float, dt obj, int 
    #d_str = first_col.cast(pa.string())
    # fix(hmld, type): trims everything after yyyy-mm-dd (10 chars)
    # TODO: is HML the only model that needs this? Does DHS? NOPE. WAIT YES. AHH
    d_str = pc.utf8_slice_codeunits(first_col.cast(pa.string()), 
                                                    start=0, stop=10)
    clean_str = pc.replace_substring_regex(d_str, pattern=r"(\.0$|[-/ ])", replacement="")

    lengths = pc.utf8_length(clean_str)
    is_year = pc.equal(lengths, 4)   # YYYY
    is_month = pc.equal(lengths, 6)  # YYYYMM

    # chain logic: year adds '0101', month adds '01'
    date_str = pc.if_else(
        is_year,
        pc.binary_join_element_wise(clean_str, pa.array(["0101"] * len(table)), ""),
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


# new
def _decimalize(table: pa.Table, schema: pa.Schema, precision: int) -> pa.Table:
    """
    Private helper: properly decimalizes a pa.Table.

    Use model's schema and decimalize any matching pa.float64() 
    columns. Converts to pa.decimal128(), returns pa.float64().
    Use before renaming.
    """
    # for debug
    decimalized_cols = []

    decimal_type = pa.decimal128(18, precision)
    divisor = pa.scalar(100, type=decimal_type)

    for field in schema:
        if pa.types.is_temporal(field.type) or not pa.types.is_floating(field.type):
            continue

        col_name = field.name
        if col_name in table.column_names:
            idx = table.schema.get_field_index(col_name)

            precise_col = pc.divide(pc.cast(table.column(idx), decimal_type),
                                    divisor)
            table = table.set_column(idx, col_name, 
                                     pc.cast(precise_col, pa.float64()))

        # debuggn
        decimalized_cols.append(field.name)
        if decimalized_cols:
            msg=(f"Decimalized {len(decimalized_cols)} "
                f"columns: {', '.join(decimalized_cols)}")
            log.info(msg)

    return table


# TODO: redo below, user date inputs... 
### used ONLY for user input and get/set start/end. TODO: REDO
def _roll_to_eom(dt: datetime) -> str:
    """Roll a datetime to the last day of its month."""
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    return dt.replace(day=last_day).strftime("%Y-%m-%d")

def _validate_date(date_input: None | str | int, is_end: bool = False) -> str | None:
    """Standardizes date input to a ISO (YYYY-MM-DD) string.

    Args:
        `date_input`: the date str. "2023", "202305", "2023-05-15"
        `is_end`: if True, snaps YYYY to Dec 31st and YYYY-MM to the 
          last calendar day of its month. If False, defaults to the 
          first day of the period.

    Returns:
        str : a 'YYYY-MM-DD' string, or None if input is None.

    Raises:
        ValueError: If the input cannot be parsed as a valid date.
    """
    if date_input is None:
        return None

    raw_str = str(date_input).strip()
    #clean_str = raw_str.replace("-", "").replace("/", "")
    clean_str = re.sub(r'\D', '', raw_str) # Removes anything that isn't a digit
    try:
        # YYYY, YYYYMM: don't allow parser to guess YYYYMM as YYMMDD!
        if clean_str.isdigit():
            if len(clean_str) == 4:
                year = int(clean_str)
                return f"{year}-12-31" if is_end else f"{year}-01-01"

            if len(clean_str) == 6:
                dt = datetime.strptime(clean_str, "%Y%m") # 6 chars IS %Y%m, parser
                return _roll_to_eom(dt) if is_end else dt.strftime("%Y-%m-01")

        # Parser
        #dt = parser.parse(raw_str)
        # this sets a default fallback for dates which shouldn't ever be hit
        dt = parser.parse(raw_str, default=datetime(1960, 1, 1))
        
        # input give a day? "YYYY-MM" (len 7) or "YYYY/MM", then no
        day_given = (len(clean_str) == 8 or raw_str.count('-') == 2 or raw_str.count('/') == 2)

        if is_end and not day_given:
            return _roll_to_eom(dt)
        # they gave day, use it.
        return dt.strftime("%Y-%m-%d")

    except (ValueError, OverflowError):
        raise ValueError(f"Invalid date: '{date_input}'. Use YYYY, YYYY-MM, or YYYY-MM-DD.")# tests -- yyyy, yyyymm yyyy-mm start dates 
# test:
# yyyy, yyyy-mm, yyyymm, yyyymmdd end dates 
# That they return as intended, and yyyy-mm isn't filling in today's day.
# isn't reading YYYYMM as YYMMDD (201204 becoming 2020-12-04)
# isn't filling in today's month or current year.
# IS TESTED WITH ALL RETURNED VALUES IN SOURCE DATA.





