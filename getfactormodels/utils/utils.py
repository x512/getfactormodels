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
import pyarrow.csv as pv
from dateutil import parser
from .data_utils import round_to_precision

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
    "QualityMinusJunk": r"\b(qmj|aqr_qmj|qualityminusjunk|quality)\b",
    "BettingAgainstBeta": r"\b(bab|aqr_bab|bettingagainstbeta|betting)\b",
    "BarillasShanken": r"\b(bs|bs6|barillas|shanken)\b", })

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
        from datetime import datetime
        log.warning("No data. Used timestamp for filename.")
        date_str = f"no_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # filter out the Nones...
    parts = [model_name, freq, _ff_region]
    #join it together...
    base = "_".join(str(p).lower() for p in parts if p)
    #what a beautiful filename!!
    return f"{base}_{date_str}.csv"


def _save_to_file(table: pa.Table, filepath: str | Path, model_instance=None):
    """Private helper: save a table to file. 
    - Uses pyarrow, falls back to pandas to write .pkl.
    """
    #TODO: require user to have pandas installed for pkl, or use python's pickle...
    _name = _generate_filename(model_instance) if model_instance else "factors.csv"
    full_path = _prepare_filepath(filepath, _name)
    ext = full_path.suffix.lower()

    table = table.combine_chunks()

    # FIXME: TODO:
    # temp fix. RF cols are 4 decimals from every source.
    # TODO: should be done, test with rounding util.
    # Avoids returning any rounding errors in RF cols in all models
    _rfs = {'RF', 'R_F', 'AQR_RF'}
    for i, name in enumerate(table.schema.names):
        clean_name = name.upper().strip()
        if clean_name in _rfs:
            rounded_col = pc.round(table.column(name), ndigits=4)
            table = table.set_column(i, name, rounded_col)

    try:
        if ext == '.parquet':
            import pyarrow.parquet as pq
            pq.write_table(table, full_path, compression='snappy')
            
        elif ext in ('.feather', '.arrow', '.ipc'):
            import pyarrow.feather as pf
            pf.write_feather(table, full_path, compression='uncompressed')

        elif ext == '.csv':
            write_options = pv.WriteOptions(include_header=True)
            pv.write_csv(table, full_path, write_options=write_options)

        elif ext == '.txt':
            write_options = pv.WriteOptions(delimiter='\t')
            pv.write_csv(table, full_path, write_options=write_options)

        elif ext in ('.md', '.markdown'):
            try:
                with open(full_path, 'w', encoding='utf-8') as f:
                    for line in _stream_table_to_md(table):
                        f.write(line + '\n')
            except Exception as e:
                raise OSError(f"Failed to write markdown to {full_path}: {e}")

        elif ext == '.pkl':
        # pd ----------------------------------- #
            df = table.to_pandas()
            df.to_pickle(full_path)
        # -------------------------------------- #
        else:
            supported = ['.parquet', '.feather', '.csv', '.txt', '.pkl', '.md']
            raise ValueError(f"Extension '{ext}' not supported. Options: {supported}")

    except Exception as e:
        raise OSError(f"Failed to write {ext} file to {full_path}: {e}") from e


def _stream_table_to_md(table, precision=4):  # TODO: use model's _precision
    """Generator that yields markdown rows.
    
    - Reduces memory usage for writing larger tables.
    """
    yield "| " + " | ".join(table.column_names) + " |"
    yield "| " + " | ".join(["-----"] * len(table.column_names)) + " |"

    str_columns = []
    rfs = {'RF', 'R_F', 'AQR_RF', 'RF_AQR'} 

    for name in table.column_names:
        col = table.column(name)
        clean_name = name.upper().strip()
        
        # set precision per col
        curr_prec = 4 if clean_name in rfs else precision

        if pa.types.is_timestamp(col.type) or pa.types.is_date(col.type):
            clean_col = col.cast(pa.date32()).cast(pa.string()).to_pylist()
        
        elif pa.types.is_floating(col.type):
            # f-strings to force decimals
            data = col.to_pylist()
            clean_col = [
                f"{x:.{curr_prec}f}" if x is not None else "" 
                for x in data
            ]
        else:
            clean_col = col.cast(pa.string()).to_pylist()

        str_columns.append(clean_col)
    for row in zip(*str_columns):
        yield "| " + " | ".join(row) + " |"


def _roll_to_eom(dt: datetime) -> str:
    """Roll a datetime to the last day of its month."""
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    return dt.replace(day=last_day).strftime("%Y-%m-%d")


# TODO: redo below, user date inputs... 
### used ONLY for user input and get/set start/end. TODO: REDO
def _validate_date(date_input: None | str | int, is_end: bool = False) -> str | None:
    """Internal helper: standardizes date input to an ISO (YYYY-MM-DD) str.

    - Takes a date_input, "2023", "202310", "2023-10" and converts it to 
      YYYY-MM-DD. If end_date is True, sets day to last of m/q/y period.
    - Used for start_date/end_date inputs.

    Args
        `date_input`: the date str.
        `is_end`: if True, snaps YYYY to Dec 31st and YYYY-MM to the 
          last calendar day of its month. Else, defaults to the first
          day of the period.
    """
    if date_input is None:
        return None

    raw_str = str(date_input).strip()
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
