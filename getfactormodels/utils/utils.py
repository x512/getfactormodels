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
import logging
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv

#import warnings

log = logging.getLogger(__name__) #TODO: consistent logging.

"""Model utils and I/O utils."""

_model_map = {
    "3": ["3", "ff3", "famafrench3"],
    "4": ["4", "ff4", "carhart", "car"],
    "5": ["5", "ff5", "famafrench5"],
    "6": ["6", "ff6", "famafrench6"],
    "Q": ["q", "qfactors", "q-factors", "q_factors", "q5", "hmxz"],
    "Qclassic": ["q4", "qclassic", "q-classic", "q_classic", "classic_q"],
    "HMLDevil": ["hmld", "hmldevil", "hml_devil", "devil"],
    "QMJ": ["qmj", "quality", "qualityminusjunk"],
    "BAB": ["bab", "betting", "bettingainstbeta"],
    "Mispricing": ["mispricing", "mis", "misp"],
    "Liquidity": ["liq", "liquidity"],
    "ICR": ["icr", "intermediary", "hkm"],
    "DHS": ["dhs", "behavioural", "behaviour"],
    "BarillasShanken": ["bs", "bs6", "barillasshanken", "barillas-shanken"],
}

_MODEL_INPUT_MAP = MappingProxyType(_model_map)

def _get_model_key(model_input: str | int) -> str:
    """Converts user input (e.g. 'ff3', 'hmld') to the model key.
    
    >>> _get_model_key('3')
    '3'
    >>> _get_model_key('ff6')
    '6'
    >>> _get_model_key(icr)
    'ICR'
    >>> _get_model_key('Bab')
    'BAB'
    """
    val = str(model_input).lower().strip()
    
    for key, alias in _MODEL_INPUT_MAP.items():
        if val in alias:
            return key
            
    return val


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
    """Private helper: create filename using metadata from a model instance.
    
    Used if directory is provided, or just an extension, intended to be used 
    for -o flag with no param.
    """
    # TODO: one day add a name property to models...
    # TODO: if user used 'carhart' use carhart, if they used (ff)4, use ff.

    # 3 to "ff3", FF models only ones that accept int.
    raw_name = getattr(model, 'model', model.__class__.__name__.replace('Factors', ''))
    model_name = f"ff{raw_name}" if str(raw_name).isdigit() else raw_name

    freq = model.frequency
    region = getattr(model, 'region', None)
    country = getattr(model, 'country', None)

    if hasattr(model, 'data') and model.data.num_rows > 0:
        date_col = model.data.column(0)

        # pc.min/max give scalar, as_py makes scalar dt
        start_dt = pc.min(date_col).as_py()
        end_dt = pc.max(date_col).as_py()

        date_str = f"{start_dt.strftime('%Y%m%d')}-{end_dt.strftime('%Y%m%d')}"
    else:
        date_str = f"no_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    parts = [model_name, freq, region, country, date_str]
    filename = "_".join(str(p).lower() for p in parts if p)

    return f"{filename}.csv"
    

def _save_to_file(table: pa.Table, filepath: str | Path, model_instance=None):
    """Private helper: save a table to file.
    
    - Uses pyarrow, falls back to pandas to write .pkl.
    """
    #TODO: require user to have pandas installed for pkl, or use python's pickle...
    _name = _generate_filename(model_instance) if model_instance else "factors.csv"
    full_path = _prepare_filepath(filepath, _name)
    ext = full_path.suffix.lower()

    table = table.combine_chunks()

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
                msg = f"Failed to write markdown to {full_path}"
                log.exception(msg) 
                raise OSError(f"{msg}: {e}")

        elif ext == '.pkl':
            df = table.to_pandas()
            df.to_pickle(full_path)
        else:
            supported = ['.parquet', '.feather', '.csv', '.txt', '.pkl', '.md']
            raise ValueError(f"Extension '{ext}' not supported. Options: {supported}")

    except Exception as e:
        raise OSError(f"Failed to write {ext} file to {full_path}: {e}") from e


def _stream_table_to_md(table: pa.Table, precision: int = 4):
    """Generator that yields markdown rows.
    
    - Reduces memory usage for writing larger tables.
    """
    yield "| " + " | ".join(table.column_names) + " |"
    yield "| " + " | ".join(["----"] * len(table.column_names)) + " |"

    str_columns = []
    rfs = {'RF', 'R_F', 'AQR_RF', 'RF_AQR'} 

    # clean floats, col presentation
    for name in table.column_names:
        col = table.column(name)  
        if pa.types.is_floating(col.type):
            prec = 4 if name.upper() in rfs else precision
            data = col.to_pylist()
            # f-strings to force decimals.
            # fix: prepend positives with an empty space.
            # - a space bef the f-string dot: space for positive, '-' for neg.
            # fix: alignment when col is all positives, rm the double spaces.
            contains_negs = pc.any(pc.less(col.fill_null(0), 0)).as_py()
            fmt = f" .{prec}f" if contains_negs else f".{prec}f"
            clean_col = [f"{x:{fmt}}" if x is not None else "" for x in data]
        else:
            clean_col = col.cast(pa.string()).to_pylist()
        str_columns.append(clean_col)

    for row in zip(*str_columns):
        yield "| " + " | ".join(row) + " |"

