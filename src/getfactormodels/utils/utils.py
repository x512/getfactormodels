# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import logging
from datetime import datetime
#import warnings
from io import BytesIO
from pathlib import Path
from types import MappingProxyType
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
from getfactormodels.utils.http_client import _HttpClient

log = logging.getLogger(__name__) #TODO: consistent logging.

"""Model utils and I/O utils."""

_MODEL_ALIASES = {
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
    "VME": ["vme", "valmom", "valueandmomentumeverywhere"],
    "AQR6": ["aqr6", "aqr", "aqrfactors"],
    "HCAPM": ["hccapm", 'hcapm', 'hc-capm'],
    "ConditionalCAPM": ["jwcapm", "plcapm", "jwccapm", "plccapm", "ccapm"],
}

_LOOKUP = MappingProxyType({
    alias: key 
    for key, aliases in _MODEL_ALIASES.items() 
    for alias in aliases
})

def _get_model_key(user_input: str | int) -> str:
    """Converts user input (e.g. 'ff3', 'hmld') to the model key.
    
    >>> _get_model_key('ff3')
    '3'
    >>> _get_model_key('Bab')
    'BAB'
    """
    if not user_input:
        return ""
        
    val = str(user_input).lower().strip()
    
    return _LOOKUP.get(val, val)


def _prepare_filepath(filepath: str | Path | None, filename: str) -> Path:
    if filepath is None:
        return Path.cwd() / filename

    is_explicit_dir = str(filepath).endswith(("/", "\\"))
    user_path = Path(filepath).expanduser()

    if is_explicit_dir or user_path.is_dir():
        final_path = user_path / filename
    else:
        if not user_path.suffix:
            user_path = user_path.with_suffix(".csv")
        final_path = user_path
    
    # make sure it exists!
    final_path.parent.mkdir(parents=True, exist_ok=True)
    return final_path


def _generate_filename(model: 'FactorModel') -> str: # type: ignore [reportUndefinedVariable]   TODO: FIXME
    """Private helper: create filename using metadata from a model instance.
    
    Used if directory is provided, or just an extension, intended to be used 
    for -o flag with no param.
    """
    # 3 to "ff3", FF models only ones that accept int.
    raw_name = getattr(model, 'model', model.__class__.__name__.replace('Factors', ''))
    model_name = f"ff{raw_name}" if str(raw_name).isdigit() else raw_name

    freq = model.frequency
    region = getattr(model, 'region', None)

    if hasattr(model, 'data') and model.data.num_rows > 0:
        date_col = model.data.column(0)

        # pc.min/max give scalar, as_py makes scalar dt
        start_dt = pc.min(date_col).as_py()
        end_dt = pc.max(date_col).as_py()

        date_str = f"{start_dt.strftime('%Y%m%d')}-{end_dt.strftime('%Y%m%d')}"
    else:
        date_str = f"no_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    parts = [model_name, freq, region, date_str]
    filename = "_".join(str(p).lower() for p in parts if p)

    return f"{filename}.csv"
    

def _save_to_file(table: pa.Table, filepath: str | Path, model_instance=None):
    """Private helper: save a table to file.
    
    - Uses pyarrow, falls back to pandas to write .pkl.
    """
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
            with open(full_path, 'w', encoding='utf-8') as f:
                for line in _stream_table_to_md(table, precision = 6):
                    f.write(line + '\n')

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
    try:
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
    except Exception as e:
                    msg = f"Failed to write markdown to {full_path}"
                    log.exception(msg) 
                    raise OSError(f"{msg}: {e}")


def read_from_fred(series: str | list[str] | dict[str, str], 
                   frequency: str = 'a', 
                   aggregate: str = 'avg', 
                   client=None) -> pa.Table:
    """Internal utility to download and align FRED time-series data into a pa.Table.

    Simple helper. Each series is downloaded and aligned on the col 'date'. Date is 
    cast to Date32. Not for the public API.

    If models require data from FRED, re-use the client!

    Args:
        series: can be a single series ID ('GDP'), list of series ID's, or a 
                dict to rename cols ({'A033RC1A027NBEA': 'income'}).
        frequency: fred standard codes for frequency (y, q, m). Accepts 'y' as 'a'.
        client: Optional _HttpClient. If None, manages its own lifecycle.

    """
    _freq = {'y': 'a', 'a': 'a', 'q': 'q', 'm': 'm'}
    fred_freq = _freq.get(frequency.lower(), frequency)

    if isinstance(series, str):
        mapping = {series: series}
    elif isinstance(series, list):
        mapping = {s: s for s in series}
    else:
        mapping = series
    
    internal_client = client if client else _HttpClient()
    
    try:
        tables = []
        for s_id, col_name in mapping.items():
            url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={s_id}&fq={fred_freq}&agg={aggregate}"
            data = internal_client.download(url)
            
            t = pv.read_csv(BytesIO(data))
            t = t.rename_columns(["date", col_name])
            
            schema = t.schema.set(0, pa.field("date", pa.date32()))
            tables.append(t.cast(schema))
       
        result = tables[0]
        for next_table in tables[1:]:
            result = result.join(next_table, keys="date")
            
        return result.sort_by([("date", "ascending")])

    finally:
        if client is None:
            internal_client.close()
