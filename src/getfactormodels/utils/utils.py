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
#from getfactormodels.models.aqr_models import HMLDevilFactors, QMJFactors
from getfactormodels.utils.date_utils import offset_period_eom
from getfactormodels.utils.http_client import _HttpClient
import sys
log = logging.getLogger(__name__) #TODO: consistent logging.

"""Model utils and I/O utils."""

_MODEL_REGISTRY = {
    "ff3": {
        "class": "FamaFrenchFactors",
        "name": "Fama-French 3-Factor Model",
        "aliases": ["3", "famafrench3"],
    },
    "ff4": {
        "class": "CarhartFactors",
        "name": "Carhart/Fama-French 4-Factors",
        "aliases": ["4", "car", "carhart", "famafrench4"],
    },
    "ff5": {
        "class": "FamaFrenchFactors",
        "name": "Fama-French 5-Factors",
        "aliases": ["5", "famafrench5"],
    },
    "ff6": {
        "class": "FamaFrenchFactors",
        "name": "Fama-French 6-Factors",
        "aliases": ["6", "famafrench6"],
    },
    "q": {
        "class": "QFactors",
        "name": "Augmented q5 Factors",
        "aliases": ["q5", "hmxz", "qfactors"],
    },
    "qc": {
        "class": "QFactors",
        "name": "q-Factors (Classic)",
        "aliases": ["q4", "classicq", "qclassic"],
    },
    "hmld": {
        "class": "HMLDevilFactors",
        "name": "AQR: HML Devil",
        "aliases": ["devil", "hmldevil"],
    },
    "qmj": {
        "class": "QMJFactors",
        "name": "AQR: Quality Minus Junk",
        "aliases": ["quality", "quality-minus-junk"],
    },
    "bab": {
        "class": "BABFactors",
        "name": "AQR: Betting Against Beta",
        "aliases": ["betting", "betting-against-beta"],
    },
    "vme": {
        "class": "VMEFactors",
        "name": "AQR: Value & Momentum Everywhere",
        "aliases": ["valmom", "value-and-momentum-everywhere"],
    },
    "aqr6": {
        "class": "AQR6Factors",
        "name": "AQR: 6-Factor Model",
        "aliases": ["aqr", "aqrfactors", "a6"],
    },
    "mis": {
        "class": "MispricingFactors",
        "name": "Mispricing Factors",
        "aliases": ["misp", "mispricing"],
    },
    "liq": {
        "class": "LiquidityFactors",
        "name": "Liquidity Factors",
        "aliases": ["liquidity"],
    },
    "icr": {
        "class": "ICRFactors",
        "name": "Intermediary Capital Ratio",
        "aliases": ["intermediary"],
    },
    "dhs": {
        "class": "DHSFactors",
        "name": "DHS Behavioral Factors",
        "aliases": ["behaviour", "behavior", "dhs"],
    },
    "bs": {
        "class": "BarillasShankenFactors",
        "name": "Barillas-Shanken 6-Factors",
        "aliases": ["bs6", "barillasshanken"],
    },
    "hcapm": {
        "class": "HighIncomeCCAPM",
        "name": "CCAPM: High-Income/Affluent Household",
        "aliases": ["hcapm", "hicapm", "ccapm-hi"],
    },
    "pcapm": {
        "class": "ConditionalCAPM",
        "name": "CCAPM: Premium-Labour",
        "aliases": ["jwcapm", "plcapm", "ccapm-pl"],
    },
}


_LOOKUP = {
    alias.lower().replace("-", "").replace("_", ""): key 
    for key, data in _MODEL_REGISTRY.items() 
    for alias in data.get("aliases", []) + [key]
}


def get_model_key(user_input: str | int) -> str:
    if not user_input:
        raise ValueError("Model name cannot be empty.")

    val = str(user_input).lower().strip().replace("-", "").replace("_", "")

    if val in _LOOKUP:
        return _LOOKUP[val]

    raise ValueError(f"Unknown model or portfolio: '{user_input}'")


def get_model_class(key: str) -> str:
    if key not in _MODEL_REGISTRY:
        key = get_model_key(key)
    return _MODEL_REGISTRY[key]["class"]


def list_models():
    """Lists all model's ID's, name and aliases."""
    header = f"{'ID':<7} {'MODEL NAME':<35} {'ALIASES'}"
    sys.stderr.write(f"{header}\n")
    
    for key, data in _MODEL_REGISTRY.items():
        name = (data['name'][:30] + '...') if len(data['name']) > 33 else data['name']
        aliases = ", ".join(data['aliases'])
        
        row = f"{key:<7} {name:<35} {aliases}\n"
        sys.stderr.write(row)
        
    sys.stderr.write("\n")
    sys.exit(0)


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


def read_from_fred(series_map: dict, frequency: str, client) -> pa.Table:
    """Internal utility to download and align FRED time-series data into a pa.Table.

    Args:
        series: dict of series_id, col renames
        frequency: fred standard codes for frequency (y, q, m).
        client: Optional _HttpClient. If None, manages its own lifecycle.

    """
    freq_map = {'y': 'Annual', 'm': 'Monthly', 'q': 'Quarterly'}
    fred_freq = freq_map.get(frequency[0], 'Monthly')
    
    _client = client if client else _HttpClient()
    
    try:
        tables = []
        for s_id, col_name in series_map.items():
            url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={s_id}&fq={fred_freq}" #agg not needed yet
            data = _client.download(url)
            
            t = pv.read_csv(BytesIO(data))
            t = t.rename_columns(["date", col_name])
            
            t = t.cast(pa.schema([
                ('date', pa.date32()),
                (col_name, pa.float64()),
            ]))
            tables.append(t)

        result = tables[0]
        for next_table in tables[1:]:
            result = result.join(next_table, keys="date")
        
        t = result.sort_by([("date", "ascending")])
        return offset_period_eom(t, frequency)

    finally:
        if client is None:
            _client.close()

