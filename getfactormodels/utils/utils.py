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
import re
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
import pandas as pd

log = logging.getLogger(__name__) #TODO: consistent logging.

__model_input_map = MappingProxyType({
    "3": r"\b((f?)f)?3\b|(ff)?1993",
    "5": r"\b(ff)?5|ff2015\b",
    "4": r"\b(c(ar(hart)?)?4?|ff4|carhart1997|4)\b",
    "6": r"\b(ff)?6|ff2018\b",
    "Q": r"\b(q(5)?|hmxz)\b",
    "Qclassic": r"\b(q4|q(_)?classic)|classic_q\b",
    "Mispricing": r"\b(sy4?|mispricing)|misp|yuan$|m4|mis|sy\b",
    "Liquidity": r"^(il)?liq(uidity)?|(pastor|ps|sp)$",
    "ICR": r"\bicr|hkm\b",
    "DHS": r"^(\bdhs\b|behav.*)$",
    "HMLDevil": r"\bhml(_)?d(evil)?|hmld\b",
    "BarillasShanken": r"\b(bs|bs6|barillas|shanken)\b" })


def _get_model_key(model):
    """Convert a model name to a model key.

    >>> _get_model_key('ff1993')
    '3'
    >>> _get_model_key('liQ')
    'Liquidity'
    >>>  _get_model_key('q4_factors')
    'Qclassic'
    >>> _get_model_key('ICR')
    'ICR'
    """
    model = str(model)

    for key, regex in __model_input_map.items():
        if re.match(regex, model, re.I):
            return key
    raise ValueError(f'Invalid model: {model}')


# TODO: Will redo as a Writer class with use pyarrow
# changing: no longer uses filename, output_dir, just filepath. Always returns Path
# change: now uses filepath and a generated filename. base model uses this! (timestamped files not helpful)
def _prepare_filepath(filepath: str | Path | None, filename: str) -> Path:
    if filepath is None:
        return Path.cwd() / filename

    user_path = Path(filepath).expanduser()

    if user_path.is_dir():
        # directory, append filename
        final_path = user_path / filename
    else:
        # file path
        # add ext if missing, default .csv 
        if not user_path.suffix:
            user_path = user_path.with_suffix(".csv")
        
        user_path.parent.mkdir(parents=True, exist_ok=True)
        final_path = user_path

    return final_path


def _generate_filename(model: 'FactorModel') -> str: # TODO typehint err 
    """creates a default filename using metadata from the model instance."""
    # TODO: one day add a name property to models...
    _name = getattr(model, 'model', model.__class__.__name__.replace('Factors', ''))
    
    # 3 to "ff3", FF models only ones that accept int. TODO: make 4 "carhart" if no region?
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


# TODO: check if ICR model has no RF or Mkt Excess return column
# Base will do this
def _pd_rearrange_cols(data):
    """Rearranges columns of a df to put 'Mkt-RF' first and 'RF' last."""
    if isinstance(data, pd.Series):
        return data

    if not isinstance(data, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame or Series")

    cols = list(data.columns)
    
    if 'Mkt-RF' in cols:
        cols.insert(0, cols.pop(cols.index('Mkt-RF')))
        log.debug("`Mkt-RF` column moved to start")

    if 'RF' in cols:
        cols.append(cols.pop(cols.index('RF')))
        log.debug("`RF` column moved to end of df.")

    return data.loc[:, cols]


def _validate_date(date_input):
    """Converts date formats to a standardized str format."""
    if date_input is None:
        return None

    if isinstance(date_input, int):
        # int should be in the format YYYYMMDD or YYYYMM.
        date_str = str(date_input)
        
        if len(date_str) == 8: # YYYYMMDD
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        
        if len(date_str) == 6:
             return f"{date_str[:4]}-{date_str[4:]}-01"
             
        raise ValueError(f"invalid length int: {date_str}")
    
    # is a timestamp
    if isinstance(date_input, pd.Timestamp):
        return date_input.strftime("%Y-%m-%d")

    # is str input
    if isinstance(date_input, str):
        try:
            # cnvert to datetime
            return pd.to_datetime(date_input).strftime("%Y-%m-%d")
        except (ValueError, pd.errors.ParserError) as err:
            raise ValueError("Incorrect date format, use YYYY-MM-DD.") from err
    try:
        # already a dt object
        return date_input.strftime("%Y-%m-%d")
    except AttributeError:
        raise TypeError(f"Unsupported date type: {type(date_input)}")

### HANDLED BY BASE MODEL NOW!
def _slice_dates(data, start_date=None, end_date=None):
    """Slice the dataframe to the specified date range."""
    if start_date is None and end_date is None:
        return data

    if start_date is not None:
        start_date = _validate_date(start_date)
    if end_date is not None:
        end_date = _validate_date(end_date)

    start = _validate_date(start_date) if start_date else None
    end = _validate_date(end_date) if end_date else None

    if not isinstance(data.index, pd.DatetimeIndex):
        try:
            data.index = pd.to_datetime(data.index)
        except Exception as e:
            raise ValueError("error parsing dates") from e
    
    return data.loc[start:end]


# TODO: KILLING THIS
# Change: moved filepath stuff to a _prepare_filepath helper for now...
def _process(data, start_date=None, end_date=None, filepath=None):
    if not isinstance(data, (pd.DataFrame, pd.Series)):
        raise ValueError("Input data must be a pandas DataFrame or Series")

    data = _pd_rearrange_cols(data)
    data = _slice_dates(data, start_date, end_date)

    if filepath:
        _save_to_file(data, filepath=filepath)

    return data
    

