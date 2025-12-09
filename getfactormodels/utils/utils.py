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

logging.basicConfig(level=logging.ERROR) #default
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
    "BarillasShanken": r"\b(bs|bs6|barillas|shanken)\b", })


def _get_model_key(model):
    """
    Convert a model name to a model key.

    >>> _get_model_key('ff1993')
    '3'
    >>> _get_model_key('liQ')
    'liquidity'
    >>>  _get_model_key('q4_factors')
    'q_classic'
    >>> _get_model_key('ICR')
    'icr'
    """
    model = str(model)

    for key, regex in __model_input_map.items():
        if re.match(regex, model, re.I):
            return key
    raise ValueError(f'Invalid model: {model}')


# TODO: Will redo as a Writer class with use pyarrow
# changing: no longer uses filename, output_dir, just filepath. Always returns Path 
def _prepare_filepath(filepath=None) -> Path:
    if filepath is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filepath = Path.cwd() / f"data_{timestamp}.csv"
        print(f"No filepath provided, creating: {filepath.name}")
        return filepath
    
    filepath = Path(filepath).expanduser()
    
    if filepath.is_dir():
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filepath = filepath / f"data_{timestamp}.csv"
        print(f"Directory provided, creating: {filepath.name}")
    
    return filepath


def _save_to_file(data, filepath=None):
    if not isinstance(data, (pd.DataFrame, pd.Series)):
            raise ValueError('Data is not a pandas DataFrame or Series')
    full_path = _prepare_filepath(filepath)
    
    if full_path is None:
        raise ValueError("Failed to prepare filepath")
    
    extension = full_path.suffix.lower()
 
    if full_path.is_file():
        print(f'File exists: {full_path.name} - overwriting...')

    try:
        if extension == '.txt':
            data.to_csv(str(full_path), sep='\t')
        elif extension == '.csv':
            data.to_csv(str(full_path))
        elif extension == '.xlsx':
            data.to_excel(str(full_path))
        elif extension == '.pkl':
            data.to_pickle(str(full_path))
       # elif extension == '.md':
       #    #.md removed for now 
        else:
            supported = ['.txt', '.csv', '.xlsx', '.pkl']
            raise ValueError(f'Unsupported file extension: {extension}. Must be one of: {supported}')

        print(f"File saved to: {full_path}")
    except Exception as e:
        raise IOError(f"Failed to save file to {full_path}: {str(e)}")


# TODO: check if ICR model has no RF or Mkt Excess return column
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

# Change: moved filepath stuff to a _prepare_filepath helper for now...
def _process(data, start_date=None, end_date=None, filepath=None):
    if not isinstance(data, (pd.DataFrame, pd.Series)):
        raise ValueError("Input data must be a pandas DataFrame or Series")

    data = _pd_rearrange_cols(data)
    data = _slice_dates(data, start_date, end_date)

    if filepath:
        _save_to_file(data, filepath=filepath)

    return data
    

