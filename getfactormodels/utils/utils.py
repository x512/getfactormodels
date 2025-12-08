# -*- coding: utf-8 -*-
import re
import zipfile as zip
from datetime import datetime
from io import BytesIO, StringIO
from pathlib import Path
from types import MappingProxyType
import pandas as pd
import requests
from dateutil import parser

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
    * This provides more flexibility in input by converting various model names
    to a standardized model key.

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


def get_file_from_url(url):
    """Get a file from a URL and return its content as a StringIO object."""
    response = requests.get(url, verify=True, timeout=15)
    response.raise_for_status()
    response_content = response.content.decode('utf-8')
    content = StringIO(response_content)
    return content


def get_zip_from_url(url):
    """Download a zip file from a URL and return a ZipFile object."""
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        content = response.content
    except (KeyboardInterrupt, Exception) as e:
        print(f"An error occurred downloading the zip file from {url}: {e}")
        raise

    return zip.ZipFile(BytesIO(content))


def _save_to_file(data, filename=None, output_dir=None):
    """Save a pandas dataFrame to a file."""
    if isinstance(data, (pd.DataFrame, pd.Series)):
        formats = {
            '.txt': lambda filename: data.to_csv(filename, sep='\t'),
            '.csv': data.to_csv,
            '.xlsx': data.to_excel,  # TODO: style with writer
            '.pkl': data.to_pickle,
            '.md': data.to_markdown, }

        if filename is None:
            filename = datetime.now().strftime('%Y_%m_%d-%H%M') \
                + '.csv'
        elif '.' not in filename:
            filename += '.csv'

        # If no output directory is provided, use cwd
        if output_dir is None:
            output_dir = Path.cwd()
        else:
            # Expand the '~' character in the output directory
            output_dir = Path(output_dir).expanduser()

        # Create the full file path
        filename = output_dir / filename

        # Check if file exists
        if filename.is_file():
            print('File exists: overwriting...')

        for ext, func in formats.items():
            if str(filename).endswith(ext):
                func(str(filename))
                print(f"File saved to: {filename}")
                break

        else:
            raise ValueError('Unsupported file extension')
    else:
        raise ValueError('Data is not a pandas DataFrame or Series')


def _rearrange_cols(data):
    """Rearrange the columns of the dataframe.
    * NOTE: this is faster:
            cols = data.columns.values
            cols_order = np.concatenate(([np.where(cols == 'Mkt-RF')[0], \
                np.where((cols != 'Mkt-RF') & (cols != 'RF'))[0], \
                    np.where(cols == 'RF')[0]]))
            return data.iloc[:, cols_order]
    """
    # [TODO] ICR model has no RF or Mkt Excess return column
    if isinstance(data, pd.Series):
        return data
    cols = list(data.columns)
    if 'Mkt-RF' in cols:
        cols.insert(0, cols.pop(cols.index('Mkt-RF')))
    if 'RF' in cols:
        cols.append(cols.pop(cols.index('RF')))
    return data.loc[:, cols]


def _validate_date(date_str):
    """Use `dateutil.parser.parse` to validate a date format."""
    if date_str is None:
        return None
    if isinstance(date_str, pd.Timestamp):
        return date_str.strftime("%Y-%m-%d")
    try:
        return parser.parse(date_str).strftime("%Y-%m-%d")
    except ValueError as err:
        raise ValueError("Incorrect date format, use YYYY-MM-DD.") from err


def _slice_dates(data, start_date=None, end_date=None):
    """Slice the dataframe to the specified date range."""
    if start_date is None and end_date is None:
        return data

    if start_date is not None:
        start_date = _validate_date(start_date)
    if end_date is not None:
        end_date = _validate_date(end_date)

    return data.loc[slice(start_date, end_date)]


def _process(data: pd.DataFrame,
             start_date: str = None,
             end_date: str = None,
             filepath: str = None) -> pd.DataFrame:
    """Process the data and optionally save it to a file.
    Note: the `filepath` takes a filename, path or directory.
    """
    data = _rearrange_cols(data)
    data = _slice_dates(data, start_date, end_date)

    if filepath:
        # Convert the filepath to a Path object and expand the '~' character
        filepath = Path(filepath).expanduser()

        # If filepath is a directory, append a default file name to it
        if filepath.is_dir():
            filename = datetime.datetime.now().strftime('%Y%m%d%H%M')
            filepath = filepath / filename

        dir_path, filename = filepath.parent, filepath.name

        _save_to_file(data, filename, dir_path)

    return data
