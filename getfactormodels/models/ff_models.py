# -*- coding: utf-8 -*-
"""Private module: helper functions for the Fama-French models.

Includes functions for constructing the URL for the specified model (or
momentum factor) and frequency, reading the CSV into a DataFrame, and
processing the data.

Functions:
- ``_ff_construct_url`` - get the URL for the specified model and frequency.
- ``_ff_read_csv_from_zip`` - reads the .csv from a .zip into a dataframe.
- ``_ff_process_data`` - processes the data.
- ``_ff_get_mom`` - fetches the momentum factor data as a pd.Series.
- ``_get_ff_factors`` - returns the Fama French 3, 5, or 6, or Carhart 4 factor
                        model data.

Notes:
- ``_get_ff_factors`` is the one that's called by ``get_ff_factors`` in
    ``models.py``.
- ``_ff_get_mom`` is only for returning the raw data for the 4 and 6 factor
    models construction.

"""
import numpy as np
import pandas as pd
from ..utils.utils import (  # noqa - todo: fix relative import from parent modules banned
    _process, get_zip_from_url)


def _ff_construct_url(model="3", frequency="M"):
    """Construct and return the URL for the specified model and frequency."""
    frequency = frequency.upper()

    if frequency == "W" and model not in ["3", "4"]:  # why 4?
        raise ValueError("Weekly data is only available for the Fama \
                         French 3 factor model at the moment.")

    base_url = "https://mba.tuck.dartmouth.edu"
    ftp = "pages/faculty/ken.french/ftp"

    file = f'F-F_{"Research_Data_" if model in ["3", "4", "5", "6"] else ""}'
    file += ("Factors" if model in ["3", "4"]
             else "5_Factors_2x3" if model in ["5", "6"]
             else "")
    file += "_daily" if frequency == "D" \
        else "_weekly" if frequency == "W" else ""
    file += "_CSV.zip"

    return f"{base_url}/{ftp}/{file}"


def _ff_read_csv_from_zip(zip_file, model=None):
    """Read the FF Factors CSV into a dataframe."""
    try:
        filename = zip_file.namelist()[0]
        with zip_file.open(filename) as file:
            data = pd.read_csv(
                file,
                skiprows=12 if 'momentum' in filename.lower() else 3 if 'ly' in filename.lower() else 2,  # noqa: E501
                index_col=0,
                header=0,
                parse_dates=False,
                skipfooter=1,
                engine="python")

            data.index = data.index.astype(str)
            data.index = data.index.str.strip()
            data.index.name = "date"
            data = data.dropna()
    except Exception as e:
        print(f"Error reading file: {e}")
        return None
    return data


def _ff_process_data(data, model, frequency) -> pd.DataFrame:
    """Process and return the data based on the provided model and frequency.
    """
    frequency = frequency.lower()

    if frequency == 'm':
        data = data[data.index.str.len() == 6]
    elif frequency == 'y':
        data = data[data.index.str.len() == 4]
    else:
        data = data[data.index.str.len() == 8]

    try:
        if frequency == 'm':
            data.index = pd.to_datetime(data.index, format='%Y%m') \
                + pd.offsets.MonthEnd(0)
        else:
            data.index = pd.to_datetime(data.index, format='%Y%m%d')

    except Exception:
        data.index = pd.to_datetime(data.index, format='%Y') \
            + pd.offsets.YearEnd(0, month=12)

    data.index.name = "date"

    # All values (eg, 4/D, are <5% distinct).
    # If <10% distinct, categorize
    # if len(data) / data.nunique() < 10:
    #     data = data.astype('category')

    return data


def _ff_get_mom(frequency) -> pd.Series:
    """Fetch and return the momentum factor data as a pd.Series.
        * Note: only for returning the raw data for the 4 and 6 factor models.
    """
    frequency = frequency.upper()
    base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"
    file = "F-F_Momentum_Factor_daily_CSV.zip" if frequency == "D" \
        else "F-F_Momentum_Factor_CSV.zip"
    url = f"{base_url}/{file}"

    csv = _ff_read_csv_from_zip(get_zip_from_url(url))

    csv.columns = ["MOM"]
    csv.index.name = "date"

    csv.index = csv.index.astype(str)
    csv.index = csv.index.str.strip()

    return csv


def _get_ff_factors(model: str = "3",
                    frequency: str = "M",
                    start_date=None,
                    end_date=None) -> pd.DataFrame:
    """Return the Fama French 3, 5, or 6, or Carhart 4 factor model data.

        * Note: This is the function that's called by get_ff_factors in main.
    """
    if frequency is None:
        frequency = "M"

    if frequency.upper() not in ["D", "M", "Y", "W"]:
        raise ValueError("Frequency must be one of: D, M, Y, or W.")
    elif model not in ["3", "5", "6", "4"]:
        raise ValueError(f"Invalid model passed to private function \
                     _get_ff_factors, must be one of: 3, 5, 6, or 4, \
                     not {model}. If you see this error message please \
                     submit an issue at:\
                         https://github.com/x512/getfactormodels/issues/")

    url = _ff_construct_url(model, frequency)
    zip = get_zip_from_url(url)
    csv = _ff_read_csv_from_zip(zip, model)

    if model in ["4", "6"]:
        mom = _ff_get_mom(frequency)
        if model == "6":
            mom = mom.rename(columns={"MOM": "UMD"})
        mom = pd.DataFrame(mom)
        csv = csv.join(mom, how="left")

    data = _ff_process_data(csv, model, frequency)
    data = data.apply(pd.to_numeric, errors='ignore')

    if start_date is not None or end_date is not None:
        data = data.loc[start_date:end_date]

    data = data.dropna()

    data = np.multiply(data, 0.01)
    return _process(data, start_date, end_date)
