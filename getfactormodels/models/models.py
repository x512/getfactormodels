# -*- coding: utf-8 -*-
"""
models
=======
Functions for retrieving and processing multi-factor model data.

Functions for fetching data for a variety of factor models. The data can be
returned for different frequencies and for a specified date range. The function
supports a variety of model names that match specific regex patterns, including
'liquidity', 'icr', 'dhs', 'q', 'q_classic', 'ff3', 'ff5', 'ff6', 'carhart4',
'hml_devil', 'barrilas_shanken', and 'mispricing'.

Functions
---------
- `ff_factors`: Retrieves data for a specified Fama-French or Carhart factor
    model.
- `q_factors`: Retrieves the q-factor model data from global-q.org.
- `q_classic_factors`: Retrieves the original 4-factor "q" model of Hou, Xue,
    and Zhang (2015).
- `dhs_factor`: Retrieves the Daniel-Hirshleifer-Sun Behavioural factors.
- `icr_factors`: Retrieves the He, Kelly, Manela (2017) ICR factors.
- `hml_devil_factors`: Retrieves the HML Devil factors from AQR.
- `barillas_shanken_factors`: Constructs the 6-factor model of Barillas and
    Shanken.
- `carhart_factors`: Retrieves the Carhart 4-factor model data.
- `liquidity_factors`: Retrieves the Pastor-Stambaugh liquidity factors.
- `mispricing_factors`: Retrieves the Stambaugh-Yuan (201x) mispricing factors.
"""
import os
import pickle
from io import BytesIO
from typing import Optional, Union
import numpy as np
import pandas as pd
import requests
from getfactormodels.utils.utils import _process, get_file_from_url
from .ff_models import _get_ff_factors


def ff_factors(model: str = "3",  # TODO: fix: _get_ff_factors filepath param
               frequency: str = "M",
               start_date: str = None,
               end_date: str = None,
               output: str = None) -> pd.DataFrame:
    """Get data for a specified Fama-French or Carhart factor model.

    This function returns a DataFrame containing the 3-factor (1993), 5-factor
    (2015), or 6-factor (2018) model of Fama & French, or Carhart's (1997)
    4-factor model. Data is available in daily, weekly, monthly, and annual
    frequencies. If an output is specified, saves the data to a file.

    Notes:
    - Only the 3-factor model offers weekly data.
    - Dates should be in ``YYYY-MM-DD`` format, but anything that
      ``dateutil.parser.parse()`` can interpret will work.

    Parameters:
        model (str, int): the Fama-French or Carhart factor data to return. 3, 4, 5
            or 6 (default: 3).
        frequency (str): the frequency of the data. Accepts D, W, M or Y
            (default: M).
        start_date (str, optional): the start date of the data, as YYYY-MM-DD.
        end_date (str, optional): the end date of the data, as YYYY-MM-DD.
        output (str, optional): a filename, directory, or filepath. If no
            extension is provided, will output a '.csv'. Accepts '.txt',
            '.csv', '.md', '.xlsx', '.pkl'.

    Returns:
        pandas.DataFrame: factor data, indexed by date.
    """
    model = str(model)
    data = _get_ff_factors(model, frequency, start_date, end_date)
    return _process(data, start_date, end_date, filepath=output)


def liquidity_factors(frequency: str = "M",
                      start_date: str = None,
                      end_date: str = None,
                      output: str = None) -> pd.DataFrame:
    """Retrieve the Pastor-Stambaugh liquidity factors. Monthly data only."""
    url = 'https://research.chicagobooth.edu/'
    url += '-/media/research/famamiller/data/liq_data_1962_2022.txt'

    if frequency.lower() != 'm':
        print('Liquidity factors are only available for monthly frequency.')
        raise ValueError("Frequency must be 'm'.")

    # Get .csv here...
    data = get_file_from_url(url)

    # Headers are last commented line
    headers = [line[1:].strip().split('\t')
               for line in data.readlines() if line.startswith('%')][-1]

    # Fix: was losing first line of data
    data.seek(0)

    # ...read .csv here
    data = pd.read_csv(data, sep='\\s+', names=headers,
                       comment='%', index_col=0)

    data.index.name = 'date'
    data.index = data.index.astype(str)

    data = data.rename(columns={'Agg Liq.': 'AGG_LIQ',
                                'Innov Liq (eq8)': 'INNOV_LIQ',
                                'Traded Liq (LIQ_V)': 'TRADED_LIQ'})

    # The first 65 values in the traded liquidity series are -99.000000.
    data['TRADED_LIQ'] = data['TRADED_LIQ'].replace(-99.000000, 0)

    if frequency.lower() == 'm':
        data.index = pd.to_datetime(data.index, format='%Y%m') \
            + pd.offsets.MonthEnd(0)

    return _process(data, start_date, end_date, filepath=output)


def mispricing_factors(frequency: str = "M",
                       start_date: str = None,
                       end_date: str = None,
                       output: str = None) -> pd.DataFrame:
    """Retrieve the Stambaugh-Yuan mispricing factors. Daily and monthly."""
    if frequency.lower() not in ["d", "m"]:
        print("Mispricing factors are only available for daily and monthly \
            frequency.")
        raise ValueError("Frequency must be 'd' or 'm'.")
        return None

    file = "M4d" if frequency == "d" else "M4"
    url = f"https://finance.wharton.upenn.edu/~stambaug/{file}.csv"

    data = get_file_from_url(url)

    data = pd.read_csv(data, index_col=0, parse_dates=False,
                       date_format="%Y%m%d", engine="pyarrow")  # only model
                                                                # using pyarrow?  # noqa

    data = data.rename(columns={"SMB": "SMB_SY",
                                "MKTRF": "Mkt-RF"}).rename_axis("date")

    if frequency == "d":
        data.index = pd.to_datetime(data.index, format="%Y%m%d")
    elif frequency == "m":
        data.index = pd.to_datetime(data.index, format="%Y%m")
        data.index = data.index + pd.offsets.MonthEnd(0)

    return _process(data, start_date, end_date, filepath=output)


def q_factors(frequency: str = "M",
              start_date: str = None,
              end_date: str = None,
              output: str = None,
              classic: bool = False) -> pd.DataFrame:
    """Retrieve the q-factor model data."""
    frequency = frequency.upper()
    file = {"M": "monthly",
            "D": "daily",
            "Q": "quarterly",
            "W": "weekly",
            "Y": "annual", }.get(frequency)

    base_url = 'https://global-q.org/uploads'
    url = f"{base_url}/1/2/2/6/122679606/q5_factors_{file}_2022.csv"

    index_cols = [0, 1] if frequency in ["M", "Q"] else [0]
    data = pd.read_csv(
        url, parse_dates=False, index_col=index_cols, float_precision="high")

    if classic:
        data = data.drop(columns=["R_EG"])

    data = data.rename(columns={"R_F": "RF"})

    data = np.multiply(data, 0.01)

    if frequency in ["M", "Q"]:
        # Need to insert "-" (monthly) or "Q" (quarterly) into date str.
        data = data.reset_index()
        col = "quarter" if frequency == "Q" else "month"
        char = "Q" if frequency == "Q" else "-"

        data["date"] = pd.PeriodIndex(
            data["year"].astype(str)
            + char
            + data[col].astype(str), freq=frequency
        ).to_timestamp(how="end")

        data["date"] = data["date"].dt.normalize()
        data = data.drop(["year", col], axis=1).set_index("date")

    if frequency == "Y":
        data.index = pd.to_datetime(data.index.astype(str)) \
            + pd.offsets.YearEnd(0)
    else:
        data.index = pd.to_datetime(data.index.astype(str))

    data.columns = data.columns.str.upper()
    data.index.name = "date"
    data = data.rename(columns={"R_MKT": "Mkt-RF"})

    return _process(data, start_date, end_date, filepath=output)


# Daniel-Hirshleifer-Sun Behavioural Factors
def dhs_factors(frequency: str = "M",
                start_date: str = None,
                end_date: str = None,
                output: str = None) -> pd.DataFrame:
    """Retrieve DHS factors from sheets on Lin Sun's website."""
    frequency = frequency.lower()
    base_url = "https://docs.google.com/spreadsheets/d/"

    if frequency.lower() == "m":
        file = "1RxYLbCfk19m8fnniiJYfaj3yI55ZPaoi/export?format=xlsx"
    elif frequency.lower() == "d":
        file = "1KnCP-NVhf2Sni8bVFIVyMxW-vIljBOWE/export?format=xlsx"
    else:
        print("Frequency must be either 'M' (monthly) or 'D' (daily).")
        raise ValueError("Frequency must be 'M' or 'D'.")
    # TODO: use the link to the Google Sheet instead of the actual sheet.

    url = base_url + file

    response = requests.get(url, verify=True, timeout=20)
    file = BytesIO(response.content)

    data = pd.read_excel(file, index_col="Date",
                         usecols=['Date', 'FIN', 'PEAD'], engine='openpyxl',
                         header=0, parse_dates=False)
    data.index.name = "date"

    if frequency.lower() == "d":
        data.index = pd.to_datetime(data.index, format="%m/%d/%Y")
    else:
        data.index = pd.to_datetime(data.index, format="%Y%m")
        data.index = data.index + pd.offsets.MonthEnd(0)

    data = np.multiply(data, 0.01)  # Decimalize before FF factors!

    ff = _get_ff_factors(model="3", frequency=frequency,
                         start_date=data.index[0], end_date=data.index[-1])
    ff = ff.round(4)
    # Note: FF source data is to 4 decimals; re-rounding here to avoid
    #       rounding errors (e.g., 0.02 --> 0.019999999999999997)
    data = pd.concat([ff["Mkt-RF"], data, ff["RF"]], axis=1)
    data.index.name = "date"

    return _process(data, start_date, end_date, filepath=output)


def icr_factors(frequency: str = "M",
                start_date: str = None,
                end_date: str = None,
                output: str = None) -> pd.DataFrame:
    """Retrieve the He, Kelly, Manela (2017) ICR factors.
    * Daily since 1999-05-03; quarterly and monthly since 1970.
    """
    # TODO: Do we need Mkt-RF and RF [seen reffered to as 2-factor model]?
    frequency = frequency.lower()

    if frequency not in ["d", "m", "q"]:
        raise ValueError("Frequency must be 'd', 'm' or 'q'.")

    base_url = "https://voices.uchicago.edu/zhiguohe"
    file = {"d": "daily", "m": "monthly", "q": "quarterly"}.get(frequency)
    url = f"{base_url}/files/2023/10/He_Kelly_Manela_Factors_{file}.csv"

    df = get_file_from_url(url)
    df = pd.read_csv(df)
    df = df.rename(columns={df.columns[0]: "date"})

    # Just doing dates here for now...
    if frequency == "q":
        # The dates are YYYYQ. [19752 -> 1975Q2]
        df["date"] = df["date"].astype(str)
        df["date"] = df["date"].str[:-1] + "Q" + df["date"].str[-1]
        df["date"] = pd.PeriodIndex(df["date"], freq="Q").to_timestamp() \
            + pd.offsets.QuarterEnd(0)

    df = df.rename(columns={
            "intermediary_capital_ratio": "IC_RATIO",
            "intermediary_capital_risk_factor": "IC_RISK_FACTOR",
            "intermediary_leverage_ratio_squared": "INT_LEV_RATIO_SQ",
            "intermediary_value_weighted_investment_return": "INT_VW_ROI", })

    if frequency == "m":
        df["date"] = pd.to_datetime(df["date"], format="%Y%m")
        df["date"] = df["date"] + pd.offsets.MonthEnd(0)

    elif frequency == "d":
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")

    df = df.set_index("date")

    return _process(df, start_date, end_date, filepath=output)


def q_classic_factors(frequency: str = "M",
                      start_date: str = None,
                      end_date: str = None,
                      output: str = None) -> pd.DataFrame:
    """Retrieve the classic q-factor model of Hou, Xue, and Zhang (2015)."""
    return q_factors(frequency, start_date, end_date, output=output,
                     classic=True)


def carhart_factors(frequency: str = "M",
                    start_date: str = None,
                    end_date: str = None,
                    output: str = None) -> pd.DataFrame:
    """Retrieve the Carhart 4-factor model data."""
    data = _get_ff_factors(model='4', frequency=frequency,
                           start_date=start_date,
                           end_date=end_date)
    return _process(data, start_date, end_date, filepath=output)


def _create_cache():
    cache_dir = os.path.expanduser('~/.cache/getfactormodels')
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    return cache_dir


def hml_devil_factors(frequency='M',
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None,
                      output: Optional[str] = None,
                      series=False) -> Union[pd.Series, pd.DataFrame]:
    """***EXPERIMENTAL***

    Retrieve the HML Devil factors from AQR.com. [FIXME: Slow.]

    Notes:
    - Slow. Very slow. So we implement a cache and it doesn't need to run
    again until tomorrow (daily) or next month.

    Parameters:
        frequency (str): The frequency of the data. M, D (default: M)
        start_date (str, optional): The start date of the data, YYYY-MM-DD.
        end_date (str, optional): The end date of the data, YYYY-MM-DD.
        output (str, optional): The filepath to save the output data.
        series (bool, optional): If True, return the HML Devil factors as a
            pandas Series.

    Returns:
        pd.DataFrame: the HML Devil model data indexed by date.
        pd.Series: the HML factor as a pd.Series
    """
    _create_cache()  # TODO: allow config and cache_file [not pickle]
    pickle_file = os.path.expanduser(f'~/.cache/getfactormodels/hml_devil_{frequency}.pkl')  # noqa

    # Get the current date and the date of the file creation
    current_date = pd.to_datetime('today')
    if os.path.exists(pickle_file):
        file_date = pd.to_datetime(os.path.getmtime(pickle_file), unit='s')

        # If the pickle file exists and is not expired, load the df from it
        if (frequency.lower() == 'd' and file_date.day == current_date.day) or \
                (frequency.lower() != 'd' and file_date.month == current_date.month):  # noqa
            with open(pickle_file, 'rb') as f:
                if series:
                    return _process(pickle.load(f),  # read pickle instead, csv ?  # noqa
                                    start_date, end_date).HML_DEVIL
                else:
                    data = _process(pickle.load(f), start_date, end_date)
                    data = data.dropna()
                    return data
        # If the pickle file is expired, delete it
        else:
            os.remove(pickle_file)

    base_url = 'https://www.aqr.com/-/media/AQR/Documents/Insights/'
    file = 'daily' if frequency.lower() == 'd' else 'monthly'
    url = f'{base_url}/Data-Sets/The-Devil-in-HMLs-Details-Factors-{file}.xlsx'

    print('Downloading HML Devil factors from AQR... This can take a while. Please be patient or something.')  # noqa

    # TODO: A progress bar until something's figured out? download_with_progress  # noqa
    # TODO: Handle interupts SIGINT, etc.
    response = requests.get(url, verify=True, timeout=180)
    xls = pd.ExcelFile(BytesIO(response.content))

    sheets = {0: 'HML Devil', 4: 'MKT', 5: 'SMB', 7: 'UMD', 8: 'RF'}
    dfs = []

    df_dict = pd.read_excel(xls,
                            sheet_name=list(sheets.values()),
                            skiprows=18,
                            header=0,
                            index_col=0,
                            parse_dates=True)

    for sheet_index, sheet_name in sheets.items():
        df = df_dict[sheet_name]

        # Use 'USA' col, except for the RF sheet, use cols first two cols
        df = df[['USA']] if sheet_index != 8 else df.iloc[:, 0:1]
        # TODO: allow for other countries

        df.columns = [sheet_name]
        dfs.append(df)

    data = pd.concat(dfs, axis=1)
    data.rename(columns={'MKT': 'Mkt-RF',
                         'HML Devil': 'HML_DEVIL'}, inplace=True)
    data = data.astype(float)

    with open(pickle_file, 'wb') as f:
        pickle.dump(data, f)

    if os.path.exists('hml_devil.csv'):
        os.remove('hml_devil.csv')

    data.index.name = 'date'

    data.index = pd.to_datetime(data.index)

    if frequency.lower() == 'd':
        data = data.dropna()

    if series:
        return _process(data, start_date, end_date, filepath=output).HML_DEVIL

    return _process(data, start_date, end_date, filepath=output)


def barillas_shanken_factors(frequency: str = 'M',
                             start_date: str = None,
                             end_date: str = None,
                             output: str = None) -> pd.DataFrame:
    """***Experimental.***

    Constructs the 6-factor model of Barillas and Shanken.  It's a
    combination of the 5-factor model of Fama and French (2015), the q-factor
    model of Hou, Xue, and Zhang (2015), and Asness and Frazzini's HML Devil.
    This is the factor model with the highest posterior inclusion probability
    in Barillas and Shanken (2018).

    Note:
        - Relies on the HML Devil factors being retrieved (which is very slow).

    Returns:
        pd.DataFrame: A timeseries of the factor data.
    """
    q = q_factors(frequency=frequency, classic=True)[['R_IA', 'R_ROE']]
    ff = ff_factors(model='6', frequency=frequency)[['Mkt-RF', 'SMB', 'UMD',
                                                     'RF']]

    df = pd.merge(q, ff, left_index=True, right_index=True, how='inner')

    hml_devil = hml_devil_factors(frequency=frequency, start_date=start_date,
                                  series=True)

    hml_devil = hml_devil.rename('HML_m')
    hml_devil.index.name = 'date'

    df = pd.merge(df, hml_devil, left_index=True,
                  right_index=True, how='inner')

    return _process(df, start_date, end_date, filepath=output)
