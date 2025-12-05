# TODO: break this all up into models/*.py !
# TODO: httpx, a client class, model classes... 
from __future__ import annotations
from io import BytesIO
from typing import Optional, Union
#import diskcache as dc
import pandas as pd

from getfactormodels.http_client import HttpClient #testing http_client with a few models

class HMLDevil:
    """ Retrieve the HML Devil factors from AQR.com.

    Notes:
    - Very slow. This model implements a cache, and second run today/this month 
      should be faster.

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

    # CHECK WHEN HML DEVIL STARTS?? Can get back to 1927 (FF3), but NaNs for HMLD

    def __init__(self, frequency: str = 'm', start_date: Optional[str] = None,
                 end_date: Optional[str] = None, output_file: Optional[str] = None):
        self.frequency = frequency.lower()

        if self.frequency not in ["d", "m", "q"]: 
            raise ValueError("Frequency must be 'd', 'm' or 'q'")

        self.start_date = start_date
        self.end_date = end_date
        self.output_file = output_file

        base_url = 'https://www.aqr.com/-/media/AQR/Documents/Insights/'
        file = 'daily' if self.frequency == 'd' else 'monthly'

        self.url = f'{base_url}/Data-Sets/The-Devil-in-HMLs-Details-Factors-{file}.xlsx'

        # From func - cache stuff
        #self.cache_dir = Path('~/.cache/getfactormodels/aqr/hml_devil').expanduser()
        #self.cache_dir.mkdir(parents=True, exist_ok=True)
        #self.cache = dc.Cache(str(self.cache_dir)) # diskcache requires a string path

    def download(self, series: bool = False) -> Union[pd.Series, pd.DataFrame]:
        """
        Download the HML Devil factor from AQR 
        - Pub:
        - Data: AQR.com's datasets
        If is series is True, returns pd.Series
        """
        return self._download(series=series)


    def _download(self, series: bool = False) -> Union[pd.Series, pd.DataFrame]:
        """
        Retrieves the HML Devil factors, using cache if available.
        """
        #current_date = datetime.date.today().strftime('%Y-%m-%d')
        #cache_key = ('hmld', self.frequency, self.start_date, self.end_date, current_date)
        #data, cached_end_date = self.cache.get(cache_key, default=(None, None))

        #if data is not None and (self.end_date is None or self.end_date <= cached_end_date):
        #    print("Data retrieved from cache.")
        #    return data['HML_Devil'] if series else data
        xls = self._aqr_download_data()
        data = self._aqr_process_data(xls)

        if self.start_date:
            data = data[data.index >= self.start_date]
        if self.end_date:
            data = data[data.index <= self.end_date]

        #actual_end_date = data.index.max().strftime('%Y-%m-%d') if not data.empty else self.end_date
        #self.cache[cache_key] = (data, actual_end_date)

        #if self.output_file:
        #     data.to_csv(self.output_file) # TODO: filewriter when pa
        return data


    def _aqr_download_data(self):  #FIX:context manager!
        with HttpClient(timeout=8.0) as client:
            resp = client.download(self.url, as_bytes=True)
            xls = pd.ExcelFile(BytesIO(resp))
            return xls


    def _aqr_process_data(self, xls: pd.ExcelFile) -> pd.DataFrame:
        """Process the downloaded Excel file."""
        sheets = {0: 'HML Devil', 4: 'MKT', 5: 'SMB', 7: 'UMD', 8: 'RF'}
        dfs = []
        
        df_dict = pd.read_excel(
            xls,
            sheet_name=list(sheets.values()),
            skiprows=18,
            header=0,
            index_col=0,
            parse_dates=True
        )

        for sheet_index, sheet_name in sheets.items():
            df = df_dict[sheet_name]
            # Only take USA column for non-RF sheets
            if sheet_index not in [8, 0]:  # 8 is RF, 0 is HML Devil
                df = df[['USA']]
            else:
                df = df.iloc[:, 0:1]  # First column
            df.columns = [sheet_name]
            dfs.append(df)

        data = pd.concat(dfs, axis=1)
        data = data.dropna(subset=['RF', 'UMD'])
        data = data.astype(float)

        # Rename columns to standard factor names
        data.rename(columns={
            'MKT': 'Mkt-RF', 
            'HML Devil': 'HML_Devil'
        }, inplace=True)
        #TEST 
        #print(data.to_string(float_format=lambda x: f'{x:.6f}'))
        return data
