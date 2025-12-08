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
# TODO: break this all up into models/*.py !
# TODO: httpx, a client class, model classes... (done done and done) 
from io import BytesIO
from typing import Optional, Union  # can drop Union for "|" if dropping py 3.9
import pandas as pd  # need to type everything properly,
from getfactormodels.http_client import HttpClient

                                         # implement typechecking, testing

class HMLDevil:
    """Retrieve the HML Devil factors from AQR.com.

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
    def __init__(self, frequency: str = 'm', start_date: Optional[str] = None,
                 end_date: Optional[str] = None, output_file: Optional[str] = None, cache_ttl: int = 43200):
        
        self.cache_ttl = cache_ttl
        self.frequency = frequency.lower()

        if self.frequency not in ["d", "m", "q"]: 
            raise ValueError("Frequency must be 'd', 'm' or 'q'")

        self.start_date = start_date
        self.end_date = end_date
        self.output_file = output_file

        base_url = 'https://www.aqr.com/-/media/AQR/Documents/Insights/'
        file = 'daily' if self.frequency == 'd' else 'monthly'
        self.url = f'{base_url}/Data-Sets/The-Devil-in-HMLs-Details-Factors-{file}.xlsx'

        # From func - cache stuff -- OLD CACHE STUFF
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
        xls = self._aqr_download_xls()
        data = self._aqr_process_data(xls)

        if self.start_date:
            data = data[data.index >= self.start_date]
        if self.end_date:
            data = data[data.index <= self.end_date]

        # Will be a util for file writer. TODO
        #actual_end_date = data.index.max().strftime('%Y-%m-%d') if not data.empty else self.end_date
        #self.cache[cache_key] = (data, actual_end_date)

        #if self.output_file:
        #     data.to_csv(self.output_file) # TODO: filewriter when pa
        return data


    def _aqr_download_xls(self) -> pd.ExcelFile:
        with HttpClient(timeout=15) as client:  # should cache do 12? 24? Hours from the file modified dt?
            # Really should check cache here! (cache_ttl was below frequency in
            # init and errored the whole model?!)... Not good! FIXME FIXME after
            # base model
            resp = client.download(self.url)
            xls = pd.ExcelFile(BytesIO(resp))
            return xls

    # FIXME: takes ages reading (not just downloading) TODO TODO
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
            # Only take USA column for non-RF sheets   # TODO: countries
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
            'MKT': 'Mkt-RF',     # TODO: double check!! This could just be mkt.
            'HML Devil': 'HML_Devil'
        }, inplace=True)

        # Drops all data where HML_Devil is NaN
        data = data.dropna(subset=['HML_Devil'])

        return data

# TODO: can see the last modified date in debug log; possibly set cache
# according to this
