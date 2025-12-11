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
from typing import Any
import pandas as pd
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.utils import _process


class HMLDevilFactors(FactorModel): # Note HMLDevil -> HMLDevilFactors (to keep consistent)
    """Retrieve the HML Devil factors from AQR.com.

    Notes:
    - Very slow. This model implements a cache, and second run today/this month 
      should be faster.

    Parameters:
        frequency (str): The frequency of the data. M, D (default: M)
        start_date (str, optional): The start date of the data, YYYY-MM-DD.
        end_date (str, optional): The end date of the data, YYYY-MM-DD.
        output_file (str, optional): The filepath to save the output data.


    Returns:
        pd.DataFrame: the HML Devil model data indexed by date.
        """
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m"]  # TODO: Check AQR's HML Devil frequencies again. 

    def __init__(self, frequency: str = 'm', cache_ttl: int = 43200, **kwargs: Any) -> None:
        self.cache_ttl = cache_ttl
        super().__init__(frequency=frequency, cache_ttl=cache_ttl, **kwargs)

    def _get_url(self) -> str:
        base_url = 'https://www.aqr.com/-/media/AQR/Documents/Insights/'
        file = 'daily' if self.frequency == 'd' else 'monthly'

        return f'{base_url}/Data-Sets/The-Devil-in-HMLs-Details-Factors-{file}.xlsx'


    def download(self):
        _data = self._download()
        xls = pd.ExcelFile(BytesIO(_data))
        data = self._aqr_process_data(xls)

        if data is None:
            print("Error.")

        return data
# --------------------------------------------------------------------------------#
    def _aqr_process_data(self, xls) -> pd.DataFrame:
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

        return _process(data, self.start_date, self.end_date, filepath=self.output_file)
        # From func - cache stuff -- OLD CACHE STUFF
        #self.cache_dir = Path('~/.cache/getfactormodels/aqr/hml_devil').expanduser()
        #self.cache_dir.mkdir(parents=True, exist_ok=True)
        #self.cache = dc.Cache(str(self.cache_dir)) # diskcache requires a string path
# TODO: can see the last modified date in debug log; possibly set cache
# according to this
