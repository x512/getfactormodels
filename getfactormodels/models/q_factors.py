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
import io
import pandas as pd
from getfactormodels.http_client import HttpClient
from getfactormodels.utils.utils import _process


class QFactors:
    """
    Download the q or q5 factor models from global-q.org.

    params:
        frequency (str): The data frequency ('d', 'w', 'm', 'q', 'y'). Defaults to 'm'.
        start_date (str, datetime): The start date YYYY-MM-DD
        end_date (str, datetime): The end date YYYY-MM-DD
        output_file (str):
        classic (bool): If True, returns the classic 4-factor model (drops R_EG).
        cache_ttl (int): Time-to-live for cache in seconds (default 86400s/1 day).

    Sources:

    - Hou, Kewei, Haitao Mo, Chen Xue, and Lu Zhang, 2021, An augmented q-factor model
      with expected growth, Review of Finance 25 (1), 1-41. (q5 model)
    - Hou, Kewei, Chen Xue, and Lu Zhang, 2015, Digesting anomalies: An investment
      approach, Review of Financial Studies 28 (3), 650-705. (Classic q-factor model)
    
    Data Source URL: https://global-q.org/factors.html
    """
    # Note: weekly wednesday-to-wednesday needs to be added TODO:
    def __init__(self, frequency='m', start_date=None, end_date=None,
                 output_file=None, classic=False, cache_ttl: int = 86400):
        self.frequency = frequency.lower()
        
        if self.frequency not in ["d", "m", "w", "q"]:
            raise ValueError("Frequency must be 'd', 'w', 'm' or 'q'")

        self.file = {'m': "monthly",
                "d": "daily",
                "q": "quarterly",
                "w": "weekly",
                "y": "annual", }.get(self.frequency)
        self.classic = classic
        self.url = f'https://global-q.org/uploads/1/2/2/6/122679606/q5_factors_{self.file}_2024.csv' # TODO: YEAR
        self.start_date = start_date
        self.end_date = end_date
        self.output_file = output_file
        self.cache_ttl = cache_ttl

    def download(self) -> pd.DataFrame:
        return self._download()

    def _download(self) -> pd.DataFrame:
        """
        Downloads the factor data using HttpClient and processes it.
        This method will use the attributes set during class instantiation.
        """
        with HttpClient(timeout=8.0) as client:
            _data = client.download(self.url, self.cache_ttl)

        _file = io.StringIO(_data.decode('utf-8'))

        index_cols = [0, 1] if self.frequency in ["m", "q"] else [0]
        data = pd.read_csv(_file, parse_dates=False, index_col=index_cols, float_precision="high")

        if self.classic:
            data = data.drop(columns=["R_EG"])

        data = data.rename(columns={"R_F": "RF"})
        data = data * 0.01

        if self.frequency in ["m", "q"]:
            # Need to insert "-" (monthly) or "Q" (quarterly) into date str.
            data = data.reset_index()
            col = "quarter" if self.frequency == "q" else "month"
            char = "q" if self.frequency == "q" else "-"

            data["date"] = pd.PeriodIndex(
                data["year"].astype(str)
                + char
                + data[col].astype(str), freq=self.frequency.upper()  #fix:FutureWarning 'm' is deprecated, use 'M'
            ).to_timestamp(how="end")

            data["date"] = data["date"].dt.normalize()
            data = data.drop(["year", col], axis=1).set_index("date")

        if self.frequency == "y":
            data.index = pd.to_datetime(data.index.astype(str)) \
                + pd.offsets.YearEnd(0)
        elif self.frequency not in ["m", "q"]:
            data.index = pd.to_datetime(data.index.astype(str))

        data.columns = data.columns.str.upper()
        data.index.name = "date"
        data = data.rename(columns={"R_MKT": "Mkt-RF"})

        return _process(data, self.start_date, self.end_date,
                        filepath=self.output_file)
