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
from getfactormodels.utils.utils import \
    _process  # tangled mess. after replacing req, untangle... TODO


class ICRFactors:
    def __init__(self, frequency='m', start_date=None, end_date=None,
                 output_file=None, cache_ttl: int = 86400):
        self.frequency = frequency.lower()    #TODO: base model ....

        if self.frequency not in ["d", "m", "q"]:
            raise ValueError("Frequency must be 'd', 'm' or 'q'")

        self.start_date = start_date
        self.end_date = end_date
        self.output_file = output_file
        self.cache_ttl = cache_ttl

        # _construct_url in a base TODO
        _file = {"d": "daily", "m": "monthly", "q": "quarterly"}.get(self.frequency)
        _url = f"https://zhiguohe.net/wp-content/uploads/2025/07/He_Kelly_Manela_Factors_{_file}_250627.csv"

        self.url = _url


    def download(self):
        """
        Download the Intermediary Capital Ratio factors of He, 
        Kelly & Manela (2017)
        """
        return self._download(self.start_date, self.end_date, self.output_file)

    def _download (self, start_date, end_date, output_file):
        """Download and process the Intermediary Capital Ratio factors data."""
        with HttpClient(timeout=5.0) as client:
            _data = client.download(self.url, self.cache_ttl)
        
        if _data is None:
            print("Error downloading")

        data = io.StringIO(_data.decode('utf-8'))
       
        # back to pd, old func stuff
        df = pd.read_csv(data)
        df = df.rename(columns={df.columns[0]: "date"})

        if self.frequency == "q":
            # Quarterly dates are in a YYYYQ format [19752 to 1975Q2]
            df["date"] = df["date"].astype(str)
            df["date"] = df["date"].str[:-1] + "Q" + df["date"].str[-1]
            # Converts YYYYQ to a timestamp at the eoq
            df["date"] = pd.PeriodIndex(df["date"], freq="Q").to_timestamp() \
                + pd.offsets.QuarterEnd(0)

        elif self.frequency == "m":
            df["date"] = pd.to_datetime(df["date"], format="%Y%m")
            df["date"] = df["date"] + pd.offsets.MonthEnd(0)

        elif self.frequency == "d":
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")

        df = df.rename(columns={
                "intermediary_capital_ratio": "IC_RATIO",
                "intermediary_capital_risk_factor": "IC_RISK_FACTOR",
                "intermediary_leverage_ratio_squared": "INT_LEV_RATIO_SQ",
                "intermediary_value_weighted_investment_return": "INT_VW_ROI",
            })

        df = df.set_index("date")

        return _process(df, start_date, end_date, filepath=output_file)

