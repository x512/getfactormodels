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


class MispricingFactors:
    """Stambaugh-Yuan mispricing factors.

    - Frequencies: monthly ('m'), daily ('d')
    - Factors: SMB_SY, RMW, CMA Mkt-RF, RF
        * Note: the Mispricing SMB factor is renamed SMB_SY.
    - Pub: R. F. Stambaugh and Y. Yuan, ‘Mispricing Factors’, The
      Review of Financial Studies, vol. 30, no. 4, pp. 1270–1315,
      12 2016.
    - Source: https://finance.wharton.upenn.edu/~stambaug/ 
    - Date: 01-01-1963 - 2016-12-31
    """
    def __init__(self, frequency = 'm', start_date=None, end_date=None,
                 output_file=None, cache_ttl=86400):

        if frequency.lower() not in ["d", "m"]:
            raise ValueError("Mispricing factors are only available for daily (d) and "
                         "monthly (m) frequency.")
        
        self.frequency = frequency.lower()
        self.start_date = start_date
        self.end_date = end_date
        self.output_file = output_file
        self.cache_ttl = cache_ttl

        _file_url = "M4d" if self.frequency == "d" else "M4"
        _url = f"https://finance.wharton.upenn.edu/~stambaug/{_file_url}.csv"
        self.url = _url

    def download(self):
        """Download the Stambaugh-Yuan Mispricing factors."""
        return self._download(self.start_date, self.end_date, self.output_file)

    def _download(self, start_date, end_date, output_file):
        """Retrieve the Stambaugh-Yuan mispricing factors. Daily and monthly."""
        with HttpClient() as client:
            _data = client.download(self.url, self.cache_ttl)
            _data = _data.decode('utf-8')

        data = pd.read_csv(io.StringIO(_data),
                           index_col=0,
                           parse_dates=False,
                           date_format="%Y%m%d",
                           engine="pyarrow")

        data = data.rename(columns={"SMB": "SMB_SY",
                                    "MKTRF": "Mkt-RF"}).rename_axis("date")

        if self.frequency == "d":
            data.index = pd.to_datetime(data.index, format="%Y%m%d")

        elif self.frequency == "m":
            data.index = pd.to_datetime(data.index, format="%Y%m")
            data.index = data.index + pd.offsets.MonthEnd(0)

        # then into the mystical spaghetti _process still
        return _process(data, start_date, end_date, filepath=output_file)

# NOTES:
# - Need to do helpers for:
#    get mkt-rf, rf, and add them if needed.
#    check, correct freq/model names; input validation
#    datetime utils
#    debug util -- traceback
