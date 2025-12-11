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
from typing import Any
import pandas as pd
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.utils import _process


# TODO: proper class docstr's.
class MispricingFactors(FactorModel):
    """Mispricing factors of Stambaugh & Yuan (2016).

    Downloads the Mispricing factors of R. F. Stambaugh and Y. Yuan. Data from
    1963 to 2016. Note: the SMB factor is returned as SMB_SY.

    Args:
        frequency(str, optional): 'm' (monthly), 'd' (daily)
        start_date (str, optional): The start date YYYY-MM-DD.
        end_date (str, optional): The end date YYYY-MM-DD.
        output_file (str, optional): Optional file path to save to file. Supports csv, pkl.
        classic (bool, optional): returns the classic 4-factor q-factor model. Default: False.
        cache_ttl (int, optional): Cached download time-to-live in seconds (default: 86400).
    Returns:
        pd.DataFrame: timeseries of factor data.

    References:
    - Pub: R. F. Stambaugh and Y. Yuan, ‘Mispricing Factors’, The Review of 
    Financial Studies, vol. 30, no. 4, pp. 1270–1315, 12 2016.

    Data source: https://finance.wharton.upenn.edu/~stambaug/
    """
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def _get_url(self) -> str:
        _file_url = "M4d.csv" if self.frequency == "d" else "M4.csv"
        url = f"https://finance.wharton.upenn.edu/~stambaug/{_file_url}"

        return url

    def download(self):
        """Download Stambaugh-Yuan (2016) Mispricing factor data."""
        _data = self._download() #in base_model
        data = self._read(_data)

        return data

    def _read(self, data):
        """Reads the Mispricing factors CSV."""
        _data = data.decode('utf-8')
        data = pd.read_csv(io.StringIO(_data),
                           index_col=0,
                           parse_dates=False,
                           date_format="%Y%m%d",
                           engine="pyarrow")

        data = data.rename(columns={"SMB": "SMB_SY",
                                    "MKTRF": "Mkt-RF"}).rename_axis("date")
        ## NOTICING A PATTERN....
        if self.frequency == "d":
            data.index = pd.to_datetime(data.index, format="%Y%m%d")

        elif self.frequency == "m":
            data.index = pd.to_datetime(data.index, format="%Y%m")
            data.index = data.index + pd.offsets.MonthEnd(0)

        # then into the mystical spaghetti _process still
        return _process(data, self.start_date,
                        self.end_date, filepath=self.output_file)
