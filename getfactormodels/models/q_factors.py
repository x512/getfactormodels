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


class QFactors(FactorModel):
    """Download q-factor data from global-q.org.

    Args:
        frequency (str): The data frequency ('d', 'w', 'm', 'q', 'y', or 'w2w'). Default: 'm'.
        start_date (str, optional): The start date YYYY-MM-DD.
        end_date (str, optional): The end date YYYY-MM-DD.
        output_file (str, optional): Optional file path to save to file. Supports csv, pkl.
        classic (bool, optional): returns the classic 4-factor q-factor model. Default: False.
        cache_ttl (int, optional): Cached download time-to-live in seconds (default: 86400).

    Returns:
        pd.DataFrame: timeseries of factors

    References:
    - Hou, Kewei, Haitao Mo, Chen Xue, and Lu Zhang, 2021, An augmented q-factor model
    with expected growth, Review of Finance 25 (1), 1-41. (q5 model)
    - Hou, Kewei, Chen Xue, and Lu Zhang, 2015, Digesting anomalies: An investment
    approach, Review of Financial Studies 28 (3), 650-705. (Classic q-factor model)

    Data Source: https://global-q.org/factors.html
    """
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "w", "w2w", "m", "q", "y"] # test

    def __init__(self, classic: bool = False, **kwargs: Any) -> None:
        self.classic = classic 
        super().__init__(classic=classic, **kwargs)

    def _get_url(self) -> str:
        file = {'m': "monthly", 
                "d": "daily",
                "q": "quarterly", 
                "w": "weekly",
                "w2w": "weekly_w2w",
                "y": "annual", }.get(self.frequency)

        url = 'https://global-q.org/uploads/1/2/2/6/122679606'
        url += f'/q5_factors_{file}_2024.csv' # TODO: YEAR
        return url

   # def download(self) -> pd.DataFrame:
   #     _data = self._download()
   #     data = self._read(_data)
   #     df = self._parse_q_factors(data)
#
 #       return df

    def _read(self, data) -> pd.DataFrame:
        _file = io.StringIO(data.decode('utf-8'))
        index_cols = [0, 1] if self.frequency in ["m", "q"] else [0]
        data = pd.read_csv(_file, parse_dates=False, index_col=index_cols, float_precision="high")
        data = self._parse_q_factors(data)

        return data

    def _parse_q_factors(self, data) -> pd.DataFrame:
        if self.frequency in ["m", "q"]: 
            data = data.reset_index()

            # Need to insert "-" or "Q" into date monthly/quarterly str.
            col = "quarter" if self.frequency == "q" else "month"
            char = "q" if self.frequency == "q" else "-"

            # Combines year, period cols into a PeriodIndex to Timestamp
            data["date"] = pd.PeriodIndex(
                data["year"].astype(str) + char + data[col].astype(str),
                freq=self.frequency.upper()  #fix:FutureWarning 'm' is deprecated, use 'M'
            ).to_timestamp(how="end")

            data["date"] = data["date"].dt.normalize()

            data = data.drop(["year", col], axis=1).set_index("date")

        elif self.frequency == "y":
            data.index = pd.to_datetime(data.index.astype(str)) + pd.offsets.YearEnd(0)

        else: # daily, weekly
            data.index = pd.to_datetime(data.index.astype(str))

        data.columns = data.columns.str.upper()
        data.index.name = "date"

        if self.classic:
            data = data.drop(columns=["R_EG"])

        # TODO: check: its market, not mkt-rf!
        data = data.rename(columns={"R_MKT": "Mkt-RF", "R_F": "RF"})

        data = data * 0.01

        #TODO: slicing, saving, validating date to base model? util? Use here.
        return _process(data, self.start_date, self.end_date,
                        filepath=self.output_file)
