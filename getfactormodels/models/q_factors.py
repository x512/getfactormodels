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
import pandas as pd
import io
#import diskcache as dc  # TODO: caching properly

from getfactormodels.utils.utils import _process
from getfactormodels.http_client import HttpClient

class QFactors:
    #not really for docstr
    """Download the q or q-classic factor models.
    - q5-factor model
    - Classic q-factor model of Hou, Xue, and Zhang (2015).
    - decimals: 4
    - Authors: Hou, Kewei, Haitao Mo, Chen Xue, and Lu Zhang (2021);
      Hou, Kewei, Chen Xue, and Lu Zhang (2015).
    Sources:
    - Hou, Kewei, Haitao Mo, Chen Xue, and Lu Zhang, 2021, An
      augmented q-factor model with expected growth, Review of Finance
      25 (1), 1-41. Editor's Choice. This article constructs the
      expected growth factor in the q5 model from January 1967 onward.
    - Hou, Kewei, Chen Xue, and Lu Zhang, 2015, Digesting anomalies: An
      investment approach, Review of Financial Studies 28 (3), 650-705.
      Editor's Choice. This article constructs the q-factors series
      from January 1972 onward.
    - data source: https://global-q.org/factors.html
    """
    # Changed (from func):
    # freq.uppercase to lowercase
    # stringIO needs to be wrapped around file
    # dropped numpy, again the model only used it for multiply.
    # Note: weekly wednesday-to-wednesday needs to be added TODO
    # Need to do all typing everywhere
    def __init__(self, frequency='m', start_date=None, end_date=None,
                 output_file=None, classic=False): # classic=False default
        self.frequency = frequency.lower()
        
        if self.frequency not in ["d", "m", "q"]: 
            raise ValueError("Frequency must be 'd', 'm' or 'q'")

        self.file = {'m': "monthly",
                "d": "daily",
                "q": "quarterly",
                "w": "weekly",
                "y": "annual", }.get(self.frequency)
        self.classic = classic   # store the state
        self.url = f'https://global-q.org/uploads/1/2/2/6/122679606/q5_factors_{self.file}_2024.csv' # TODO: YEAR
        self.start_date = start_date
        self.end_date = end_date
        self.output_file = output_file
     #   self.client = HttpClient(timeout=8.0)

    def download(self) -> pd.DataFrame:
        """public wrapper."""
        return self._download()

    def _download(self) -> pd.DataFrame:
        """
        Downloads the factor data using HttpClient and processes it.
        This method will use the attributes set during class instantiation.
        """
        with HttpClient(timeout=5.0) as client:
            _file = client.download(self.url)

        index_cols = [0, 1] if self.frequency in ["m", "q"] else [0]
        data = pd.read_csv(io.StringIO(_file), parse_dates=False, index_col=index_cols, float_precision="high")

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
                        filepath=self.output_file)  # type err: TODO

