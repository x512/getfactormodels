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
from getfactormodels.models.fama_french import FamaFrenchFactors
from getfactormodels.utils.utils import \
    _process  # TODO: URGENT: FIXME: Untangle this


class DHSFactors:
    # roughing in infos, not approp for docstr but need TODO a reliable
    # way of getting and setting these when more models are redone. Most
    # importantly the copyright/attribution info! TODO
    """Retrieves the Daniel-Hirshliefer-Sun behavioural factors 
    - Factors: FIN and PEAD factor (returns)
    - Freq: monthly ('m'), daily ('d')
    - Date range: 1972-07-01 - 2023-12-31

    - Source: "Short and Long Horizon Behavioral Factors," Kent Daniel,
                  David Hirshleifer and Lin Sun, Review of Financial
                  Studies, 2020, 33 (4): 1673-1736.
 
    - Datasource: https://sites.google.com/view/linsunhome provides
      two links to daily and monthly data.
    - Authors:  Kent Daniel, David Hirshleifer, Lin Sun
    * Warnings: depr notice, wrap in bytesIO
    """
    def __init__(self, frequency='m', start_date=None, end_date=None,
                 output_file=None, cache_ttl: int = 86400):
        #validate freq
        self.frequency = frequency.lower()

        if self.frequency == 'm':
            gs_id = '1VwQcowFb5c0x3-0sQVf1RfIcUpetHK46'
        elif self.frequency == 'd':
            gs_id = '1lWaNCuHeOE-nYlB7GA1Z2-QQa3Gt8UJC' 
        else:
            raise ValueError("Frequency must be daily ('d') or monthly ('m')")

        self.url = f'https://docs.google.com/spreadsheets/d/{gs_id}/export?format=xlsx' #possibly unbound
        #input validations
        self.start_date = start_date
        self.end_date = end_date
        # if start or end date, parse dates with pa ?
        self.output_file = output_file
        self.cache_ttl = cache_ttl   #test

    # do base class if we're repeating? later. TODO
    def download(self):
        """Download the DHS Behavioural factors."""
        return self._download(self.start_date, self.end_date, self.output_file)

    def _download(self, start_date, end_date, output_file):
        """Retrieve the DHS behavioural factors. Daily and monthly."""
        # - start, end, output: keeping here until _process, and the data transformations/date
        #   validations are untangled... TODO
        with HttpClient() as client:
            _bytes = client.download(self.url, self.cache_ttl)

        _file = io.BytesIO(_bytes)

        data = pd.read_excel(_file, index_col="Date",
                         usecols=['Date', 'FIN', 'PEAD'], engine='openpyxl',
                         header=0, parse_dates=False)
        data.index.name = "date"

        if self.frequency == "d":
            data.index = pd.to_datetime(data.index, format="%m/%d/%Y")
        else:
            data.index = pd.to_datetime(data.index, format="%Y%m")
            data.index = data.index + pd.offsets.MonthEnd(0)

        # decimalize
        data = data * 0.01    # TODO: Decimal types possibly

        # Need Fama-French Factors -- only model without rf or mkt-rf?
        # Get the RF and Mkt-FF from FF3. TODO: store Mkt-RF and RF; make function.

        try:
            ffdata = FamaFrenchFactors(model="3", frequency=self.frequency,
                                 start_date=data.index[0], end_date=data.index[-1])
            ff = ffdata.download()
            ff = ff.round(4)
            data = pd.concat([ff["Mkt-RF"], data, ff["RF"]], axis=1)
        except NameError:
            print("Warning: _get_ff_factors function not found. Skipping FF merge.")

        data.index.name = "date"

        return _process(data, start_date, end_date, filepath=output_file)
    # There's a second sheet, 'Details' wh. Construction, Universe, Period, More details
    # TODO: when decided on implementing .info()
