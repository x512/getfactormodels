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
import pyarrow as pa
from pyarrow.compute import divide
from python_calamine import CalamineWorkbook
from getfactormodels.models.base import FactorModel
from getfactormodels.models.fama_french import FamaFrenchFactors
from datetime import datetime

class DHSFactors(FactorModel):
    """Download the DHS Behavioural Factors.

  Downloads the Behavioural Factors of Kent Daniel, David Hirshleifer, and 
  Lin Sun (DHS). Data from 1972-07-01 until end of 2023.

  Args:
  `frequency` (`str`): the frequency of the data. `m` or `d` (default: `m`)
  `start_date` (`str, optional`): the start date of the data, `YYYY-MM-DD`
  `end_date` (`str, optional`): the end date of the data, `YYYY-MM-DD`
  `output_file` (`str, optional`): the filepath to save the output data.

  Returns:
    `pd.Dataframe` : timeseries of factors.

  References:
  - Short and Long Horizon Behavioral Factors," Kent Daniel, David 
  Hirshleifer and Lin Sun, Review of Financial Studies, 2020, 33 (4):
  1673-1736.

  Data source: https://sites.google.com/view/linsunhome/

  Schema:
  - PEAD (float64): Post-Earnings Announcement Drift.
  - FIN  (float64): Financing Factor.
  --- 
  Note: kwargs are passed to the base FactorModel.
    """
    # roughing in infos, not approp for docstr but need TODO a reliable
    # way of getting and setting these when more models are redone. Most
    # importantly the copyright/attribution info! TODO
    @property
    def _frequencies(self) -> list[str]:
        return ['d', 'm']


    def __init__(self, frequency: str = 'm', **kwargs: Any) -> None:
        """Initialize the DHS factor model."""
        # TODO: docstrings, class, init, module level...
        super().__init__(frequency=frequency, **kwargs)


    @property  #like q-factors, a dynamic schema (DHS factors swap cols)
    def schema(self) -> pa.Schema:
        """DHS schema"""
        _date = [('Date', pa.timestamp('ms'))]
        _fin = [('FIN', pa.float64())]
        _pead = [('PEAD', pa.float64())]

        if self.frequency == 'd':
            # d = Date, FIN, PEAD
            return pa.schema(_date + _fin + _pead)
        else:
            # m = Date, PEAD, FIN (they swap)
            return pa.schema(_date + _pead + _fin)


    def _get_url(self) -> str:
        """(Internal) Constructs the Google Sheet URL for DHS monthly and daily factors."""
        base_url = 'https://docs.google.com/spreadsheets/d/'

        if self.frequency == 'd':
            gsheet_id = '1lWaNCuHeOE-nYlB7GA1Z2-QQa3Gt8UJC'
            #info_id =
        else:
            gsheet_id = '1VwQcowFb5c0x3-0sQVf1RfIcUpetHK46'
            #info_sheet_id = '#gid=96292754'

        return  f'{base_url}{gsheet_id}/export?format=xlsx'  # back to xlsx with calamine. need those decimals!



    def _read(self, data: bytes) -> pd.DataFrame | pa.Table:
        """Read the DHS factor data into a Dataframe or Table.
        
        NOTE: ICR factors are exported as an XLSX from Google Sheets to retain 
        decimal precision. Exporting as CSV would result in 2 decimal places. 
        """
        # Uses calamine to read the google sheets xlsx export, retaining precision!
        try:
            workbook = CalamineWorkbook.from_filelike(io.BytesIO(data))
            rows = workbook.get_sheet_by_name(workbook.sheet_names[0]).to_python()

            headers = [str(h).strip() for h in rows[0]]
            
            _dict = {}
            for i, name in enumerate(headers):
                _dict[name] = [row[i] for row in rows[1:]]
            
            if self.frequency == 'm':
                _dict['Date'] = [datetime.strptime(str(int(d)), '%Y%m') for d in _dict['Date']]
            else:
                _dict['Date'] = [
                    datetime.combine(d, datetime.min.time()) if not hasattr(d, 'hour') else d 
                    for d in _dict['Date']
                ]
            
            table = pa.Table.from_pydict(
                {name: _dict[name] for name in self.schema.names}, 
                schema=self.schema
            )
            
            table.validate(full=True) #explicit validation
            
            column_order = ['Date', 'PEAD', 'FIN']
            table = table.select(column_order)

            #  decimalizing here for now
            for col in ["FIN", "PEAD"]:
                idx = table.schema.get_field_index(col)
                table = table.set_column(idx, col, divide(table.column(col), 100.0))
            #--------------------------------------------------------------#
            df = table.to_pandas().set_index('Date')

            #base.py can handle this.
            if self.frequency == 'm':
                df.index = df.index + pd.offsets.MonthEnd(0)  # NO IT CANT YET AHH

            df.index.name = 'date'
            # return here and wrap in base 


            # wrap in FF Mkt-RF and RF
            try:
                ff = FamaFrenchFactors(model="3", frequency=self.frequency,
                                       start_date=self.start_date, end_date=self.end_date).download()
                df = pd.concat([ff["Mkt-RF"], df, ff["RF"]], axis=1).dropna()
            except Exception as e:
                self.log.warning(f"FF Merge skipped: {e}")

            return df

        except Exception as e:
            self.log.error(f"DHS Read Failure: {e}")
            return pd.DataFrame()

