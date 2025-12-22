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
from getfactormodels.utils.utils import _offset_period_eom


class DHSFactors(FactorModel):
    """
    DHS Behavioural Factors (Daniel-Hirshleifer-Sun, 2020)

    Downloads the Behavioural Factors of Kent Daniel, David Hirshleifer, and 
    Lin Sun (DHS). Data from July 1972 - December, 2023.

    Args:
        frequency (str): The data frequency, 'm'.
        start_date (str, optional): The start date YYYY-MM-DD.
        end_date (str, optional): The end date YYYY-MM-DD.
        output_file (str, optional): Optional file path to save to file. Supports csv, pkl.
        cache_ttl (int, optional): Cached download time-to-live in seconds (default: 86400).

    Returns:
        pd.Dataframe: timeseries of factors.

    References:
    - Short and Long Horizon Behavioral Factors," Kent Daniel, David 
    Hirshleifer and Lin Sun, Review of Financial Studies, 2020, 33 (4):
    1673-1736.

    Data source: https://sites.google.com/view/linsunhome/

    Factors: FIN, PEAD
    ---
    TODO:
    """
    # copyright/attribution info! TODO
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
        _date = [('Date', pa.string())]   # 197202, err as int64.
        _fin = [('FIN', pa.float64())]
        _pead = [('PEAD', pa.float64())]
        # m = Date, PEAD, FIN. d = Date, FIN, PEAD.
        if self.frequency == 'd':
            return pa.schema(_date + _fin + _pead)
        return pa.schema(_date + _pead + _fin)


    def _get_url(self) -> str:
        """(Internal) Constructs the Google Sheet URL for DHS monthly and daily factors."""
        base_url = 'https://docs.google.com/spreadsheets/d/'

        if self.frequency == 'd':
            gsheet_id = '1lWaNCuHeOE-nYlB7GA1Z2-QQa3Gt8UJC'
            #info_id =
        else:
            gsheet_id = '1VwQcowFb5c0x3-0sQVf1RfIcUpetHK46'
            #info_sheet_id = '#gid=96292754'  # Construction, Universe, Period, More Details

        return  f'{base_url}{gsheet_id}/export?format=xlsx'  # back to xlsx with calamine. need those decimals!

    # Uses calamine to read a xlsx. Uses pyarrow, converts to dataframe for FF at end still. TODO: FIXME
    def _read(self, data: bytes) -> pa.Table:
        workbook = CalamineWorkbook.from_filelike(io.BytesIO(data))
        rows = workbook.get_sheet_by_name(workbook.sheet_names[0]).to_python()

        headers = [str(h).strip() for h in rows[0]]
        _dict = {name: [row[i] for row in rows[1:]] for i, name in enumerate(headers)}

        # fix: load without schema 
        table = pa.Table.from_pydict(_dict)

        # force date to str (eom expects str)
        table = table.set_column(0, "Date", table.column(0).cast(pa.string()))

        table = _offset_period_eom(table, self.frequency)

        # TODO: FIXME: _offset_period_eom should return col as same name. 
        if "date" in table.column_names:
            table = table.rename_columns(["Date"] + table.column_names[1:])

        # selecting to avoid the extra col error when Year Month present
        table = table.select(self.schema.names).cast(self.schema)

        # m/d swap PEAD/FIN cols, this returns them in same orders
        output_names = ['date', 'PEAD', 'FIN']
        table = table.select(['Date', 'PEAD', 'FIN']).rename_columns(output_names)
        for col in ["FIN", "PEAD"]:
            idx = table.schema.get_field_index(col)
            table = table.set_column(idx, col, divide(table.column(col), 100.0))

        table.validate(full=True)  #TODO remove full when base does this
        # return table

        # wrap in FF Mkt-RF and RF
        df = table.to_pandas().set_index('date')
        #--------------------------------------------------------------#
        df.index.name = 'date'
        df.index = pd.to_datetime(df.index)
        # TODO: make this util using .extract()
        try:
            ff = FamaFrenchFactors(model="3", frequency=self.frequency,
                                   start_date=self.start_date, end_date=self.end_date).download()
            df = pd.concat([ff["Mkt-RF"], df, ff["RF"]], axis=1).dropna()
        except Exception as e:
            self.log.warning(f"FF Merge skipped: {e}")

        return df

