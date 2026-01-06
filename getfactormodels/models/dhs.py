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
import pyarrow as pa
from python_calamine import CalamineWorkbook
from getfactormodels.models.base import FactorModel
from getfactormodels.models.fama_french import FamaFrenchFactors
from getfactormodels.utils.arrow_utils import (
    round_to_precision,
    scale_to_decimal,
)
from getfactormodels.utils.date_utils import offset_period_eom


class DHSFactors(FactorModel):
    """Download and process the DHS Behavioural Factors.

    Behavioural Factors of Kent Daniel, David Hirshleifer, and 
    Lin Sun (DHS). Data from July 1972 - December, 2023. Factors: FIN, PEAD

    Args:
        frequency (str): The data frequency, 'm'.
        start_date (str, optional): The start date YYYY-MM-DD.
        end_date (str, optional): The end date YYYY-MM-DD.
        output_file (str, optional): Optional file path to save to file.
            Supports csv, pkl.
        cache_ttl (int, optional): Cached download time-to-live in 
            seconds (default: 86400).

    References:
    - Short and Long Horizon Behavioral Factors," Kent Daniel, David 
    Hirshleifer and Lin Sun, Review of Financial Studies, 2020, 33 (4):
    1673-1736.

    """
    # Data source: https://sites.google.com/view/linsunhome/
    # copyright/attribution info! TODO
    @property
    def _frequencies(self) -> list[str]:
        return ['d', 'm']

    def __init__(self, frequency: str = 'm', **kwargs: Any) -> None:
        """Initialize the DHS factor model."""
        super().__init__(frequency=frequency, **kwargs)

    @property
    def _precision(self) -> int:
        return 12

    @property  
    def schema(self) -> pa.Schema:
        """DHS schema"""
        _date = [('Date', pa.string())]   # 197202
        _fin = [('FIN', pa.float64())]
        _pead = [('PEAD', pa.float64())]
        
        # FIN and PEAD, swap cols between frequencies.
        if self.frequency == 'd':
            return pa.schema(_date + _fin + _pead)

        return pa.schema(_date + _pead + _fin)

    def _get_url(self) -> str:
        """(Internal) Constructs the Google Sheet URL for DHS monthly and daily factors."""
        base_url = 'https://docs.google.com/spreadsheets/d/'

        if self.frequency == 'd':
            sheet_id = '1lWaNCuHeOE-nYlB7GA1Z2-QQa3Gt8UJC'
            #info_id =
        else:
            sheet_id = '1VwQcowFb5c0x3-0sQVf1RfIcUpetHK46'
            #info_sheet_id = '#gid=96292754'  # Construction, Universe, Period, More Details

        return  f'{base_url}{sheet_id}/export?format=xlsx'

    # Exported as .xlsx from Google, then read with Calamine. 
    # CSV export would only give 2 decimals.

    def _read(self, data: bytes) -> pa.Table:
        wb = CalamineWorkbook.from_filelike(io.BytesIO(data))

        rows = wb.get_sheet_by_name(wb.sheet_names[0]).to_python()
        headers = [str(h).strip() for h in rows[0]]

        _dict = {name: [row[i] for row in rows[1:]] for i, name in enumerate(headers)}

        # fix: load without schema here
        table = pa.Table.from_pydict(_dict)

        # Enforce schema here
        table = table.select(self.schema.names).cast(self.schema)  # casts date to str here
        
        # Requires scaling, source in pct
        table = scale_to_decimal(table)

        # Offset, before FF
        table = offset_period_eom(table, self.frequency)
        
        if "Date" in table.column_names:
            table = table.rename_columns(["date"] + table.column_names[1:])

        # Wrap in FF Mkt-RF and RF
        _ff = FamaFrenchFactors(model='3', 
                                frequency=self.frequency)._extract_as_table(['Mkt-RF', 'RF'])

        # join, then combine_chunks!
        table = table.join(_ff, keys="date", join_type="inner")
        
        return table.combine_chunks() #.sort_by([("date", "descending")])

