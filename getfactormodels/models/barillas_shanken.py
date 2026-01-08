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
import sys
from typing import Any, override
import pyarrow as pa
from getfactormodels.utils.arrow_utils import (
    rearrange_columns,
    select_table_columns,
    round_to_precision,
)
from .aqr_models import HMLDevilFactors
from .base import FactorModel
from .fama_french import FamaFrenchFactors
from .q_factors import QFactors


class BarillasShankenFactors(FactorModel):
    """Download the Barillas-Shanken 6 Factor Model (2018).

    A combination of the 5-factor model of Fama and French (2015), the q-factor
    model of Hou, Xue, and Zhang (2015), and Asness and Frazzini's HML Devil (2013).
    This is the factor model with the highest posterior inclusion probability
    in Barillas and Shanken (2018).

    Args:
        frequency (str): the frequency of the data. d, m. (default: m).
        start_date (str, optional): start date, YYYY-MM-DD.
        end_date (str, optional): end date, YYYY-MM-DD.
        cache_ttl (int, optional): Cache time-to-live in seconds 
            (default: 86400).

    References:
    - F. Barillas and J. Shanken, ‘Comparing Asset Pricing Models’, Journal of 
    Finance, vol. 73, no. 2, pp. 715–754, 2018.

    ---
    NOTE: Relies on HML Devil which is slow to download.
    - This model uses the higher precision "RF" returned by AQR's HML_Devil.
    - Factors: R_IA and R_ROE from q-factors. Mkt-RF, SMB and UMD from Fama-French.
    AQR's HML_Devil and RF.

    """
    def __init__(self, **kwargs: Any) -> None:
        """Initialize the Barillas-Shanken 6-Factor model."""
        super().__init__(**kwargs)
    
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m"]
    
    @property
    def _precision(self) -> int:
        return 8
    
    @property
    def schema(self) -> pa.Schema:
        return pa.schema([
            ('date', pa.string()),
            ('Mkt-RF', pa.float64()),
            ('SMB', pa.float64()),
            ('HML_Devil', pa.float64()),
            ('R_IA', pa.float64()),
            ('R_ROE', pa.float64()),
            ('UMD', pa.float64()),
            ('RF_AQR', pa.float64()),
        ])

    @override
    def _get_table(self) -> pa.Table:
        if self._data is not None:
             return self._data
        
        table = self._construct()
        table = rearrange_columns(table)

        table.validate(full=True)  # base validates here.
        table = table.combine_chunks()

        self._data = table
        return self._data


    def _construct(self) -> pa.Table:
        # Get tables
        q_t = QFactors(frequency=self.frequency)._get_table()
        ff_t = FamaFrenchFactors(model='6', frequency=self.frequency)._get_table()
        devil_t = HMLDevilFactors(frequency=self.frequency)._get_table()
        
        # Slice
        q = select_table_columns(q_t, ['R_IA', 'R_ROE'])
        ff = select_table_columns(ff_t, ['Mkt-RF', 'SMB', 'UMD'])
        devil = select_table_columns(devil_t, ['HML_Devil', 'RF_AQR'])
        
        # Join
        table = ff.join(q, keys='date', join_type='inner')
        table = table.join(devil, keys='date', join_type='inner')

        return table.select(self.schema.names).cast(self.schema)

    def _get_url(self) -> str:
        """Composite model: no remote source."""
        return ""

    def _read(self, data: bytes) -> pa.Table:
        """Composite model: constructed via sub-models, not parsed from bytes."""
        self.log.warning("BarillasShanken: _read called on composite model.")
        return pa.Table()

