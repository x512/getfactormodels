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
from typing import Any, override
import pyarrow as pa
import pyarrow.compute as pc
from getfactormodels.utils.utils import _offset_period_eom
from .base import FactorModel
from .fama_french import FamaFrenchFactors
from .hml_devil import HMLDevilFactors
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

    Returns:
        pd.DataFrame: A timeseries of the factor data.

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
        super().__init__(**kwargs)
    
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m"]
    
    #TODO: set precision to min of models.
    @property
    def _precision(self) -> int:
        return 3
    
    @property
    def schema(self) -> pa.Schema:
        """Barillas-Shanken schema."""
        return pa.schema([
            ('date', pa.string()),
            ('Mkt-RF', pa.float64()),
            ('SMB', pa.float64()),
            ('HML_Devil', pa.float64()),
            ('R_IA', pa.float64()),
            ('R_ROE', pa.float64()),
            ('UMD', pa.float64()),
            ('RF', pa.float64()),
        ])

    @override
    def _get_table(self) -> pa.Table:
        if self._data is not None:
             return self._data
        
        # TODO: FIXME(HML_Devil): end date only, messes everything up (HML Devil, and BS6 by ext.)
        # BAND-AID: If they specified an end date but no start date, 
        # force the start to earliest AQR date: 1967-01-01
        if self.end_date is not None and self.start_date is None:
            self.log.info("End date detected without start date. Clamping to 1967-01-01 for BS6 compatibility.")
            self._start_date = "1967-01-01"
        # HML needs redo -- source isn't what it once was, so...

        table = self._construct()

        table = self._rearrange_columns(table)
        table = self._slice_to_range(table)

        # Base class requires this!
        self._data = table

        return self._data

    def _construct(self) -> pa.Table:
        """Private: builds the Barillas-Shanken factor model.
        
        Creates the model by calling .extract() on QFactors (R_IA, R_ROE),
        HMLDevilFactors (HML_Devil, RF) and FamaFrenchFactors (Mkt-RF, 
        SMB and UMD).
        ---
        NOTE:
        - Uses the higher-precision RF from AQR as the risk-free rate (RF).
        - Construction is triggered by the .data property override.
        """
        self.log.info("Constructing Barillas-Shanken 6 Factor Model...")
        
        print("- Downloading HML Devil Factors...")
        _q = QFactors(frequency=self.frequency).extract(['R_IA', 'R_ROE'])
        
        print("- Downloading Fama French Factors...")
        _ff = FamaFrenchFactors(model='6', frequency=self.frequency).extract(['Mkt-RF', 'SMB', 'UMD'])
        
        print("- Downloading HML Devil Factors...")
        _devil = HMLDevilFactors(frequency=self.frequency).extract(['HML_Devil', 'RF'])

        # join FF and Q, then with HML Devil
        table = _ff.join(_q, keys='date')
        table = table.join(_devil, keys='date')

        #enforce/cast schema
        table = table.select(self.schema.names).cast(self.schema)
        
        # using this util to cast TODO break it up 
        table = _offset_period_eom(table, self.frequency)

        return table

    def _get_url(self) -> str:
        """Composite model: no remote source."""
        return ""

    def _read(self, data: bytes) -> pa.Table:
        """Composite model: constructed via sub-models, not parsed from bytes."""
        self.log.warning("BarillasShanken: _read called on composite model.")
        return pa.Table()

