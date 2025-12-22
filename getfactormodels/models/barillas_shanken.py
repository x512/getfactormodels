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
import pandas as pd
from .base import FactorModel
from .fama_french import FamaFrenchFactors
from .hml_devil import HMLDevilFactors
from .q_factors import QFactors
import pyarrow as pa


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
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m"]

    @property
    def schema(self) -> pa.Schema:
        """Barillas-Shanken schema."""
        return pa.schema([
            ('date', pa.string()),  # string  #('date', pa.timestamp('ns')), # pandas index becomes timestamp
            ('Mkt-RF', pa.float64()),
            ('SMB', pa.float64()),
            ('HML_Devil', pa.float64()),
            ('R_IA', pa.float64()),
            ('R_ROE', pa.float64()),
            ('UMD', pa.float64()),
            ('RF', pa.float64()),
        ])

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    @override
    def download(self) -> pd.DataFrame:
        """Overrides base download() to ensure export works."""
        df = self.data # hit the data property below
        if self.output_file:
            self.to_file(df, self.output_file)
        return df

    @property
    def data(self) -> pd.DataFrame: #| pa.Table: #TODO: table
        """Access or construct the Barillas-Shanken factor model."""
        if self._data is not None:
            return self._data

        # for this model, construction is the download process
        df = self._construct()
        
        _ordered = self._rearrange_columns(df)
        _sliced = self._slice_to_range(_ordered)
        
        self._data = _sliced
        return self._data


    def _construct(self) -> pd.DataFrame:
        """Private: builds the Barillas-Shanken factor model.
        
        Creates the model by calling .extract() on QFactors (R_IA, R_ROE),
        HMLDevilFactors (HML_Devil, RF) and FamaFrenchFactors (Mkt-RF, 
        SMB and UMD).
        ---
        NOTE:
        - Uses the higher-precision RF from AQR as the risk-free rate (RF).
        - Construction is triggered by the .data property override.
        """
        #TODO: Check, double-check the source precisions.

        self.log.info("Constructing Barillas-Shanken 6 Factor Model...")

        self.log.info("Downloading q-factors...")  #TODO: check if classic is faster
        _q = QFactors(frequency=self.frequency).extract(['R_IA', 'R_ROE'])

        self.log.info("Downloading Fama-French factors...")
        _ff = FamaFrenchFactors(model='6', frequency=self.frequency).extract(['Mkt-RF', 'SMB', 'UMD'])
        
        self.log.info("Downloading the HML_Devil factors. This can take up to a minute, please be patient...")
        _devil = HMLDevilFactors(frequency=self.frequency).extract(['HML_Devil', 'RF'])

        # Quick work around: using pd to join, back to table and 
        #   enforce the schema.
        # pd ------------------------------------------------------ #
        #join not merge
        df = _ff.join([_q, _devil], how='inner')
        # fix: index to yyyymmdd str (schema)
        df.index = df.index.strftime('%Y%m%d')
        # pa-------------------------------------------------------#
        table = pa.Table.from_pandas(df.reset_index())
        # schema enforement for Barillas Shanken here...
        _ordered = self.schema.names
        table = table.select(_ordered).cast(self.schema)
        table.validate(full=True)

        _df = table.to_pandas().set_index('date')
        # pd-------------------------------------------------------#
        return _df


    def _get_url(self) -> str:
        """Composite model: no remote source."""
        return ""

    def _read(self, data: bytes) -> pd.DataFrame:
        """Composite model: constructed via sub-models, not parsed from bytes."""
        self.log.warning("BarillasShanken: _read called on composite model.")
        return pd.DataFrame()

