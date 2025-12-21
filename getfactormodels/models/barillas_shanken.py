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
    NOTE: Relies on HML Devil being retreived, which is slow. 
    """
    # Not ideal but working
    #TODO: extract instead of dl
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def _get_url(self) -> str:  # I guess?
        msg = "_read called on BarillasShanken: no data source url for barillas, returning empty str."
        self.log.warning(msg)
        return ""

    def _read(self, data: bytes) -> pd.DataFrame:
        # Must exist to satisfy the base class contract.
        self.log.warning("_read called on a constructed model (Barillas Shanken). Don't do that.")
        return pd.DataFrame()

    @property
    def data(self) -> pd.DataFrame:
        """Access the processed dataset, constructing it if necessary.
        
        NOTE:
        - Barillas Shanken is a composite model. This property overrides the base 
        `FactorModel.data` to trigger construction from sub-models (Fama-French, 
        Q-Factors, and HML Devil) rather than trying to initiate a download.
        """
        if self._data is not None:
            return self._data

        # for this model, construction is the download process
        df = self._construct_bs()
        
        _ordered = self._rearrange_columns(df)
        _sliced = self._slice_to_range(_ordered)
        
        self._data = _sliced
        return self._data

    @override
    def download(self) -> pd.DataFrame:
        """Overrides base download() to ensure export works."""
        df = self.data # Hits the new property above
        if self.output_file:
            self.to_file(df, self.output_file)
        return df

    # ----------------------------------------------------------------- #
    def _construct_bs(self) -> pd.DataFrame:
        """Internal: Build the Barillas-Shanken factor model.

        Extracts the "R_IA" and "R_ROE" from q-factors, the "HML_Devil" 
        and "RF" factors from AQR, and "Mkt-RF", "SMB" and "UMD" factors 
        from the Fama-French 6-Factor model.

        ---
        NOTE: uses the higher precision "RF" returned by AQR's HML_Devil.
        TODO: double check actual hml devil precision...
        TODO: q, ff and hml_d returning pa.Tables.
        """
        print("- Getting q factors...")
        _q = QFactors(frequency=self.frequency, classic=True).extract(['R_IA', 'R_ROE'])
        if self.frequency == 'm':
            _q.index = _q.index + pd.offsets.MonthEnd(0)  # TODO

        print("- Getting Fama-French factors...")
        ff = FamaFrenchFactors(model='6', frequency=self.frequency).extract(['Mkt-RF', 'SMB', 'UMD'])

        df = _q.merge(ff, left_index=True, right_index=True, how='inner')        

        print("- Getting HML_Devil factor (this can take a while,"
            " please be patient)...")

        hmld_data = HMLDevilFactors(frequency=self.frequency)

        hml_d = hmld_data.download()
        hml_d = hml_d[['HML_Devil', 'RF']]

        hml_d.index.name = 'date'
        df = df.merge(hml_d, left_index=True, right_index=True, how='inner')

        return df
