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
from getfactormodels.utils.utils import _slice_dates
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
        `frequency` (`str`): the frequency of the data. `d`, `m` (default: `m`)
        (default: `m`). Note: Only the 3 factor model is available in weekly.
        `start_date` (`str, optional`): the start date of the data, as 
            YYYY-MM-DD.
        `end_date` (`str, optional`): the end date of the data, as YYYY-MM-DD.
        `cache_ttl` (`int, optional`): Cached download time-to-live in seconds 
            (default: `86400`).

    Note:
        - Relies on the HML Devil factors being retrieved (which is very slow).

    Returns:
        pd.DataFrame: A timeseries of the factor data.

    References:
    - F. Barillas and J. Shanken, ‘Comparing Asset Pricing Models’, Journal of 
    Finance, vol. 73, no. 2, pp. 715–754, 2018.
    """
    # Not ideal but working
    #TODO: extract instead of dl
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def _get_url(self) -> str:  # I guess?
        msg = "_read called on BarillasShanken. Constructed from other models. No URL."
        self.log.warning(msg)
        return ""

    def _read(self, data: bytes) -> pd.DataFrame:
        # Must exist to satisfy the base class contract.
        self.log.warning("_read called on a constructed model (Barillas Shanken). Don't do that.")
        return pd.DataFrame()

    @override
    def download(self) -> pd.DataFrame:
        """Overrides base download() for barillas shanken."""
        if self._data is not None:
            self.log.debug("Data loaded. Returning stored DataFrame.")
            return self._data

        df = self._construct_bs()
        
        self._data = df 
        
        return df
    # ----------------------------------------------------------------- #
    def _construct_bs(self) -> pd.DataFrame:
        """Constructs the Barillas 6 factor model from other models"""

        print("  - Getting q factors...")
        _q = QFactors(frequency=self.frequency, 
                      classic=True).extract(['R_IA', 'R_ROE']) 
        # test: if this fails .extract is bad
        
        print("  - Getting Fama-French factors...")
        ffdata = FamaFrenchFactors(model='6', frequency=self.frequency)
        ff = ffdata.download()[['Mkt-RF', 'SMB', 'UMD']]

        # Merge q and Fama-French factors
        df = _q.merge(ff, left_index=True,
                     right_index=True, how='inner')

        print("  - Getting HML_Devil factor (this can take a while,"
            "please be patient)...")

        hmld_data = HMLDevilFactors(frequency=self.frequency,
                                    start_date=self.start_date,
                                    end_date=self.end_date)

        hml_d = hmld_data.download()
        # NOTE: Taking the 'RF' from AQR's series since it's here,
        # and it's the same data as Fama-French but to 4 decimals. 
        # Mkt-RF shows a difference though -- and bs6 should use 
        # the mkt-rf of ff!
        hml_d = hml_d[['HML_Devil', 'RF']]

        hml_d.index.name = 'date'
        df = df.merge(hml_d, left_index=True, right_index=True, how='inner')
        df = _slice_dates(df, self.start_date, self.end_date)

        return df
