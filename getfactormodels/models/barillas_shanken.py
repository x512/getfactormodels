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
from typing import Any
import pandas as pd
from getfactormodels.utils.utils import _slice_dates
from .base import FactorModel
from .fama_french import FamaFrenchFactors
from .hml_devil import HMLDevilFactors
from .q_factors import QFactors

#TODO: logging!
class BarillasShankenFactors(FactorModel):
    """Download the Barillas-Shanken 6 Factor Model (2018).

    A combination of the 5-factor model of Fama and French (2015), the q-factor
    model of Hou, Xue, and Zhang (2015), and Asness and Frazzini's HML Devil.
    This is the factor model with the highest posterior inclusion probability
    in Barillas and Shanken (2018).

    Note:
        - Relies on the HML Devil factors being retrieved (which is very slow).

    Returns:
        pd.DataFrame: A timeseries of the factor data.
    """
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def _get_url(self) -> str:  # I guess?
        raise NotImplementedError("no url for Barillas-Shanken")

    def download(self) -> pd.DataFrame:
        df = self._download()
        df = _slice_dates(df, self.start_date, self.end_date)
        df = df.sort_index()
        return df

    # OVERRIDE -- TODO: return bytes, then construct. This could cause problems..
    # TODO: FIXME:
    def _download(self) -> pd.DataFrame:
        """Constructs the Barillas 6 factor model from other models"""

        print("  - Getting q factors...")
        qdata = QFactors(frequency=self.frequency, classic=True)
        q = qdata.download()[['R_IA', 'R_ROE']]

        print("  - Getting Fama-French factors...")
        ffdata = FamaFrenchFactors(model='6', frequency=self.frequency)
        ff = ffdata.download()[['Mkt-RF', 'SMB', 'UMD']]

        # Merge q and Fama-French factors
        df = q.merge(ff, left_index=True,
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

        return df
