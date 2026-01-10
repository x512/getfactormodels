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
import pyarrow as pa
from getfactormodels.utils.arrow_utils import select_table_columns
from getfactormodels.utils.http_client import _HttpClient
from .aqr_models import HMLDevilFactors
from .base import CompositeModel
from .fama_french import FamaFrenchFactors
from .q_factors import QFactors


class BarillasShankenFactors(CompositeModel):
    """Download the Barillas-Shanken 6 Factor Model (2018).

    Combines: Mkt-RF, SMB (FF), IA, ROE (Hou-Xue-Zhang), 
    and UMD, HML_Devil (AQR).

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
    def __init__(self, frequency: str = 'm', cache_ttl: int = 86400, **kwargs): 
        """Initialize the Barillas-Shanken 6-Factor model."""
        super().__init__(frequency=frequency, cache_ttl=cache_ttl, **kwargs)

    def _construct(self, client: _HttpClient) -> pa.Table:
        ff = FamaFrenchFactors(model='3', frequency=self.frequency).load(client=client)
        q = QFactors(frequency=self.frequency).load(client=client)
        aqr = HMLDevilFactors(frequency=self.frequency).load(client=client)

        mkt_smb = select_table_columns(ff.data, ['Mkt-RF', 'SMB']) # might switch to using aqr's data (SMB = SMB FF)
        ia_roe = select_table_columns(q.data, ['R_IA', 'R_ROE'])
        umd_hml = select_table_columns(aqr.data, ['UMD', 'HML_Devil', 'RF_AQR'])

        table = (
            mkt_smb.join(ia_roe, keys='date')
            .join(umd_hml, keys='date')
        )
        # TODO: helper util to drop NaNs only if continuous from the edges
        return table.select(self.schema.names).cast(self.schema)

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
