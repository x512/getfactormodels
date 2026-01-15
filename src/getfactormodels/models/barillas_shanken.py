# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
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

    References:
        F. Barillas and J. Shanken, 2018. Comparing Asset Pricing Models. 
        Journal of Finance, vol. 73, no. 2, pp. 715–754.

    """
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
    def _frequencies(self) -> list[str]: return ["d", "m"]
    
    @property
    def _precision(self) -> int: return 8
    
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
