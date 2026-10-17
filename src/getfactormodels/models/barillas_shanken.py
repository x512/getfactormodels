# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
"""Barillas and Shanken (2018) 6-Factor Model implementation."""

import pyarrow as pa
from getfactormodels.utils.arrow_utils import select_table_columns
from getfactormodels.utils.http_client import _HttpClient
from .aqr_models import HMLDevilFactors
from .base import CompositeModel
from .fama_french import FamaFrenchFactors
from .q_factors import QFactors


class BarillasShankenFactors(CompositeModel):
    """Barillas-Shanken 6-Factor Model (2018).

    Constructs a factor model combining Mkt-RF, SMB (Fama-French 
    3 Factor model); IA, ROE (Hou-Xue-Zhang q-factor model) and UMD, 
    HML_Devil, RF_AQR (AQR data).

    References:
        F. Barillas and J. Shanken, 2018. Comparing Asset Pricing Models. 
        Journal of Finance, vol. 73, no. 2, pp. 715–754.
    """
    def __init__(self, frequency: str = 'm', **kwargs):
        """Barillas-Shanken 6-Factor Model.

        Args:
            frequency (str, optional): The frequency of the data ('m' for monthly, 
                'd' for daily). Default is 'm'.
            **kwargs: Additional keyword arguments passed down to component models 
                (start_date, end_date, cache_ttl, output_file).
        """
        kwargs.pop('model', None)
        super().__init__(frequency=frequency, **kwargs)
        
        # Init upfront so metadata, URLs, and cache keys work
        self.ff = FamaFrenchFactors(model='3', frequency=frequency, **kwargs)
        self.q = QFactors(frequency=frequency, **kwargs)
        self.aqr = HMLDevilFactors(frequency=frequency, **kwargs)
        self.instances = [self.ff, self.q, self.aqr]

    @property
    def components(self) -> list:
        """Submodels of this composite model."""
        return self.instances

    def _construct(self, client: _HttpClient) -> pa.Table:
        self.ff.load(client=client)
        self.q.load(client=client)
        self.aqr.load(client=client)

        mkt_smb = select_table_columns(self.ff.data, ['Mkt-RF', 'SMB'])
        ia_roe = select_table_columns(self.q.data, ['IA', 'ROE'])
        umd_hml = select_table_columns(self.aqr.data, ['UMD', 'HML_Devil', 'RF_AQR'])

        table = (
            mkt_smb.join(ia_roe, keys='date')
            .join(umd_hml, keys='date')
        )
        return table.select(self.schema.names).cast(self.schema)

    @property
    def _frequencies(self) -> list[str]: return ["d", "m"]
    
    @property
    def _precision(self) -> int: return 8
    
    @property
    def schema(self) -> pa.Schema:
        """Barillas-Shanken schema."""
        return pa.schema([
            ('date', pa.string()),
            ('Mkt-RF', pa.float64()),
            ('SMB', pa.float64()),
            ('HML_Devil', pa.float64()),
            ('IA', pa.float64()),
            ('ROE', pa.float64()),
            ('UMD', pa.float64()),
            ('RF_AQR', pa.float64()),
        ])

