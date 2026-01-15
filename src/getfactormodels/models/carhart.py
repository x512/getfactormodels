# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
from .fama_french import FamaFrenchFactors


class CarhartFactors(FamaFrenchFactors):
    """Download and process the Carhart 4-Factor model data.
    
    This model extends the Fama-French 3-factor model to 4 factors, 
    adding a momentum factor (MOM).

    References:
        M. Carhart, 1997. On Persistence in Mutual Fund Performance.
        Journal of Finance, vol. 52, no. 1, pp. 57–82.
        ...

    """
    @property
    def _frequencies(self) -> list[str]: return ["d", "m", "y"]

    def __init__(self, frequency='m', region=None, **kwargs):
        """Initialize the Carhart 4-Factor model."""
        kwargs.pop('model', None)
        super().__init__(frequency=frequency,
                         model=4,   # enforce model for FF 
                         region=region,
                         **kwargs)
