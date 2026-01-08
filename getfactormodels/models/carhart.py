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
from .fama_french import FamaFrenchFactors


class CarhartFactors(FamaFrenchFactors): # inheritence ooo
    """Download and process the Carhart 4-Factor model data.
    
    This model extends the Fama-French 3-factor model to 4 factors, 
    adding a momentum factor (MOM).

    References:
    - M. Carhart, ‘On Persistence in Mutual Fund Performance’, Journal
      of Finance, vol. 52, no. 1, pp. 57–82, 1997.

    Data source: 
        https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html
    """
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m", "y"]

    def __init__(self, frequency='m', region=None, **kwargs):
        """Initialize the Carhart 4-Factor model."""
        super().__init__(frequency=frequency,
                         model=4,   # enforce model for FF 
                         region=region,
                         **kwargs)
