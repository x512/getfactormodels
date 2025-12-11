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
#import logging #todo
from getfactormodels.utils.utils import _process
from .fama_french import FamaFrenchFactors


class CarhartFactors(FamaFrenchFactors): # inheritence ooo
    """Download the Carhart 4-Factor model.

    References: 
    - M. Carhart, ‘On Persistence in Mutual Fund Performance’, Journal
    of Finance, vol. 52, no. 1, pp. 57–82, 1997.

    Data source: k. french data lib
    """
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m", "y"]

    def __init__(self, frequency='m', **kwargs):
        model = '4'                     # enforce model 
        super().__init__(frequency=frequency, 
                         model=model,   # give it to ff 
                         **kwargs)


    def download(self):
        """Downloads Carhart 4-factor data."""
        # call the parent download logic.      
        _data = super().download() 
        
        if _data.empty:
            raise ValueError("ERR: returned data is empty.")

        return _process(_data, self.start_date, self.end_date, self.output_file)
