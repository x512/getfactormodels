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
from getfactormodels.utils.utils import _process
from .ff_models import _get_ff_factors

import logging #todo
#TODO: clean up getting ff, getting mom, this.
#
# still just using old func, just a wrapper
class CarhartFactors:
    """Download the Carhart 4-Factor model data.

    NOTES:
    - Fama-French 4-factor model/Fama-French 3-Factor model with a Momentum factor (MOM).
    - Factors: Mkt-RF, SMB, HML, MOM
    - Author: Mark M. Carhart
    - pub: M. Carhart, ‘On Persistence in Mutual Fund Performance’,
      Journal of Finance, vol. 52, no. 1, pp. 57–82, 1997.
    - Data source: k. french data lib
    """
    def __init__(self, frequency='m', start_date=None, end_date=None,
                 output_file=None):
        self.frequency = frequency.lower()

        if frequency.lower() not in ['d', 'm', 'y']:
            raise ValueError("Carhart factors are only available for daily (d),"
                         "monthly (m) and yearly ('y') frequencies.")

        self.start_date = start_date
        self.end_date = end_date
        self.output_file = output_file

    def download(self):
        data = _get_ff_factors(model='4', frequency=self.frequency, start_date=self.start_date,
                               end_date=self.end_date)
        if data is None:
            raise ValueError("ERR: returned data is None?")

        return _process(data, self.start_date, self.end_date, self.output_file)
