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
__version__ = "0.0.7"
import logging

logging.getLogger('getfactormodels').setLevel(logging.DEBUG)

from .__main__ import FactorExtractor, get_factors
from .models import (BarillasShankenFactors, DHSFactors, CarhartFactors,
                     FamaFrenchFactors, HMLDevilFactors, ICRFactors, LiquidityFactors,
                     MispricingFactors, QFactors)

    # ADD CARHART BACK TODO TODO FIXME

__all__ = [ "MispricingFactors",
            "DHSFactors",
            "ICRFactors",
            "QFactors",
            "LiquidityFactors",
            "CarhartFactors",   #TODO: FIXME: dev, while moving to base models carhart's erroring
            "HMLDevilFactors",
            "FamaFrenchFactors",
            "BarillasShankenFactors",
            "FactorExtractor",
            "get_factors",
           ]

