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
__version__ = "0.0.6"

#from getfactormodels.models.carhart import CarhartFactors
from .models import MispricingFactors, DHSFactors, ICRFactors, QFactors #dev:new classes
from .models import LiquidityFactors, CarhartFactors    # dev:new classes
from .models import HMLDevil, FamaFrenchFactors

from .__main__ import FactorExtractor, get_factors
from .models.models import (barillas_shanken_factors, ff_factors)

__all__ = [ "MispricingFactors",       # new classes using http_client (withhttpx)
           "DHSFactors",               # Drops deps: requests, numpy, tabulate
            "ICRFactors",
            "QFactors",
            "LiquidityFactors",
            "CarhartFactors",
            "HMLDevil",
            "FamaFrenchFactors",
            "barillas_shanken_factors",
            "ff_factors",               # old ff model (need to replace in other models)
            "FactorExtractor",          # old class, funcs (TODO replace)
            "get_factors",
            #"q_factors",
            #"q_classic_factors",
            #"liquidity_factors",
            #"hml_devil_factors",
            #"carhart_factors",
            #"mispricing_factors",      # replaced with class
            #"dhs_factors",
            #"icr_factors",
           ]

