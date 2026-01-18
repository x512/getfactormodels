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
import logging
from .main import get_factors
from .models import (
    AQR6Factors,
    BABFactors,
    BarillasShankenFactors,
    CarhartFactors,
    DHSFactors,
    FamaFrenchFactors,
    HMLDevilFactors,
    ICRFactors,
    LiquidityFactors,
    MispricingFactors,
    QFactors,
    QMJFactors,
    VMEFactors,
    ConditionalCAPM,
    HighIncomeCCAPM,
)

logger = logging.getLogger('getfactormodels')
logger.addHandler(logging.NullHandler())

# TODO: setup logging 
# see: https://docs.python.org/3/howto/logging-cookbook.html#logging-to-multiple-destinations
# or use warnings??

__version__ = "0.0.16"
__all__ = [ 
    "get_factors",
    "BABFactors",
    "BarillasShankenFactors",
    "CarhartFactors",
    "DHSFactors",
    "FamaFrenchFactors",
    "HMLDevilFactors",
    "ICRFactors",
    "LiquidityFactors",
    "MispricingFactors",
    "QFactors",
    "QMJFactors",
    "VMEFactors",
    "AQR6Factors",
    "ConditionalCAPM",
    "HighIncomeCCAPM",
]


