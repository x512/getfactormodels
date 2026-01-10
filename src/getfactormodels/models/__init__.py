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
from .aqr_models import (
    AQR6Factors,
    BABFactors,
    HMLDevilFactors,
    QMJFactors,
    VMEFactors,
)
from .barillas_shanken import BarillasShankenFactors
from .carhart import CarhartFactors
from .dhs import DHSFactors
from .fama_french import FamaFrenchFactors
from .icr import ICRFactors
from .liquidity import LiquidityFactors
from .mispricing import MispricingFactors
from .q_factors import QFactors
