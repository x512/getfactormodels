#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

from .__main__ import FactorExtractor, get_factors
from .models import models  # noqa: F401, RUF100 (silent flake8 in VScode)
from .models.models import (barillas_shanken_factors, carhart_factors,
                            dhs_factors, ff_factors, hml_devil_factors,
                            icr_factors, liquidity_factors, mispricing_factors,
                            q_classic_factors, q_factors)

__all__ = ["FactorExtractor",
           "ff_factors",
           "icr_factors",
           "q_factors",
           "q_classic_factors",
           "mispricing_factors",
           "dhs_factors",
           "liquidity_factors",
           "hml_devil_factors",
           "barillas_shanken_factors",
           "carhart_factors",
           "get_factors", ]
