# -*- coding: utf-8 -*-
# MIT License
#
# Copyright (c) 2023 S. Martin
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
__version__ = "0.0.3"

from .__main__ import FactorExtractor, get_factors
from .models import models  # noqa: F401
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
