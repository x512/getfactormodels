#!/usr/bin/env python3
# getfactormodels: A Python package to retrieve financial factor model data.
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
import logging
import warnings
from typing import Literal 
from collections.abc import Sequence
from getfactormodels import models as factor_models
from getfactormodels.exceptions import GetFactorModelsDeprecationWarning
from getfactormodels.models.base import (
    Collection,
    Composite,
    FactorModel,
    PortfolioBase,
    RegionMixin,
)
from getfactormodels.utils.cli import _cli
from getfactormodels.utils.registry import get_model_class, get_model_key

log = logging.getLogger("getfactormodels")


# In main.py
def portfolio(
    source: str = "ff",
    industry: int | None = None,
    sort: str | list[str] | None = None,
    grid: str | None = None,
    count: int | None = None,
    weights: str = "vw",
    bivariate: bool = False,
    **kwargs,
):
    src = source.lower()

    if src == "ff":
        from getfactormodels.models.fama_french import _get_ff_portfolios
        return _get_ff_portfolios(
            industry=industry, sort=sort, grid=grid, count=count, weights=weights, **kwargs
        )

    elif src == "q":
        from getfactormodels.models.q_factors import _get_q_portfolios
        return _get_q_portfolios(
            sort=sort, grid=grid, count=count, weights=weights, bivariate=bivariate, **kwargs
        )

    raise ValueError(f"Unknown portfolio source: '{src}'")

    # Pluralise? hmm
def anomaly(
    name: str | Sequence[str],
    frequency: Literal["d", "m"] = "m",
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    region: str = "usa",
    weight: Literal["vw_cap", "vw", "ew"] = "vw_cap",
    as_percentage: bool = False,
    **kwargs,
):
    """Download JKP Global Factor data.

    Args:
        name: Factor or theme code(s) — e.g. 'bm', 'mom', 'quality'.
        frequency: 'd' or 'm'.
        region: JKP region code (default 'usa').
        weight: 'vw_cap', 'vw', or 'ew'.
    """
    from getfactormodels.models.jkp import JKPFactors

    return JKPFactors(
        factor=name,
        frequency=frequency,
        region=region,
        weight=weight,
        start_date=start_date,
        end_date=end_date,
        as_percentage=as_percentage,
        **kwargs,
    )



def model(
    model: str | int | list[str | int] = "3",
    region: str | None = None,
    frequency: str = "m",
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    # force: bool = False,  #not implemented
    clear_cache: bool = False,
    **kwargs,
) -> FactorModel | Collection:
    """Download factor model data.

    Args:
        model (str | list[str]): Model identifier.
        region: Geographical region (e.g., 'usa', 'developed'). Defaults to model's default.
        frequency: Data frequency ('d', 'w', 'm', 'y').
        start_date: Optional start date (YYYY-MM-DD).
        end_date: Optional end date (YYYY-MM-DD).
        clear_cache: Clear local cache entries for this model before returning.
    """
    # normalize single-item list to str
    if isinstance(model, list) and len(model) == 1:
        model = model[0]

    # Collection
    if isinstance(model, list):
        model_keys = [get_model_key(m) for m in model]
        instance = Collection(
            model_keys=model_keys,
            region=region,
            frequency=frequency,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )

    # single FactorModel request
    else:
        model_key = get_model_key(model)
        class_name = get_model_class(model_key)
        model_class = getattr(factor_models, class_name, None)

        if model_class is None:
            raise ImportError(
                f"Class '{class_name}' not found in getfactormodels.models"
            )

        if issubclass(model_class, RegionMixin):
            if region is not None:
                kwargs["region"] = region
        elif region is not None and str(region).lower() not in ("usa", "us"):
            raise ValueError(
                f"Model '{class_name}' does not support region: {region}"
            )

        # kwargs
        kwargs.pop("model", None)  # Prevent duplicate 'model' kwarg
        if not issubclass(model_class, Composite):
            kwargs["model"] = model_key

        instance = model_class(
            frequency=frequency,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )

    if clear_cache:
        instance.clear_cache()

    return instance


def get_factors(*args, **kwargs):  # noqa
    """DEPRECATED: Use `model()` instead."""
    warnings.warn(
        "get_factors() is deprecated and will be removed in a future version. "
            "Please use model() for factor data or portfolio() for return data.",
        category=GetFactorModelsDeprecationWarning,
        stacklevel=2,
    )
    return model(*args, **kwargs)


def main():
    _cli()


if __name__ == "__main__":
    main()
