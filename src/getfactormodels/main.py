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


def portfolio(
    frequency: str = "m",
    grid: str | None = None,
    count: int | None = None,
    sort: str | list[str] = "size",
    weights: Literal["vw", "ew"] | None = None,
    industry: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    source: str = "ff",      # 'q', 'ff', or 'aqr'
    dividends: bool = True,  # False for ex-div (retx_vw)
    **kwargs,
) -> PortfolioBase | Collection:
    """Download portfolio return data.

    Args:
        frequency: Data frequency ('d', 'm', 'y').
        grid: Interaction grid layout for bivariate/multivariate sorts (e.g. '2x3').
        count: Number of portfolios for univariate sorts (e.g. 10 for deciles).
        sort: Factor(s) to sort on (e.g., 'size', ['size', 'bm']).
        weights: Weighting scheme ('vw' or 'ew').
        industry: Number of industry portfolios.
        start_date: Optional start date YYYY-[MM-DD].
        end_date: Optional end date YYYY[-MM-DD].
        source: Data source identifier ('ff', 'q').
        dividends: if false, ex-divs (q only)
    """
    source = source.lower()

    if kwargs.get("region") is not None:  # TODO: FIXME: Removing this when AQR is ready, after FF refactor.
        raise NotImplementedError(
            "Regional data is not yet supported for portfolios (US only). "
                "Please omit the region parameter."
        )

    if "formed_on" in kwargs:
        warnings.warn(
            "The 'formed_on' parameter is deprecated and will be removed in a future release. "
                "Please use 'sort' instead.",
            category=GetFactorModelsDeprecationWarning,
            stacklevel=2,
        )
        sort = kwargs.pop("formed_on")

    params = {
        "grid": grid,
        "count": count,
        "sort": sort,
        "weights": weights if weights is not None else "vw",
        "industry": industry,
        "frequency": frequency,
        "start_date": start_date,
        "end_date": end_date,
        "dividends": dividends,
        **kwargs,
    }

    if source == "q":
        from getfactormodels.models.q_factors import _get_q_portfolios

        return _get_q_portfolios(**params)

    if source in ["ff", "famafrench"]:
        if frequency == "w":
            is_weekly = (
                isinstance(sort, (list, tuple))
                and frozenset(str(x).lower() for x in sort)
                == frozenset(["size", "bm"])
                and (grid in (None, "2x3", "6"))
            )
            if not is_weekly:
                raise ValueError(
                    "Fama-French weekly portfolio data is only available for the size "
                        "and book-to-market (2x3) portfolio. "
                        "\nUse 'd', 'm', or 'y' for other Fama-French portfolios."
                )
        from getfactormodels.models.fama_french import _get_ff_portfolios

        return _get_ff_portfolios(**params)

    raise ValueError(f"Portfolio source '{source}' not recognized.")

# Pluralise? hmm
def anomaly(
    name: str | list[str] = "abr_1",
    frequency: str = "m",
    sort: str | int = "decile",
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    provider: str = "q",
    cache_ttl: int | None = None,
    **kwargs,
):
    """Download anomaly datasets.

    Args:
        name: Anomaly factor name(s) or category (e.g., 'abr_1', 'momentum', ['abr_1', 'sue_1']).
        frequency: Data frequency. Currently supports monthly ('m') and annual ('y').
        sort: Portfolio sort type ('decile', '2x3', or '3x5').
        start_date: Optional start date (YYYY-[MM-DD]).
        end_date: Optional end date (YYYY-[MM-DD]).
        provider: Data provider identifier, only 'q' available.
        cache_ttl: Optional cache Time-To-Live in seconds.
    """
    src = provider.lower().strip()

    params = {
        "name": name,
        "frequency": frequency,
        "sort": sort,
        "start_date": start_date,
        "end_date": end_date,
        "cache_ttl": cache_ttl,
        **kwargs,
    }

    if src in ["q", "global-q"]:
        from getfactormodels.models.q_factors import _get_q_anomalies

        return _get_q_anomalies(**params)

    raise NotImplementedError(f"Anomalies not supported for source: '{src}'")


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
