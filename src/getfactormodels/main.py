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
from typing import Literal, Any 
from collections.abc import Sequence
from getfactormodels import models as factor_models
from getfactormodels.exceptions import GetFactorModelsDeprecationWarning
from getfactormodels.models.base import (
    Collection,
    Composite,
    FactorModel,
    RegionMixin,
    Base,
)
from getfactormodels.utils.cli import _cli
from getfactormodels.registry.model_registry import get_model_class, get_model_key

log = logging.getLogger("getfactormodels")


def portfolio(
    factors: str | Sequence[str | Sequence[str]] | None = None,
    sort: str | None = None,
    *,
    source: Literal["ff", "q"] = "ff",
    industry: int | None = None,
    weights: Literal["vw", "ew"] = "vw",
    dividends: bool = True,
    frequency: str = "m",
    start_date: str | None = None,
    end_date: str | None = None,
    **kwargs: Any,
) -> Base:
    """Construct a cross-sectional portfolio dataset.

    Args:
        factors: Factor(s) or factor group(s) to sort on. Ignored when
            ``industry`` is set.
        sort: Sort specification. ``'bivariate'`` selects the source's
            default 2-way sort (FF: '5x5', Q: '3x5'). Source-specific
            vocabulary:
              FF — '2x3', '2x4x4', '5x5', '10x10', 'bivariate', '3', '5', '10';
              Q  — '2x3', '2x3x3', '3x5', 'decile', 'bivariate'.
            Defaults to the source's default sort.
        source: Data source. 'ff' (Fama-French) or 'q' (q-factors).
        industry: Industry count for FF industry portfolios. Mutually
            exclusive with ``factors`` and ``sort``.
        weights: 'vw' (value-weighted) or 'ew' (equal-weighted).
        dividends: Include dividends in returns.
        frequency: Data frequency.
        start_date, end_date: Optional date range.
        **kwargs: Forwarded to the source constructor.

    Raises:
        ValueError: On mixed arguments (``industry`` with
            ``factors``/``sort``) or invalid ``source``.
    """
    src = source.lower()
    if src not in ("ff", "q"):
        raise ValueError(f"Unknown portfolio source: {source!r}. Valid: 'ff', 'q'.")

    if industry is not None:
        if src != "ff":
            raise ValueError(f"'industry' requires source='ff', got {source!r}.")
        if factors is not None or sort is not None:
            raise ValueError("Cannot combine 'industry' with 'factors'/'sort'.")
        from getfactormodels.models.ff_industries import FamaFrenchIndustryPortfolios
        return FamaFrenchIndustryPortfolios(
            industries=industry,
            weights=weights,
            dividends=dividends,
            frequency=frequency,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )

    if src == "ff":
        from getfactormodels.models.ff_portfolios import FamaFrenchSorts
        return FamaFrenchSorts(
            factors=factors or "size",
            sort=sort or "10",
            weights=weights,
            dividends=dividends,
            frequency=frequency,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )

    if weights != "vw":
        raise ValueError(
            "Global-Q portfolios provide value-weighted returns only; "
            f"got weights={weights!r}. Use source='ff' for equal-weighted."
        )
    # Q publishes 3x5 as its bivariate sort; the universal "bivariate"
    # request maps to it here. Other sorts pass through unchanged.
    q_sort = "3x5" if sort == "bivariate" else sort
    from getfactormodels.models.q_factors import GlobalQPortfolios
    return GlobalQPortfolios(
        factor=factors or ["me", "ia", "roe"],
        sort=q_sort,
        dividends=dividends,
        frequency=frequency,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def model(
    model: str | int | list[str | int] = "3",
    region: str | None = None,
    frequency: str = "m",
    start_date: str | None = None,
    end_date: str | None = None,
    *,
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
