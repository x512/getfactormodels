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
from getfactormodels.models.base import FactorModel, Collection, RegionMixin, Composite
from getfactormodels.utils.cli import _cli
from getfactormodels.utils.registry import get_model_class, get_model_key

log = logging.getLogger("getfactormodels")


def clear_global_cache() -> int:
    """Clear all cached data files across all models globally."""
    from platformdirs import user_cache_path
    from getfactormodels.utils.cache import _Cache

    cache_dir = user_cache_path(appname="getfactormodels", appauthor="x512", ensure_exists=True)
    with _Cache(cache_dir) as cache:
        return cache.clear_all()


def portfolio(
    frequency: str = 'm',
    grid: str | None = None,
    count: int | None = None,
    sort: str | list[str] = 'size',
    weights: Literal['vw', 'ew'] | None = None,
    industry: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    #output_dir: str | Path | None = None,
    #filename: str | None = None,
    #file_format: Literal['feather', 'ipc', 'parquet', 'csv', 'md', 'txt'] = 'csv'
    source: str = 'ff',                     # 'q', 'ff', or 'aqr'
    dividends: bool = True,                 # False for ex-div (retx_vw)
    **kwargs,
):
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

    if kwargs.get('region') is not None:
        raise NotImplementedError(
            "Regional data is not yet supported for portfolios (US only). "
            "Please omit the region parameter."
        )

    if 'formed_on' in kwargs:
        warnings.warn(
            "The 'formed_on' parameter is deprecated and will be removed in a future release. "
            "Please use 'sort' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        sort = kwargs.pop('formed_on')

    params = {
        "grid": grid,
        "count": count,
        "sort": sort,
        "weights": weights if weights is not None else 'vw',
        "industry": industry,
        "frequency": frequency,
        "start_date": start_date,
        "end_date": end_date,
        "dividends": dividends,
        **kwargs,
    }

    if source == 'q':
        from getfactormodels.models.q_factors import _get_q_portfolios
        return _get_q_portfolios(**params)

    if source in ['ff', 'famafrench']:
        if frequency == 'w':
            is_weekly = (
                isinstance(sort, (list, tuple)) and
                frozenset(str(x).lower() for x in sort) == frozenset(['size', 'bm']) and
                (grid in (None, '2x3', '6'))
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

def anomaly(
    name: str | list[str] = 'abr_1',
    frequency: str = 'm',
    sort: str | int = 'decile',
    start_date: str | None = None,
    end_date: str | None = None,
    #output_dir: str | Path | None = None,
    #filename: str | None = None,
    #file_format: Literal['feather', 'parquet', 'csv'] = 'feather',
    *,
    provider: str = 'q',
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
    #TODO:
    #output to...
    #output_opts = {"output_dir": output_dir, "filename": filename, "file_format": file_format}

    params = {
        "name": name,
        "frequency": frequency,
        "sort": sort,
        "start_date": start_date,
        "end_date": end_date,
        "cache_ttl": cache_ttl,
        #   **output_opts,
        **kwargs,
    }

    if src in ["q", "global-q"]:
        from getfactormodels.models.q_factors import _get_q_anomalies
        return _get_q_anomalies(**params)
    
    raise NotImplementedError(f"Anomalies not supported for source: '{src}'")


def model(
    model: str | int | list[str | int] = "3",
    region: str = 'usa',
    frequency: str = 'm',
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    #force: bool = False,  #not implemented properly yet
    clear_cache: bool = False, # ye
    **kwargs,
) -> FactorModel | Collection:
    """Download factor model data.

    Args:
        model (str | list[str]): Model identifier.
        region: Geographical region (e.g., 'usa', 'developed').
        frequency: Data frequency ('d', 'w', 'm', 'y').
        start_date: Optional start date (YYYY-MM-DD).
        end_date: Optional end date (YYYY-MM-DD).
        force: Bypass local cache and force fresh redownload.
        clear_cache: Clear local cache entries for this model before returning.
    """
    if isinstance(model, list):
        model_input = model[0] if len(model) == 1 else model
    else:
        model_input = model

    if isinstance(model_input, list):
        model_keys = [get_model_key(m) for m in model_input]
        collection = Collection(
            model_keys=model_keys,
            region=region,
            frequency=frequency,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )
        if clear_cache:
            collection.clear_cache()
        return collection

    model_key = get_model_key(model_input)
    class_name = get_model_class(model_key)

    model_class = getattr(factor_models, class_name, None)
    if model_class is None:
        raise ImportError(f"Class '{class_name}' not found in getfactormodels.models")

    is_regional = issubclass(model_class, RegionMixin)
    if not is_regional:
        if region is not None and region.lower() not in ['usa', 'us']:
            raise ValueError(f"Model '{class_name}' does not support region: {region}")
        kwargs.pop('region', None)
    else:
        kwargs['region'] = region

    kwargs.pop('model', None)

    # In main.py, inside model():
    if issubclass(model_class, Composite):
        instance = model_class(
            frequency=frequency,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )
    else:
        instance = model_class(
            model=model_key,
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
        FutureWarning,
        stacklevel=2,
    )
    return model(*args, **kwargs)


def main():
    _cli()


if __name__ == "__main__":
    main()
