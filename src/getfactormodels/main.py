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
from getfactormodels import models as factor_models
from getfactormodels.models.base import FactorModel, RegionMixin
from getfactormodels.utils.cli import _cli
from getfactormodels.utils.registry import get_model_class, get_model_key
from pathlib import Path
from typing import Literal

log = logging.getLogger("getfactormodels")


def clear_global_cache() -> int:
    """Clear all cached data files across all models globally.

    Returns:
        int: Total number of cache entries removed.
    """
    from platformdirs import user_cache_path
    from getfactormodels.utils.cache import _Cache

    cache_dir = user_cache_path(appname="getfactormodels", appauthor="x512", ensure_exists=True)
    with _Cache(cache_dir) as cache:
        return cache.clear_all()


def portfolio(
    formed_on: str | list[str] = 'size',
    sort: str | int | None = None,
    industry: int | None = None,
    weights: str = 'vw',
    frequency: str = 'm',
    start_date: str | None = None,
    end_date: str | None = None,
    *, 
    source: str = 'ff',
    **kwargs,
):
    """Download portfolio return data.

        * Currently supports Fama-French sorts and industry portfolios, US only.

    Args:
        source: Data source identifier (e.g., 'ff', 'q').
        formed_on: Factor(s) to sort on (e.g., 'size', 'bm').
        sort: 'decile', '5x5', also accepts integers (10, 25, etc.)
        industry: Number of industry portfolios.
        weights: Weighting scheme ('vw' or 'ew').
        frequency: Data frequency ('d', 'm', 'y').
        start_date: Optional start date YYYY-[MM-DD].
        end_date: Optional end date YYYY[-MM-DD].
    """
    source = source.lower()

    params = {
        "formed_on": formed_on,
        "sort": sort,
        "industry": industry,
        "weights": weights,
        "frequency": frequency,
        "start_date": start_date,
        "end_date": end_date,
        **kwargs,
    }
    if source == 'q':
        from getfactormodels.models.q_factors import _get_q_portfolios
        return _get_q_portfolios(**params)
        
    if source in ['ff', 'famafrench']:
        from getfactormodels.models.fama_french import _get_ff_portfolios
        return _get_ff_portfolios(**params)

    raise ValueError(f"Portfolio source '{source}' not recognized.")
"""
def portfolio(
    source: str = 'ff',                                # 'q', 'ff', or 'aqr'
    frequency: str = 'm',                              # 'd', 'w', 'm', 'q', 'y'
    grid: str = '2x3',                                 # '2x3', '2x3x3', '3x5', etc.
    count: int | None = None,                          # deciles/quintiles count
    sort: str | list[str] = 'size',                    # sorting variables
    industry: int | None = None,                       # industry portfolio code (e.g., 48, 49)
    weights: Literal['vw', 'ew'] | None = None,        # value-weighted or equal-weighted
    start_date: str | None = None,
    end_date: str | None = None,
    #output_dir: str | Path | None = None,              # directory to save output
    #filename: str | None = None,                       # custom file name
    #file_format: Literal['feather', 'parquet', 'csv', 'md', 'txt'] = 'csv', # storage format
    *,
    cache_ttl: int | None = None,
    dividends: bool = True,                            # True for cum-div (ret_vw), False for ex-div (retx_vw)
    **kwargs,
):


    Args:
        source: Data source identifier ('ff', 'q', 'aqr').
        frequency: Data frequency ('d', 'w', 'm', 'q', 'y').
        grid: Portfolio sort grid / dimensions (e.g., '2x3', '2x3x3', '3x5').
        count: Optional numeric count for sorts (e.g., deciles).
        sort: Factor(s) or variable(s) to sort on (e.g., 'size', ['size', 'bm']).
        industry: Optional industry portfolio classification count.
        weights: Weighting scheme ('vw' or 'ew').
        start_date: Optional start date YYYY-[MM-DD].
        end_date: Optional end date YYYY-[MM-DD].
        cache_ttl: Optional cache Time-To-Live in seconds.
        dividends: True for total returns (cum-div), False for capital gains (ex-div).
    
    src = source.lower().strip()

    params = {
        "frequency": frequency,
        "grid": grid,
        "count": count,
        "sort": sort,
        "industry": industry,
        "weights": weights,
        "start_date": start_date,
        "end_date": end_date,
        "cache_ttl": cache_ttl,
        "dividends": dividends,
        **kwargs,
    }

    if src in ['q', 'global-q']:
        from getfactormodels.models.q_factors import _get_q_portfolios
        return _get_q_portfolios(**params)
        
    if src in ['ff', 'famafrench']:
        from getfactormodels.models.fama_french import _get_ff_portfolios
        return _get_ff_portfolios(**params)

    raise ValueError(f"Portfolio source '{src}' not recognized.")

"""

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
    else:
        raise NotImplementedError(f"Anomalies not supported for source: '{source}'")


def model(
    model: str | int | list[str | int] = 3,
    region: str = 'usa',
    frequency: str = 'm',
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    #force: bool = False,  #not implemented properly yet
    clear_cache: bool = False, # ye
    **kwargs,
) -> FactorModel:
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
        # Multi-model collection
        from getfactormodels.models.base import ModelCollection
        model_keys = [get_model_key(m) for m in model_input]
        collection = ModelCollection(
            model_keys=model_keys,
            region=region,
            frequency=frequency,
            start_date=start_date,
            end_date=end_date,
            #force=force,
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

    instance = model_class(
        model=model_key,
        frequency=frequency,
        start_date=start_date,
        end_date=end_date,
        #force=force,
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
