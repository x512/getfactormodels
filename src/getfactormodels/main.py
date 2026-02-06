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
from getfactormodels import models as factor_models
from getfactormodels.models.base import FactorModel, RegionMixin
from getfactormodels.utils.utils import _get_model_key, get_model_class
from getfactormodels.utils.cli import _cli
import warnings 

log = logging.getLogger("getfactormodels")


def portfolio(
    source: str = 'ff',
    formed_on: str | list[str] = 'size',
    sort: str | int | None = None,
    industry: int | None = None,
    weights: str = 'vw',
    frequency: str = 'm',
    start_date: str | None = None,
    end_date: str | None = None,
    **kwargs
):
    """Download portfolio return data.

    * Currently supports Fama-French sorts and industry portfolios,
    US only.

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
        **kwargs
    }

    if source in ['ff', 'famafrench']:
        from getfactormodels.models.fama_french import _get_ff_portfolios
        return _get_ff_portfolios(**params)

    raise ValueError(f"Portfolio source '{source}' not recognized.")


def model(
    model: str | int | list[str | int] = 3,
    region: str = 'usa',
    frequency: str = 'm',
    start_date: str | None = None,
    end_date: str | None = None,
    **kwargs
) -> FactorModel:   #Self 
    """Download factor model data.
    
    Args:
        model (str | list[str]): Model identifier.
        region: Geographical region (e.g., 'usa', 'developed').
        frequency: Data frequency ('d', 'w', 'm', 'y').
        start_date: Optional start date (YYYY-MM-DD).
        end_date: Optional end date (YYYY-MM-DD).
    """
    if isinstance(model, list):
        if len(model) == 1:
            model = model[0]
        else:
            from getfactormodels.models.base import ModelCollection
            return ModelCollection(
                model_keys=model, 
                region=region, 
                frequency=frequency, 
                start_date=start_date, 
                end_date=end_date, 
                **kwargs
            )

    model_key = _get_model_key(model)
    class_name = get_model_class(model_key) #str 
    
    model_class = getattr(factor_models, class_name, None) #obj
    if model_class is None:
        raise ImportError(f"Class '{class_name}' not found in getfactormodels.models")

    is_regional = issubclass(model_class, RegionMixin)
    if not is_regional:
        if region is not None and region.lower() not in ['usa', 'us']:
             raise ValueError(f"Model '{class_name}' does not support region: {region}")
        kwargs.pop('region', None)
    else:
        kwargs['region'] = region
    
    return model_class(
        frequency=frequency,
        start_date=start_date, 
        end_date=end_date,
        **kwargs
    )


def get_factors(*args, **kwargs): #noqa
    """DEPRECATED: Use `model()` instead.

    This function will be removed in a future release.
    """
    warnings.warn(
        "get_factors() is deprecated and will be removed in a future version. "
            "Please use model() for factor data or portfolio() for return data.",
        FutureWarning,
        stacklevel=2
    )
    return model(*args, **kwargs)


def main():
    _cli()

if __name__ == "__main__":
    main()
