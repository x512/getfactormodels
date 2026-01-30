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
from getfactormodels.utils.utils import _get_model_key
from getfactormodels.utils.cli import _cli
import warnings 

log = logging.getLogger("getfactormodels")


def portfolio(source: str = 'ff', **kwargs):
    """Factory to retrieve Portfolio Returns.
    
    Only Fama French industry portfolios and sorts at the min.
    """
    source = source.lower()
    # FF only
    if source in ['ff', 'famafrench']:
        from getfactormodels.models.fama_french import _get_ff_portfolios
        return _get_ff_portfolios(**kwargs)
    raise ValueError(f"Portfolio source '{source}' not recognized.")


def model(model: str | int | list[str | int] = 3, **kwargs) -> FactorModel: #Self
    """
    """
    if isinstance(model, list):
        if len(model) == 1:
            model = model[0]  # Extract single item from list
        else:
            from getfactormodels.models.base import ModelCollection
            return ModelCollection(model_keys=model, **kwargs)

    # don't pop, so can check later, or let class handle it.
    region = kwargs.get('region', 'usa')
    model_key = _get_model_key(model)

    model_class_map = {
        "3": "FamaFrenchFactors",
        "5": "FamaFrenchFactors",
        "6": "FamaFrenchFactors",
        "4": "CarhartFactors",
        "Qclassic": "QFactors",
        "HighIncomeCCAPM": "HighIncomeCCAPM",
        "ConditionalCAPM": "ConditionalCAPM",
    }

    class_name = model_class_map.get(model_key, f"{model_key}Factors")
    factor_class = getattr(factor_models, class_name, None)

    if not factor_class:
        raise ValueError(f"Model '{model}' not recognized.")

    if model_key in ("3", "5", "6"):
        kwargs["model"] = model_key
    
    if model_key == "Qclassic":
        kwargs["classic"] = True

    
    if issubclass(factor_class, RegionMixin):
        kwargs['region'] = region
    elif region:
        log.warning(f"  '{class_name}' does not support regions. Ignoring: region '{region}'")
    
    return factor_class(**kwargs)


def get_factors(*args, **kwargs):
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
