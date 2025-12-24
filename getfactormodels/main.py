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
from typing import Any
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.cli import parse_args
from getfactormodels.utils.utils import _get_model_key, _save_to_file
from .models import (
    BarillasShankenFactors,
    CarhartFactors,
    DHSFactors,
    FamaFrenchFactors,
    HMLDevilFactors,
    ICRFactors,
    LiquidityFactors,
    MispricingFactors,
    QFactors,
)


def get_factors(model: str | int = 3,
                frequency: str = "m",
                *,   #keyword all but model/freq
                start_date: str | None = None,
                end_date: str | None = None,
                output_file: str | None = None,
                region: str | None = None,
                **kwargs: Any) -> FactorModel:
    """Get and process factor model data.

    The primary entry point for the getfactormodels package. Maps the 'model' 
    param the specific FactorModel subclass and initializes it with the 
    requested parameters.

    Args:
        model (str, int): the name of the factor model.
            one of: '3', '4', '5', '6', 'carhart', 'liq', 'misp', 'icr',
            'dhs', 'qclassic', 'q', 'hml_d'.
        frequency  (str, optional): the frequency of the data. Default: 'm'.
            'd' 'w' 'w2w' 'm' 'q' or 'y'
        start_date (str, optional): start date, YYYY-MM-DD.
        end_date (str, optional): end date, YYYY-MM-DD.
        output(str, optional): filepath to write returned data to, e.g. "~/some/dir/some_file.csv"

    """
    model_key = _get_model_key(model)

    model_map = {
        "3": FamaFrenchFactors, "4": CarhartFactors,
        "5": FamaFrenchFactors, "6": FamaFrenchFactors,
        "Q": QFactors, "Qclassic": QFactors,
        "Mispricing": MispricingFactors,
        "Liquidity": LiquidityFactors,
        "ICR": ICRFactors,
        "DHS": DHSFactors,
        "HMLDevil": HMLDevilFactors,
        "BarillasShanken": BarillasShankenFactors,
    }

    if model_key not in model_map:
        raise ValueError(f"Unknown model: '{model}' (mapped to '{model_key}')")

    factorclass = model_map[model_key]

    if not factorclass:
        raise ValueError(f"Unknown model '{model}' (mapped to '{model_key}').")

    params = {
        "frequency": frequency.lower(),
        "start_date": start_date,
        "end_date": end_date,
        "output_file": output_file,
        **kwargs, # cache_ttl etc
    }

    if factorclass is FamaFrenchFactors:
        params.update({"model": model_key, "region": region})
    elif factorclass is CarhartFactors:
        params.update({"region": region})
    
    elif model_key == "Qclassic":
        params["classic"] = True

    return factorclass(**params)


def main():
    args = parse_args()
    # TODO: list models
    if not args.model:
        print("Error: The -m/--model argument is required.")
        return

    model_obj = get_factors(
        model=args.model,
        frequency=args.frequency,
        start_date=args.start,
        end_date=args.end,
        region=args.region,
    )

    if args.extract:
        model_obj.extract(args.extract)
    elif args.drop:
        model_obj.drop(args.drop)

    display_data = model_obj.data

    if args.output:
        _save_to_file(display_data, args.output, model_instance=model_obj)

    if not args.quiet:
        print(display_data)
        
if __name__ == "__main__":
    main()
