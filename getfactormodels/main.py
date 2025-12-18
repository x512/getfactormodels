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
from getfactormodels.utils.cli import parse_args
from getfactormodels.utils.utils import _get_model_key 
from .models import (  # todo: dont import all models, just QFactor and FamaFrench (function needs changing)
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


# TEMPORARY MINIMAL REWORK (until the keymaps and insane regex is dropped
# and base class and FactorExtractor done)
# just getting get_factors to work!
def get_factors(model: str | int = 3,
                frequency: str = "m",
                start_date: str | None = None,
                end_date: str | None = None,
                output_file: str | None = None,
                *,
                region: str | None = None,
                ):
    """Get data for a specific factor model.

    Main public function.

    Args:
        model (str, float): the name of the factor model ('3', '4', '5', '6', 'carhart', 
                            'liq', 'misp', 'icr', 'dhs', 'qclassic', 'q', 'hml_d').
        frequency  (str, optional): the frequency of the data. ('d' 'w' 'm' 'q' or 'y', default: 'm')
        start_date (str, optional): start date of the returned data, YYYY-MM-DD.
        end_date (str, optional): end date of returned data, YYYY-MM-DD.
        output(str, optional): filepath to write returned data to, e.g. "~/some/dir/some_file.csv"

    """
    # NOTE: Parses input, assigns it to a factor model class, calls it, and returns the data.
    #
    # TODO: FIXME. kwargs. TODO: default outputs in CLI.
    #
    frequency = frequency.lower()
    region = region
    model_key = _get_model_key(model)

    factor_instance = None
    if region is not None and model_key not in ['3', '4', '5', '6']:
        _model = _get_model_key(model)
        raise ValueError(
            f"Region '{region}' is not supported for the '{_model}'. "
            "The region parameter is only available for Fama-French models (3, 4, 5, 6).",
        )
    if model_key in ["3", "4", "5", "6"]:
        factor_instance = FamaFrenchFactors(model=model_key,
                                            frequency=frequency,
                                            start_date=start_date,
                                            end_date=end_date,
                                            output_file=output_file,
                                            region=region)

    elif model_key == "Qclassic":
        factor_instance = QFactors(frequency=frequency, start_date=start_date,
                                   end_date=end_date, output_file=output_file, classic=True)

    else:
        # Class loading: tries CamelCaseFactors then UPPERCASEFactors
        class_name_camel = f"{model_key}Factors"
        class_name_upper = f"{model_key.upper()}Factors"

        # search for the class in the global scope
        FactorClass = globals().get(class_name_camel) or globals().get(class_name_upper)

        if not FactorClass:
            raise ValueError(f"Invalid model: '{model_key}'. "
                             f"Tried:'{class_name_camel}','{class_name_upper}'.")

        factor_instance = FactorClass(frequency=frequency,
                                      start_date=start_date,
                                      end_date=end_date,
                                      cache_ttl=86400,
                                      output_file=output_file)

    return factor_instance

 
def main():
    args = parse_args()
    
    model_obj = get_factors(
        model=args.model,
        frequency=args.frequency,
        start_date=args.start,
        end_date=args.end,
        region=args.region,
        output_file=args.output  # base handles output
    )

    if args.extract:
        data = model_obj.extract(args.extract)
    elif args.drop:
        data = model_obj.drop(args.drop)
    else:
        data = model_obj.data
    
    if args.output:
        model_obj.to_file(args.output)
    
    else:
        if not args.quiet: 
            print(data)
