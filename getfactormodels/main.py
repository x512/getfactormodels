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
import os
import sys
from pathlib import Path
from typing import Any
import pyarrow.csv as pv
from getfactormodels.models.aqr_models import _AQRModel
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.cli import parse_args
from getfactormodels.utils.data_utils import filter_table_by_date
from getfactormodels.utils.utils import _generate_filename, _get_model_key
from .models import (
    BABFactors,
    BarillasShankenFactors,
    CarhartFactors,
    DHSFactors,
    FamaFrenchFactors,
    HMLDevilFactors,
    ICRFactors,
    LiquidityFactors,
    MispricingFactors,
    QFactors,
    QMJFactors,
)

_MODEL_MAP = {
    "3": FamaFrenchFactors, "4": CarhartFactors,
    "5": FamaFrenchFactors, "6": FamaFrenchFactors,
    "Q": QFactors, "Qclassic": QFactors,
    "Mispricing": MispricingFactors,
    "Liquidity": LiquidityFactors,
    "ICR": ICRFactors,
    "DHS": DHSFactors,
    "HMLDevil": HMLDevilFactors,
    "BarillasShanken": BarillasShankenFactors,
    "BettingAgainstBeta": BABFactors,
    "QualityMinusJunk": QMJFactors, 
}

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

    if model_key not in _MODEL_MAP:
        raise ValueError(f"Unknown model: '{model}' (mapped to '{model_key}')")

    factorclass = _MODEL_MAP.get(model_key)

    if not factorclass:
        raise ValueError(f"Unknown model '{model}' (mapped to '{model_key}').")

    params = {
        "frequency": frequency.lower(),
        "start_date": start_date,
        "end_date": end_date,
        "output_file": output_file,
        **kwargs, # cache_ttl, country, region, etc
    }

    
    if factorclass is FamaFrenchFactors:
        params.update({"model": model_key, "region": region})
    elif factorclass is CarhartFactors:
        params.update({"region": region})
        params.pop("model", None)  # Remove model from carhart here, it's in its super() call.
    
    elif model_key == "Qclassic":
        params["classic"] = True

    return factorclass(**params)


def main():
    args = parse_args()
    # TODO: list models

    if not args.model:
        print("Error: The -m/--model argument is required.", file=sys.stderr)
        sys.exit(1)
        #return
    
    model_key = _get_model_key(args.model)
    factor_class = _MODEL_MAP.get(model_key)

    if not factor_class:
        print(f"Error: Unknown model '{args.model}'", file=sys.stderr)
        sys.exit(1)

    if args.country:
        if not issubclass(factor_class, _AQRModel):
            print(f"Error: '{args.model}' doesn't support --country, only AQR models do.", file=sys.stderr)
            sys.exit(1)

    model_obj = get_factors(
        model=args.model,
        frequency=args.frequency,
        start_date=args.start,
        end_date=args.end,
        region=args.region,
        country=args.country,
    )

    # These update model_obj._data and model_obj._df = None
    if args.extract:
        model_obj.extract(args.extract)
    elif args.drop:
        model_obj.drop(args.drop)

    table = model_obj._data

    # Save table, not the display df
    if args.output:
        model_obj.to_file(args.output)

        if not args.quiet:
            actual_path = Path(args.output).expanduser()
            if actual_path.is_dir():
                actual_path = actual_path / _generate_filename(model_obj)

            print(f"Data saved to: {actual_path.resolve()}", file=sys.stderr)
    
    # fix: jupyter runs "%%bash" commands in a subprocess (not 
    # interactive), and output is the entire arrow table.
    nb_env = 'ipykernel' in sys.modules or 'JPY_PARENT_PID' in os.environ
    
    if not sys.stdout.isatty() and not nb_env:
        # for pipe/redirects, uses the raw table to csv stream, writes to buffer
        table = model_obj._get_table()
        sliced = filter_table_by_date(table, model_obj.start_date, model_obj.end_date)
        pv.write_csv(sliced, sys.stdout.buffer)
    
    else: #we're interactive, or in a jupyter notebook: write df preview to stdout
        if not args.quiet:
            print(model_obj.data) #uses pandas
if __name__ == "__main__":
    main()
