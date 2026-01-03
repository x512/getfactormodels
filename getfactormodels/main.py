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
import logging
from pathlib import Path
from typing import Any
import pyarrow.csv as pv
from getfactormodels.models.aqr_models import _AQRModel
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.cli import parse_args
from getfactormodels.utils.data_utils import (
    filter_table_by_date,
    print_table_preview,
)
from getfactormodels.utils.utils import _generate_filename, _get_model_key
from getfactormodels import models as factor_models

logger = logging.getLogger("getfactormodels")

def get_factors(model: str | int = 3,
                frequency: str = "m",
                *,
                start_date: str | None = None,
                end_date: str | None = None,
                output_file: str | None = None,
                region: str | None = None,
                **kwargs: Any) -> FactorModel:
    """Get and process factor model data.

    The primary entry point for the getfactormodels package. Maps the 'model' 
    param the specific FactorModel subclass and initializes it with the 
    requested parameters.

    Args
        model (str, int): the name of the factor model.
            one of: '3', '4', '5', '6', 'carhart', 'liq', 'misp', 'icr',
            'dhs', 'qclassic', 'q', 'hml_d', 'bab', 'qmj'.
        frequency  (str, optional): the frequency of the data. Default: 'm'.
            'd' 'w' 'w2w' 'm' 'q' or 'y'
        start_date (str, optional): start date, YYYY-MM-DD.
        end_date (str, optional): end date, YYYY-MM-DD.
        output(str, optional): filepath to write returned data to, e.g. "~/some/dir/some_file.csv"

    """
    # Get model key, adds "Factors" to key if needed, and calls that class.
    model_key = _get_model_key(model)  # uses MODEL_INPUT_MAP
    if model_key in ["3", "4", "5", "6"]:
        class_name = "FamaFrenchFactors" if model_key != "4" else "CarhartFactors"
    else:
        class_name = f"{model_key}Factors" if not model_key.endswith("Factors") else model_key

    factor_class = getattr(factor_models, class_name, None)    

    if not factor_class:
        msg = f"Model '{model}' not recognized."
        logger.error(msg)
        raise ValueError(msg)
    
    # base params
    params = {
        "frequency": frequency.lower(),
        "start_date": start_date,
        "end_date": end_date,
        "output_file": output_file,
        **kwargs,
    }

    if model_key in ("3", "5", "6"):
        params["model"] = model_key
        params["region"] = region
    elif model_key == "4":
        params["region"] = region
    elif model_key == "Qclassic":
        params["classic"] = True
    elif region: 
        params["region"] = region

    return factor_class(**params)

def main():
    args = parse_args()
    # TODO: list models
    if not args.model:
        print("Error: The -m/--model argument is required.", file=sys.stderr)
        sys.exit(1)
    
    try:
        model_obj = get_factors(
            model=args.model,
            frequency=args.frequency,
            start_date=args.start,
            end_date=args.end,
            region=args.region,
            country=args.country,
        )

        if args.country and not isinstance(model_obj, _AQRModel):
            print(f"Error: '{args.model}' doesn't support --country, only AQR models do.", file=sys.stderr)
            sys.exit(1)
    except ValueError as e:
        # Just prints the error, not traceback, and exit. TODO: style warnings in __init__ maybe.
        print(f"{e}")
        sys.exit(1)

    # These update model_obj._data and model_obj._view = None
    if args.extract:
        model_obj.extract(args.extract)
    elif args.drop:
        model_obj.drop(args.drop)

    table = model_obj._data  #todo: err if empty

    # Saves view not table
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
            table = model_obj.data
            print(f"{model_obj.__class__.__name__} ({model_obj.frequency})", file=sys.stderr)
            #new print previewer. cmonn
            print_table_preview(table, n_rows=4)
            # zamn

if __name__ == "__main__":
    main()
