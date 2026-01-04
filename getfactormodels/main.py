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
import os
import sys
from pathlib import Path
import pyarrow.csv as pv
from getfactormodels import models as factor_models
from getfactormodels.models.aqr_models import _AQRModel
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.cli import parse_args
from getfactormodels.utils.utils import _generate_filename, _get_model_key

log = logging.getLogger("getfactormodels")

def get_factors(model: str | int = 3, **kwargs) -> FactorModel: #Self
    """Get and process factor model data.

    The primary entry point for the getfactormodels package. Maps the 'model' 
    param the specific FactorModel subclass and initializes it with the 
    requested parameters.

    Args:
        model (str, int): the name of the factor model.
            one of: '3', '4', '5', '6', 'carhart', 'liq', 'misp', 'icr',
            'dhs', 'qclassic', 'q', 'hml_d', 'bab', 'qmj'.
        frequency  (str, optional): the frequency of the data. Default: 'm'.
            'd' 'w' 'w2w' 'm' 'q' or 'y'
        start_date (str, optional): start date, YYYY-MM-DD.
        end_date (str, optional): end date, YYYY-MM-DD.
        output(str, optional): filepath to write returned data to, e.g. "~/some/dir/some_file.csv"
        **kwargs: keyword args passed to the base model.

    """
    model_key = _get_model_key(model)
    
    model_class_map = {
        "3": "FamaFrenchFactors",
        "5": "FamaFrenchFactors",
        "6": "FamaFrenchFactors",
        "4": "CarhartFactors",
        "Qclassic": "QFactors", # Handled via 'classic=True' in kwargs
    }
    
    class_name = model_class_map.get(model_key, f"{model_key}Factors")
    factor_class = getattr(factor_models, class_name, None)

    if not factor_class:
        raise ValueError(f"Model '{model}' not recognized.")

    if model_key in ("3", "5", "6"):
        kwargs["model"] = model_key
    if model_key == "Qclassic":
        kwargs["classic"] = True

    return factor_class(**kwargs)


def main():
    args = parse_args()
    # TODO: list models
    if not args.model:
        print("Error: The -m/--model argument is required.", file=sys.stderr)
        sys.exit(1)
    
    try:
        model_obj = get_factors(**vars(args)) # now don't need to manually map each param to args
        
        if args.country and not isinstance(model_obj, _AQRModel):
            print(f"Error: '{args.model}' doesn't support --country, only AQR models do.", file=sys.stderr)
            sys.exit(1)
    except ValueError as e:
        # print error, not traceback, and exit. TODO: style warnings in __init__ maybe.
        print(f"{e}")
        sys.exit(1)

    if args.extract:
        model_obj.extract(args.extract)
    elif args.drop:
        model_obj.drop(args.drop)

    if model_obj.data.num_rows == 0:
        log.error("No data returned.")
        sys.exit(1)

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
        # model_obj.data is already filtered and columns selected!
        pv.write_csv(model_obj.data, sys.stdout.buffer)

    else: #we're interactive, or in a jupyter notebook: write df preview to stdout
        if not args.quiet:
            sys.stderr.write(f"{str(model_obj)}\n")  #uses model's __str__ (using table preview)
            # zamn

if __name__ == "__main__":
    main()
