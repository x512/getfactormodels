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
from getfactormodels.models.base import FactorModel, RegionMixin
from getfactormodels.utils.cli import parse_args
from getfactormodels.utils.utils import _generate_filename, _get_model_key

log = logging.getLogger("getfactormodels")

def get_factors(model: str | int | list[str | int] = 3, **kwargs) -> FactorModel: #Self
    """Get and process factor model data.

    This function initializes a specific FactorModel subclass based on the 
    requested model param.

    Args:
        model (str | int): the the name of the factor model. Default: 3
            - Fama-French: '3', '5', '6' ('ff3', 'famafrench3')
            - Carhart: '4', 'carhart', 'car', 'ff4'.
            - q-Factors: 'q', 'q5', or 'qclassic', 'q4'
            - HML Devil: 'hmld' 
            - Betting Against Beta: 'bab'
            - Quality Minus Junk: 'qmj'
            - Pastor-Stambaugh Liquidity: 'liq', 'liquidity'
            - Stambaugh-Yuan Mispricing: 'misp', 'mispricing'
            - Intermediary Capital Risk: 'icr'
            - Daniel-Hirshleifer-Sun Behavioral Factors: 'dhs'
            - Barillas Shanken 6-Factors: 'bs', 'bs6'
        frequency (str): data frequency. Availability varies by model.
            - One of: 'd' 'w' 'w2w' 'm' 'q' or 'y'. Default: 'm'.
        start_date (str): optional start date, YYYY-MM-DD.
        end_date (str): optional end date, YYYY-MM-DD.
        output_file (str): optional path or string to save data to disk.
        **kwargs: model specific parameters:
            - region (str): Geographic region (e.g., 'US', 'Developed')
            - classic (bool): Use the 4-factor version of the Q-model.

    Returns:
        FactorModel: a container object with the requested data. 
            Provides the data as a pa.Table via `.data`, or DataFrame 
            via `.to_pandas()` and `.to_polars()`. Supports the 
            DataFrame Interchange Protocol.

    Raises:
        ValueError: the model key is unrecognized or date is invalid.
        RuntimeError: the data download failed after retries.

    Example:
        >>> m = get_factors(3, start_date="2020-01", frequency='m')
        >>> model.extract(['SMB', 'HML'])
        >>> df = model.to_polars()
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


def main():
    args = parse_args()

    if args.list_regions:
        from getfactormodels.utils.cli import _cli_list_regions
        _cli_list_regions()
        sys.exit(0)

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
        )

        model_obj.load()
   
        if not len(model_obj):
            log.error("No data returned.")
            sys.exit(1)

    except (ValueError, RuntimeError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    if args.extract:
        model_obj.extract(args.extract)
    elif args.drop:
        model_obj.drop(args.drop)

    if args.output:
        model_obj.to_file(args.output)

        if not args.quiet:
            actual_path = Path(args.output).expanduser()
            if actual_path.is_dir():
                actual_path = actual_path / _generate_filename(model_obj)

            print(f"Data saved to: {actual_path.resolve()}", file=sys.stderr)

    # fix: '%%bash' commands are run in a subprocess (output was entire table)
    nb_env = 'ipykernel' in sys.modules or 'JPY_PARENT_PID' in os.environ

    if not sys.stdout.isatty() and not nb_env: # piped
        # model_obj.data's been filtered. Write csv stream of it:
        pv.write_csv(model_obj.data, sys.stdout.buffer)

    else:
        # we're interactive/IPython: print preview of table to stderr. 
        # uses the model_obj's __str__ (which prints the Table preview)
        if not args.quiet:
            sys.stderr.write(f"{str(model_obj)}\n")

if __name__ == "__main__":
    main()
