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
    args = parse_args()

    if args.list_regions:
        from getfactormodels.utils.cli import _cli_list_regions
        _cli_list_regions()
        sys.exit(0)

    if args.portfolio and not args.on:
        print("Error: --portfolio requires --on [factors] (e.g., --on size bm)", file=sys.stderr)
        sys.exit(1)

    if not args.portfolio and not args.on and not args.model:
        print("Error: The -m/--model or --portfolio arguments are required.", file=sys.stderr)
        sys.exit(1)

    try:
        rhs = None
        lhs = None

        # Factor models
        if args.model:
            rhs = model(
                model=args.model,
                frequency=args.frequency,
                start_date=args.start,
                end_date=args.end,
                region=args.region,
            )
        # Portoflios
        if args.on or args.industry:
            lhs = portfolio(
                source='ff', 
                industry=args.industry,
                formed_on=args.on,
                sort=args.sort,
                weights=args.weights,
                frequency=args.frequency,
                start_date=args.start,
                end_date=args.end
            )

        if rhs and lhs:
            rhs.load()
            lhs.load()
            _table = rhs.data.join(lhs.data, keys="date", join_type="left outer")
            # a FactorModel object is needed (for to_file/extract/drop etc.),
            # this uses the RHS instance, then updates its _data.
            model_obj = rhs
            model_obj._data = _table
        else:
            model_obj = rhs or lhs
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
