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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import argparse
import logging
import os
import sys
import textwrap
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
import pyarrow.csv as pv
from getfactormodels.utils.registry import _cli_list_models
from getfactormodels.utils.utils import _generate_filename

log = logging.getLogger("getfactormodels")

# TODO: clean up cli. Especially help. Add list models.

def _get_version():
    # Avoids importing __init__ for the ver.
    try: return version("getfactormodels")
    except PackageNotFoundError: return "unknown"

#TODO: Move this to registry, or _cli_list_models here.
def _cli_list_regions():
    """Helper to display regions and exit."""
    from getfactormodels.models.aqr_models import _AQRModel
    from getfactormodels.models.fama_french import FamaFrenchFactors
    
    print(f"\nFAMA-FRENCH MODELS:\n  {textwrap.fill(', '.join(FamaFrenchFactors.list_regions()), width=70)}")
    print(f"\nAQR MODELS:\n  {textwrap.fill(', '.join(_AQRModel.list_regions()), width=70)}")
    print("\n  Note: accepts aliases 'us', 'jpn', 'uk', and 'ger'.")
    sys.exit(0)


def parse_args() -> argparse.Namespace:
    """CLI arg parser for getfactormodels."""
    parser = argparse.ArgumentParser(
        prog='getfactormodels',
        description='Download datasets for various factor models.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''Example usage:
    getfactormodels --model ff3 --frequency m --start 2000-01-01 --end 2010
    getfactormodels -m 5 -f m --extract SMB RF -o '~/file.csv'
    getfactormodels -m ff6 --drop 'RF'
    getfactormodels -m hml_devil --region jpn
    getfactormodels -p industry 30
    getfactormodels --model ff3 liq --portfolio op bm 
    getfactormodels -m liq -f m -p q -n 2x3x3 
        ''', 
    )

    parser.add_argument('--ver', '--version', action='version', version=f'getfactormodels {_get_version()}')
    # TODO AGPL Warranty flag
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress output to console.')
    parser.add_argument('-v', '--verbose', action='store_true', help="verbose output (set log to debug)")
    parser.add_argument('--list-models', action='store_true', help="Show all models and exit")
    parser.add_argument('-m', '--model', nargs="+", metavar="MODEL", 
                        help="The model/s to use, e.g., 'liquidity', 'icr', "
                        "'ff3'. Accepts ints for Fama-French models, 3, 4, 5, 6.")
    
    parser.add_argument('-f', '--frequency', type=str, default='m',
                        choices=['d', 'w', 'w2w', 'm', 'q', 'y'], metavar="FREQ",
                        help="Data frequency (default: 'm'). Note: 'w2w' (Wed-to-Wed) is "
                        "only available for q-factors.")
    
    parser.add_argument('-s', '--start', required=False, metavar="YYYY[-MM-DD]", 
                        help='the start date.')
    
    parser.add_argument('-e', '--end', required=False, metavar="YYYY[-MM-DD]", 
                        help='the end date.')

    parser.add_argument('-o', '--output', type=str, required=False, default=None, metavar="PATH",
                        help='filename/filepath to save the data to.')

    parser.add_argument('-d', '--drop', nargs='+', metavar='FACTOR', 
                        help="drop specific factor(s) from a model.")

    parser.add_argument('-x', '--extract', nargs='+', metavar="FACTOR",
                        help='extract specific factor(s) from a model.')

    parser.add_argument('-r', '--region', dest='region', metavar='REGION',
                        help="Region or country code for AQR/FF models. "
                        "Use `--list-regions` to see all valid regions.")
    
    parser.add_argument('--list-regions', action='store_true', 
                        help="show all supported regions and exit")
    
    # Portfolio Options
    port_group = parser.add_argument_group('Portfolio Options')
    #NOTE: dest='formed on'
    port_group.add_argument('-p', '--portfolio', '--on', '--by', 
                            dest='formed_on', nargs='+', metavar='FACTOR',
                            help="Factors to sort on (e.g., size, bm, inv) or 'industry'.")
    port_group.add_argument('-n', '--sort', '--count', dest='sort', metavar='SORT',
                            help="Number of portfolios or grid (e.g., 10, 5x5, 2x3).")
    port_group.add_argument('-I', '--industry', type=int, dest='ind_count',
                            help="Shortcut for Fama-French industry portfolios (e.g., -I 12).")
    port_group.add_argument('-W', '-w', '--weights', '--weight', choices=['vw', 'ew'], default='vw',
                            help="Weighting scheme (default: vw).")
    port_group.add_argument('--src', '--source', default='ff', choices=['ff', 'q'],
                            help="Data source: 'ff' (Fama-French) or 'q' (Q-factor/HXZ).")
    #port_group.add_argument('--ex-div', '--exdiv' 
    
    parser.set_defaults(industry=None)
    args = parser.parse_args()

    # fix: check for portfolio here 
    args.is_portfolio = bool(args.ind_count or args.formed_on)

    if args.formed_on:
        args.formed_on = [item.strip().lower() for s in args.formed_on for item in s.split(',')]
        
        # "industry" as sort 
        if args.formed_on[0] in ['industry', 'ind']:
            args.ind_count = int(args.formed_on[1]) if len(args.formed_on) > 1 else 12
            args.formed_on = None
            args.sort = None
        
            # "q" as sort - only 2 types of portfolios: checks for 
            # ia roe eg or q. Needs refinement.
        else:
            q_cols = {'ia', 'roe', 'eg', 'q'}
            if any(k in args.formed_on for k in q_cols):
                args.src = 'q'
                if args.formed_on == ['q']:
                    args.formed_on = None # use defaults

    if args.ind_count:
        args.industry = args.ind_count
        args.sort = None
        args.formed_on = None
    else:
        args.industry = None

    return args


# From main.py
def _cli():
    from getfactormodels.main import model, portfolio
    args = parse_args()

    if args.list_regions:
        _cli_list_regions()
    if args.list_models:
        _cli_list_models()

    try:
        rhs, lhs = None, None
        
        if args.model:
            rhs = model(
                model=args.model, 
                frequency=args.frequency, 
                start_date=args.start, 
                end_date=args.end, 
                region=args.region
            )

        if args.is_portfolio:
            lhs = portfolio(
                source=args.src, 
                industry=args.industry,
                formed_on=args.formed_on,
                sort=args.sort,  # -n flag
                weights=args.weights,
                frequency=args.frequency,
                start_date=args.start,
                end_date=args.end
            )

        if rhs and lhs:
            rhs.load()
            lhs.load()
            # fix: sort after join! (eg, -m misp ff3 -p 2x3 -b size op wasn't returning full table)
            _table = rhs.data.join(lhs.data, keys="date", join_type="inner").sort_by("date")
            # A FactorModel object is needed (for to_file/extract/drop etc.),
            # this uses the RHS instance, then updates its _data property.
            model_obj = rhs
            model_obj._data = _table
        else:
            model_obj = rhs or lhs
         
            if model_obj is None:
                log.error("No data returned.")
                print("'getfactormodels --list-models' to see available options.", file=sys.stderr)
                sys.exit(1)
            model_obj.load()

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
