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
from pathlib import Path

import pyarrow.csv as pv

from getfactormodels._metadata import (
    __copyright__,
    __description__,
    __license__,
    __license_text__,
    __title__,
    __version__,
)
from getfactormodels.utils.registry import _cli_list_models, _cli_list_regions
from getfactormodels.utils.utils import _generate_filename

log = logging.getLogger("getfactormodels")


def parse_args() -> argparse.Namespace:
    """CLI argument parser for getfactormodels."""
    parser = argparse.ArgumentParser(
        prog=__title__,
        description=__description__,
        #usage="%(prog)s [-m MODEL ...] [-p PORTFOLIO ...] [options]",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(f"""\
        Examples:
          getfactormodels --model ff3 --frequency m --start 2000-01-01 --end 2010
          getfactormodels -m 5 -f m --extract SMB RF -o '~/file.csv'
          getfactormodels -m ff6 --drop 'RF'              
          getfactormodels -p industry 30 -o ~/ind30.csv
          getfactormodels -m ff5 -p 2x3 -x SMB HML -o ~/data.csv

        {__copyright__}.
        Distributed under {__license__}. See '--license' for details.
        """),
    )

    # MODEL OPTIONS
    parser.add_argument('-m', '--model', nargs="+", metavar="MODEL", 
                        help="The model/s to use, e.g., 'liquidity', 'icr', "
                        "'ff3'. Accepts ints for Fama-French models, 3, 4, 5, 6.")
    
    parser.add_argument('-f', '--frequency', type=str, default='m',
                        choices=['d', 'w', 'w2w', 'm', 'q', 'y'], metavar="FREQ",
                        help="Data frequency (default: 'm'). Note: 'w2w' (Wed-to-Wed) is "
                        "only available for q-factors.")

    parser.add_argument('-r', '--region', dest='region', metavar='REGION',
                        help="Region or country code for AQR/FF models. "
                        "Use `--list-regions` to see all valid regions.")

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

    # Cache Management
    #
    #parser.add_argument("-F", "--force", action="store_true", help="Bypass local cache and force download.")
    #parser.add_argument("--clear-cache", action="store_true", help="Clear cache for the specified model.")
    #
    parser.add_argument("--delete-global-cache", action="store_true", help="Wipe all cached entries and exit.")

    # METADATA/CLI CONTROLS
    parser.add_argument("-V", "--version", 
                        action="version", version=f"%(prog)s v{__version__} — {__copyright__}",
                        )
    parser.add_argument("--license",
                        action="version", version=__license_text__, help="Print software license notice and exit.",
                        )
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress output to console.')
    parser.add_argument('-v', '--verbose', action='store_true', help="verbose output (set log to debug)")
    parser.add_argument('--list-models', action='store_true', help="Show all models and exit")
    parser.add_argument('--list-regions', action='store_true', 
                        help="show all supported regions and exit")
    
    # PORTFOLIO OPTIONS
    port_group = parser.add_argument_group('Portfolio Options')
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
    from getfactormodels.main import model, portfolio, clear_global_cache
    args = parse_args()

    if args.list_regions:
        _cli_list_regions()
        sys.exit(0) #explicitly exit after info flags

    if args.list_models:
        _cli_list_models()
        sys.exit(0)

    if args.delete_global_cache:
        clear_global_cache()
        if not args.quiet:
            print("Global cache deleted.", file=sys.stderr)
        sys.exit(0)

    try:
        rhs, lhs = None, None
        
        if args.model:
            rhs = model(
                model=args.model, 
                frequency=args.frequency, 
                start_date=args.start, 
                end_date=args.end, 
                region=args.region,
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
                end_date=args.end,
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
