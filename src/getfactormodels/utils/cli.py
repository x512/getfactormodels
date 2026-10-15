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
from getfactormodels.utils.utils import _generate_filename
from getfactormodels.utils.registry import _MODEL_REGISTRY

log = logging.getLogger("getfactormodels")


def parse_args() -> argparse.Namespace:
    """CLI argument parser for getfactormodels."""
    parser = argparse.ArgumentParser(
        prog=__title__,
        description=__description__,
        usage="%(prog)s [-m MODEL ...] [-p PORTFOLIO ...] [options]",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(f"""\
        Examples:
          getfactormodels --model ff3 --frequency m --start 2000-01-01 --end 2010
          getfactormodels -m 5 -f m --extract SMB RF -o '~/file.csv'         
          getfactormodels -p industry 30 -o ~/ind30.csv
          getfactormodels -m ff5 -p 2x3 -x SMB HML -o ~/data.csv

        {__copyright__}.
        Distributed under {__license__}. See '--license'.
        """),
    )
    parser.add_argument("-V", "--version", 
                        action="version", version=f"%(prog)s (v{__version__}) {__copyright__}",
                        )
    parser.add_argument("--license",
                        action="version", version=__license_text__, help="Print software license notice and exit.",
                        )
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress output to console.')
    parser.add_argument('-v', '--verbose', action='store_true', help="Verbose output, enable debug logging.")
    parser.add_argument('--list-models', action='store_true', help="Show all models and exit")
    parser.add_argument('--list-regions', action='store_true', 
                        help="Show all supported geographic regions and exit")
    # Need: list portfolios, and shorthand model -ls showing id/name/freq only?
    # MODEL OPTIONS
    parser.add_argument('-m', '--model', nargs="+", metavar="MODEL", 
                        help="Model(s) requested ('icr', 'ff3'). Also " 
                        "accepts 3 4 5 6 for Fama-French models.")
    
    parser.add_argument('-f', '--frequency', type=str, default='m',
                        choices=['d', 'w', 'w2w', 'm', 'q', 'y'], metavar="FREQ",
                        help="Data frequency (default: 'm'). d w m q y w2w.")

    parser.add_argument('-r', '--region', dest='region', metavar='REGION',
                        help="Region/country code for models that support it.")

    parser.add_argument('-s', '--start', required=False, metavar="DATE", 
                        help='the start date.')
    
    parser.add_argument('-e', '--end', required=False, metavar="DATE", 
                        help='the end date.')

    parser.add_argument('-o', '--output', type=str, required=False, default=None, metavar="PATH",
                        help='A filename or path to save the data to.')
    #TODO: output dir, outfile file, format.

    parser.add_argument('-d', '--drop', nargs='+', metavar='FACTOR', 
                        help="Remove specific factors from a model.")

    parser.add_argument('-x', '--extract', nargs='+', metavar="FACTOR",
                        help='Keep only these specific factors from a model.')

    # Cache Management
    #
    #parser.add_argument("-F", "--force", "--no-cache" action="store_true", help="Bypass local cache and force download.")
    #parser.add_argument("--clear-cache", action="store_true", help="Clear cache for the specified model.")
    #
    parser.add_argument("--delete-global-cache", action="store_true", 
                        help="Delete all cached dataset files from disk and exit.")

    # METADATA/CLI CONTROLS
    
    
    # PORTFOLIO OPTIONS
    port_group = parser.add_argument_group('Portfolio Options')
    port_group.add_argument('-p', '--portfolio', '--on', '--by', 
                            dest='sort', nargs='+', metavar='FACTOR',
                            help="Factors to sort on (e.g., size, bm, or ia roe for q-factors).")
    port_group.add_argument('-n', '--sort', '--count', '--grid', dest='sort_spec', metavar='SPEC',
                            help="Portfolio layout, count, or grid specification (e.g., '10', '2x3', '5x5', 'decile').")
    port_group.add_argument('-I', '--industry', type=int, dest='ind_count',
                            help="Shortcut for Fama-French industry portfolios (e.g., -I 12).")
    port_group.add_argument('-W', '-w', '--weights', '--weight', choices=['vw', 'ew'], default='vw',
                            help="Weighting scheme (default: vw).")
    port_group.add_argument('--src', '--source', default='ff', choices=['ff', 'q'],
                            help="Portfolio datasource, accepts 'q' or 'ff'.")
    port_group.add_argument('--ex-div', '--exdiv', dest='dividends', action='store_false',
                            help="Exclude dividends (use ex-dividend returns retx_vw).")

    # ANOMALY OPTIONS
    anom_group = parser.add_argument_group('Anomaly Options')
    anom_group.add_argument('-a', '--anomaly', '--anom',
                        dest='anomaly_names', nargs='+', metavar='ANOM',
                        help="Anomaly or categories to retrieve (e.g., abr_1, momentum, rer).")
    parser.add_argument('--list-anomalies', action='store_true',
                        help="Show all available q-factor anomalies and categories and exit.")

    #parser.add_argument('--as-percent', action='store_true',
    #help="Scale factor values to percentages.")
    
    #--decimals COUNT

    #data source citation

    # new, uses the url @property 
    parser.add_argument('--url', action='store_true', help="List target URLs for the specified models and exit.")
   

    parser.set_defaults(industry=None)
    args = parser.parse_args()

    # fix: check for portfolio here 
    args.is_anomaly = bool(args.anomaly_names)
    args.is_portfolio = bool(args.ind_count or args.sort)

    if args.anomaly_names:
        args.anomaly_names = [item.strip().lower() for s in args.anomaly_names for item in s.split(',')]

    # Smart default sorting depending on what was requested
    if args.sort is None:
        if args.is_anomaly and not args.is_portfolio:
            args.sort = 'decile'
        elif args.is_portfolio and not args.is_anomaly:
            args.sort = '2x3x3'

    if args.sort:
        args.sort = [item.strip().lower() for s in args.sort for item in s.split(',')]
        
        # "industry" as sort 
        if args.sort[0] in ['industry', 'ind']:
            args.ind_count = int(args.sort[1]) if len(args.sort) > 1 else 12
            args.sort = None
        
            # "q" as sort - only 2 types of portfolios: checks for 
            # ia roe eg or q. Needs refinement.
        else:
            q_cols = {'ia', 'roe', 'eg', 'q'}
            if any(k in args.sort for k in q_cols):
                args.src = 'q'
                if args.sort == ['q']:
                    args.sort = None # use defaults

    if args.ind_count:
        args.industry = args.ind_count
        args.sort = None
    else:
        args.industry = None

    return args


# From main.py
def _cli():
    from getfactormodels.main import model, portfolio, clear_global_cache
    args = parse_args()

    if args.list_regions:
        _cli_list_regions()
        sys.exit(0)

    if args.list_models:
        _cli_list_models()
        sys.exit(0)

    if args.list_anomalies:
        _cli_list_anomalies()
        sys.exit(0)

    if args.delete_global_cache:
        clear_global_cache()
        if not args.quiet:
            print("Global cache deleted.", file=sys.stderr)
        sys.exit(0)

    # ------------------------------------------------------
    # NEW -- testing 
    if args.url:
        from getfactormodels.main import model
        from pprint import pprint
        
        m = model(
            model=args.model,
            frequency=args.frequency,
            start_date=args.start,
            end_date=args.end,
            region=args.region,
        )
        
        pprint(m.url)
        sys.exit(0)
    # ------------------------------------
    # Initialize unified parameters on args
    args.grid = None
    args.count = None
    
    if args.sort_spec:
        spec = str(args.sort_spec).strip().lower()
        # Check if it's a grid (e.g., '2x3', '5x5', '2x3x3')
        if 'x' in spec:
            args.grid = spec
        # Check if it's a digit integer (e.g., '10', '25', '6')
        elif spec.isdigit():
            args.count = int(spec)
        else:
            # Otherwise treat it as a sort keyword (e.g., 'decile', 'quintile')
            # If args.sort isn't set yet, or prepend/handle it:
            if not args.sort:
                args.sort = [spec]

    try:
        rhs, lhs, anom_obj = None, None, None

        if args.is_portfolio and args.region:
            raise NotImplementedError(
                "Regional portfolio data not yet supported. Please omit the -r / --region flag."
            )
        
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
                sort=args.sort,
                grid=args.grid,
                count=args.count,
                weights=args.weights,
                frequency=args.frequency,
                start_date=args.start,
                end_date=args.end,
                dividends=args.dividends,
            )

        if args.is_anomaly:
            from getfactormodels.main import anomaly
            anom_obj = anomaly(
                source='q',
                name=args.anomaly_names,
                frequency=args.frequency,
                #sort=args.sort,
                start_date=args.start,
                end_date=args.end,
            )

        # Handle joining or single dataset selection
        active_components = [c for c in [rhs, lhs, anom_obj] if c is not None]

        if len(active_components) > 1:
            # Load and side-by-side join multiple components on 'date'
            for comp in active_components:
                comp.load()
            
            _table = active_components[0].data
            for comp in active_components[1:]:
                _table = _table.join(comp.data, keys="date", join_type="inner")
            
            model_obj = active_components[0]
            model_obj._data = _table.sort_by("date")
        else:
            model_obj = active_components[0] if active_components else None
         
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


def _cli_list_anomalies():
    """Lists all available q-factor anomalies categorized by group."""
    from getfactormodels.models.q_factors import _QAnomalies
    
    sys.stderr.write("ANOMALIES: all available anomalies from global-q.\n")
    for category, factors in sorted(_QAnomalies.Q_REGISTRY.items()):
        factor_list = ", ".join(sorted(factors))
        sys.stderr.write(f"\n{category.upper()}:\n    {textwrap.fill(factor_list, width=70, subsequent_indent='    ')}\n")
    sys.stderr.write("\n  Note: You can pass individual anomaly tickers (-a abr_1) or categories (-a momentum).\n\n")


def _cli_list_regions():
    """Helper to display regions and exit."""
    from getfactormodels.models.aqr_models import _AQRModel
    from getfactormodels.models.fama_french import FamaFrenchFactors
    
    print(f"\nFAMA-FRENCH MODELS:\n  {textwrap.fill(', '.join(FamaFrenchFactors.list_regions()), width=70)}")
    print(f"\nAQR MODELS:\n  {textwrap.fill(', '.join(_AQRModel.list_regions()), width=70)}")
    print("\n  Note: accepts aliases 'us', 'jpn', 'uk', and 'ger'.")


def _cli_list_models():
    """Lists all model ID's, names, frequencies and aliases to stderr."""
    from getfactormodels.main import model as model_factory
    header = f"{'ID':<7} {'MODEL NAME':<32} {'FREQ':<15} {'ALIASES'}"
    sys.stderr.write(f"{header}\n")

    for key in _MODEL_REGISTRY:
        data = _MODEL_REGISTRY[key]

        freqs = "d,m"  # Safe default fallback
        try:
            # Try default first
            inst = model_factory(key)
            freqs = ",".join(inst._frequencies)
        except (ValueError, NotImplementedError):
            try:
                # Try fallback frequency 'm' or 'd' for regional models
                inst = model_factory(key, frequency='m', region='ch')
                freqs = ",".join(inst._frequencies)
            except (ValueError, NotImplementedError):
                pass

        name = (data['name'][:28] + '...') if len(data['name']) > 31 else data['name']
        aliases = ", ".join(data['aliases'])

        row = f"{key:<7} {name:<32} {freqs:<15} {aliases}\n"
        sys.stderr.write(row)

    sys.stderr.write("\n")
