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
from pyarrow.lib import as_buffer
from getfactormodels._metadata import (
    __copyright__,
    __description__,
    __license__,
    __license_text__,
    __title__,
    __version__,
)
from getfactormodels.utils.registry import _MODEL_REGISTRY
from getfactormodels.utils.utils import _generate_filename
from getfactormodels.utils.arrow_utils import  print_table_preview

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
          getfactormodels -a abr_1 rer -s 2010-06 -f w --width 3

        {__copyright__}.
        Distributed under {__license__}. See '--license'.
        """),
    )
    parser.add_argument("-V", "--version", action="version", 
                        version=f"%(prog)s-{__version__}")
    
    parser.add_argument("--license", action="version", version=__license_text__, 
                        help="Print software license notice and exit.")
    
    parser.add_argument('-q', '--quiet', action='store_true', 
                        help='Suppress output to console.')
    
    parser.add_argument('-v', '--verbose', action='store_true', 
                        help="Verbose output, enable debug logging.")
    
    parser.add_argument('--list-models', action='store_true', 
                        help="Show all models and exit")
    
    parser.add_argument('--list-regions', action='store_true', 
                        help="Show all supported geographic regions and exit")

    parser.add_argument('--list-anomalies', action='store_true',
                        help="Show all available q-factor anomalies and exit.")

    parser.add_argument('--url', '--urls', action='store_true', 
                        help="List target URLs and exit.")
    
    parser.add_argument("--delete-global-cache", action="store_true", 
                        help="Delete all cached dataset files from disk and exit.")

    # MODEL OPTIONS
    parser.add_argument('-m', '--model', nargs="+", metavar="MODEL", 
                        help="Model(s) requested ('icr', 'ff3'). Also " 
                        "accepts 3 4 5 6 for Fama-French models.")
    
    parser.add_argument('-f', '--frequency', type=str, default='m',
                        choices=['d', 'w', 'w2w', 'm', 'q', 'y'], metavar="FREQ",
                        help="Data frequency 'd' 'w' 'm' (default) 'q' 'y' 'w2w'.")

    parser.add_argument('-r', '--region', '--location', '-l', dest='region', metavar='REGION',
                        help="Region/country code for models that support it.")

    parser.add_argument('-s', '--start', required=False, metavar="DATE", 
                        help='the start date.')
    
    parser.add_argument('-e', '--end', required=False, metavar="DATE", 
                        help='the end date.')

    parser.add_argument('-o', '--output', type=str, required=False, default=None, metavar="PATH",
                        help='A filename or path to save the data to.')

    parser.add_argument('-d', '--drop', nargs='+', metavar='FACTOR', 
                        help="Remove specific factors from a model.")

    parser.add_argument('-x', '--extract', nargs='+', metavar="FACTOR",
                        help='Keep specific factors from a model.')

    parser.add_argument(
        "--as-percent", '--pct',
        action="store_true", 
        help="Return returns as percentages (e.g. 5.0 instead of 0.05)"
    )

    # PORTFOLIO OPTIONS
    port_group = parser.add_argument_group('Portfolio Options')
    port_group.add_argument('-p', '--portfolio', '--on', 
                            dest='sort', nargs='+', metavar='FACTOR',
                            help="Factors to sort on (size, bm, or ia roe for q-factors).")
    
    port_group.add_argument('-n', '--sort', '--count', '--grid', dest='sort_spec', metavar='SPEC',
                            help="Portfolio count, sort or grid. ('10', '2x3', '5x5', 'decile').")
    
    port_group.add_argument('-I', '--industry', type=int, dest='ind_count',
                            help="Shortcut for Fama-French industry portfolios (-I 12).")
    
    port_group.add_argument('-W', '--weights', choices=['vw', 'ew'], default='vw',
                            help="Weighting scheme (default: vw).")
    
    port_group.add_argument('--src', '--source', default='ff', choices=['ff', 'q'],
                            help="Portfolio datasource, accepts 'q' or 'ff'.")
    
    port_group.add_argument('--ex-div', '--exdiv', dest='dividends', action='store_false',
                            help="Exclude dividends (use ex-dividend returns retx_vw).")

    # ANOMALY OPTIONS
    anom_group = parser.add_argument_group('Anomaly Options')
    anom_group.add_argument('-a', '--anomaly', '--anom',
                        dest='anomaly_names', nargs='+', metavar='ANOM',
                        help="Anomaly or categories to retrieve (abr_1, momentum, rer).")

    anom_group.add_argument('-b', '--bivariate', '--3x5',
                        action='store_true',
                        help="Use bivariate (2-way) sorts instead of default deciles.")
    
    # PREVIEW / DISPLAY CONTROLS
    parser.add_argument('--no-prev', '--no-preview', action='store_true',
                            help="Suppress the terminal table preview output.")
    
    parser.add_argument('-H', '--height', type=int, default=6, metavar='ROWS',
                            help="Number of head/tail rows to show in preview (default: 6).")
    
    parser.add_argument('-w', '--width', type=int, default=5, metavar='COLS',
                            help="Number of data columns to show. Not incl. truncated column or index. (default: 5).")

    parser.set_defaults(industry=None)
    args = parser.parse_args()

    args.is_anomaly = bool(args.anomaly_names)
    args.is_portfolio = bool(args.ind_count or args.sort)

    if args.anomaly_names:
        args.anomaly_names = [item.strip().lower() for s in args.anomaly_names for item in s.split(',')]

    if args.sort is None:
        if args.is_anomaly and not args.is_portfolio:
            args.sort = ['decile']
        elif args.is_portfolio and not args.is_anomaly:
            args.sort = ['2x3x3']

    if args.sort:
        args.sort = [item.strip().lower() for s in args.sort for item in s.split(',')]
        
        if args.sort[0] in ['industry', 'ind']:
            args.ind_count = int(args.sort[1]) if len(args.sort) > 1 else 12
            args.sort = None
        else:
            q_cols = {'ia', 'roe', 'eg', 'q'}
            if any(k in args.sort for k in q_cols):
                args.src = 'q'
                if args.sort == ['q']:
                    args.sort = None

    args.industry = args.ind_count if args.ind_count else None

    args.grid, args.count = None, None
    if args.sort_spec:
        spec = str(args.sort_spec).strip().lower()
        if 'x' in spec:
            args.grid = spec
        elif spec.isdigit():
            args.count = int(spec)
        elif not args.sort:
            args.sort = [spec]

    return args


def _quick_exits(args: argparse.Namespace, model_obj=None) -> None:
    from getfactormodels.main import clear_global_cache

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
    
    if args.url and model_obj:
        from pprint import pprint
        pprint(model_obj.url)
        sys.exit(0)


def _build_model(args: argparse.Namespace):
    """Instantiates and combines requested models, portfolios, or anomalies."""
    from getfactormodels.main import anomaly, model, portfolio
    from getfactormodels.models.base import Collection

    if args.is_portfolio and args.region:
        raise NotImplementedError("Regional portfolio data not yet supported. Please omit the -r / --region flag.")

    components = []

    if args.model:
        components.append(model(
            model=args.model,
            frequency=args.frequency,
            start_date=args.start,
            end_date=args.end,
            region=args.region,
            as_percentage=args.as_percent,
        ))

    if args.is_portfolio:
        components.append(portfolio(
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
            as_percentage=args.as_percent,
        ))

    if args.is_anomaly:
        components.append(anomaly(
            provider='q',
            name=args.anomaly_names,
            frequency=args.frequency,
            sort='3x5' if args.bivariate else 'decile',
            start_date=args.start,
            end_date=args.end,
            as_percentage=args.as_percent,
        ))

    if not components:
        log.error("No data returned.")
        print("'getfactormodels --list-models' to see available options.", file=sys.stderr)
        sys.exit(1)

    return components[0] if len(components) == 1 else Collection(instances=components)


def _cli():
    args = parse_args()

    _quick_exits(args)

    try:
        model_obj = _build_model(args)
    except (ValueError, RuntimeError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    _quick_exits(args, model_obj=model_obj)

    try:
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

    nb_env = 'ipykernel' in sys.modules or 'JPY_PARENT_PID' in os.environ

    if not sys.stdout.isatty() and not nb_env:
        pv.write_csv(model_obj.data, sys.stdout.buffer)
    else:
        if not args.quiet and not args.no_prev:
            region_label = f" ({model_obj.region})" if hasattr(model_obj, "region") and model_obj.region else ""
            header = f"{model_obj.name}{region_label}\n"
            preview_str = print_table_preview(model_obj.data, n_rows=args.height, n_cols=args.width)
            if preview_str:
                sys.stderr.write(f"{header}{preview_str}\n")


if __name__ == '__main__':
    _cli()


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
    from getfactormodels.models.aqr_models import HMLDevilFactors
    from getfactormodels.models.fama_french import FamaFrenchFactors
    
    print(f"\nFAMA-FRENCH MODELS:\n  {textwrap.fill(', '.join(FamaFrenchFactors.list_regions()), width=70)}")
    print(f"\nAQR MODELS:\n  {textwrap.fill(', '.join(HMLDevilFactors.list_regions()), width=70)}")
    print("\n  Note: accepts aliases 'us', 'jpn', 'uk', and 'ger'.")


def _cli_list_models():
    """Lists all model IDs, names, frequencies and aliases to stderr."""
    from getfactormodels.main import model as model_factory
    header = f"{'ID':<7} {'MODEL NAME':<32} {'FREQ':<15} {'ALIASES'}"
    sys.stderr.write(f"{header}\n")

    for key in _MODEL_REGISTRY:
        data = _MODEL_REGISTRY[key]

        freqs = "d,m" 
        try:
            inst = model_factory(key)
            freqs = ",".join(inst.frequencies)
        except (ValueError, NotImplementedError):
            try:
                inst = model_factory(key, frequency='m', region='ch')
                freqs = ",".join(inst.frequencies)
            except (ValueError, NotImplementedError):
                pass

        name = (data['name'][:28] + '...') if len(data['name']) > 31 else data['name']
        aliases = ", ".join(data['aliases'])

        row = f"{key:<7} {name:<32} {freqs:<15} {aliases}\n"
        sys.stderr.write(row)

    sys.stderr.write("\n")

# TODO:
#   -F/--force 
#   --clear-cache per model.
#   --as-percent 
#   --decimals
#   data source citation, reference paper citation
#   --output to --output-dir, --format, --output, --fname or something
#   --list-portfolios
#   --list-models should prob show only id/name/freq? or have --list-models-long and --list-models idk.
    #parser.add_argument("-F", "--force", "--no-cache" action="store_true", help="Bypass local cache and force download.")
    #parser.add_argument("--clear-cache", action="store_true", help="Clear cache for the specified model.")
    #parser.add_argument('--as-percent', action='store_true', help="Scale factor values to percentages.")
