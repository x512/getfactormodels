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
import argparse
from importlib.metadata import PackageNotFoundError, version
from getfactormodels.models.aqr_models import _AQRModel
from getfactormodels.models.fama_french import FamaFrenchFactors


def _get_version():
    """Avoids importing __init__ for the ver. no."""
    try:
        return version("getfactormodels")
    except PackageNotFoundError:
        return "unknown"


def parse_args() -> argparse.Namespace:
    """CLI arg parser for getfactormodels."""
    parser = argparse.ArgumentParser(
        prog='getfactormodels',
        description='Download datasets for various factor models.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''Example usage:
    getfactormodels --model ff3 --frequency m
    getfactormodels -m liquidity -f m --start 2000-01-01 --end 2009-12-31
    getfactormodels -m 5 -f m -e 2009-12-31 --extract SMB RF -o '~/file.csv'
    getfactormodels -m Carhart -f d -s 2000-01-01 -x MOM -o file
    getfactormodels -m hml_devil --country jpn
        ''', 
    )

    aqr_countries = _AQRModel.list_countries() 
    ff_regions = FamaFrenchFactors.list_regions()

    parser.add_argument('-v', '--version', action='version', version=f'getfactormodels {_get_version()}')
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress output to console.')

    parser.add_argument('-m', '--model', 
                        #'model',    # no dash, makes it positional. Still enforced in main (if model's missing, after eg, --list)
                        nargs="?",   # makes it optional 
                        metavar="MODEL",
                        help="the model to use, e.g., 'ff3', 'q'."
                        " Accepts 3, 4, 5, 6 in place of ff3, carhart, ff5, ff6.")

    parser.add_argument('-f', '--frequency', type=str, default='m',
                        choices=['d', 'w', 'w2w', 'm', 'q', 'y'], metavar="FREQ",
                        help="Data frequency (default: 'm'). Note: 'w2w' (Wed-to-Wed) is "
                        "only available for q-factors.")

    parser.add_argument('-s', '--start', required=False, metavar="YYYY-MM-DD",
                        help='the start date.')

    parser.add_argument('-e', '--end', required=False, metavar="YYYY-MM-DD",
                        help='the end date.')

    parser.add_argument('-o', '--output', type=str, required=False, default=None, metavar="PATH",
                        help='filename/filepath to save the data to.')

    parser.add_argument('-d', '--drop', nargs='+', metavar='FACTOR', 
                        help="drop specific factor(s) from a model. Name should match column value.")

    parser.add_argument('-x', '--extract', nargs='+', metavar="FACTOR",
                        help='extract specific factor(s) from a model, name should match column value.')
    # region and country will probably be combined.
    parser.add_argument('-r', '--region', metavar='REGION',
                        help=f"Fama-French models only: region code. Options: {', '.join(ff_regions)}")
    
    parser.add_argument('-c', '--country', metavar='COUNTRY',
                        help=f"AQR models only. ISO-3 letter country code. "
                        f"Options: {', '.join(aqr_countries)}")  
    # list 
    # verbose
    args = parser.parse_args()

    if args.frequency == 'w2w' and args.model.lower() not in {'q', 'qclassic'}:
        parser.error(f"'w2w' frequency is not supported by '{args.model}'.")

    #if args.country and args.model.lower() not in ['hmld', 'hml_devil', 'hmldevil']:
    #    parser.error(f"'{args.model}' doesn't support the --country param. Only HML Devil factors do.")

    return args
