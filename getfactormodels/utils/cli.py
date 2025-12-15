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


def parse_args() -> argparse.Namespace:
    """Argument parser, allowing for command line arguments.
    This is the function used in pyproject.toml to run the CLI."""
    parser = argparse.ArgumentParser(
        description='Download datasets for various factor models.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''Example usage:
            getfactormodels --model ff3 --frequency m
            getfactormodels --model liquidity --frequency m --start 2000-01-01 --end 2009-12-31
            getfactormodels -m ff5 -f m -e 2009-12-31 --extract SMB RF -o '~/file.csv'
            getfactormodels -m Carhart -f d -s 2000-01-01 -e 2009-12-31 -x MOM -o file 
            '''
    )
    parser.add_argument('-m', '--model', type=str, required=True, metavar="MODEL_ID",
                        help='the model to use.')
    parser.add_argument('-f', '--frequency', type=str,
                        required=False, default='m', choices=['d', 'w', 'm', 'q', 'y'],
                        help='the frequency of the data.')
    parser.add_argument('-s', '--start', type=str, required=False, metavar="YYYY-MM-DD",
                        help='The start date for the data.')
    parser.add_argument('-e', '--end', type=str, required=False, metavar="YYYY-MM-DD",
                        help='The end date for the data.')
    parser.add_argument('-o', '--output', type=str, required=False, default=None, metavar="FILEPATH",
                        help='The file to save the data to.')
    parser.add_argument('-x', '--extract', required=False, nargs='+', metavar="FACTOR",
                        help='Extract specific factor(s) from a model.')
    parser.add_argument('-r', '--region', type=str, required=False, #metavar="REGION_ID",
                        choices=['us', 'developed', 'developed ex us', 'europe', 'japan',  # TODO: nicer descript 
                                 'asia pacific ex japan', 'north america', 'emerging'],
                        help='Developed/International and Emeriging markets regions (Fama French models only).')
    parser.add_argument('-R', '--norf', action='store_true',
                        help='Don\'t include the risk-free rate column (RF).')
    parser.add_argument('-M', '--nomktrf', action='store_true',
                        help='Drop the Mkt-RF column.')

    return parser.parse_args()
