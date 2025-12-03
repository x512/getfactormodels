# -*- coding: utf-8 -*-
import argparse


def parse_args() -> argparse.Namespace:
    """Argument parser, allowing for command line arguments.
    This is the function used in pyproject.toml to run the CLI."""
    parser = argparse.ArgumentParser(
        description='Retrieve and structure data for factor models.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''Example usage:
        python main.py -m 3 -f M -s 1961-01-01 -e 1990-12-31
        python main.py --model icr --frequency M --end 1990-12-31 --no_rf -o '~/icr.csv' '''  # noqa
    )
    parser.add_argument('-m', '--model', type=str, required=True,
                        help='The model to use.')
    parser.add_argument('-f', '--freq', '--frequency', type=str,
                        required=False, default='M', help='The frequency of\
                        the data. Valid options are D, W, M, Q, A.')
    parser.add_argument('-s', '--start', type=str, required=False,
                        help='The start date for the data.')
    parser.add_argument('-e', '--end', type=str, required=False,
                        help='The end date for the data.')
    parser.add_argument('-o', '--output', type=str, required=False,
                        default='~/getfactormodels.csv',
                        help='The file to save the data to.')
    parser.add_argument('--no_rf', '--no-rf', '--norf', action='store_true',
                        help='Drop the RF column from the DataFrame.')
    parser.add_argument('--no_mkt', '--no-mkt', '--nomkt', action='store_true',
                        help='Drop the Mkt-RF column from the DataFrame.')
    return parser.parse_args()
