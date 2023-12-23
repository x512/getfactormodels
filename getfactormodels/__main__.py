#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Optional
import pandas as pd
from dateutil import parser
# ruff: noqa: RUF100
from getfactormodels.models.models import \
    barillas_shanken_factors  # noqa: F401
from getfactormodels.models.models import carhart_factors  # noqa: F401, E501
from getfactormodels.models.models import (dhs_factors, ff_factors,
                                           hml_devil_factors, icr_factors,
                                           liquidity_factors,
                                           mispricing_factors,
                                           q_classic_factors, q_factors)
from getfactormodels.utils.cli import parse_args
from getfactormodels.utils.utils import _get_model_key, _process


def get_factors(model: str = "3",
                frequency: Optional[str] = "M",
                start_date: Optional[str] = None,
                end_date: Optional[str] = None,
                output: Optional[str] = None) -> pd.DataFrame:
    """Get data for a specified factor model.

    Return a DataFrame containing the data for the specified model and
    frequency. If an output is specified, factor data is saved to a file.

    Notes:
    - Any string matching a model's regex (e.g., `liq` for `liquidity`) can be
      used as a model name.
    - Dates should be in ``YYYY-MM-DD`` format, but anything that
      ``dateutil.parser.parse()`` can interpret will work.
    - Weekly data is only available for the q-factor and Fama-French 3-factor
      models.

    Parameters:
        model (str): the factor model to return. One of: `liquidity`,
            `icr`, `dhs`, `q`, `q_classic`, `ff3`, `ff5`, `ff6`, `carhart4`,
            `hml_devil`, `barrilas_shanken`, or `mispricing`.
        frequency (str): the frequency of the data. D, W, M or A (default: M).
        start_date (str, optional): the start date of the data, YYYY-MM-DD.
        end_date (str, optional): the end date of the data, YYYY-MM-DD.
        output (str, optional): a filename, directory, or filepath. Accepts
            '.txt', '.csv', '.md', '.xlsx', '.pkl' as file extensions.

    Returns:
        pandas.DataFrame: factor data, indexed by date.
    """
    frequency = frequency.lower()
    model = _get_model_key(model)

    # Get the function by its name, if it exists call it with params
    if model in ["3", "4", "5", "6"]:
        return ff_factors(model, frequency, start_date, end_date)
    else:
        function_name = f"{model}_factors"
        function = globals().get(function_name)

    if not function:
        raise ValueError(f"Invalid model: {model}")

    df = function(frequency, start_date, end_date, output)

    return df


class FactorExtractor:
    """
    Extracts factor data based on specified parameters.

    Args:
        model : str
            The factor model to use. Defaults to '3'.
        frequency (str, optional): The frequency of the data. Defaults to 'M'.
        start_date (str, optional): The start date of the data.
        end_date (str, optional): The end date of the data.

    Methods:
        drop_rf: Drops the 'RF' column from the DataFrame.
        save_factors: Saves the factor data to a file.
    """

    def __init__(self,
                 model: str = '3',
                 frequency: Optional[str] = 'M',
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None,
                 output: Optional[str] = None):
        self.model: str = model
        self.frequency: str = frequency
        self.start_date = self.validate_date_format(start_date) if start_date \
            else None
        self.end_date = self.validate_date_format(end_date) if end_date \
            else None
        self.output = output
        self._no_rf = False
        self._no_mkt = False
        self.df = None

    def no_rf(self) -> None:
        """Sets the _no_rf flag to True."""
        self._no_rf = True

    def no_mkt(self) -> None:
        """Sets the _no_mkt flag to True."""
        self._no_mkt = True

    @staticmethod
    def validate_date_format(date_string: str) -> str:
        """
        Validate the date format.

        Raises:
            ValueError: If the date format is incorrect.
        """
        try:
            return parser.parse(date_string).strftime("%Y-%m-%d")
        except ValueError as err:
            error_message = "Incorrect date format, use YYYY-MM-DD."
            raise ValueError(error_message) from err

    def get_factors(self) -> pd.DataFrame:
        """Fetch the factor data and store it in the class."""
        self.df = get_factors(
            model=self.model,
            frequency=self.frequency,
            start_date=self.start_date,
            end_date=self.end_date,
            output=self.output)

        if self._no_rf:
            self.df = self.drop_rf(self.df.copy())  # create a copy before drop
        if self._no_mkt:
            self.df = self.drop_mkt(self.df.copy())

        return self.df

    def drop_rf(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """Drop the ``RF`` column from the DataFrame."""
        # get_factors if not already done
        if df is None:
            df = self.get_factors()

        if "RF" in df.columns:
            df = df.drop(columns=["RF"])
        else:
            print("`drop_rf` was called but no RF column was found.")

        return df

    def drop_mkt(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """Drop the ``MKT`` column from the DataFrame."""
        if df is None:
            df = self.get_factors()

        if "Mkt-RF" in df.columns:
            df = df.drop(columns=["Mkt-RF"])
        else:
            print("`drop_mkt` was called but no MKT column was found.")

        return df

    def to_file(self, filename: str):
        """
        Save the factor data to a file.

        Args:
            filename (str): The name of the file to save the data to.
        """
        if self.df is None:
            raise ValueError("No data to save. Fetch factors first.")

        # TODO: could call _save_to_file directly
        _process(self.df, filepath=filename)


def main():
    args = parse_args()

    extractor = FactorExtractor(model=args.model, frequency=args.freq,
                                start_date=args.start, end_date=args.end)
    if args.no_rf:
        extractor.no_rf()
    if args.no_mkt:
        extractor.no_mkt()

    df = extractor.get_factors()

    if args.output:
        extractor.to_file(args.output)
        print(f'File saved to "{Path(args.output).resolve()}"')

    else:
        print(df)


if __name__ == '__main__':
    main()
