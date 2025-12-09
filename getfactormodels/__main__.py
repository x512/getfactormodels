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
from pathlib import Path
from typing import Optional
import pandas as pd
from dateutil import parser
from getfactormodels.models import *
from getfactormodels.models.fama_french import FamaFrenchFactors
from getfactormodels.models.hml_devil import HMLDevil
from getfactormodels.models.q_factors import QFactors
from getfactormodels.utils.cli import parse_args
from getfactormodels.utils.utils import _get_model_key, _process

#import logging  #TODO

#
# TEMPORARY MINIMAL REWORK (until the keymaps and insane regex is dropped
# and base class and FactorExtractor done)
# just getting get_factors to work!
#

def get_factors(model: str|int = 3,
                frequency: Optional[str] = "m",
                start_date: Optional[str] = None,
                end_date: Optional[str] = None,
                output: Optional[str] = None):
    """Get data for a specified factor model."""
    frequency = frequency.lower()
    model_key = _get_model_key(model)

    factor_instance = None

    if model_key in ["3", "4", "5", "6"]:
        factor_instance = FamaFrenchFactors(frequency, start_date,
                                            end_date, output, model_key)

    elif model_key == "Qclassic":
        factor_instance = QFactors(frequency, start_date, end_date,
                                   output, classic=True)

    elif model_key == "HMLDevil":
        factor_instance = HMLDevil(frequency, start_date, end_date, output)
        
    else:
        # Class loading: tries CamelCaseFactors then UPPERCASEFactors
        class_name_camel = f"{model_key}Factors"
        class_name_upper = f"{model_key.upper()}Factors"
        
        # search for the class in the global scope
        FactorClass = globals().get(class_name_camel) or globals().get(class_name_upper)

        if not FactorClass:
            raise ValueError(f"Invalid model: '{model_key}'. Tried class names: '{class_name_camel}' or '{class_name_upper}'.")
            
        factor_instance = FactorClass(frequency, start_date, end_date, output)

    return factor_instance.download()



### zzzzzzzzzzzzzz. Old mess. This will be repurposed for extracting factors,
# ie, returns a combination of factors from models. For now, leaving it.
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
        self.start_date = self.validate_date_format(start_date) if start_date else None
        self.end_date = self.validate_date_format(end_date) if end_date else None
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
        _process(self.df, filepath=filename)  #lol FIXME


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
        output_path = Path(args.output)
        extension = output_path.suffix

        if not extension:
            extension = '.csv'
 
        _filename = f"{args.model}_{args.freq}_{args.start}_{args.end}{extension}"
        _output_path = output_path.parent / _filename

        extractor.to_file(_output_path)
        #print(f'File saved to "{Path(args.output).resolve()}"')
        # it's wrong, and it's printed correctly by _save_to_file anyway...
        # fixes duplicate print
        print("") # looks weird without a preview
        print(df)

    else:
        print(df)

if __name__ == "__main__":
    main()
