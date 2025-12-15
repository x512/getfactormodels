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
import logging  # TODO logging
from pathlib import Path
from typing import List
import pandas as pd
from dateutil import parser
from getfactormodels.models import *
from getfactormodels.models.fama_french import FamaFrenchFactors
from getfactormodels.models.q_factors import QFactors
from getfactormodels.utils.cli import parse_args
from getfactormodels.utils.utils import _get_model_key, _process, _save_to_file

# TEMPORARY MINIMAL REWORK (until the keymaps and insane regex is dropped
# and base class and FactorExtractor done)
# just getting get_factors to work!

def get_factors(model: str | int = 3,
                frequency: str = "m",
                start_date: str | None = None,
                end_date: str | None = None,
                output_file: str | None = None,
                *,
                region: str | None = None,
                ):
    """Get data for a specific factor model.

    Main public function.

    Args:
        model (str, float): the name of the factor model ('3', '4', '5', '6', 'carhart', 
                            'liq', 'misp', 'icr', 'dhs', 'qclassic', 'q', 'hml_d').
        frequency  (str, optional): the frequency of the data. ('d' 'w' 'm' 'q' or 'y', default: 'm')
        start_date (str, optional): start date of the returned data, YYYY-MM-DD.
        end_date (str, optional): end date of returned data, YYYY-MM-DD.
        output(str, optional): filepath to write returned data to, e.g. "~/some/dir/some_file.csv"

    """
    # NOTE: Parses input, assigns it to a factor model class, calls it, and returns the data.

    #
    # TODO: FIXME. kwargs. TODO: default outputs in CLI.
    #
    frequency = frequency.lower()
    region = region
    model_key = _get_model_key(model)

    factor_instance = None
    if region != None and model_key not in ['3', '4', '5', '6']:
        _model = _get_model_key(model)
        raise ValueError(
            f"Region '{region}' is not supported for the '{_model}'. "
            "The region parameter is only available for Fama-French models (3, 4, 5, 6)."
        )
    if model_key in ["3", "4", "5", "6"]:
        factor_instance = FamaFrenchFactors(model=model_key,
                                            frequency=frequency,
                                            start_date=start_date,
                                            end_date=end_date,
                                            output_file=output_file,
                                            region=region)

    elif model_key == "Qclassic":
        factor_instance = QFactors(frequency=frequency, start_date=start_date,
                                   end_date=end_date, output_file=output_file, classic=True)

    else:
        # Class loading: tries CamelCaseFactors then UPPERCASEFactors
        class_name_camel = f"{model_key}Factors"
        class_name_upper = f"{model_key.upper()}Factors"

        # search for the class in the global scope
        FactorClass = globals().get(class_name_camel) or globals().get(class_name_upper)

        if not FactorClass:
            raise ValueError(f"Invalid model: '{model_key}'. "
                             f"Tried:'{class_name_camel}','{class_name_upper}'.")

        factor_instance = FactorClass(frequency=frequency,
                                      start_date=start_date,
                                      end_date=end_date,
                                      cache_ttl=86400,
                                      output_file=output_file)

    return factor_instance.download()

# CLI (TODO)
### zzzzzzzzzzzzzz. Old mess. Being removed. TODO: (one day) extract factors into a composite model.
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
                 frequency: str = 'm',
                 start_date: str | None = None,
                 end_date: str | None = None,
                 output: str | None = None,
                 *,
                 region: str | None = None):
        self.model: str = model
        self.frequency: str = frequency
        self.start_date = self.validate_date_format(start_date) if start_date else None
        self.end_date = self.validate_date_format(end_date) if end_date else None
        self.output = output
        self._no_rf = False
        self._no_mkt = False
        self.df = None
        self.region = region

    def no_rf(self) -> None:
        self._no_rf = True

    def no_mkt(self) -> None:
        self._no_mkt = True

    def extract(self, data, factor: str | List[str]) -> pd.Series | pd.DataFrame:
        """Retrieves a single factor (column) from the dataset."""
        #data = self.download()

        if data.empty:
            raise RuntimeError("DataFrame empty: can not extract a factor.")

        if isinstance(factor, str):
            if factor not in data.columns:
                raise ValueError(f"Factor '{factor}' not available.")       
            return data[factor]

        elif isinstance(factor, list):
            return data[factor]

    @staticmethod
    def validate_date_format(date_string: str) -> str:
        """Validate the date format.
        Raises:
            ValueError: If the date format is incorrect.
        """
        try:
            return parser.parse(date_string).strftime("%Y-%m-%d")
        except ValueError as err:
            error_message = "Incorrect date format, use YYYY-MM-DD."
            raise ValueError(error_message) from err

    # Holy. TODO: Clean it up. Incorp into base 
    def get_factors(self) -> pd.DataFrame:
        """Fetch the factor data and store it in the class."""
        self.df = get_factors(
            model=self.model,
            frequency=self.frequency,
            start_date=self.start_date,
            end_date=self.end_date,
            output_file=self.output,  
            region=self.region)

        if self._no_rf:
            self.df = self.drop_rf(self.df.copy())  # create a copy before drop
        if self._no_mkt:
            self.df = self.drop_mkt(self.df.copy())

        return self.df

    def drop_rf(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop the ``RF`` column from the DataFrame."""

        if "RF" in df.columns:
            df = df.drop(columns=["RF"])
        else:
            print("`drop_rf` was called but no RF column was found.")

        return df

    def drop_mkt(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop the ``MKT`` column from the DataFrame."""

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

    extractor = FactorExtractor(model=args.model,
                                frequency=args.freq,
                                start_date=args.start,
                                end_date=args.end,
                                region=args.region,)
    if args.norf:
        extractor.no_rf()
    if args.nomkt:
        extractor.no_mkt()

    df = extractor.get_factors()

    if args.extractfactor:
        df = extractor.extract(df, args.extractfactor)
    if args.output:
        output_path = Path(args.output).expanduser()

        if args.output:     # TODO: cache
            output_path = Path(args.output)
            min_date = df.index.min() 
            max_date = df.index.max()

            actual_start = min_date.strftime('%Y%m%d')
            actual_end = max_date.strftime('%Y%m%d')
            # if NaT, might be outside available data... to do warn/catch 

            _filename = f"{args.model}_{args.freq.upper()}_{actual_start}-{actual_end}"
            _ext = '.csv'

            user_path = Path(args.output).expanduser()
            output_path = None

            if user_path.is_dir():
                output_path = user_path / (_filename + _ext)
            elif user_path.suffix:
                output_path = user_path
            else: #add ext to user provided 'file' or 'dir/file'
                output_path = user_path.parent / (user_path.name + _ext)

            if output_path:
                _save_to_file(df, output_path)
    print(df)

if __name__ == "__main__":
    main()
