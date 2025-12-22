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
import io
from typing import Any
import pandas as pd
import pyarrow as pa
#import pyarrow.compute as pc
from pyarrow.compute import replace_substring_regex
import pyarrow.csv as pv
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.utils import _offset_period_eom


class ICRFactors(FactorModel):
    """
    Download the Intermediary Capital Ratio of He, Kelly, Manela (2017)

    Args:
        frequency (str): The data frequency ('d', 'm', 'q') (default: 'm').
        start_date (str, optional): The start date YYYY-MM-DD for the factor data
        end_date (str, optional): The end date YYYY-MM-DD
        output_file (str, optional): optional filepath to save data to. Supports .csv, .pkl.
        cache_ttl (int): Time-to-live for cache in seconds (default: 86400).

    Returns:
        pd.DataFrame

    References:
    - Z. He, B. Kelly, and A. Manela, ‘Intermediary asset pricing: New evidence
    from many asset classes’, Journal of Financial Economics, vol. 126, no. 1, 
    pp. 1–35, 2017. (https://doi.org/10.1016/j.jfineco.2017.08.002)

    Data source: https://zhiguohe.net
    ---
    NOTES:
    - Quarterly data source (as of Dec 2025) contains duplicate entries 
      for 2025Q1.
    - Data availability: Monthly/Quarterly: 1970. Daily: 1999-05-03.
    - NaNs: daily IC_RISK factor has 2180 empty leading values, begins in 2008. (quarterly, 
    monthly IC_RISK goes back to 1970).
    - Precision: Quarterly: 4 decimals before 2013, 18 after.
    - FACTORS:
        - IC_RATIO: Intermediary Capital Ratio 
        - IC_RISK: Intermediary Capital Risk Factor 
        - VW_IR: Intermediary Value Weighted Investment Return
        - LEV_SQ: Intermediary Leverage Ratio Squared
    """
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m", "q"]

    def __init__(self, frequency: str = 'm', **kwargs: Any) -> None:
        super().__init__(frequency=frequency, **kwargs)

    def _get_url(self) -> str:
        _file = {"d": "daily", 
                 "m": "monthly", 
                 "q": "quarterly"}.get(self.frequency)
        return f"https://zhiguohe.net/wp-content/uploads/2025/07/He_Kelly_Manela_Factors_{_file}_250627.csv"

    @property
    def schema(self) -> pa.Schema:
        _date_col = {'d': 'yyyymmdd', 'm': 'yyyymm'}.get(self.frequency, 'yyyyq')

        return pa.schema([
            (_date_col, pa.string()),
            ('intermediary_capital_ratio', pa.float64()),
            ('intermediary_capital_risk_factor', pa.float64()),
            ('intermediary_value_weighted_investment_return', pa.float64()),
            ('intermediary_leverage_ratio_squared', pa.float64()),
        ])


    def _read(self, data: bytes):
        """Reads the ICR factors data using PyArrow."""
        convert_opts = pv.ConvertOptions(
            column_types=self.schema, 
            include_columns=self.schema.names,
            null_values=[".", "NA", "nan", ""],
            check_utf8=True,
        )
        try:
            table = pv.read_csv(
                io.BytesIO(data),
                convert_options=convert_opts
            )

        except (pa.ArrowInvalid, KeyError) as e:
            raise ValueError(f"Error reading csv for {self.__class__.__name__}: {e}") from e

        if self.frequency == "q":
            # quarters to last day of the month. 20211 to 2021-03-31, etc. For the util.
            date_array = table.column(0).cast(pa.string())
            date_array = replace_substring_regex(date_array, pattern="1$", replacement="0301")
            date_array = replace_substring_regex(date_array, pattern="2$", replacement="0601")
            date_array = replace_substring_regex(date_array, pattern="3$", replacement="0901")
            date_array = replace_substring_regex(date_array, pattern="4$", replacement="1201")
            table = table.set_column(0, "Date", date_array)

        table = _offset_period_eom(table, self.frequency)
        # TODO: base should report any repeats in date col 
        # TODO: possibly check if last row is a duplicate quarter and drop it 
        output_cols = ["date", "IC_RATIO", "IC_RISK", "VW_IR", "LEV_SQ"] # renamed. Docstr needs to list factors.. TODO..

        if len(table.column_names) == len(output_cols):
            table = table.rename_columns(output_cols)

        table.validate()
        #return table
        df = table.to_pandas().set_index('date')
        # ------------------------------------------------------------------- #
        df.index = pd.to_datetime(df.index)
        precision = 8 if self.frequency == 'd' else 4  #TODO: check precision.

        return df.round(precision)

## NOTES ##########################
# - 20251, 20251: err, 'qtr in progress', or q2?
# - Daily data: starts at 1999-05-03, others 1970.
# - The factor "IC_RISK_FACTOR" doesn't start until 2008.
#     20071231,0.054184042885248127,,0.002794355636822843,340.60983767904156
#     20080102,0.0534001171270667,-0.014500376955897757,-0.014935307118801533,350.68370058245614
#     NaNs (daily only): IC_RISK_FACTOR: first 2180 values
# - Decimals change at 2013, quarterly:
#     20123,0.0433,0.0599,0.1086,534.2535
#     20124,0.049,0.1108,0.1367,417.2462
#     20131,0.047232556885232674,0.01682231506466723,0.0711293544918774,448.2466860180383
#     20132,0.049717547164912494,0.03834382965518407,0.05416122668963008,404.5578300568576
