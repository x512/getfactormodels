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
import pyarrow.compute as pc
import pyarrow.csv as pv
from getfactormodels.models.base import FactorModel


class ICRFactors(FactorModel):
    """Download the Intermediary Capital Ratio of He, Kelly, Manela (2017)

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
    """
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m", "q"]

    def __init__(self, frequency: str = 'm', **kwargs: Any) -> None:
        #self.frequency = frequency   already set in base class
        super().__init__(frequency=frequency, **kwargs)

    def _get_url(self) -> str:
        _file = {"d": "daily", 
                 "m": "monthly", 
                 "q": "quarterly"}.get(self.frequency)
        return f"https://zhiguohe.net/wp-content/uploads/2025/07/He_Kelly_Manela_Factors_{_file}_250627.csv"

    def _read(self, data: bytes):
        """Reads the ICR factors data using PyArrow."""

        if self.frequency == 'd':
            _date_col = "yyyymmdd"

        elif self.frequency == 'm':
            _date_col = "yyyymm"

        else: # 'q'
            _date_col = "yyyyq"

        SCHEMA = pa.schema([
            (_date_col, pa.string()),
            ('intermediary_capital_ratio', pa.float64()),
            ('intermediary_capital_risk_factor', pa.float64()),
            ('intermediary_value_weighted_investment_return', pa.float64()),
            ('intermediary_leverage_ratio_squared', pa.float64()),
        ])
        try:
            table = pv.read_csv(
                io.BytesIO(data),
                #readopts?
                convert_options=pv.ConvertOptions(
                    column_types=SCHEMA, 
                    #timestamp_parsers= can't use with Qtr
                    include_columns=SCHEMA.names, # fix: forces strict header check (test was passing when eg only one col returned)
                    check_utf8=True,  # extra validation
                ),
            )
        except (pa.ArrowInvalid, KeyError) as e:
            raise ValueError(f"Error reading csv for {self.__class__.__name__}. "
                f"Check if source headers changed: {e}") from e

        #if _date_col not in table.schema.names:
        #    raise KeyError(f"Column {_date_col} not found")
        #date_array: pa.array = table[_date_col]   pa Array?
        #date_array: pa.ChunkedArray = table.column(_date_col)
        #date_col_name = SCHEMA.names[0] #use the Schema to get the column by index not name
        date_array: pa.ChunkedArray = table.column(0) #uses table.column with integer index, faster (skips str to index lookup)

        if self.frequency == "q":
            # replace Q with MM-DD, e.g., replaces 20251 with 2025-03-31
            date_array = pc.replace_substring_regex(date_array, pattern="1$", replacement="-03-31") # type: ignore[reportAttributeAccessIssue] 
            date_array = pc.replace_substring_regex(date_array, pattern="2$", replacement="-06-30") # type: ignore
            date_array = pc.replace_substring_regex(date_array, pattern="3$", replacement="-09-30") # type: ignore
            date_array = pc.replace_substring_regex(date_array, pattern="4$", replacement="-12-31") # type: ignore

            # now parses
            parsed_dates = pc.strptime(date_array, format="%Y-%m-%d", unit='ms')

        elif self.frequency == "m":
            # Parse YYYYMM -> Timestamp (Defaults to Start of Month)
            parsed_dates = pc.strptime(date_array, format="%Y%m", unit='ms')

        else: # 'd'
            parsed_dates = pc.strptime(date_array, format="%Y%m%d", unit='ms')

        # replaces date col (str) with the timestamp array
        date_index = table.schema.get_field_index(_date_col)  #finds index to replace it exactly.
        table = table.set_column(date_index, "date", parsed_dates)

        output_cols = ["date", "IC_RATIO", "IC_RISK_FACTOR", "INT_VW_ROI", "INT_LEV_RATIO_SQ"]
        
        # Validation: len match to ensure rename correctly. If col counts differ then source may have changed
        if len(table.column_names) == len(output_cols):
            table = table.rename_columns(output_cols)

        # ------------------------------------------------------------------- #
       # df = table.to_pandas()  #NaNs made here
        df = table.to_pandas(
            date_as_object=False,  # dates stay as datetime64[ns]
            use_threads=True,
        )

        if self.frequency in ['m', 'q']:
            # PyArrow parsed to 1st of month; switch to Month End like other models.
            df['date'] = df['date'] + pd.offsets.MonthEnd(0)

        df = df.set_index("date")

        precision = 8 if self.frequency == 'd' else 4  #TODO: handle these consistently
        df = df.round(precision)
        return df

"""
NOTES:
Every model seems to have a few intracacies about it, and some have missing data. 
Starting to bring some notes together for docs...

    - Decimals change at 2013, quarterly:
        20123,0.0433,0.0599,0.1086,534.2535
        20124,0.049,0.1108,0.1367,417.2462
        20131,0.047232556885232674,0.01682231506466723,0.0711293544918774,448.2466860180383
        20132,0.049717547164912494,0.03834382965518407,0.05416122668963008,404.5578300568576
        20133,0.0552094260389565,0.09972195221722374,0.043373602808057576,328.07530072071125

    - Err in source? 20251 then again 20251 (last upload was end of q2. Either this is a typo or it's a 'qtr in progress'.

    - Daily data: starts at 1999-05-03. The factor "IC_RISK_FACTOR" doesn't start until 2008.
        20071231,0.054184042885248127,,0.002794355636822843,340.60983767904156
        20080102,0.0534001171270667,-0.014500376955897757,-0.014935307118801533,350.68370058245614
        NaNs (daily only): IC_RISK_FACTOR: first 2180 values

    - Month/Qtr IC_RISK_FACTOR goes back to start of 1970.

"""















