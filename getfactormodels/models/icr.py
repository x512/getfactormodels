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
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.utils import _process


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

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def _get_url(self) -> str:
        _file = {"d": "daily", 
                 "m": "monthly", 
                 "q": "quarterly"}.get(self.frequency)
        return f"https://zhiguohe.net/wp-content/uploads/2025/07/He_Kelly_Manela_Factors_{_file}_250627.csv"


    def _read(self, data) -> pd.DataFrame:
        """Helper to remove TODO"""
        _data = io.StringIO(data.decode('utf-8'))
       
        # back to pd, old func stuff
        df = pd.read_csv(_data)
        df = df.rename(columns={df.columns[0]: "date"})

        # TODO: moving similar date validations to base
        if self.frequency == "q":
            # Quarterly dates are in a YYYYQ format [19752 to 1975Q2]
            df["date"] = df["date"].astype(str)
            df["date"] = df["date"].str[:-1] + "Q" + df["date"].str[-1]
            # Converts YYYYQ to a timestamp at the eoq
            df["date"] = pd.PeriodIndex(df["date"], freq="Q").to_timestamp() \
                + pd.offsets.QuarterEnd(0)
#PATTERN
        elif self.frequency == "m":
            df["date"] = pd.to_datetime(df["date"], format="%Y%m")
            df["date"] = df["date"] + pd.offsets.MonthEnd(0)

        elif self.frequency == "d":
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")

        df = df.rename(columns={
                "intermediary_capital_ratio": "IC_RATIO",
                "intermediary_capital_risk_factor": "IC_RISK_FACTOR",
                "intermediary_leverage_ratio_squared": "INT_LEV_RATIO_SQ",
                "intermediary_value_weighted_investment_return": "INT_VW_ROI",
            })

        df = df.set_index("date")
        
        # TODO: daily returns 15 decimals; 
        # Decimal type possibly to avoid rounding errors.
        df = df.round(8) if self.frequency == 'd' else df.round(4) # RUF/SIM108
        
        #TODO: check if decimalizing anything?

        return _process(df, self.start_date,
                        self.end_date, filepath=self.output_file)

