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
import pyarrow as pa
import pyarrow.csv as pv
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.data_utils import offset_period_eom, parse_quarterly_dates


class ICRFactors(FactorModel):
    """
    Download the Intermediary Capital Ratio of He, Kelly, Manela (2017).

    Monthly and quarterly data start in 1970, daily begins on 1999-05-03.

    Args
        frequency (str): The data frequency, 'd' 'm' 'q' (default: 'm')
        start_date (str, optional): The start date YYYY-MM-DD
        end_date (str, optional): The end date YYYY-MM-DD
        output_file (str, optional): optional filepath to save data to. 
          Supports .csv, .pkl.
        cache_ttl (int): Time-to-live for cache in seconds 
          (default: 86400).

    References
    - Z. He, B. Kelly, and A. Manela, ‘Intermediary asset pricing: New evidence
    from many asset classes’, Journal of Financial Economics, vol. 126, no. 1, 
    pp. 1–35, 2017. (https://doi.org/10.1016/j.jfineco.2017.08.002)

    ---
    Notes
    - quarterly data (as of Dec 2025) contains a duplicate entry
      for 2025Q1.
    - NaNs: daily IC_RISK factor doesn't begin until 2008.
    - Precision: Quarterly: 4 decimals before 2013, 18 after.

    Factors
    - IC_RATIO: Intermediary Capital Ratio 
    - IC_RISK: Intermediary Capital Risk Factor 
    - VW_IR: Intermediary Value Weighted Investment Return
    - LEV_SQ: Intermediary Leverage Ratio Squared

    """
    # Data source: https://zhiguohe.net

    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m", "q"]

    def __init__(self, frequency: str = 'm', **kwargs: Any) -> None:
        """Initialize the ICR Factor model."""
        super().__init__(frequency=frequency, **kwargs)

    @property
    def _precision(self) -> int: return 8 if self.frequency == 'd' else 4  # TODO: check 

    @property
    def schema(self) -> pa.Schema:
        date_col = {'d': 'yyyymmdd', 'm': 'yyyymm'}.get(self.frequency, 'yyyyq')

        return pa.schema([
            (date_col, pa.string()),
            ('intermediary_capital_ratio', pa.float64()),
            ('intermediary_capital_risk_factor', pa.float64()),
            ('intermediary_value_weighted_investment_return', pa.float64()),
            ('intermediary_leverage_ratio_squared', pa.float64()),
        ])


    def _get_url(self) -> str:
        _file = {"d": "daily", "m": "monthly", 
                 "q": "quarterly"}.get(self.frequency)

        return f"https://zhiguohe.net/wp-content/uploads/2025/07/He_Kelly_Manela_Factors_{_file}_250627.csv"


    def _read(self, data: bytes):
        """Reads the source ICR factor data from zhiguohe.net."""
        try:
            reader = pv.open_csv(
                io.BytesIO(data),
                read_options=pv.ReadOptions(block_size=1024*1024*2),
                convert_options=pv.ConvertOptions(
                    column_types=self.schema,
                    include_columns=self.schema.names,
                    null_values=[".", "NA", "nan", ""],
                ),
            )
            table = pa.Table.from_batches(reader)

            if self.frequency == "q":
                table = parse_quarterly_dates(table)   # TEST: new util for icr, q. 

            table = offset_period_eom(table, self.frequency)
            # TODO: base should report any repeats in date col 
            # TODO: possibly check if last row is a duplicate quarter and drop it 
            output_cols = ["date", "IC_RATIO", "IC_RISK", "VW_IR", "LEV_SQ"]
            return table.rename_columns(output_cols).combine_chunks()

        except (pa.ArrowIOError, pa.ArrowInvalid) as e:
            msg = f"{self.__class__.__name__}: reading failed: {e}"
            self.log.error(msg)
            raise ValueError(msg) from e
