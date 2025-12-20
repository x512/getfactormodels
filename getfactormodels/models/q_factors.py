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
import pyarrow.csv as pv
from pyarrow.compute import (  # type: ignore[reportAttributeAccessIssue]
    binary_join_element_wise,
    replace_substring_regex,
    strptime,
)
from getfactormodels.models.base import FactorModel


class QFactors(FactorModel):
    """Download q-factor data from global-q.org.

    Args:
        frequency (str): the frequency of the data. d, m, y, q, w, w2w.
        start_date (str, optional): start date, YYYY-MM-DD.
        end_date (str, optional): end date, YYYY-MM-DD.
        classic (bool, optional): returns original 4-factor model.
        cache_ttl (int, optional): cache TTL in seconds.

    Returns:
        pd.DataFrame: timeseries of factors.
    References:
    - Hou, Kewei, Haitao Mo, Chen Xue, and Lu Zhang, 2021, An augmented q-factor model
    with expected growth, Review of Finance 25 (1), 1-41. (q5 model)
    - Hou, Kewei, Chen Xue, and Lu Zhang, 2015, Digesting anomalies: An investment
    approach, Review of Financial Studies 28 (3), 650-705. (Classic q-factor model)

    Data Source: https://global-q.org/factors.html
    """
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "w", "w2w", "m", "q", "y"]

    @property
    def schema(self) -> pa.Schema:  # testing dynamic schema -- nice
        """Returns model schema with normalised names."""  
        factors = [  # TODO: lowercase previous pyarrow models, add property
                   ("r_f", pa.float64()),
                   ("r_mkt", pa.float64()),
                   ("r_me", pa.float64()),
                   ("r_ia", pa.float64()),
                   ("r_roe", pa.float64()),
                   ("r_eg", pa.float64()),
            ]

        if self.frequency in ["m", "q"]:
            period_col = "month" if self.frequency == "m" else "quarter"
            time_cols = [("year", pa.int64()), (period_col, pa.int64())]
        elif self.frequency == "y":
            time_cols = [("year", pa.int64())]
        else:
            time_cols = [("date", pa.int64())]  # 'w'/'w2w' = "date", 'd' = "DATE"

        return pa.schema(time_cols + factors)

    def __init__(self, *, classic: bool = False, **kwargs: Any) -> None:
        self.classic = classic 
        super().__init__(classic=classic, **kwargs)

    def _get_url(self) -> str:
        file = {'m': "monthly", 
                "d": "daily",
                "q": "quarterly", 
                "w": "weekly",
                "w2w": "weekly_w2w",
                "y": "annual",
                }.get(self.frequency)

        url = 'https://global-q.org/uploads/1/2/2/6/122679606'
        url += f'/q5_factors_{file}_2024.csv' # TODO: YEAR
        return url


    def _read(self, data: bytes) -> pd.DataFrame:
        # Normalizing headers
        _header_raw = data.splitlines()[0].decode("utf-8")
        header_line = _header_raw.lower().split(",")

        try:
            table = pv.read_csv(
                io.BytesIO(data),
                read_options=pv.ReadOptions(column_names=header_line, skip_rows=1),
                convert_options=pv.ConvertOptions(
                    column_types=self.schema,
                    include_columns=self.schema.names,
                ),
            )
        except Exception as e:
            raise ValueError(f"error reading csv: {self.frequency}: {e}") from e

        if self.frequency in ["m", "q"]:
            _year = table.column(0).cast(pa.string())
            _period = table.column(1).cast(pa.string())

            period_str = replace_substring_regex(_period, r"^(\d)$", r"0\1")

            if self.frequency == "q":
                _p = replace_substring_regex(period_str, "^01$", "0331")
                _p = replace_substring_regex(_p, "^02$", "0630") # chains, so _p to _p
                _p = replace_substring_regex(_p, "^03$", "0930")
                _p = replace_substring_regex(_p, "^04$", "1231") #fix: was returning 093031, ^ makes it exactly match.
            else:
                _p = binary_join_element_wise(period_str, "01", "")
            # str = year + period
            date_str = binary_join_element_wise(_year, _p, "")

        else:
            date_str = table.column(0).cast(pa.string())
            
            if self.frequency == "y":
                date_str = binary_join_element_wise(date_str, "1231", "")

        dates = strptime(date_str, format="%Y%m%d", unit="ns")
        table = table.append_column("date", dates)

        # drop cols: m/q have 2 columns at start (Year, Period), others have 1
        if self.frequency in ["m", "q"]:
            # Year is at 0, Month/Quarter is at 1. can clear 0 twice:
            table = table.remove_column(0).remove_column(0)
        else: #drop date or year col
            table = table.remove_column(0)

        if self.classic and "r_eg" in table.column_names:
            table = table.drop(["r_eg"])

        columns = [n.upper() for n in table.column_names]
        columns = ["Mkt-RF" if n == "R_MKT" else "RF" if n == "R_F" else n 
            for n in columns]
        table = table.rename_columns(columns)

        # ------------------------------------------------------------------ #
        data = table.to_pandas().set_index("DATE").rename_axis("date")
        
        # DECIMALIZING HERE. TODO: PyArrow helper to decimalize all floats, not str/int/timestamp (pyarrow temporal?)
        data /= 100.0
        return data
