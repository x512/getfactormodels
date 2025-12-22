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
import pyarrow.compute as pc
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.utils import _offset_period_eom


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
    ---
    TODO:
    """
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "w", "w2w", "m", "q", "y"]

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

    @property
    def schema(self) -> pa.Schema:
        factors = [
            ("R_F", pa.float64()),
            ("R_MKT", pa.float64()),
            ("R_ME", pa.float64()),
            ("R_IA", pa.float64()),
            ("R_ROE", pa.float64()),
            ("R_EG", pa.float64()),
        ]
        
        if self.frequency in ["m", "q"]:
            time_cols = [("year", pa.string()), ("period", pa.string())] #force 'period' here
        elif self.frequency == "y":
            time_cols = [("year", pa.string())]

        else:
            time_cols = [("date", pa.string())]

        return pa.schema(time_cols + factors)


    def _read(self, data: bytes) -> pd.DataFrame | pa.Table:
        """Reads the Augmented q5 factors from q-global.com"""
        read_opts = pv.ReadOptions(column_names=self.schema.names, skip_rows=1)
        conv_opts = pv.ConvertOptions(column_types=self.schema)

        table = pv.read_csv(
            io.BytesIO(data),
            read_options=read_opts,
            convert_options=conv_opts,
        )

        # join m/q year/period
        if self.frequency in ["m", "q"]:
            _year = table.column("year").cast(pa.string())
            _period = table.column("period").cast(pa.int32())

            if self.frequency == "q":
                _period = pc.multiply(_period, 3)

            # padding
            _p_str = _period.cast(pa.string())
            _p_clean = pc.if_else(pc.equal(pc.utf8_length(_p_str), 1),
                                  pc.binary_join_element_wise(pa.scalar("0"), _p_str, ""),
                                  _p_str)
            # join the 2 cols
            date_str = pc.binary_join_element_wise(_year, _p_clean, "")
            table = table.set_column(0, "date", date_str).remove_column(1)

        else:
            # y, d, w: single col
            date_str = table.column(0).cast(pa.string())
            if self.frequency == "y":
                date_str = pc.binary_join_element_wise(date_str, pa.scalar("12"), "")
            table = table.set_column(0, "date", date_str)

        table = _offset_period_eom(table, self.frequency)
 
        rename_map = {"R_F": "RF", 
                      "R_MKT": "Mkt-RF"}
        col_renames = [rename_map.get(n, n) for n in table.column_names]
        table = table.rename_columns(col_renames)
        
        _ordered = ['date', 'Mkt-RF', 'R_ME', 'R_IA', 'R_ROE', 'RF']  #base will handle this
        
        if not self.classic:
            _ordered.insert(5, 'R_EG')
            
        table = table.select(_ordered)

        table.validate(full=True)
        #return table

        data = table.to_pandas().set_index("date")
        # -------------------------------------------------------#
        return data / 100.0
