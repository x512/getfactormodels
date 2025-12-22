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
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.utils import _offset_period_eom

# TODO: proper class docstr's.
class MispricingFactors(FactorModel):
    """Mispricing factors of Stambaugh & Yuan (2016).

    Downloads the Mispricing factors of R. F. Stambaugh and Y. Yuan. Data from
    1963 to 2016. Note: the SMB factor is returned as SMB_SY.

    Args:
        frequency(str, optional): 'm' (monthly), 'd' (daily)
        start_date (str, optional): The start date YYYY-MM-DD.
        end_date (str, optional): The end date YYYY-MM-DD.
        output_file (str, optional): Optional file path to save to file. Supports csv, pkl.
        classic (bool, optional): returns the classic 4-factor q-factor model. Default: False.
        cache_ttl (int, optional): Cached download time-to-live in seconds (default: 86400).

    Returns:
        pd.DataFrame: timeseries of factor data.

    References:
    - Pub: R. F. Stambaugh and Y. Yuan, ‘Mispricing Factors’, The Review of 
    Financial Studies, vol. 30, no. 4, pp. 1270–1315, 12 2016.

    Data source: https://finance.wharton.upenn.edu/~stambaug/
    """
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def _get_url(self) -> str:
        base_url = "https://finance.wharton.upenn.edu/~stambaug"
        file_name = "M4d" if self.frequency == "d" else "M4"
        return f"{base_url}/{file_name}.csv"

    @property
    def schema(self) -> pa.Schema:
        # fix: schema wasn't enforced. 
        #  M4d/M4 have differen't date columns.
        #  by skipping the first row and applying the schema names, it still 
        #  worked. Might be all that's needed though (types)
        date_col = "DATE" if self.frequency == "d" else "YYYYMM"  # checked with renames, it's enforced now...
        return pa.schema([
            (date_col, pa.string()),  # TODO: FIXME: int32
            ('MKTRF', pa.float64()),
            ('SMB', pa.float64()),
            ('MGMT', pa.float64()),
            ('PERF', pa.float64()),
            ('RF', pa.float64()),
        ])


    def _read(self, data):
        """Reads the Mispricing factors CSV."""
        try:
            read_opts = pv.ReadOptions(
                #  skip_rows=1,
                # column_names=self.schema.names,
                autogenerate_column_names=False)

            parse_opts = pv.ParseOptions(
                delimiter=',',
                ignore_empty_lines=True,
            )

            convert_opts = pv.ConvertOptions(
                column_types=self.schema,
                # timestamp_parsers=["%Y%m%d", "%Y%m"],
                include_columns=self.schema.names,
            )

            table = pv.read_csv(
                io.BytesIO(data),
                read_options=read_opts,
                parse_options=parse_opts,
                convert_options=convert_opts,
            )

            # fixed: _offset_period_eom helper utility works on col 0 (no need to rename date)
            #print(table)
            table = _offset_period_eom(table, self.frequency) 
            #print(table)

            table = table.rename_columns([
                'date',     # YYYYMM
                'Mkt-RF',   # MKTRF
                'SMB_SY',   # SMB
                'MGMT', 
                'PERF',
                'RF',
            ])

            #initial_rows = table.num_rows
            #table = table.drop_null()
            #self.log.debug(f"Read {table.num_rows} rows (dropped {initial_rows - table.num_rows} NaNs)")
            # No NaNs in data source

            df = table.to_pandas().set_index('date')
            # pandas line --------------------------------------------------- #
            return df.round(8 if self.frequency == 'd' else 4)

            # TODO: empty pa.Table if need 
        except (pa.ArrowIOError, pa.ArrowInvalid) as e:
            self.log.error(f"Reading or parsing failed for Mispricing factors: {e}")
            column_names = ['Mkt-RF', 'SMB_SY', 'MGMT', 'PERF', 'RF']

            return pd.DataFrame(
                columns=pd.Index(column_names),   # fixes typehint err erportArgumentType.
                index=pd.DatetimeIndex([], name='date')
            ).astype(float)
