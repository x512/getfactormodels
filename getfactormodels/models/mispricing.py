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
#import pyarrow.compute as pc
from pyarrow.compute import (subtract, ceil_temporal)
import pyarrow as pa
import pyarrow.csv as pv
from getfactormodels.models.base import FactorModel


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
    def schema(self):
        return pa.schema([
            ('YYYYMM', pa.timestamp('ms')),
            ('MKTRF',  pa.float64()),
            ('SMB',    pa.float64()),
            ('MGMT',   pa.float64()),
            ('PERF',   pa.float64()),
            ('RF',     pa.float64()),
        ])

    def _read(self, data):
        """Reads the Mispricing factors CSV."""
        try:
            # TEST: reading CSV with PyArrow
            # will be a CSV reader
            read_opts = pv.ReadOptions(
                skip_rows=1,
                column_names=['YYYYMM', 'MKTRF', 'SMB', 'MGMT', 'PERF', 'RF'],
            )

            parse_opts = pv.ParseOptions(
                delimiter=',',
                ignore_empty_lines=True,
            )

            convert_opts = pv.ConvertOptions(
                column_types=self.schema,
                timestamp_parsers=["%Y%m%d", "%Y%m"],
            )

            table = pv.read_csv(
                io.BytesIO(data),
                read_options=read_opts,
                parse_options=parse_opts,
                convert_options=convert_opts,
            )

            if self.frequency == 'm':
                dates = table.column('YYYYMM') 
                 
                # ceil to the first day of the NEXT month
                # 'unit' is str not temporal
                next_month = ceil_temporal(dates, 1, unit='month')
                
                # subtract a day in ms
                one_day_ms = pa.scalar(86400000, type=pa.duration('ms'))
                
                snapped_dates = subtract(next_month, one_day_ms)
                
                # replace the column
                date_idx = table.schema.get_field_index('YYYYMM')
                table = table.set_column(date_idx, 'YYYYMM', snapped_dates)
            
            table = table.rename_columns([
                'date',     # YYYYMM -> date
                'Mkt-RF',   # MKTRF -> Mkt-RF
                'SMB_SY',   # SMB -> SMB_SY
                'MGMT', 
                'PERF',
                'RF',
            ])

            # Debug, testing
            initial_rows = table.num_rows
            table = table.drop_null()
            self.log.debug(f"Read {table.num_rows} rows (dropped {initial_rows - table.num_rows} NaNs)")


            # pandas line --------------------------------------------------- #
            df = table.to_pandas()

            df = df.set_index('date')
            df.index.name = 'date'

            #df = df.replace([-99.99, -999], pd.NA).dropna()               
            return df

            # returns an empty dataframe that base class expects for this model.
            # TODO: FIXIME: cleanup, handle errors properly everywhere...
        except (pa.ArrowIOError, pa.ArrowInvalid) as e:
            self.log.error(f"Reading or parsing failed for Mispricing factors: {e}")
            column_names = ['Mkt-RF', 'SMB_SY', 'MGMT', 'PERF', 'RF']

            return pd.DataFrame(
                columns=pd.Index(column_names),   # fixes typehint err erportArgumentType.
                index=pd.DatetimeIndex([], name='date')
            ).astype(float)
