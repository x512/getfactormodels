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
from getfactormodels.utils.utils import (_pd_rearrange_cols, _save_to_file,
                                         _slice_dates)


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

    SCHEMA = pa.schema([
        ('YYYYMM', pa.timestamp('ms')),
        ('MKTRF', pa.float64()),
        ('SMB', pa.float64()),
        ('RMW', pa.float64()),
        ('CMA', pa.float64()),
        ('RF', pa.float64()),
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
                column_types=self.SCHEMA,
                timestamp_parsers=["%Y%m%d", "%Y%m"],
            )

            table = pv.read_csv(
                io.BytesIO(data),
                read_options=read_opts,
                parse_options=parse_opts,
                convert_options=convert_opts,
            )

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
            final_rows = table.num_rows
            rows_dropped = initial_rows - final_rows

            if rows_dropped > 0:
                msg = (f"{rows_dropped} NaN rows dropped."
                      f"({initial_rows} -> {final_rows}).")
                self.log.debug(msg)
            else:
                msg = f"No NaNs in {final_rows} rows."
                self.log.debug(msg)

            # pandas line --------------------------------------------------- #
            df = table.to_pandas()

            df = df.set_index('date')
            df.index.name = 'date'

            #df = df.replace([-99.99, -999], pd.NA).dropna()

            if self.frequency == "m":
                # PyArrow gives the 1st of the month, shift to end.
                df.index = df.index + pd.offsets.MonthEnd(0)  # this is whats messing user input hmm
                # will keep uncommented while every other model does this.
            # TODO: If using end of month (which it is), then need to 
            # parse user input dates, THEN shift to end of month. FIXME.
            df = _pd_rearrange_cols(df)
            
            df = _slice_dates(df, self.start_date, self.end_date)

            if self.output_file:
                _save_to_file(df, filepath=self.output_file)

            return df
 
        # returns an empty dataframe that base class expects for this model.
        # TODO: FIXIME: cleanup, handle errors properly everywhere...
        except (pa.ArrowIOError, pa.ArrowInvalid) as e:
            err_msg = f"Reading or parsing failed for Mispricing factors: {e}"
            self.log.error(err_msg)
            column_names = ['Mkt-RF', 'SMB_SY', 'MGMT', 'PERF', 'RF']

            return pd.DataFrame(columns=column_names, # type: ignore [reportArgumentType] 
                                index=pd.DatetimeIndex([], name='date'))
