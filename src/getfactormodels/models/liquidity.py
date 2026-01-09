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
import re
from typing import Any
import pyarrow as pa
import pyarrow.csv as pv
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.date_utils import offset_period_eom


class LiquidityFactors(FactorModel):
    """Liquidity factors of Pastor-Stambaugh (2003).

    Download the Pastor-Stambaugh Liquidity Factors. Only available 
    in monthly.

    Args:
        frequency (str): The data frequency, 'm'.
        start_date (str, optional): The start date YYYY-MM-DD.
        end_date (str, optional): The end date YYYY-MM-DD.
        output_file (str, optional): file path to save to.
          Supports csv, pkl.
        cache_ttl (int, optional): download time-to-live in secs 
        (default: 86400).

    References:
    - L. Pastor and R. Stambaugh, ‘Liquidity Risk and Expected Stock 
      Returns’, Journal of Political Economy, vol. 111, no. 3, pp. 
      642–685, 2003.

    ---

    Note:
        the leading 65 values of TRADED_LIQ are NaNs.
    """
    @property
    def _frequencies(self) -> list[str]: 
        return ["m"]

    def __init__(self, frequency: str = 'm', **kwargs: Any) -> None:
        """Initialize the Liquidity Factors model."""
        super().__init__(frequency=frequency, **kwargs)

    @property
    def _precision(self) -> int:
        return 8

    @property
    def schema(self) -> pa.Schema:
        return pa.schema([
            ('Month', pa.int64()),
            ('Agg Liq.', pa.float64()),
            ('Innov Liq (eq8)', pa.float64()),
            ('Traded Liq (LIQ_V)', pa.float64()),
        ])

    def _get_url(self) -> str:
        return 'https://finance.wharton.upenn.edu/~stambaug/liq_data_1962_2024.txt'

    def _read(self, data: bytes) -> pa.Table:
        try:
            _text = data.decode('utf-8')
            _lines = [
                re.sub(r'\s+', '\t', line.strip()) 
                for line in _text.splitlines() 
                if line.strip() and not line.startswith('%')
            ]

            _data = '\n'.join(_lines).encode('utf-8')

            # open stream reader not read whole thing
            reader = pv.open_csv(
                io.BytesIO(_data),
                read_options=pv.ReadOptions(
                    column_names=self.schema.names,
                    block_size=1024*1024*2,
                ),
                parse_options=pv.ParseOptions(delimiter='\t'),
                convert_options=pv.ConvertOptions(
                    column_types=self.schema,
                    null_values=["-99", "-99.0", "-99.000000"],
                ),
            )

            # Batch processing
            batches = []
            for batch in reader:
                batches.append(batch)

            # .from_batches!!!
            table = pa.Table.from_batches(batches)
            table = offset_period_eom(table, self.frequency)
            #table.validate()
            return table.rename_columns(['date', 'AGG_LIQ', 
                                         'INNOV_LIQ', 'TRADED_LIQ'])   #TODO: fix, not enforcing schema...

        except (pa.ArrowIOError, pa.ArrowInvalid) as e:
                msg = f"{self.__class__.__name__}: reading failed: {e}"
                self.log.error(msg)
                raise ValueError(msg) from e
