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
from getfactormodels.utils.utils import _offset_period_eom


class LiquidityFactors(FactorModel):
    """Liquidity factors of Pastor-Stambaugh (2003).
    
    Download the Pastor-Stambaugh Liquidity Factors.

    Args:
        frequency (str): The data frequency, 'm'.
        start_date (str, optional): The start date YYYY-MM-DD.
        end_date (str, optional): The end date YYYY-MM-DD.
        output_file (str, optional): Optional file path to save to file. 
        Supports csv, pkl.
        cache_ttl (int, optional): Cached download time-to-live in secs 
        (default: 86400).
    
    Returns:
        pd.Dataframe: timeseries of factors.

    References:
    - L. Pastor and R. Stambaugh, ‘Liquidity Risk and Expected Stock 
    Returns’, Journal of Political Economy, vol. 111, no. 3, pp. 
    642–685, 2003.
    
    Data source: https://finance.wharton.upenn.edu/~stambaug/
    ---
    NOTES: only available in monthly.
    - NaNs: the leading 65 values in TRADED_LIQ.
    """
    @property
    def _frequencies(self) -> list[str]:
        return ["m"]

    def __init__(self, frequency: str = 'm', **kwargs: Any) -> None:
        super().__init__(frequency=frequency, **kwargs)

    @property   # already decimalized, m=8
    def _precision(self) -> int:
        return 10

    @property
    def schema(self) -> pa.Schema:
        return pa.schema([
            ('Month', pa.int64()),
            ('Agg Liq.', pa.float64()),
            ('Innov Liq (eq8)', pa.float64()),
            ('Traded Liq (LIQ_V)', pa.float64()),
        ])

    
    def _get_url(self) -> str:
        #TODO: Backup data sources: https://research.chicagobooth.edu/-/media/research/famamiller/data/liq_data_1962_2024.txt')
        return 'https://finance.wharton.upenn.edu/~stambaug/liq_data_1962_2024.txt'
    

    def _read(self, data: bytes) -> pa.Table:
        _text = data.decode('utf-8')
        _lines = [
            re.sub(r'\s+', '\t', line.strip()) 
            for line in _text.splitlines() 
            if line.strip() and not line.startswith('%')
        ]
        
        _data = '\n'.join(_lines).encode('utf-8')

        read_opts = pv.ReadOptions(
            column_names=self.schema.names,
            autogenerate_column_names=False,
            skip_rows=0,
        )

        parse_opts = pv.ParseOptions(
            delimiter='\t',
            ignore_empty_lines=True,
        )

        convert_opts = pv.ConvertOptions(
            column_types=self.schema,
            null_values=["-99", "-99.0", "-99.000000"],
            include_columns=self.schema.names,
        )
        try:
            table = pv.read_csv(
                io.BytesIO(_data),
                read_options=read_opts,
                parse_options=parse_opts,
                convert_options=convert_opts,
            )
        except (pa.ArrowInvalid, KeyError) as e:
            raise ValueError(f"Error reading csv for {self.__class__.__name__}: {e}") from e
        
        # month only freq for liquidity (could send d/w through anyway, no-op)
        table = _offset_period_eom(table, self.frequency)

        table.validate()  # explicit validation! in base: table.validate(full=True) 

        table = table.rename_columns(['date', 'AGG_LIQ', 
                                      'INNOV_LIQ', 'TRADED_LIQ',])
        return table
