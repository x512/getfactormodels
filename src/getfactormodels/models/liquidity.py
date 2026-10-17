# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
"""Liquidity factors of Pastor-Stambaugh (2003)."""
import io
import re
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.csv as pv
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.date_utils import offset_period_eom


class LiquidityFactors(FactorModel):
    """Liquidity factors of Pastor-Stambaugh (2003).

    Download the Pastor-Stambaugh Liquidity Factors. Only available 
    in monthly frequency.

    References:
        L. Pastor and R. Stambaugh (2003). Liquidity Risk and Expected Stock 
        Returns. Journal of Political Economy, vol. 111, no. 3, pp. 642–685.

    Note:
        - The leading 65 values of 'TRADED_LIQ' are NaNs.
    """
    @property
    def _frequencies(self) -> list[str]: return ["m"]

    @property
    def _precision(self) -> int: return 8

    @property
    def schema(self) -> pa.Schema:
        """Schema for Liquidity factors."""
        return pa.schema([
            ('Month', pa.int64()),
            ('Agg Liq.', pa.float64()),
            ('Innov Liq (eq8)', pa.float64()),
            ('Traded Liq (LIQ_V)', pa.float64()),
        ])


    def _get_url(self) -> str:
        last_year = datetime.now(tz=timezone.utc).year - 1 # noqa: UP017    keep this until strictly >=3.11 (datetime.UTC)
        base_url = "https://faculty.wharton.upenn.edu/wp-content/uploads/2016/11/liq_data_1962_"

        return f"{base_url}{last_year}.txt" # TODO: check the URL with check_connection, fallback to year before.


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
                                         'INNOV_LIQ', 'TRADED_LIQ']).combine_chunks()   #TODO: fix, not enforcing schema...

        except (pa.ArrowIOError, pa.ArrowInvalid) as e:
                msg = f"{self.__class__.__name__}: reading failed: {e}"
                self.log.error(msg)
                raise ValueError(msg) from e
