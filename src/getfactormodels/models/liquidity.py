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

    Downloads the Pastor-Stambaugh Liquidity Factors. Only available
    in monthly frequency.

    References:
        L. Pastor and R. Stambaugh (2003). Liquidity Risk and Expected Stock
        Returns. Journal of Political Economy, vol. 111, no. 3, pp. 642–685.

    Note:
        - The leading 65 values of 'TRADED_LIQ' are NaNs (-99).
    """

    @property
    def frequencies(self) -> list[str]:
        return ["m"]

    @property
    def _precision(self) -> int:
        return 8

    @property
    def schema(self) -> pa.Schema:
        """Schema for Pastor-Stambaugh Liquidity factors."""
        return pa.schema([
            ("date", pa.date32()),
            ("AGG_LIQ", pa.float64()),
            ("INNOV_LIQ", pa.float64()),
            ("TRADED_LIQ", pa.float64()),
        ])

    # Testing: models whose URLs update sometime at the start of the year. Possibly 
    #          a temp fix, download a list of this year, last year URLs.
    #
    @property
    def url(self) -> list[str]:
        """Return a list of potential URLs (current year, then previous year)."""
        current_year = datetime.now(tz=timezone.utc).year - 1
        prev_year = current_year - 1
        base_url = "https://faculty.wharton.upenn.edu/wp-content/uploads/2016/11/liq_data_1962_"
        
        return [
            f"{base_url}{current_year}.txt",
            f"{base_url}{prev_year}.txt",
        ]


    def _read(self, data: bytes | dict[str, bytes]) -> pa.Table:
        """Parse raw text into a PyArrow Table."""
        if isinstance(data, dict):
            raise TypeError(f"{self.__class__.__name__} expects a single file, but received a dict.")
            
        # raw text has whitespace-separated columns and comment lines
        text = data.decode("utf-8")
        lines = []
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("%"):
                continue
            # whitespace with a tab
            lines.append(re.sub(r"\s+", "\t", line))
            
        cleaned_data = "\n".join(lines).encode("utf-8")

        parse_schema = pa.schema([
            ("date", pa.int64()),
            ("AGG_LIQ", pa.float64()),
            ("INNOV_LIQ", pa.float64()),
            ("TRADED_LIQ", pa.float64()),
        ])

        try:
            table = pv.read_csv(
                io.BytesIO(cleaned_data),
                read_options=pv.ReadOptions(column_names=parse_schema.names),
                parse_options=pv.ParseOptions(delimiter="\t"),
                convert_options=pv.ConvertOptions(
                    column_types=parse_schema,
                    null_values=["-99", "-99.0", "-99.000000"],
                ),
            )
        except (pa.ArrowIOError, pa.ArrowInvalid) as e:
            msg = f"{self.__class__.__name__}: reading failed: {e}"
            self.log.error(msg)
            raise ValueError(msg) from e

        table = offset_period_eom(table, self.frequency)
        
        return table.cast(self.schema).combine_chunks()
