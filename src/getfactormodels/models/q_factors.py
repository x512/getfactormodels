# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import io
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.arrow_utils import scale_to_decimal
from getfactormodels.utils.date_utils import (
    offset_period_eom,
    parse_quarterly_dates,
)


class QFactors(FactorModel): 
    """Download and process q-factor data from global-q.org.

    Args:
        frequency (str): the frequency of the data. d, m, y, q, w, w2w.
        start_date (str, optional): start date, YYYY-MM-DD.
        end_date (str, optional): end date, YYYY-MM-DD.
        classic (bool, optional): returns original 4-factor model.
        output_file (str, optional): Path to save the data automatically.
        cache_ttl (int, optional): cache TTL in seconds.

    References:
    - Hou, Kewei, Haitao Mo, Chen Xue, and Lu Zhang, 2021, An augmented 
      q-factor model with expected growth, Review of Finance 25 (1), 
      1-41.
    - Hou, Kewei, Chen Xue, and Lu Zhang, 2015, Digesting anomalies: An 
      investment approach, Review of Financial Studies 28 (3), 650-705.

    """
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "w", "w2w", "m", "q", "y"]

    def __init__(self, *, classic: bool = False, **kwargs) -> None:
        """Initialize the QFactors model."""
        self.classic = classic
        super().__init__(**kwargs)
    
    @property
    def _precision(self) -> int: return 6

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
            #force 'period' here
            time_cols = [("year", pa.string()), ("period", pa.string())]
        elif self.frequency == "y":
            time_cols = [("year", pa.string())]
        else: # d/w/w2w, force 'date'
            time_cols = [("date", pa.string())]

        return pa.schema(time_cols + factors)


    def _get_url(self) -> str:
        file = {'m': "monthly", 
                "d": "daily",
                "q": "quarterly", 
                "w": "weekly",
                "w2w": "weekly_w2w",
                "y": "annual",
                }.get(self.frequency)

        url = 'https://global-q.org/uploads/1/2/2/6/122679606'
        url += f'/q5_factors_{file}_2024.csv'
        return url


    def _format_date_column(self, table: pa.Table) -> pa.Table:
        freq = self.frequency
        cols = table.column_names

        if freq == 'q':
            return parse_quarterly_dates(table=table)
        
        if "year" in cols and "period" in cols:
            yr = table.column("year").cast(pa.string())
            month = pc.utf8_lpad(table.column("period").cast(pa.string()), width=2, padding="0")
            dates = pc.binary_join_element_wise(yr, month, "")
            table = table.drop(["year", "period"]).combine_chunks()
            return table.add_column(0, "date", dates)

        if "year" in cols:
            idx = cols.index("year")
            return table.set_column(idx, "date", table.column(idx).cast(pa.string()))

        return table


    def _read(self, data: bytes) -> pa.Table:
        try:
            read_opts = pv.ReadOptions(
                column_names=self.schema.names, 
                skip_rows=1,
                block_size=1024*1024*2,
            )
            conv_opts = pv.ConvertOptions(column_types=self.schema)

            reader = pv.open_csv(
                io.BytesIO(data),
                read_options=read_opts,
                convert_options=conv_opts,
            )
            table = pa.Table.from_batches(reader)

            table = table.cast(self.schema).select(self.schema.names)
            if self.classic and "R_EG" in table.column_names:
                table = table.drop(["R_EG"])

            table = self._format_date_column(table)
            table = offset_period_eom(table, self.frequency)

            table = scale_to_decimal(table)

            # Removing R_ prefixes 
            renames = {"R_F": "RF_Q", 
                       "R_MKT": "Mkt-RF",  #TODO: make all Mkt-RF "rm_rf" and make all headers lowercase.  
                       "R_ME": "ME",
                       "R_IA": "IA",
                       "R_ROE": "ROE",
                       "R_EG": "EG",
                       }
            table = table.rename_columns([renames.get(n, n) for n in table.column_names])

            return table.combine_chunks()

        except (pa.ArrowIOError, pa.ArrowInvalid) as e:
            msg = f"{self.__class__.__name__}: reading failed: {e}"
            self.log.error(msg)
            raise ValueError(msg) from e
