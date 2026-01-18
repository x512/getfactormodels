# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import pyarrow as pa
import pyarrow.compute as pc
from getfactormodels.models.base import CompositeModel, RegionMixin
from getfactormodels.utils.utils import read_from_fred


class ConditionalCAPM(CompositeModel, RegionMixin):
    """Conditional CAPM (CCAPM) of Jaganathan & Wang (1996).

    An implementations of the 'Premium-Labour', or 'Human Capital'
    CCAPM of Jaganathan and Wang (1996).

    - LBR: Smooth growth in per-capita labor income.
    - PREM: Lagged yield spread (Baa - Aaa) as a conditioning variable.

    References:
        R. Jagannathan and Z. Wang (1996). The conditional CAPM
        and the cross-section of expected returns. The Journal of
        Finance 51: 3–53.

    """
    @property 
    def _regions(self) -> list[str]: return ['usa']
    
    def __init__(self, frequency='m', **kwargs):
        super().__init__(**kwargs)
        self.frequency = frequency.lower()
        self.region = kwargs.get('region', 'usa')

    @property
    def _precision(self) -> int: return 8

    @property
    def _frequencies(self) -> list[str]: return ['m', 'q', 'y']

    @property
    def schema(self) -> pa.Schema:
        return pa.Schema([
            ('date', pa.date32()),
            ('LBR', pa.float64()),
            ('PREM', pa.float64()),
        ])

    def _construct(self, client) -> pa.Table:
        m_series = {
            "POPTHM": "pop",        # Total Population
            "BAA": "baa",
            "AAA": "aaa",
            "A576RC1": "income",    # Compensation of Employees (Monthly), (table 2.2A here)
        }

        # read_from_fred returns offset dates. ('m', not self.frequency)
        t = read_from_fred(m_series, frequency='m', client=client)

        table = self._jw_calc(t)

        table = self._resample(table)

        return table.combine_chunks()


    def _resample(self, table: pa.Table) -> pa.Table:
        """Downsample monthly to Q or Y by taking the last available month."""
        if self.frequency == 'm':
            return table
        months = pc.month(table.column("date"))

        if self.frequency == 'q':
            mask = pc.is_in(months, value_set=pa.array([3, 6, 9, 12]))
        elif self.frequency == 'y':
            mask = pc.equal(months, 12)
        else:
            return table

        return table.filter(mask).combine_chunks()


    def _jw_calc(self, table: pa.Table) -> pa.Table:
        """Calculate JW factors.

        - R_LBR(t): (L(t-1) + L(t-2)) / (L(t-2) + L(t-3)) - 1
        - PREM(t): Lagged spread (Baa - Aaa) at t-1
        """
        # Per Capita Labor Income (L)
        l_pc = pc.divide(table.column("income"), table.column("pop"))
        #l_pc = pc.multiply(..., 1000)
        
        # Credit spread/default risk (Baa - Aaa), pct to decimal here
        spread = pc.divide(pc.subtract(table.column("baa"), table.column("aaa")), 100.0)

        n_total = table.num_rows
        # 3 lags for LBR calculation (t-1, t-2, t-3). Result starts at index 3.
        n_final = n_total - 3
        if n_final <= 0:
            raise ValueError("Insufficient data to calculate lags.")

        # Calculate R_LBR:
        # slice for the formula
        l_tm1 = l_pc.slice(2, n_final)
        l_tm2 = l_pc.slice(1, n_final)
        l_tm3 = l_pc.slice(0, n_final)

        # calc: (l_tm1 + l_tm2) / (l_tm2 + l_tm3) - 1
        r_lbr = pc.subtract(pc.divide(pc.add(l_tm1, l_tm2), pc.add(l_tm2, l_tm3)), 1.0)

        # PREM is the spread at t-1. Return period t at index 3, t-1 is index 2
        prem_lagged = spread.slice(2, n_final)

        dates = table.column("date").slice(3, n_final)

        return pa.Table.from_arrays(
            [dates, r_lbr, prem_lagged],
            names=["date", "LBR", "PREM"],
        )

