# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import pyarrow as pa
import pyarrow.compute as pc
from getfactormodels.models.base import CompositeModel
from getfactormodels.utils.arrow_utils import scale_to_decimal
from getfactormodels.utils.date_utils import offset_period_eom
from getfactormodels.utils.utils import read_from_fred


# might rename PremiumLaboutCCAPM
class ConditionalCAPM(CompositeModel):
    """Conditional CAPM (CCAPM) of Jaganathan & Wang (1996).

    An implementations of the 'Premium-Labour', or 'Human Capital' 
    conditional CAPM of Jaganathan and Wang (1996). Data from FRED.

    Not tested. Don't trust it... maybe.

    - LBR: Smooth growth in per-capita labor income.
    - PREM: Lagged yield spread (Baa - Aaa) as a conditioning variable.

    References:
        R. Jagannathan and Z. Wang (1996). The conditional CAPM
        and the cross-section of expected returns. The Journal of 
        Finance 51: 3–53.

    """

    @property
    def _frequencies(self) -> list[str]:
        return ['y']

    @property
    def schema(self) -> pa.Schema:
        return pa.schema([
            ('date', pa.int32()),
            ('LBR', pa.float64()),
            ('PREM', pa.float64()),
            ('Mkt-RF', pa.float64()),
        ])

    def _construct(self, client) -> pa.Table:
        data_series = {
            "A033RC1A027NBEA": "income",
            "POPTHM": "pop",
            "BAA": "baa",
            "AAA": "aaa",
        }

        fred_table = read_from_fred(series=data_series, 
                                    frequency=self.frequency, 
                                    aggregate='eop', 
                                    client=client)

        data_table = offset_period_eom(fred_table, self.frequency) #important: EOM offset before calc

        jw_table = self._jw_calc(data_table)

        # PREM to decimal
        prem_scaled = scale_to_decimal(jw_table.select(['PREM']))
        table = jw_table.drop(['PREM']).append_column('PREM', prem_scaled.column(0))

        table.validate()
        return table.combine_chunks()

        #ff = FamaFrenchFactors(model='3', frequency=self.frequency).load(client=client)
        #mkt = select_table_columns(ff.data, ['Mkt-RF', 'RF'])
        #return jw_table.join(mkt, keys="date", join_type="inner").combine_chunks()


    def _jw_calc(self, table: pa.Table) -> pa.Table:
        """J&W (1996) factor construction using FRED data.

        - LBR: Smooth growth in per-capita labor income.
        - PREM: Lagged yield spread (Baa - Aaa) as a conditioning variable.

        """ 
        # Per-capita labor income and Yield Spread
        l_pc = pc.divide(table.column("income"), table.column("pop"))
        spread = pc.subtract(table.column("baa"), table.column("aaa"))

        n = table.num_rows
        out_len = n - 2

        # LBR: Growth at t (using t and t-1)
        l_t = l_pc.slice(1, out_len)
        l_t_minus_1 = l_pc.slice(0, out_len)
        r_labor = pc.subtract(pc.divide(l_t, l_t_minus_1), 1.0)

        # PREM: Spread at t-1
        prem_lagged = spread.slice(0, out_len)

        # dates aligned to period t
        dates = table.column("date").slice(2, out_len)

        return pa.Table.from_arrays(
            [dates, r_labor, prem_lagged],
            names=["date", "LBR", "PREM"],
        )
