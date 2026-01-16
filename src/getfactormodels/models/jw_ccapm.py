# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
from logging import raiseExceptions
import pyarrow as pa
import pyarrow.compute as pc
from getfactormodels.models.base import CompositeModel, RegionMixin
from getfactormodels.utils.date_utils import offset_period_eom
from getfactormodels.utils.utils import read_from_fred



# might rename PremiumLabourCCAPM
#class PremiumLabourCAPM(CompositeModel, RegionMixin):
class ConditionalCAPM(CompositeModel, RegionMixin):
    """Conditional CAPM (CCAPM) of Jaganathan & Wang (1996).

    An implementations of the 'Premium-Labour', or 'Human Capital' 
    CCAPM of Jaganathan and Wang (1996). 

    Data from FRED. Monthly, except income, 

    - LBR: Smooth growth in per-capita labor income.
    - PREM: Lagged yield spread (Baa - Aaa) as a conditioning variable.

    References:
        R. Jagannathan and Z. Wang (1996). The conditional CAPM
        and the cross-section of expected returns. The Journal of 
        Finance 51: 3–53.

    """
    def __init__(self, **kwargs):
        region = kwargs.pop('region', None)
        super().__init__(**kwargs)
        self.region = region

    @property
    def _frequencies(self) -> list[str]:
        return ['m', 'q', 'y']

    @property 
    def _precision(self) -> int:
        return 8

    @property
    def _regions(self) -> list[str]:
        return ['usa']

    @property 
    def schema(self) -> pa.Schema:
        return pa.Schema([
            ('date', pa.date32()),
            ('LBR', pa.float64()),
            ('PREM', pa.float64()),
        ])


    def _construct(self, client) -> pa.Table:
        m_series = {
            "POPTHM": "pop",
            "BAA": "baa",
            "AAA": "aaa",
        }
        
        m_table = read_from_fred(series=m_series, frequency='m', client=client)
        m_table = offset_period_eom(m_table, 'm')
        
        # Income data from FRED (COE) is quarterly.
        q_id = "COE"

        q_table = read_from_fred(series={q_id: "income"}, frequency='q', client=client)
        q_table = offset_period_eom(q_table, frequency='q').combine_chunks()

        # helper: quarterly COE to monthly 
        inc_table = self._q_wages_to_m(q_table)

        data_table = m_table.join(inc_table, keys="date", join_type="inner")
        data_table = data_table.sort_by("date").combine_chunks()

        jw_table = self._jw_calc(data_table)

        # scale PREM, not importing scale_to_decimal just for this
        prem_dec = pc.divide(jw_table.column("PREM"), 100.0)

        table = jw_table.set_column(2, "PREM", prem_dec).combine_chunks()
        return self._downsample(table)
    
        #ff = FamaFrenchFactors(model='3', frequency=self.frequency).load(client=client)
        #mkt = select_table_columns(ff.data, ['Mkt-RF', 'RF'])
        #return jw_table.join(mkt, keys="date", join_type="inner").combine_chunks()


    def _jw_calc(self, table: pa.Table) -> pa.Table:
        """J&W (1996) factor construction using FRED data.

        - LBR: Smooth growth in per-capita labor income.
          LBR = R_LBR = (L_t-1 + L_t-2) / (L_t-2 + L_t-3) - 1

        - PREM: Lagged yield spread (Baa - Aaa) as a conditioning variable.

        """ 
        # Per Capita Labor Income (L)
        l_pc = pc.divide(table.column("income"), table.column("pop"))
        spread = pc.subtract(table.column("baa"), table.column("aaa"))
        
        n_total = table.num_rows
        n_final = n_total - 3 # lag calc, we lose 3 rows
        
        if n_final <= 0:
            raise ValueError("Insufficient data rows for ConditionalCAPM lags.")

        # t is index 3...t-3 is index 0.
        l_tm1 = l_pc.slice(2, n_final)
        l_tm2 = l_pc.slice(1, n_final)
        l_tm3 = l_pc.slice(0, n_final)
        
        # JW's R_LBR: (L_t-1 + L_t-2) / (L_t-2 + L_t-3) - 1
        num = pc.add(l_tm1, l_tm2)
        den = pc.add(l_tm2, l_tm3)

        r_labor = pc.subtract(pc.divide(num, den), 1.0)
        
        # PREM (t-1 spread)
        prem_lagged = spread.slice(2, n_final)

        dates = table.column("date").slice(3, n_final)

        return pa.Table.from_arrays(
            [dates, r_labor, prem_lagged],
            names=["date", "LBR", "PREM"]
        ).combine_chunks()


    def _downsample(self, table: pa.Table) -> pa.Table:
        """Efficiently downsample monthly factors to Q or Y by filtering for EOP months."""
        if self.frequency == 'm':
            return table
        
        months = pc.month(table.column("date")) # extract the month only!
        
        if self.frequency == 'q':
            mask = pc.is_in(months, value_set=pa.array([3, 6, 9, 12]))
            table = table.filter(mask)
        
        elif self.frequency == 'y':
            mask = pc.equal(months, 12)
            table = table.filter(mask)
            
        return table.combine_chunks()


    def _q_wages_to_m(self, table: pa.Table) -> pa.Table:
        """Helper: Quarterly COE to Monthly using geometric interpolation."""
        income_col = table.column("income")
        q_dates = table.column("date")

        q_vals = income_col.to_pylist()
        monthly_levels = []
        for i in range(1, len(q_vals)):
            m_growth = (q_vals[i] / q_vals[i-1]) ** (1/3)
            m1 = q_vals[i-1] / 3.0
            monthly_levels.extend([m1, m1 * m_growth, m1 * m_growth**2])

        base_dates = q_dates.slice(0, table.num_rows - 1)
        
        # Move to the first day of each of the three months in the quarter
        d1 = pc.ceil_temporal(base_dates, 1, unit='month')
        d2 = pc.ceil_temporal(pc.add(d1, pa.scalar(1, pa.duration('ms'))), 1, unit='month')
        d3 = pc.ceil_temporal(pc.add(d2, pa.scalar(1, pa.duration('ms'))), 1, unit='month')
       
        # important: cast to date32 here
        all_dates = pa.concat_arrays([
            d1.combine_chunks().cast(pa.date32()), 
            d2.combine_chunks().cast(pa.date32()), 
            d3.combine_chunks().cast(pa.date32())
        ])
        
        all_levels = pa.array(monthly_levels, type=pa.float64())

        m_table = pa.Table.from_arrays(
            [all_dates, all_levels], 
            names=["date", "income"]
        ).sort_by("date")

        return offset_period_eom(m_table, frequency='m')



    # ignore this ------------------------------------------------------------------------
    def _abs_to_table(self) -> pa.Table:
        ...

    def _download_aus_erp(self) -> pa.Table:
        """Downloads population data from ABS.

        Downloads the "Population and components of change - national"
        dataset from the ABS. Quarterly data.

        Data:
        Data Item:
        """

    def _download_aus_wages(self) -> pa.Table:
        """Downloads wage date from ABS.

        Quarterly data.

        Data: Australian National Accounts – Income from Gross Domestic Product
        Data Item: Compensation of employees - Wages and salaries
        """

