from io import BytesIO
import pyarrow as pa
import pyarrow.compute as pc
from python_calamine import CalamineWorkbook
from getfactormodels.models.base import FactorModel
from getfactormodels.models.fama_french import FamaFrenchFactors
from getfactormodels.utils.arrow_utils import select_table_columns
from getfactormodels.utils.date_utils import offset_period_eom


class HCAPM(FactorModel):
    """Human Capital CAPM (HCAPM), Campbell & Korniotis (2007).

    An implementation of the Human Capital CCAPM (HCAPM), "using the income 
    growth of high income households, rather than aggregate income growth."

    This model replicates the methodlogy of Campbell & Korniotis (2007), 
    using the same dataset from E. Saez website (updated to 2022).

    Only available as annual.

    Factors:
        HCRT1, HCRT5, HCRT10

    References:
        [1] Campbell, S.D. & Korniotis, G.M. 2008. The human capital 
        that matters: expected returns and the income of affluent
        households. Federal Reserve Board, Finance and Economics 
        Discussion Series. 
        [2] E. Saez & T. Piketty, "Income Inequality in the United States, 
        1913-1998" Quarterly Journal of Economics, 118(1), 2003, 1-39.

    """
    @property
    def _frequencies(self) -> list[str]:
        return ['y'] 

    @property
    def schema(self) -> pa.Schema:
        return pa.schema([
            ('date', pa.int32()),
            ('p90', pa.float64()),
            ('p95', pa.float64()),
            ('p99', pa.float64()),
        ])

    def _get_url(self):
        return "https://eml.berkeley.edu/~saez/TabFig2022.xlsx"

    def _read(self, data: bytes) -> pa.Table:
        workbook = CalamineWorkbook.from_filelike(BytesIO(data))
        rows = workbook.get_sheet_by_index(10).to_python()

        clean_rows = [
            r for r in rows[5:] 
            if len(r) >= 4 and all(isinstance(r[i], (int, float)) for i in range(4))
        ]

        raw_table = pa.Table.from_pydict({
            "date": [int(r[0]) for r in clean_rows],
            "p90": [float(r[1]) for r in clean_rows],
            "p95": [float(r[2]) for r in clean_rows],
            "p99": [float(r[3]) for r in clean_rows],
        })

        hcapm_table = self._calc_log_growth(raw_table)
        hcapm_table = offset_period_eom(hcapm_table, self.frequency)

        # Join
        _ff = FamaFrenchFactors(model='3', frequency=self.frequency).load()
        mkt_rf = select_table_columns(_ff.data, ['Mkt-RF', 'RF'])

        return hcapm_table.join(mkt_rf, keys="date", join_type="inner").combine_chunks()

    
    def _calc_log_growth(self, table: pa.Table) -> pa.Table:
        n = table.num_rows
        mapping = {"HCRT10": "p90", "HCRT5": "p95", "HCRT1": "p99"}

        factor_cols = []
        for src_col in mapping.values():
            log_income = pc.ln(table.column(src_col))
            # growth: ln(y_t) - ln(y_{t-1})
            growth = pc.subtract(log_income.slice(1, n-1), log_income.slice(0, n-1))
            factor_cols.append(growth)

        years = table.column("date").slice(1, n-1)

        return pa.Table.from_arrays(
            [years] + factor_cols, 
            names=["date", "HCRT10", "HCRT5", "HCRT1"],
        )

