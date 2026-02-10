# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import io
from typing import Literal
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
from getfactormodels.models.base import FactorModel, PortfolioBase
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


class _QPortfolios(PortfolioBase):
    """Download q-factor portfolio data from global-q.org.

    Work in progress.

    q-global offers two portfolio sorts for each frequency:
    - 18 portfolios (2x3x3) on size (ME), investment-to-assets (IA) 
      and return on equity (ROE).
    - 6 portfolios (2x3) on size and Expected Growth (EG).

    Source: 
    - https://global-q.org/factors.html

    Notes:
    - Ranks are in ascending order. For size (ME): "1" is small, "2" big. 
      For IA, ROE, EG: "1" means low, "2" median, "3" high.
    """
    @property
    def _frequencies(self) -> list[str]: 
        return ["d", "w", "w2w", "m", "q", "y"]

    def __init__(self, 
                 sort: Literal['2x3', '2x3x3'] = '2x3x3',
                 weights: Literal['vw'] = 'vw',
                 *,
                 dividends: bool = True,  # default, total rets
                 **kwargs) -> None:
        # only value avail
        if weights.lower() != 'vw':
            raise ValueError(f"{self.__class__.__name__} only supports value-weighted ('vw') portfolios.") 
        kwargs['weights'] = 'vw'
        kwargs['dividends'] = dividends
        self.sort_type = sort # sort to 'sort_type' internally
        super().__init__(**kwargs)
    
    @property
    def schema(self) -> pa.Schema:
        """Schema of q-global portfolios (after processing but before pivoting)."""
        fields = [("date", pa.date32())] #since it's after offset, 'Year'/'Month', 'DATE' = 'date'.

        if self.frequency in ['d', 'm']:
            fields.append(("nstocks", pa.int32()))

        fields.append(("rank_ME", pa.int16()))

        if self.sort_type == '2x3x3':
            fields += [("rank_IA", pa.int16()), ("rank_ROE", pa.int16())]
        else:  # '2x3'
            fields += [("rank_EG", pa.int16())]

        fields += [
            ("ret_vw", pa.float64()), 
            ("retx_vw", pa.float64())
        ]
        return pa.schema(fields)


    def _get_url(self) -> str:
        base_url = 'https://global-q.org/uploads/1/2/2/6/122679606'
        freq_map = {
            'd': 'daily', 'w': 'weekly', 'w2w': 'weekly_w2w', 
            'm': 'monthly', 'y': 'annual', 'q': 'quarterly',
        }
        f_str = freq_map.get(self.frequency, 'monthly')

        slug = 'me_ia_roe' if self.sort_type == '2x3x3' else 'me_eg'
        return f'{base_url}/benportf_{slug}_{f_str}_2024.csv'
    

    def _read(self, data: bytes) -> pa.Table:
        read_opts = pv.ReadOptions(skip_rows=0)
        table = pv.read_csv(io.BytesIO(data), read_options=read_opts)

        # works for now.
        rename_map = {
            "DATE": "date",
            "Year": "year",
            "Month": "month",
            "Rank ME": "rank_ME",
            "Rank IA": "rank_IA",
            "Rank ROE": "rank_ROE",
            "Rank EG": "rank_EG",
            "Ret": "ret_vw",
            "Retx": "retx_vw",
            "N": "nstocks"
        }
        table = table.rename_columns([rename_map.get(c, c) for c in table.column_names])

        if self.frequency == 'q':  # TODO: parse quarterly dates should handle and combination of yyyy/year and period/month/quarter for freq = q.
            table = table.rename_columns([
                'period' if c == 'quarter' else c 
                for c in table.column_names
            ])
            
            table = parse_quarterly_dates(table)
    
        elif "year" in table.column_names:
            y_str = pc.cast(table['year'], pa.string())
            if "month" in table.column_names:
                m_str = pc.utf8_lpad(pc.cast(table['month'], pa.string()), width=2, padding="0")
                date_val = pc.binary_join_element_wise(y_str, m_str, "")
            else:
                date_val = pc.binary_join_element_wise(y_str, pa.scalar("12"), "")
            
            table = table.add_column(0, "date", date_val)
            table = table.drop(["year", "month"] if "month" in table.column_names else ["year"])

        table = offset_period_eom(table, self.frequency)
        # enforcing schema here. entire schema thing needs rework.
        table = table.select(self.schema.names).cast(self.schema)

        table = scale_to_decimal(table)

        # https://global-q.org/factors.html:
        #
        #   "ret_vw denotes value-weighted total (cum-dividend) returns, 
        #   and retx_vw denotes value-weighted ex-dividend returns (capital 
        #   gains)" 

        val_col = 'ret_vw' if self.dividends else 'retx_vw' 

        return self._pivot_portfolios(table, val_col)


    # this is for both portfolios AND soon anomalies. 
    # TODO: utils filter by date, quarterly offset and period offset need to be redone.
    def _pivot_portfolios(self, table: pa.Table, value_col: str) -> pa.Table:
        rank_cols = [c for c in table.column_names if c.startswith("rank_")]
        # unique dates for an index
        dates = table.select(["date"]).group_by(["date"]).aggregate([]).sort_by([("date", "ascending")])

        sort_keys = [(col, "ascending") for col in rank_cols]
        uniq_ports = table.group_by(rank_cols).aggregate([]).sort_by(sort_keys)

        columns = [dates.column("date")]
        names = ["date"]

        for port in uniq_ports.to_pylist():
            # builds column name (ME1_IA2)
            name = "_".join([f"{c.replace('rank_', '')}{port[c]}" for c in rank_cols]).lower()

            mask = None
            for col in rank_cols:
                m = pc.equal(table[col], port[col])
                mask = m if mask is None else pc.and_(mask, m)

            # filter and align
            subset = table.filter(mask).select(["date", value_col])
            # left outer for dates that exist
            aligned = dates.join(subset, keys=["date"], join_type="left outer")
            columns.append(aligned.column(value_col))
            names.append(name)

        return pa.table(columns, names=names)


# Keeping private until ff/q/aqr portfolios have some kind of unified ux
# and redone params. works.
def _get_q_portfolios(formed_on=None, sort=None, **kwargs): #q_portfolios when public?
    # conv input to str (avoids NoneType err)
    s_str = str(sort).lower().strip() if sort else ""
    
    if any(x in s_str for x in ["18", "2x3x3"]): #noqa
        final_sort = '2x3x3'
    else:
        final_sort = '2x3'
        
    return _QPortfolios(sort=final_sort, formed_on=formed_on, **kwargs)
