# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import io
from typing import Literal
from datetime import datetime
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
from getfactormodels.models.base import FactorModel, PortfolioBase
from getfactormodels.utils.arrow_utils import scale_to_decimal
from getfactormodels.utils.date_utils import (
    offset_period_eom,
    parse_quarterly_dates,
)
import zipfile



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
        last_year = datetime.now().year - 1
        url += f'/q5_factors_{file}_{last_year}.csv'
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
            ("retx_vw", pa.float64()),
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
        #return f'{base_url}/benportf_{slug}_{f_str}_2024.csv'
        return f'{base_url}/benportf_{slug}_{f_str}_2025.csv'
    

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
            "N": "nstocks",
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
            # builds column name (me1_eg2)
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




### ----------------------------------------- WIP - ANOMALIES ------------


## Q ANOMALIES ... notes
# 1-way: https://global-q.org/uploads/1/2/2/6/122679606/mom_monthly_2024.zip
# mostly deciles, some quintiles
# 2-way: https://global-q.org/uploads/1/2/2/6/122679606/me_mom_monthly_2024.zip
# 3x5, me,anomaly


# Extension of q portfolios, or add anomalies? e.g., 
# 1. Abr1 ("abr_1"), cumulative abnormal returns around earnings announcement dates, 1-month holding period; 
# 

# getfactormodels -p me,abr1 -n 3x5
# getfactormodels -p abr1 -n 3x5 (= me,abr1) 
#getfactormodels -p abr1 -n 10 #,decile,quinitile,5
#getfactormodels -p q -n 18 -a abr1 (add an anomaly to 18 q portfolios? returns the decile or quintile)
# getfactormodels -p q -n 2x3 -a abr1,me (add 2-way sort to 6 q portfolios) 

# from getfactormodels import portfolio 
#p = portfolio(source='q', sort='2x3', frequency='m', anomaly='abr1').load()
# p2 = portfolio(source='ff', sort='quintile', on='me,bm', anomaly=['abr1', 'nsi])
#  p3 = portfolio(source='aqr', on='bab', frequency='m').load()
#or just 
#  p3 = portfolio('bab', frequency='m') 
#  #add anomalies 
#  p3.anomaly=['atoq6', '2ig', 'r11_6', 'r6_6', 'abr1', 'sue1']).load()

# to add Abr1 2 way sort, is a "portfolio" like in Fama-French size,beta size,ac, size,nsi etc." 
#
#
#class _QAnomalies(PortfolioBase):
#    
#    def _download_anom_folder(self):
#        """Downloads the correct folder of anomalies for the frequency.#
#
#        Example: abr_1, weekly downloads the 42 momentum anomalies .zip for weekly. #
#
 #       """
  #      ...

        #Download the folder, then check that the anomaly str + frequency is a file, 
        # or, map everything to its folder. Not very pythonic...



class _QAnomalies(PortfolioBase):
    """Download q-factor anomaly data from global-q.org."""

    Q_REGISTRY = {
        "momentum": {
            "abr_1", "abr_6", "abr_12", "cim_1", "cim_6", "cim_12", "cm_1", "cm_6", "cm_12",
            "def_1", "def_6", "def_12", "ile_1", "ilr_1", "ilr_6", "ilr_12", "im_1", "im_6",
            "im_12", "nei_1", "p52w_6", "p52w_12", "r6_1", "r6_6", "r6_12", "r11_1", "r11_6",
            "r11_12", "re_1", "re_6", "resid6_6", "resid6_12", "resid11_1", "resid11_6",
            "resid11_12", "rs_1", "sim_1", "sim_12", "sm_1", "sm_12", "sue_1", "sue_6", "tes_1"
        },
        "value-versus-growth": {
            "bm", "bmj", "bmq_12", "cp", "cpq_1", "cpq_6", "cpq_12", "dp", "dur", "ebp",
            "em", "emq_1", "emq_6", "emq_12", "ep", "epq_1", "epq_6", "epq_12", "ir", "nop",
            "ocp", "ocpq_1", "op", "rev_1", "rev_6", "rev_12", "sp", "spq_1", "spq_6",
            "spq_12", "vfp", "vhp"
        },
        "investment": {
            "aci", "cei", "dac", "dbe", "dcoa", "dfin", "dfnl", "dii", "dlno", "dlti",
            "dnca", "dnco", "dnoa", "dpia", "dwc", "ia", "iaq_1", "iaq_6", "iaq_12", "ig",
            "ig2", "ivc", "ivg", "ndf", "nxf", "noa", "nsi", "oa", "pda", "poa", "pta", "ta"
        },
        "profitability": {
            "ato", "atoq_1", "atoq_6", "atoq_12", "cla", "claq_1", "claq_6", "claq_12",
            "cop", "cto", "ctoq_1", "ctoq_6", "ctoq_12", "droa_1", "droa_6", "droe_1",
            "droe_6", "droe_12", "eg_1", "eg_6", "eg_12", "fp_6", "fq_1", "fq_6", "fq_12",
            "gla", "glaq_1", "glaq_6", "glaq_12", "gpa", "olaq_1", "olaq_6", "olaq_12",
            "oleq_1", "oleq_6", "oleq_12", "opa", "ope", "oq_1", "pmq_1", "rnaq_1",
            "rnaq_6", "rnaq_12", "roa_1", "roa_6", "roe_1", "roe_6", "sgq_1", "tbiq_6", "tbiq_12"
        },
        "intangibles": {
            "adm", "alaq_1", "almq_1", "almq_6", "almq_12", "dls_1", "eprd", "etl", "etr",
            "hs", "ioca", "oca", "ol", "olq_1", "olq_6", "olq_12", "r1a", "r1n", "r5a",
            "r5n", "r10a", "r10n", "r15a", "r20a", "rca", "rdm", "rdmq_1", "rdmq_6",
            "rdmq_12", "rdsq_6", "rdsq_12", "rer", "vcf_1"
        },
        "frictions": {
            "beta_1", "dtv_12", "isff_1", "isq_1", "ivff_1", "ivq_1",
            "me", "srev", "sv_1", "tv_1", "tail_12"
        }
    }

    _CATEGORY_SLUG = {
        "momentum": "mom", "value-versus-growth": "vvg",
        "investment": "inv", "profitability": "prof",
        "intangibles": "intan", "frictions": "fric"
    }

    _FREQ_STR = {
        'm': 'monthly', 'y': 'annual'
    }

    @property
    def _frequencies(self) -> list[str]:
        return ["m", "y"]

    @classmethod
    def _resolve_anomalies(cls, raw_inputs: list[str]) -> list[str]:
        all_canonical = {factor: cat for cat, factors in cls.Q_REGISTRY.items() for factor in factors}
        underscore_mapping = {factor.replace("_", ""): factor for factor in all_canonical}

        resolved = []
        for item in raw_inputs:
            item_clean = item.lower().strip()
            if item_clean in cls.Q_REGISTRY:
                resolved.extend(cls.Q_REGISTRY[item_clean])
            elif item_clean in underscore_mapping:
                resolved.append(underscore_mapping[item_clean])
            elif item_clean in all_canonical:
                resolved.append(item_clean)
            else:
                raise ValueError(f"Unknown anomaly or category: '{item}'")
        return list(dict.fromkeys(resolved))

    def __init__(self, anomaly: str | list[str], sort: str | int = 'decile', **kwargs):
        freq = kwargs.get('frequency', 'm').lower().strip()
        if freq not in ['m', 'y']:
            raise ValueError(f"Frequency '{freq}' is not supported for anomalies. Use 'm' or 'y'.")

        raw_inputs = [anomaly] if isinstance(anomaly, str) else anomaly
        self.anomalies = self._resolve_anomalies(raw_inputs)

        if sort in (10, '10', 'decile'):
            self.sort_type = 'decile'
        elif sort in ('2x3', '2x3x3'):
            self.sort_type = '2x3'
        elif sort in ('3x5',):
            self.sort_type = '3x5'
        else:
            raise ValueError(f"sort must be 'decile', '2x3', or '3x5'; got {sort}")

        kwargs['weights'] = 'vw'
        kwargs['dividends'] = True
        kwargs['frequency'] = freq
        super().__init__(**kwargs)


    @classmethod
    def _get_category(cls, anomaly: str) -> str | None:
        for cat, factors in cls.Q_REGISTRY.items():
            if anomaly in factors:
                return cat
        return None

    def _get_category_url(self, category: str) -> str:
        base = 'https://global-q.org/uploads/1/2/2/6/122679606'
        freq = self._FREQ_STR[self.frequency]
        cat_slug = self._CATEGORY_SLUG[category]
        prefix = "me_" if self.sort_type in ('2x3', '3x5') else ""
        last_year = datetime.now().year - 1
        return f"{base}/{prefix}{cat_slug}_{freq}_{last_year}.zip"

    def _get_url(self) -> str:
        # URL only gets first category
        # FIXME: TODO:
        first_cat = self._get_category(self.anomalies[0])
        return self._get_category_url(first_cat)

    @property
    def schema(self) -> pa.Schema:
        fields = [("date", pa.date32())]
        if self.frequency == 'm':
            fields.append(("nstocks", pa.int32()))

        if self.sort_type == 'decile':
            fields.append(("rank_decile", pa.int16()))
        else:
            fields += [("rank_me", pa.int16()), ("rank_anomaly", pa.int16())]

        fields += [
            ("ret_vw", pa.float64()),
            ("retx_vw", pa.float64()),
        ]
        return pa.schema(fields)
    
    # adapted from .portfolio
    def _pivot_portfolios(self, table: pa.Table, value_col: str, current_anomaly: str) -> pa.Table:
        rank_cols = [c for c in table.column_names if c.startswith("rank_")]
        dates = table.select(["date"]).group_by(["date"]).aggregate([]).sort_by([("date", "ascending")])

        sort_keys = [(col, "ascending") for col in rank_cols]
        uniq_ports = table.group_by(rank_cols).aggregate([]).sort_by(sort_keys)

        columns = [dates.column("date")]
        names = ["date"]

        for port in uniq_ports.to_pylist():
            name_parts = []
            for c in rank_cols:
                clean_col = c.replace('rank_', '')
                if clean_col == 'anomaly':
                    clean_col = current_anomaly
                name_parts.append(f"{clean_col}_{port[c]}")

            name = "_".join(name_parts).lower()

            mask = None
            for col in rank_cols:
                m = pc.equal(table[col], port[col])
                mask = m if mask is None else pc.and_(mask, m)

            subset = table.filter(mask).select(["date", value_col])
            aligned = dates.join(subset, keys=["date"], join_type="left outer")
            columns.append(aligned.column(value_col))
            names.append(name)

        return pa.table(columns, names=names)


    # adapted from jupyter notebook, don't use yet!
    def _read(self, data: bytes) -> pa.Table:
        from getfactormodels.utils.http_client import _HttpClient

        try: #group anoms by cat
            cat_to_anoms = {}
            for a in self.anomalies:
                cat = self._get_category(a)
                cat_to_anoms.setdefault(cat, []).append(a)

            freq_str = self._FREQ_STR[self.frequency]
            prefix_str = "me_" if self.sort_type in ('2x3', '3x5') else ""
            val_col = 'ret_vw' if self.dividends else 'retx_vw'

            pivoted_tables = []
            first_cat = self._get_category(self.anomalies[0])

            # seeds cache with initial bytes downloaded by the base class
            zip_bytes_cache = {first_cat: data}

            with _HttpClient() as client:
                for cat, anoms in cat_to_anoms.items():
                    if cat not in zip_bytes_cache:
                        cat_url = self._get_category_url(cat)
                        zip_bytes_cache[cat] = client.download(
                            cat_url, 
                            tag="AnomalyData", 
                            model_name=f"Q-{cat.capitalize()}"
                        )

                    zip_bytes = zip_bytes_cache[cat]

                    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
                        namelist = z.namelist()

                        for a in anoms:
                            target_file = next(
                                (n for n in namelist 
                                    if f"_{a}_" in n.lower() 
                                    and freq_str in n.lower()
                                    and (not prefix_str or n.lower().startswith(prefix_str))),
                                None
                            )
                            if not target_file:
                                raise ValueError(f"Could not find file for: '{a}'.")

                            with z.open(target_file) as f:
                                csv_bytes = f.read()

                            read_opts = pv.ReadOptions(skip_rows=0)
                            table = pv.read_csv(io.BytesIO(csv_bytes), read_options=read_opts)

                            rename_map = {
                                "DATE": "date",
                                "Year": "year",
                                "Month": "month",
                                "Quarter": "period",
                                "Rank Decile": "rank_decile",
                                "Rank ME": "rank_me",
                                "Ret": "ret_vw",
                                "Retx": "retx_vw",
                                "N": "nstocks",
                            }

                            table = table.rename_columns([rename_map.get(c, c) for c in table.column_names])

                            if self.sort_type != 'decile':
                                cols = table.column_names
                                for i, col in enumerate(cols):
                                    if col.lower().startswith("rank ") and col != "rank_me":
                                        cols[i] = "rank_anomaly"
                                table = table.rename_columns(cols)

                            if "year" in table.column_names:
                                y_str = pc.cast(table['year'], pa.string())
                                if "month" in table.column_names:
                                    m_str = pc.utf8_lpad(pc.cast(table['month'], pa.string()), width=2, padding="0")
                                    date_str = pc.binary_join_element_wise(y_str, m_str, "")
                                    table = table.add_column(0, "date", date_str)
                                    table = table.drop(["year", "month"])
                                else:
                                    date_str = pc.binary_join_element_wise(y_str, pa.scalar("12"), "")
                                    table = table.add_column(0, "date", date_str)
                                    table = table.drop(["year"])

                            table = offset_period_eom(table, self.frequency)
                            table = scale_to_decimal(table)

                            pivoted = self._pivot_portfolios(table, val_col, current_anomaly=a)
                            pivoted_tables.append(pivoted)

            # join all requested anomalies on 'date'
            final_table = pivoted_tables[0]
            for t in pivoted_tables[1:]:
                final_table = final_table.join(t, keys=["date"], join_type="left outer")

            return final_table

        except ValueError as e:
            raise ValueError(str(e))
        except (pa.ArrowIOError, pa.ArrowInvalid) as e:
            raise ValueError(f"Reading failed: {e}")
    @property
    def _precision(self) -> int: 
        return 5
#def anomaly(
#    name: str | list[str] = 'abr_1',
#    frequency: str = 'm',
#    sort: str | int = 'decile',
#    output_dir: str | None = None,
#    filename: str | None = None,
#    file_format: str = 'feather',
#    *,
#    source: str = 'q',
#    **kwargs,
#):
#    """Unified entry point for downloading anomaly datasets."""
#    src = source.lower().strip()
#    if src in ["q", "global-q"]:
#        return _QAnomalies(
#            name=name,
#            frequency=frequency,
#            sort=sort,
#            output_dir=output_dir,
#            filename=filename,
#            file_format=file_format,
#            **kwargs,
#        )
#    raise NotImplementedError(f"Anomalies not yet supported for source: {source}")

def _get_q_anomalies(name: str | list[str], **kwargs):
    return _QAnomalies(anomaly=name, **kwargs) 
