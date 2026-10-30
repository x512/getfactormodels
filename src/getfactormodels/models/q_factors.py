# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
"""Global-Q factor models, portfolios, and anomaly datasets.

Provides data retrieval, parsing, and processing for q-factor models (Q4/Q5), 
benchmark portfolios, and anomaly datasets from global-q.org (Hou, Xue, and Zhang).
"""
import io
import zipfile
from datetime import datetime, timezone
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

_GLOBAL_Q_RENAME_MAP = {
    "DATE": "date",
    "Year": "year",
    "Month": "month",
    "Quarter": "period",
    "Rank ME": "rank_ME",
    "Rank IA": "rank_IA",
    "Rank ROE": "rank_ROE",
    "Rank EG": "rank_EG",
    "Rank Decile": "rank_decile",
    "Ret": "ret_vw",
    "Retx": "retx_vw",
    "N": "nstocks",
    "R_F": "RF_Q",
}

_GLOBAL_Q_FREQ_MAP = {
    'd': 'daily',
    'w': 'weekly',
    'w2w': 'weekly_w2w',
    'm': 'monthly',
    'q': 'quarterly',
    'y': 'annual',
}


def _format_date_column(table: pa.Table, frequency: str) -> pa.Table:
    """Helper: Format date columns for global-q datasets."""
    cols = table.column_names

    if frequency == 'q' or "quarter" in cols:
        if "quarter" in cols and "period" not in cols:
            table = table.rename_columns(['period' if c == 'quarter' else c for c in table.column_names])
        return parse_quarterly_dates(table=table)

    if frequency == 'y' and "year" in cols:
        idx = cols.index("year")
        return table.set_column(idx, "date", table.column(idx).cast(pa.string()))

    time_col = "period" if "period" in cols else ("month" if "month" in cols else None)

    if "year" in cols and time_col:
        yr = table.column("year").cast(pa.string())
        val = pc.utf8_lpad(table.column(time_col).cast(pa.string()), width=2, padding="0")
        dates = pc.binary_join_element_wise(yr, val, "")
        table = table.drop(["year", time_col]).combine_chunks()
        return table.add_column(0, "date", dates)

    return table

def _pivot_global_q_table(
    table: pa.Table, 
    value_col: str, 
    anomaly_name: str | None = None,
) -> pa.Table:
    """Helper: Pivot Global Q's portfolio sorts and anomalies."""
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
            if clean_col == 'anomaly' and anomaly_name:
                clean_col = anomaly_name
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


class QFactors(FactorModel): 
    """Download and process q-factor data from global-q.org.

    Attributes:
        classic (bool): Whether to return the original 4-factor model.

    References:
        - Hou, Kewei, Haitao Mo, Chen Xue, and Lu Zhang, 2021, An augmented 
        q-factor model with expected growth, Review of Finance 25 (1), 
        1-41.
        - Hou, Kewei, Chen Xue, and Lu Zhang, 2015, Digesting anomalies: An 
        investment approach, Review of Financial Studies 28 (3), 650-705.
    """
    def __init__(self, *, classic: bool = False, **kwargs) -> None:
        """Initialize the QFactors model.

        Args:
            classic (bool, optional): Returns the original 4-factor model 
                instead of the augmented 5-factor model. Default: `False`.
        """
        self.classic = classic
        super().__init__(**kwargs)

    @property
    def frequencies(self) -> list[str]:
        return ["d", "w", "w2w", "m", "q", "y"]

    @property
    def _precision(self) -> int: return 6

    @property
    def schema(self) -> pa.Schema:
        """Schema for q-factor models."""
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
    
    @property
    def url(self) -> str:
        freq = self.frequency or 'm'
        file_str = _GLOBAL_Q_FREQ_MAP.get(freq, "monthly")
        base_url = 'https://global-q.org/uploads/1/2/2/6/122679606'
        last_year = datetime.now(tz=timezone.utc).year - 1
        return f'{base_url}/q5_factors_{file_str}_{last_year}.csv'

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

            table = _format_date_column(table, self.frequency)
            table = offset_period_eom(table, self.frequency)
            table = scale_to_decimal(table)

            # Rename R_F to RF_Q and drop R_ prefixes
            table = table.rename_columns([_GLOBAL_Q_RENAME_MAP.get(c, c) for c in table.column_names])
            table = table.rename_columns([c.removeprefix("R_") for c in table.column_names])
            return table.combine_chunks()

        except (pa.ArrowIOError, pa.ArrowInvalid) as e:
            msg = f"{self.__class__.__name__}: reading failed: {e}"
            self.log.error(msg)
            raise ValueError(msg) from e


class _QPortfolios(PortfolioBase):
    """Download q-factor portfolio data from global-q.org.

    Work in progress. 

    q-global offers two portfolio sorts for each frequency:
    - "18 (2 by 3 by 3) Size, Investment-to-assets, and Return on Equity 
      Benchmark Portfolios Underlying the q-factors." 
    - "6 (2 by 3) Size and Expected Growth Benchmark Portfolios Underlying 
      the Expected Growth Factor."

    Notes:
    - Ranks are in ascending order. For size (ME): "1" is small, "2" big. 
      For IA, ROE, EG: "1" means low, "2" median, "3" high.
    - Source: https://global-q.org/factors.html
    """
    @property
    def _frequencies(self) -> list[str]:
        return list(_GLOBAL_Q_FREQ_MAP.keys())

    def __init__(self, sort: Literal['2x3', '2x3x3'] = '2x3x3', 
                 weights: Literal['vw'] = 'vw', 
                 *, 
                 dividends: bool = True, # Default, total rets
                 **kwargs) -> None:
        if weights.lower() != 'vw':
            raise ValueError(f"{self.__class__.__name__} only supports value-weighted ('vw') portfolios.") 
        kwargs['weights'] = 'vw'
        kwargs['dividends'] = dividends
        self.sort_type = sort
        super().__init__(**kwargs)

    
    @property
    def schema(self) -> pa.Schema:
        """Schema of q-global portfolios (after processing but before pivoting)."""
        fields: list[tuple[str, pa.DataType]] = [("date", pa.date32())]
        #since it's after offset, 'Year'/'Month', 'DATE' = 'date'.
        # type fields to avoid type narrowing issues

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

    @property
    def url(self) -> str:
        base_url = 'https://global-q.org/uploads/1/2/2/6/122679606'
        freq_key = self.frequency if self.frequency is not None else 'm'
        f_str = _GLOBAL_Q_FREQ_MAP.get(freq_key, 'monthly')
        slug = 'me_ia_roe' if self.sort_type == '2x3x3' else 'me_eg'
        return f'{base_url}/benportf_{slug}_{f_str}_2025.csv'
    

    def _read(self, data: bytes) -> pa.Table:
        table = pv.read_csv(io.BytesIO(data))
        table = table.rename_columns([_GLOBAL_Q_RENAME_MAP.get(c, c) for c in table.column_names])
        freq = self.frequency if self.frequency is not None else 'm'
        table = _format_date_column(table, freq)
        table = offset_period_eom(table, freq)
        table = table.select(self.schema.names).cast(self.schema)
        return _pivot_global_q_table(scale_to_decimal(table), 'ret_vw' if self.dividends else 'retx_vw')

    # https://global-q.org/factors.html:
    #
    # "For a given anomaly, we offer value-weighted returns in percent for 1-way 
    # sorted portfolios (mostly deciles, with a few quantiles) and 2-way (3 by 5) 
    # independently sorted portfolios by interacting the anomaly with size."


# Keeping private until ff/q/aqr portfolios have some kind of unified ux
# and redone params. works.
def _get_q_portfolios(
    sort: str | list[str] | None = None, grid: str | None = None, 
    count: int | None = None, weights: str | None = 'vw', **kwargs):
    """Internal helper: factory for q-factor portfolios."""
    if count == 18 or (grid and ('18' in grid or '2x3x3' in grid)):
        final_sort = '2x3x3'
    elif count == 6 or (grid and ('6' in grid or '2x3' in grid)):
        final_sort = '2x3'
    else:
        # Default based on factors or fallback
        s_str = str(sort).lower() if sort else ""
        final_sort = '2x3x3' if 'ia' in s_str or 'roe' in s_str else '2x3'

    return _QPortfolios(sort=final_sort, **kwargs)


# -------------------- WIP - ANOMALIES ------------
class _QAnomalies(PortfolioBase):
    """Download q-factor anomaly portfolio return data from global-q.org.

    Args:
        anomaly: Anomaly identifiers or category names ('abr_1', 
                 'momentum', ['bm', 'roe_1']).
        sort: Portfolio sorts (decile, or 3x5).
        frequency: Data frequency ('d', 'w', 'w2w', 'm', 'y').
        dividends: True for total returns (cum-div, ret_vw), False for capital gains (ex-div, retx_vw).
        **kwargs: Additional parameters passed to `PortfolioBase`.

    Notes:
        - Ranks are in ascending order
        - Multiple anomalies requested simultaneously are automatically joined 
          side-by-side by date.
        - 1-way sort vary: most are deciles, some are quintiles.

    References:
        - Hou, Kewei, Chen Xue, and Lu Zhang, 2020, Replicating anomalies, 
          Review of Financial Studies 33 (5), 2019-2133.
    """
    Q_REGISTRY = {
        "momentum": {
            "abr_1", "abr_6", "abr_12", "cim_1", "cim_6", "cim_12", "cm_1", "cm_6", "cm_12",
            "def_1", "def_6", "def_12", "ile_1", "ilr_1", "ilr_6", "ilr_12", "im_1", "im_6",
            "im_12", "nei_1", "p52w_6", "p52w_12", "r6_1", "r6_6", "r6_12", "r11_1", "r11_6",
            "r11_12", "re_1", "re_6", "resid6_6", "resid6_12", "resid11_1", "resid11_6",
            "resid11_12", "rs_1", "sim_1", "sim_12", "sm_1", "sm_12", "sue_1", "sue_6", "tes_1",
        },
        "value-versus-growth": {
            "bm", "bmj", "bmq_12", "cp", "cpq_1", "cpq_6", "cpq_12", "dp", "dur", "ebp",
            "em", "emq_1", "emq_6", "emq_12", "ep", "epq_1", "epq_6", "epq_12", "ir", "nop",
            "ocp", "ocpq_1", "op", "rev_1", "rev_6", "rev_12", "sp", "spq_1", "spq_6",
            "spq_12", "vfp", "vhp",
        },
        "investment": {
            "aci", "cei", "dac", "dbe", "dcoa", "dfin", "dfnl", "dii", "dlno", "dlti",
            "dnca", "dnco", "dnoa", "dpia", "dwc", "ia", "iaq_1", "iaq_6", "iaq_12", "ig",
            "ig2", "ivc", "ivg", "ndf", "nxf", "noa", "nsi", "oa", "pda", "poa", "pta", "ta",
        },
        "profitability": {
            "ato", "atoq_1", "atoq_6", "atoq_12", "cla", "claq_1", "claq_6", "claq_12",
            "cop", "cto", "ctoq_1", "ctoq_6", "ctoq_12", "droa_1", "droa_6", "droe_1",
            "droe_6", "droe_12", "eg_1", "eg_6", "eg_12", "fp_6", "fq_1", "fq_6", "fq_12",
            "gla", "glaq_1", "glaq_6", "glaq_12", "gpa", "olaq_1", "olaq_6", "olaq_12",
            "oleq_1", "oleq_6", "oleq_12", "opa", "ope", "oq_1", "pmq_1", "rnaq_1",
            "rnaq_6", "rnaq_12", "roa_1", "roa_6", "roe_1", "roe_6", "sgq_1", "tbiq_6", "tbiq_12",
        },
        "intangibles": {
            "adm", "alaq_1", "almq_1", "almq_6", "almq_12", "dls_1", "eprd", "etl", "etr",
            "hs", "ioca", "oca", "ol", "olq_1", "olq_6", "olq_12", "r1a", "r1n", "r5a",
            "r5n", "r10a", "r10n", "r15a", "r20a", "rca", "rdm", "rdmq_1", "rdmq_6",
            "rdmq_12", "rdsq_6", "rdsq_12", "rer", "vcf_1",
        },
        "frictions": {
            "beta_1", "dtv_12", "isff_1", "isq_1", "ivff_1", "ivq_1",
            "me", "srev", "sv_1", "tv_1", "tail_12",
        },
    }

    _CATEGORY_SLUG = {
        "momentum": "mom", "value-versus-growth": "vvg",
        "investment": "inv", "profitability": "prof",
        "intangibles": "intan", "frictions": "fric",
    }

    _FREQ_STR = {
        'm': 'monthly', 'y': 'annual', 'w': 'weekly', 
        'd': 'daily', 'q': 'quarterly', 'w2w': 'weekly_w2w',
    }

    @property
    def _frequencies(self) -> list[str]:
        return ["m", "y", "w", "d", "q", "w2w"]  #TODO: add d, w2w, q, 

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

    def __init__(self, 
                 anomaly: str | list[str], 
                 sort: str | int = 'decile', 
                 **kwargs):
        
        # only value-weighted ('vw') returns if weights was given
        weights = kwargs.pop('weights', 'vw')
        if weights and weights.lower() != 'vw':
            raise ValueError(
                f"{self.__class__.__name__} only supports value-weighted ('vw') returns. ",
            )

        freq = kwargs.get('frequency', 'm').lower().strip()
        raw_inputs = [anomaly] if isinstance(anomaly, str) else anomaly
        self.anomalies = self._resolve_anomalies(raw_inputs)
        sort_clean = str(sort).lower().strip()
        
        if sort_clean in (10, '10', 'decile', '1way', '1'):
            self.sort_type = 'decile'
        elif sort_clean in ('3x5', '2way', 'bivariate', '5x3'):
            self.sort_type = '3x5'
        else:
            raise ValueError(f"Invalid sort:'{sort}'. 'decile' (1-way) or '3x5' (bivariate).")

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
        prefix = "me_" if self.sort_type == '3x5' else ""
        last_year = datetime.now(tz=timezone.utc).year - 1
        return f"{base}/{prefix}{cat_slug}_{freq}_{last_year}.zip"
    
    @property
    def url(self) -> str:
        # URL only gets first category
        # FIXME: TODO:
        first_cat = self._get_category(self.anomalies[0])
        return self._get_category_url(first_cat)

    @property
    def schema(self) -> pa.Schema:
        """Schema for global-q anomalies."""
        fields: list[tuple[str, pa.DataType]] = [("date", pa.date32())]
        if self.frequency == 'm':
            fields.append(("nstocks", pa.int32()))

        if self.sort_type == 'decile':
            fields.append(("rank_decile", pa.int16()))
        else:
            fields += [("rank_me", pa.int16()), ("rank_anomaly", pa.int16())]

        fields += [("ret_vw", pa.float64()), ("retx_vw", pa.float64())]
        return pa.schema(fields)

    @property
    def _precision(self) -> int: 
        return 5

    # adapted from jupyter notebook, don't use yet!
    def _group_anomalies_by_category(self) -> dict[str, list[str]]:
        """Group anomalies by category."""
        cat_to_anoms = {}
        for a in self.anomalies:
            cat = self._get_category(a)
            cat_to_anoms.setdefault(cat, []).append(a)
        return cat_to_anoms

    def _get_cat_zips(self, cat_to_anoms: dict[str, list[str]], initial_data: bytes) -> dict[str, bytes]:
        """Download all zips for the requested anomalies."""
        from getfactormodels.utils.http_client import _HttpClient

        first_cat = self._get_category(self.anomalies[0])
        zip_bytes_cache = {first_cat: initial_data}

        with _HttpClient() as client:
            for cat in cat_to_anoms:
                if cat not in zip_bytes_cache:
                    cat_url = self._get_category_url(cat)
                    zip_bytes_cache[cat] = client.download(
                        cat_url, tag="AnomalyData", model_name=f"Q-{cat.capitalize()}",
                    )
        return zip_bytes_cache


    def _find_target_file(self, namelist: list[str], anomaly: str, 
                          freq_str: str, prefix_str: str) -> str:
        """Locate the anomaly CSV."""
        target_file = next(
            (n for n in namelist 
                if f"_{anomaly}_" in n.lower() 
                and freq_str in n.lower()
                and (not prefix_str or n.lower().startswith(prefix_str))),
            None,
        )
        if not target_file:
            raise ValueError(f"Could not find file for anomaly: '{anomaly}'.")

        return target_file


    def _read(self, data: bytes) -> pa.Table:
        try:
            cat_to_anoms = self._group_anomalies_by_category()
            zip_bytes_cache = self._get_cat_zips(cat_to_anoms, data)

            _freq_str = self._FREQ_STR[self.frequency]
            prefix_str = "me_" if self.sort_type == '3x5' else ""
            _val_col = 'ret_vw' if self.dividends else 'retx_vw'

            pivoted_tables = []
            for cat, anoms in cat_to_anoms.items():
                with zipfile.ZipFile(io.BytesIO(zip_bytes_cache[cat])) as z:
                    namelist = z.namelist()
                    for a in anoms:
                        target_file = self._find_target_file(namelist, a, _freq_str, prefix_str)
                        pivoted = self._process_anomaly_file(z, target_file, a, _val_col)
                        pivoted_tables.append(pivoted)

            if not pivoted_tables:
                raise ValueError("No anomaly tables were successfully processed.")

            final_table = pivoted_tables[0]
            for t in pivoted_tables[1:]:
                final_table = final_table.join(t, keys=["date"], join_type="left outer")

            return final_table.combine_chunks()

        except (pa.ArrowIOError, pa.ArrowInvalid) as e:
            raise ValueError(f"Reading failed: {e}") from e


    def _process_anomaly_file(
        self, z: zipfile.ZipFile, target_file: str, 
            anomaly: str, val_col: str) -> pa.Table:
        """Processes a single CSV from the zip archive into a pivoted table."""
        with z.open(target_file) as f:
            table = pv.read_csv(f)

        table = table.rename_columns([_GLOBAL_Q_RENAME_MAP.get(c, c) for c in table.column_names])

        if self.sort_type != 'decile':
            cols = [
                "rank_anomaly" if col.lower().startswith("rank ") and col != "rank_me" else col
                for col in table.column_names
            ]
            table = table.rename_columns(cols)

        table = _format_date_column(table, self.frequency)
        table = offset_period_eom(table, self.frequency)
        t = _pivot_global_q_table(table, val_col, anomaly_name=anomaly)

        return scale_to_decimal(t)


def _get_q_anomalies(name: str | list[str], **kwargs):
    return _QAnomalies(anomaly=name, **kwargs) 
