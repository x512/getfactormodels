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
from types import MappingProxyType
from typing import Any, Final, Sequence
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
from getfactormodels.models.base import FactorModel, PortfolioBase
from getfactormodels.registries import Q_ANOMALIES, Q_REGISTRY, Q_THEME_FACTORS
from getfactormodels.utils.arrow_utils import scale_to_decimal
from getfactormodels.utils.date_utils import (
    offset_period_eom,
    parse_quarterly_dates,
)

_GLOBAL_Q_RENAME_MAP: Final = {
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

_GLOBAL_Q_FREQ_MAP: Final = MappingProxyType({
    "m": "monthly",
    "y": "annual",
    "w": "weekly",
    "d": "daily",
    "q": "quarterly",
    "w2w": "weekly_w2w",
})

_CATEGORY_SLUGS: Final = MappingProxyType({
    "momentum": "mom",
    "value-versus-growth": "vvg",
    "investment": "inv",
    "profitability": "prof",
    "intangibles": "intan",
    "frictions": "fric",
})

_BENCHMARK_SLUGS: Final = MappingProxyType({
    "2x3": "me_eg",
    "2x3x3": "me_ia_roe",
})

_ALLOWED_SORTS: Final = frozenset({"2x3", "2x3x3", "3x5", "decile"})

def _normalize_key(name: str) -> str:
    """Strip underscores/hyphens and make lowercase for alias matching."""
    return name.replace("_", "").replace("-", "").lower()

_NORMALIZED_LOOKUP: Final = MappingProxyType({
    **{_normalize_key(k): k for k in Q_REGISTRY},
    **{_normalize_key(slug): name for name, slug in _CATEGORY_SLUGS.items()},
})

_FACTOR_TO_CATEGORY: Final = MappingProxyType({
    f: cat
    for cat, factors in Q_THEME_FACTORS.items()
    for f in factors
})

def _format_date_column(table: pa.Table, frequency: str) -> pa.Table:
    """Helper: Format date columns for global-q datasets."""
    cols = table.column_names

    if frequency == "q" or "quarter" in cols:
        if "quarter" in cols and "period" not in cols:
            table = table.rename_columns([
                "period" if c == "quarter" else c for c in table.column_names
            ])
        return parse_quarterly_dates(table=table)

    if frequency == "y" and "year" in cols:
        idx = cols.index("year")
        return table.set_column(
            idx, "date", table.column(idx).cast(pa.string())
        )

    time_col = (
        "period"
        if "period" in cols
        else ("month" if "month" in cols else None)
    )

    if "year" in cols and time_col:
        yr = table.column("year").cast(pa.string())
        val = pc.utf8_lpad(
            table.column(time_col).cast(pa.string()), width=2, padding="0"
        )
        dates = pc.binary_join_element_wise(yr, val, "")
        table = table.drop(["year", time_col]).combine_chunks()
        return table.add_column(0, "date", dates)

    return table

def _pivot_global_q_table(
    table: pa.Table,
    value_col: str,
    anomaly_name: str | None = None,
) -> pa.Table:
    """Helper: Pivot Global Q's portfolios and anomalies."""
    rank_cols = [c for c in table.column_names if c.startswith("rank_")]
    dates = (
        table
        .select(["date"])
        .group_by(["date"])
        .aggregate([])
        .sort_by([("date", "ascending")])
    )

    sort_keys = [(col, "ascending") for col in rank_cols]
    uniq_ports = table.group_by(rank_cols).aggregate([]).sort_by(sort_keys)

    columns = [dates.column("date")]
    names = ["date"]

    for port in uniq_ports.to_pylist():
        name_parts = []
        for c in rank_cols:
            clean_col = c.replace("rank_", "")
            if clean_col == "anomaly" and anomaly_name:
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
        self.classic = classic
        super().__init__(**kwargs)

    @property
    def frequencies(self) -> list[str]:
        return ["d", "w", "w2w", "m", "q", "y"]

    @property
    def _precision(self) -> int:
        return 6

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
            # force 'period' here
            time_cols = [("year", pa.string()), ("period", pa.string())]
        elif self.frequency == "y":
            time_cols = [("year", pa.string())]
        else:  # d/w/w2w, force 'date'
            time_cols = [("date", pa.string())]

        return pa.schema(time_cols + factors)

    @property
    def url(self) -> str:
        freq = self.frequency or "m"
        file_str = _GLOBAL_Q_FREQ_MAP.get(freq, "monthly")
        base_url = "https://global-q.org/uploads/1/2/2/6/122679606"
        last_year = datetime.now(tz=timezone.utc).year - 1
        return f"{base_url}/q5_factors_{file_str}_{last_year}.csv"

    def _read(self, data: bytes) -> pa.Table:
        try:
            read_opts = pv.ReadOptions(
                column_names=self.schema.names,
                skip_rows=1,
                block_size=1024 * 1024 * 2,
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

            table = table.rename_columns([
                _GLOBAL_Q_RENAME_MAP.get(c, c) for c in table.column_names
            ])
            table = table.rename_columns([
                c.removeprefix("R_") for c in table.column_names
            ])
            return table.combine_chunks()

        except (pa.ArrowIOError, pa.ArrowInvalid) as e:
            msg = f"{self.__class__.__name__}: reading failed: {e}"
            self.log.error(msg)
            raise ValueError(msg) from e



class GlobalQPortfolios(PortfolioBase):
    """Download q-factor portfolio data from global-q.org.

    Supports benchmark sorts and anomaly sorts (decile 1-way, 3x5 on size).
    Benchmarks:
    - "18 (2 by 3 by 3) Size, Investment-to-assets, and Return on Equity
      Benchmark Portfolios Underlying the q-factors."
    - "6 (2 by 3) Size and Expected Growth Benchmark Portfolios Underlying
      the Expected Growth Factor."

    Sort inference:
      - exact benchmark match = that sort
      - everything else = decile
      - explicitly request ``sort="3x5"`` for bivariate sorts.
    """

    _BASE_URL: Final = "https://global-q.org/uploads/1/2/2/6/122679606"
    
    @property
    def _precision(self) -> int:
        return 5

    factor: tuple[str, ...]
    sort: str

    def __init__(
        self,
        factor: str | Sequence[str],
        sort: str | None = None,
        frequency: str = "m",
        *,
        dividends: bool = True,
        **kwargs: Any,
    ) -> None:

        if frequency not in _GLOBAL_Q_FREQ_MAP:
            raise ValueError(
                f"Unsupported frequency {frequency!r}; "
                f"expected one of {sorted(_GLOBAL_Q_FREQ_MAP)}."
            )

        raw = [factor] if isinstance(factor, str) else list(factor)
        factor = self._resolve_slugs(raw)
        
        # Check if this is an exact benchmark request
        bench_set = frozenset(factor)
        is_benchmark = any(
            bench_set == frozenset(slug.split("_")) 
            for slug in _BENCHMARK_SLUGS.values()
        )

        # Anoms don't have ex-divs 
        if not dividends and not is_benchmark:
            raise ValueError(
                "Global-Q anomaly datasets do not provide ex-dividend (retx_vw) returns. "
                "Please use dividends=True (the default)."
            )

        if sort is None:
            sort = self._resolve_sort(factor)
        else:
            if is_benchmark:
                if sort not in ("2x3", "2x3x3"):
                    raise ValueError(
                        f"Invalid sort {sort!r} for benchmark factors. "
                        "Expected '2x3' or '2x3x3'."
                    )
            else:
                if sort not in ("decile", "3x5"):
                    raise ValueError(
                        f"Invalid sort {sort!r} for anomaly factors. "
                        "Expected 'decile' or '3x5'."
                    )

        if sort == "3x5":
            factor = tuple(f for f in factor if f != "me")
            if not factor:
                raise ValueError("3x5 sort requires an anomaly factor besides 'me'.")

        self._validate_factors(factor, sort)

        super().__init__(
            source="q",
            factor=factor,
            sort=sort,
            dividends=dividends,
            frequency=frequency,
            **kwargs,
        )

    @classmethod
    def _resolve_slugs(cls, raw: Sequence[str]) -> tuple[str, ...]:
        resolved: list[str] = []
        for item in raw:
            clean = item.lower().strip()
            if clean == "size":
                clean = "me"
            canonical = _NORMALIZED_LOOKUP.get(_normalize_key(clean), clean)
            if canonical in Q_THEME_FACTORS:
                resolved.extend(sorted(Q_THEME_FACTORS[canonical]))
            else:
                resolved.append(canonical)
        return tuple(dict.fromkeys(resolved))

    @classmethod
    def _resolve_sort(cls, factor: Sequence[str]) -> str:
        bench_set = frozenset(factor)
        for sort, slug in _BENCHMARK_SLUGS.items():
            if bench_set == frozenset(slug.split("_")):
                return sort
        return "decile"

    @classmethod
    def _validate_factors(cls, factor: tuple[str, ...], sort: str) -> None:
        if sort in _BENCHMARK_SLUGS:
            required = frozenset(_BENCHMARK_SLUGS[sort].split("_"))
            if frozenset(factor) != required:
                raise ValueError(
                    f"{sort} requires exactly {sorted(required)}; "
                    f"got {sorted(factor)}."
                )
            return
        for f in factor:
            if f not in Q_ANOMALIES:
                raise ValueError(f"{f!r} is not a valid Global-Q anomaly.")

    @staticmethod
    def _category_of(factor: str) -> str:
        try:
            return _FACTOR_TO_CATEGORY[factor]
        except KeyError:
            raise ValueError(f"No Global-Q category for factor: {factor!r}.") from None

    @property
    def frequencies(self) -> list[str]:
        return list(_GLOBAL_Q_FREQ_MAP)

    @property
    def _freq_slug(self) -> str:
        return _GLOBAL_Q_FREQ_MAP[self.frequency]

    @staticmethod
    def _url_year() -> int:
        return datetime.now(tz=timezone.utc).year - 1

    @property
    def url(self) -> str | list[str] | dict[str, str]:
        if self.sort in _BENCHMARK_SLUGS:
            return self._benchmark_urls()
        return self._category_urls()

    def _benchmark_urls(self) -> list[str]:
        slug = _BENCHMARK_SLUGS[self.sort]
        year = self._url_year()
        return [
            f"{self._BASE_URL}/benportf_{slug}_{self._freq_slug}_{y}.csv"
            for y in (year, year - 1)
        ]

    def _category_urls(self) -> str | dict[str, str]:
        #TODO:FIXME:Auto-failover on anomaly category URLs.
        prefix = "me_" if self.sort == "3x5" else ""
        year = self._url_year()
        url_map = {
            cat: f"{self._BASE_URL}/{prefix}{_CATEGORY_SLUGS[cat]}"
                 f"_{self._freq_slug}_{year}.zip"
            for cat in {self._category_of(f) for f in self.factor}
        }
        return next(iter(url_map.values())) if len(url_map) == 1 else url_map

    @property
    def schema(self) -> pa.Schema:
        fields: list[tuple[str, pa.DataType]] = [("date", pa.date32())]

        if self.sort in _BENCHMARK_SLUGS:
            if self.frequency in ("d", "m"):
                fields.append(("nstocks", pa.int32()))
            fields.append(("rank_ME", pa.int16()))
            if self.sort == "2x3x3":
                fields += [("rank_IA", pa.int16()), ("rank_ROE", pa.int16())]
            else:
                fields.append(("rank_EG", pa.int16()))
        else:
            if self.frequency == "m":
                fields.append(("nstocks", pa.int32()))
            if self.sort == "decile":
                fields.append(("rank_decile", pa.int16()))
            else:
                fields += [("rank_me", pa.int16()), ("rank_anomaly", pa.int16())]

        fields += [("ret_vw", pa.float64()), ("retx_vw", pa.float64())]
        return pa.schema(fields)
    
    # https://global-q.org/factors.html:
    #
    # "For a given anomaly, we offer value-weighted returns in percent for 1-way
    # sorted portfolios (mostly deciles, with a few quantiles) and 2-way (3 by 5)
    # independently sorted portfolios by interacting the anomaly with size."
    def _read(self, data: bytes | list[bytes] | dict[str, bytes]) -> pa.Table:
        if self.sort in _BENCHMARK_SLUGS:
            return self._read_benchmark(data)
        return self._read_anomalies(data)

    def _read_benchmark(self, data: bytes | list[bytes]) -> pa.Table:
        if isinstance(data, list):
            data = data[0]

        table = pv.read_csv(io.BytesIO(data))
        table = table.rename_columns(
            [_GLOBAL_Q_RENAME_MAP.get(c, c) for c in table.column_names]
        )
        table = _format_date_column(table, self.frequency)
        table = offset_period_eom(table, self.frequency)
        table = table.select(self.schema.names).cast(self.schema)
        return _pivot_global_q_table(
            scale_to_decimal(table), "ret_vw" if self.dividends else "retx_vw"
        )

    def _group_by_category(self) -> dict[str, list[str]]:
        groups: dict[str, list[str]] = {}
        for f in self.factor:
            groups.setdefault(self._category_of(f), []).append(f)
        return groups

    def _read_anomalies(self, data: bytes | dict[str, bytes]) -> pa.Table:
        by_cat = self._group_by_category()
        zips = data if isinstance(data, dict) else {next(iter(by_cat)): data}

        val_col = "ret_vw" #No ex-divs on anoms 
        prefix = "me_" if self.sort == "3x5" else ""

        tables: list[pa.Table] = []
        for cat, factors in by_cat.items():
            with zipfile.ZipFile(io.BytesIO(zips[cat])) as zf:
                names = zf.namelist()
                for f in factors:
                    target = self._find_target_file(names, f, self._freq_slug, prefix)
                    tables.append(self._process_anomaly_file(zf, target, f, val_col))

        result = tables[0]
        for t in tables[1:]:
            result = result.join(t, keys=["date"], join_type="left outer")
        return result.combine_chunks()

    @staticmethod
    def _find_target_file(
        namelist: list[str], factor: str, freq_slug: str, prefix: str
    ) -> str:
        for name in namelist:
            lower = name.lower()
            if (
                f"_{factor}_" in lower
                and f"_{freq_slug}_" in lower
                and (not prefix or lower.startswith(prefix))
            ):
                return name
        raise ValueError(f"Could not find file for factor: {factor!r}.")

    def _process_anomaly_file(
        self, zf: zipfile.ZipFile, target: str, factor: str, val_col: str
    ) -> pa.Table:
        with zf.open(target) as f:
            table = pv.read_csv(f)

        table = table.rename_columns(
            [_GLOBAL_Q_RENAME_MAP.get(c, c) for c in table.column_names]
        )

        if self.sort == "3x5":
            table = table.rename_columns([
                "rank_anomaly"
                if c.lower().startswith("rank") and c.lower() != "rank_me"
                else c
                for c in table.column_names
            ])
        else:
            table = table.rename_columns([
                "rank_anomaly" if c.lower().startswith("rank") else c
                for c in table.column_names
            ])

        table = _format_date_column(table, self.frequency)
        table = offset_period_eom(table, self.frequency)
        pivoted = _pivot_global_q_table(table, val_col, anomaly_name=factor)
        return scale_to_decimal(pivoted).combine_chunks()


# DISPATCHERS
def _get_q_portfolios(
    factor: str | list[str] | None = None,
    bivariate: bool = False,
    sort: str | Sequence[str],
    *,
    dividends: bool = True,
    **kwargs,
):
    """Unified dispatcher for Q benchmarks and anomalies."""
    if factor:
        kwargs["sort"] = "3x5" if bivariate else "decile"
        return GlobalQPortfolios(factor=factor, **kwargs)
        
    return GlobalQPortfolios(factor=["me", "ia", "roe"], **kwargs)


def _get_q_anomalies(name: str | list[str], **kwargs):
    """Backwards-compatible wrapper for anomalies."""
    return GlobalQPortfolios(factor=name, **kwargs)

