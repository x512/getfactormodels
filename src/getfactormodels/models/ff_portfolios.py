from __future__ import annotations

import io
import zipfile
from collections.abc import Sequence
from typing import Any, Final, Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv

from getfactormodels.models.base import PortfolioBase 
from getfactormodels.models.ff_breakpoints import FamaFrenchBreakpoints
from getfactormodels.utils.arrow_utils import scale_to_decimal
from getfactormodels.utils.date_utils import offset_period_eom

_FF_BASE_URL: Final[str] = (
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"
)
_FF_NULL_STRINGS: Final[tuple[str, ...]] = ("-99.99", "-999", "-99.9", "-99.990")
_FF_NULL_FLOATS: Final[tuple[float, ...]] = (-99.99, -999.0, -99.9)

FF_PORTFOLIO_FACTORS: Final[frozenset[str]] = frozenset({
    "ac", "beta", "bm", "cfp", "dp", "ep", "inv",
    "lt_rev", "mom", "ni", "op", "resvar", "size",
    "st_rev", "var"
})

MONTH_ONLY_BIVARIATE_PAIRS: Final[frozenset[tuple[str, ...]]] = frozenset({
    ("ac", "size"), ("beta", "size"), ("ni", "size"),
    ("size", "var"), ("resvar", "size"),
})

_SORT_COUNTS: Final[dict[str, str]] = {
    "2x3": "6",
    "5x5": "25",
    "10x10": "100",
    "2x4x4": "32",
}

_FACTOR_ABBR: Final[dict[str, str]] = {
    "bm": "BEME", "op": "OP", "inv": "INV", "ep": "EP", "cfp": "CFP",
    "dp": "DP", "ac": "AC", "beta": "BETA", "ni": "NI", "var": "VAR",
    "resvar": "RESVAR", "mom": "Prior_12_2", "st_rev": "Prior_1_0", 
    "lt_rev": "Prior_60_13",
}

FF_UNIVARIATE_REGISTRY: Final[dict[str, str]] = {
    "size": "Portfolios_Formed_on_ME", "bm": "Portfolios_Formed_on_BE-ME",
    "op": "Portfolios_Formed_on_OP", "inv": "Portfolios_Formed_on_INV",
    "ep": "Portfolios_Formed_on_E-P", "cfp": "Portfolios_Formed_on_CF-P",
    "dp": "Portfolios_Formed_on_D-P", "mom": "10_Portfolios_Prior_12_2",
    "st_rev": "10_Portfolios_Prior_1_0", "lt_rev": "10_Portfolios_Prior_60_13",
    "ac": "Portfolios_Formed_on_AC", "beta": "Portfolios_Formed_on_BETA",
    "ni": "Portfolios_Formed_on_NI", "var": "Portfolios_Formed_on_VAR",
    "resvar": "Portfolios_Formed_on_RESVAR",
}

_PRIOR_RETS: Final[frozenset[str]] = frozenset({"mom", "st_rev", "lt_rev"})


#Notes: possibly more flexible input: 
#   allow: 
#   int: 3, 5, 10, 
#   str: "3", "5", "10", '1x3', '1x5', '1x10', 'decile, 'bivariate',
#
#

class FamaFrenchSorts(PortfolioBase):
    def __init__(
        self,
        factors: str | Sequence[str | Sequence[str]] = "size",
        sort: Literal["2x3", "2x4x4", "5x5", "10x10", "3", "5", "10"] = "10",
        weights: str = "vw",
        frequency: str = "m",
        *,
        dividends: bool = True,
        **kwargs: Any,
    ) -> None:
        self._dividends = dividends.lower() == "true" if isinstance(dividends, str) else bool(dividends)
        self._weights = weights
        self._frequency = frequency
        self._factor_groups = self._normalize_input(factors, sort)
        self._sort = self._resolve_sort(sort)
        self._slugs = [self._resolve_slug(group) for group in self._factor_groups]

        super().__init__(
            source="ff",
            factor=factors,
            sort=self._sort,
            dividends=self._dividends,
            frequency=frequency,
            **kwargs,
        )

    @staticmethod
    def _normalize_input(
        factors: str | Sequence[str | Sequence[str]], 
        sort: str,
    ) -> list[tuple[str, ...]]:
        raw = [factors] if isinstance(factors, str) else factors
        normalized = []

        for item in raw:
            parts = (
                [f.strip().lower() for f in item.split(",") if f.strip()]
                if isinstance(item, str)
                else [str(f).strip().lower() for f in item if str(f).strip()]
            )
            if not parts:
                continue
            for f in parts:
                if f not in FF_PORTFOLIO_FACTORS:
                    raise ValueError(f"Unknown factor: {f}")
            
            # if univariate (len == 1) and sort is bivariate, pairs
            # it with 'size' now, so it gets deduplicated properly.
            if len(parts) == 1 and parts[0] != "size" and sort in _SORT_COUNTS:
                parts.append("size")

            group = tuple(sorted(parts))
            
            if group not in normalized:
                normalized.append(group)

        if not normalized:
            raise ValueError("At least one valid factor is required.")
        return normalized
    def _resolve_sort(self, sort: str) -> str:
        sort = str(sort).lower()
        for group in self._factor_groups:
            if group in MONTH_ONLY_BIVARIATE_PAIRS:
                if self._frequency == "d":
                    raise ValueError(f"Factor combination {group} does not support daily frequency.")
                if sort in ("10", "3", "5"):
                    return "5x5"
        return sort

    def _resolve_slug(self, group: tuple[str, ...]) -> str:
        # Univariates - standalone single factor tables
        if len(group) == 1:
            if slug := FF_UNIVARIATE_REGISTRY.get(group[0]):
                return slug
            raise ValueError(f"Unsupported univariate factor: {group[0]}")

        # Multivariate?
        count = _SORT_COUNTS.get(self._sort)
        if not count:
            raise ValueError(f"Unsupported sort for multivariate: {self._sort}")

        # Three-way sorts
        if len(group) == 3:
            if set(group) == {"bm", "op", "size"}:
                return "32_Portfolios_ME_BEME_OP_2x4x4"
            if set(group) == {"bm", "inv", "size"}:
                return "32_Portfolios_ME_BEME_INV_2x4x4"
            if set(group) == {"inv", "op", "size"}:
                return "32_Portfolios_ME_OP_INV_2x4x4"
            raise ValueError(f"Unsupported 3-way combination: {group}")

        # Bivariate sorts
        if len(group) == 2:
            if group == ("bm", "size"):
                return f"{count}_Portfolios_{self._sort}"

            if "size" in group:
                other = [f for f in group if f != "size"][0]
                abbr = _FACTOR_ABBR[other]
                if other in _PRIOR_RETS:
                    return f"{count}_Portfolios_ME_{abbr}"
                return f"{count}_Portfolios_ME_{abbr}_{self._sort}"

            abbrs = "_".join(_FACTOR_ABBR[f] for f in group)
            return f"{count}_Portfolios_{abbrs}_{self._sort}"

        raise ValueError(f"Unsupported factor combination: {group} for sort {self._sort}")

    @property
    def frequencies(self) -> list[str]:
        return ["d", "m", "y"]

    @property 
    def name(self) -> str:
        """The formal human-readable string representation of this instance."""
        formatted_groups = [
            "/".join(group) for group in getattr(self, "_factor_groups", [])
        ]
        
        factor_label = ", ".join(formatted_groups)
        
        sort_val = getattr(self, "sort", getattr(self, "_sort", ""))
        weights_val = getattr(self, "weights", getattr(self, "_weights", ""))
        freq_val = getattr(self, "frequency", getattr(self, "_frequency", ""))
        div_val = getattr(self, "dividends", getattr(self, "_dividends", True))
        
        div_suffix = ", ex-div" if not div_val else ""
        
        return f"Fama-French Portfolio - {sort_val} - {weights_val}, {freq_val}{div_suffix} [{factor_label}]"

    @property
    def schema(self) -> pa.Schema:
        if self._data is not None:
            return self._data.schema
        return pa.schema([pa.field("date", pa.date32())])

    @property
    def url(self) -> dict[str, str]:
        suffix = (
            "_Daily_CSV.zip"       if self._frequency == "d"
            else "_CSV.zip" if self._dividends
            else "_Wout_Div_CSV.zip"
        )
        return {
            ",".join(group): f"{_FF_BASE_URL}/{slug}{suffix}"
            for group, slug in zip(self._factor_groups, self._slugs)
        }

    def _read(self, data: dict[str, bytes] | bytes) -> pa.Table:
        if isinstance(data, bytes):
            key = ",".join(self._factor_groups[0])
            data = {key: data}

        tables = []
        for i, (key, payload) in enumerate(data.items()):
            csv_text = self._unzip_to_text(payload)
            cleaned_csv = self._clean_ff_csv(csv_text)

            table = pv.read_csv(
                io.BytesIO(cleaned_csv.encode("utf-8")),
                convert_options=pv.ConvertOptions(null_values=list(_FF_NULL_STRINGS)),
            )
            table = self._fix_ff_nulls(table)
            table = offset_period_eom(table, self._frequency)
            table = scale_to_decimal(table)

            renames = []
            for j, n in enumerate(table.column_names):
                clean = n.strip().lower().replace(" ", "_").replace("-", "_")
                if j == 0:
                    clean = "date"
                renames.append(clean)
            table = table.rename_columns(renames).combine_chunks()

            neg_cols = [c for c in table.column_names if "<=" in c]
            if neg_cols:
                table = table.drop(neg_cols)

            if self._sort in ("3", "5", "10") and not any(f in _PRIOR_RETS for f in key.split(",")):
                table = self._filter_univariate_columns(table)

            # Prefix all columns if there are multiple tables
            if len(data) > 1:
                prefix = key.replace(",", "_")
                new_names = [
                    "date" if c == "date" else f"{prefix}_{c}"
                    for c in table.column_names
                ]
                table = table.rename_columns(new_names)

            tables.append(table)

        result = tables[0]
        for t in tables[1:]:
            result = result.join(t, keys="date", join_type="inner")

        return result.combine_chunks()

    def _filter_univariate_columns(self, table: pa.Table) -> pa.Table:
        """Univariate files contain a table of tertiles, quintiles and deciles,
        this extracts the requested sort."""
        cols_to_keep = ["date"]
        for c in table.column_names:
            if c == "date":
                continue
            # Check for substrings like '2_dec' or 'qnt_2'
            if self._sort == "3" and c in ("lo_30", "med_40", "hi_30"):
                cols_to_keep.append(c)
            elif self._sort == "5" and (c in ("lo_20", "hi_20") or "qnt" in c):
                cols_to_keep.append(c)
            elif self._sort == "10" and (c in ("lo_10", "hi_10") or "dec" in c):
                cols_to_keep.append(c)
        return table.select(cols_to_keep)

    @staticmethod
    def _unzip_to_text(payload: bytes) -> str:
        with zipfile.ZipFile(io.BytesIO(payload)) as z:
            with z.open(z.namelist()[0]) as f:
                return f.read().decode("utf-8", errors="ignore")

    @staticmethod
    def _clean_ff_csv(raw_text: str) -> str:
        lines = raw_text.splitlines()
        clean_lines = []
        capturing = False

        for line in lines:
            stripped = line.strip()
            if not capturing:
                if "value weight" in line.lower() or "equal weight" in line.lower() or line.startswith(","):
                    capturing = True
                    continue

            if capturing:
                if not stripped or "Copyright" in line or "The Fama-French" in line:
                    break
                clean_lines.append(line)

        return "\n".join(clean_lines) if clean_lines else raw_text

    @staticmethod
    def _fix_ff_nulls(table: pa.Table) -> pa.Table:
        sentinels = pa.array(_FF_NULL_FLOATS, type=pa.float64())
        for i, name in enumerate(table.column_names):
            if name.lower() == "date":
                continue
            col = table.column(name)
            if not pa.types.is_floating(col.type):
                continue
            mask = pc.is_in(col, value_set=sentinels)
            table = table.set_column(i, name, pc.if_else(mask, pa.scalar(None, type=col.type), col))
        return table


# =================== WIP 
    def breakpoints(self, factor: str | None = None, **kwargs) -> FamaFrenchBreakpoints:
        """Fetch the breakpoints used to construct the portfolios."""
        # Default to the first factor passed if none is specified
        target_factor = factor or (self.factor[0] if self.factor else "me")
        # Inherit default kwargs from the parent instance
        bp_kwargs = {
            "frequency": self.frequency,
            "start_date": self.start_date,
            "end_date": self.end_date,
            **kwargs
        }
        
        return FamaFrenchBreakpoints(factor=target_factor, **bp_kwargs)
