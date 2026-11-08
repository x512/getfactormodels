"""Standalone Fama-French Industry Portfolios container & combiner."""
from __future__ import annotations

import io
import zipfile
from typing import Any, Final, Literal, Self
import re
import pyarrow as pa
import copy


from getfactormodels.exceptions import ModelNotFoundError
from getfactormodels.models.base import Base
from getfactormodels.utils.arrow_utils import (
    filter_table_by_date,
    scale_to_decimal,
)
from getfactormodels.utils.date_utils import offset_period_eom
"""Fama-French industry portfolios
  returns(freq=, weights=)# portfolio returns (default, fully processed)

  n_firms()   # firms per industry   (monthly)
  firm_size() # average firm size    (monthly, vw)
  bm_vw() # value-weighted book-to-market (annual)
  bm_agg()  # aggregate book-to-market (annual)
  sections  # dict of avail. tables: name, rows, cols, byte size
"""



##### WILL MOVE TO UTILS. #######################################
_MISSING = (-99.99, -999.0, -99.9)
_SPLIT_RE = re.compile(r"[,\s]+")
# This function parses an industry txt file from Kenneth French's data library
# it reads and returns all tables in the file.
def _parse_ff_txt(text: str) -> dict[str, pa.Table]:
    lines = text.splitlines()
    tables: dict[str, pa.Table] = {}
    i, n = 0, len(lines)

    def _tokens(line): return [t for t in _SPLIT_RE.split(line.strip()) if t]
    def _is_data(line):
        t = _tokens(line)
        return bool(t) and t[0].isdigit()
    def _next_nonblank(start):
        j = start
        while j < n and not lines[j].strip(): j += 1
        return j if j < n else None

    while i < n:
        s = lines[i].strip()
        if not s or _is_data(s):
            i += 1
            continue
            
        j = _next_nonblank(i + 1)
        if j is None or _is_data(lines[j].strip()):
            i += 1
            continue
        k = _next_nonblank(j + 1)
        if k is None or not _is_data(lines[k].strip()):
            i += 1
            continue

        title = re.sub(r"[^\w]+", "_", s.strip().replace(" -- ", " ")).strip("_")
        header = _tokens(lines[j])

        end = k
        while end < n:
            ss = lines[end].strip()
            if not ss: end += 1; continue
            if not _is_data(ss): break
            end += 1

        dates, cols = [], [[] for _ in header]
        for dl in lines[k:end]:
            toks = _tokens(dl)
            if not toks: continue
            try:
                date_val = int(toks[0])
                vals = [None if float(t) in _MISSING else float(t) for t in toks[1:]]
            except ValueError: continue
            if len(vals) == len(header):
                dates.append(date_val)
                for cidx, v in enumerate(vals): cols[cidx].append(v)

        if dates:
            is_count = "number_of_firms" in title.lower()
            dtype = pa.int64() if is_count else pa.float64()
            arrays = [pa.array(dates, type=pa.int32())] + [pa.array(c, type=dtype) for c in cols]
            schema = pa.schema([pa.field("Date", pa.int32())] + [pa.field(h, dtype) for h in header])
            tables[title] = pa.Table.from_arrays(arrays, schema=schema)
        i = end

    return tables
###################################################

_WEIGHTS = {"vw": "Value Weighted", "ew": "Equal Weighted"}
_FREQ = {"m": "Monthly", "y": "Annual", "d": "Daily"}
_FF_BASE_URL: Final[str] = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"
INDUSTRY_COUNTS: Final[frozenset[int]] = frozenset({5, 10, 12, 17, 30, 38, 48, 49})
_FREQ_LABEL: Final[dict[str, str]] = {"d": "Daily", "m": "Monthly", "y": "Annual"}


class FamaFrenchIndustryPortfolios(Base):
    """Fama-French industry portfolios.

    The full dataset is parsed once into ``self._sections`` and each
    method selects the slice it needs. 

    Industry portfolios are a return matrix, not a cross-sectional sort, 
    there is no ``sort=``/``factor=`` — hence ``Base``, not ``PortfolioBase``.

    Args:
        industries: 5, 10, 12, 17, 30, 38, 48, or 49.
        dividends: Include dividends in returns (ignored for daily).
        **kwargs: Forwarded to ``Base``
    """
    source = "ff"

    def __init__(
        self,
        industries: Literal[5, 10, 12, 17, 30, 38, 48, 49] = 5,
        *,
        dividends: bool = True,
        **kwargs: Any,
    ) -> None:
        
        ### TODO: PortfolioBase should be handling this. ###############################
        # Coerce string inputs (e.g., "5") into actual integers gracefully
        try:
            target_industries = int(industries)
        except (ValueError, TypeError):
            raise ValueError(
                f"Invalid industry input: {industries}. "
                f"Must be an integer or numeric string matching {sorted(INDUSTRY_COUNTS)}."
            )
        #################################################

        if industries not in INDUSTRY_COUNTS:
            raise ValueError(
                f"Invalid industry count: {industries}. "
                f"Must be one of {sorted(INDUSTRY_COUNTS)}."
            )

        self.industries = industries
        self.dividends = dividends
        self._sections: dict[str, pa.Table] | None = None

        super().__init__(**kwargs)

        if not self.dividends and self.frequency == "d":
            raise ModelNotFoundError(
                "Ex-dividend data is not published for daily "
                "industry portfolios; use 'm' or 'y'."
            )

    @property
    def frequencies(self) -> list[str]:
        return ["d", "m", "y"]

    @property
    def _precision(self) -> int:
        return 6

    @property
    def schema(self) -> pa.Schema:
        if self._data is not None:
            return self._data.schema
        return pa.schema([pa.field("date", pa.date32())])

    @property
    def name(self) -> str:
        base_name = f"Fama French - Industry - {self.industries} Portfolios"

        #append table name if a cloned instance:
        suffix = getattr(self, "_table_name", None) # '_table_name' is assigned by accessors.
        
        return f"{base_name} - {suffix}" if suffix else base_name

    @property
    def url(self) -> str:
        if self.frequency == "d":
            return (
                f"{_FF_BASE_URL}/"
                f"{self.industries}_Industry_Portfolios_daily_CSV.zip"
            )
        suffix = "Wout_Div_CSV.zip" if not self.dividends else "CSV.zip"
        return f"{_FF_BASE_URL}/{self.industries}_Industry_Portfolios_{suffix}"


    ## PUBLIC ACCESSORS
    @property
    def sections(self) -> dict[str, dict[str, int]]:
        """Dict. of available tables: {name: {rows, cols, bytes}}."""
        if self._sections is None:
            self._get_sections()
        
        return {
            title: {
                "rows": table.num_rows,
                "cols": table.num_columns,
                "size_bytes": table.nbytes
            }
            for title, table in self._sections.items()
        }

    def returns(
        self,
        *,
        frequency: str | None = None,
        weights: Literal["vw", "ew"] = "vw",
        **kwargs,
    ) -> pa.Table:
        """Industry portfolio returns.

        Args:
            frequency: 'd', 'm', or 'y'. Defaults to the instance frequency.
                  Must be available in the downloaded file: 'd', 'm', 'y'.
            weights: 'vw' (value-weighted) or 'ew' (equal-weighted).
            **kwargs: passed to Base.
        """
        if weights not in ("vw", "ew"):
            raise ValueError(
                f"Invalid weights scheme: {weights!r}. Must be 'vw' or 'ew'."
            )

        freq = frequency or self.frequency
        if freq not in _FREQ_LABEL:
            raise ValueError(
                f"Invalid frequency: {freq!r}. "
                    f"Must be one of {list(_FREQ_LABEL)}."
            )

        available = {"d"} if self.frequency == "d" else {"m", "y"}
        if freq not in available:
            raise ModelNotFoundError(
                f"Frequency {freq!r} not available in {self.url}. "
                f"Available: {sorted(available)}."
            )

        # default view: self.data has it, honours any .drop/.extract. 
        if freq == self.frequency and weights == "vw":
            return self.data

        weight_term = "Value-Weighted" if weights == "vw" else "Equal-Weighted"
        section = self._find_section(weight_term, "Returns", _FREQ_LABEL[freq])
        table = self._process_table(self._finalize(section, freq, scale=True))
        return filter_table_by_date(table, self._start_date, self._end_date)


    def _clone_with_table(self, table: pa.Table, *, new_freq: str, new_name: str) -> Self:
        """Shallow clone, swap data, overwrite metadata to prevent lies.

        Clones a FamaFrenchIndustry instance...

        """
        #
        # Return a shallow clone (Self) instead of a raw pa.Table so the instance
        # inherits library methods (.to_polars(), .to_file()) without re-triggering
        # __init__. PyArrow tables are immutable, so sharing _sections is safe.
        #
        new_inst = copy.copy(self)
        new_inst._data = table
        
        # fix frequency
        new_inst._frequency = new_freq 
        
        # fix name, so __str__ is correct
        new_inst._table_name = new_name
        
        # clear selected (dropped/extracted factors)
        new_inst._selected_factors = None 
        
        # Do not allow .load() to overwrite the data
        # default behaviour but important, so explicit.
        new_inst.no_cache = False
        
        return new_inst


    # These don't align to the instances frequency! 
    # These are just table from within the industries portfolio 
    # files, lets users access the data if needed.
    def n_firms(self) -> Self:
        self._require_not_daily("Number of firms")
        section = self._find_section("Number of Firms")
        return self._access(section, "m", scale=False, name="Number of Firms")

    def firm_size(self) -> Self:
        self._require_not_daily("Average firm size")
        section = self._find_section("Firm Size")
        return self._access(section, "m", scale=False, name="Firm Size")

    def bm_vw(self) -> Self:
        self._require_not_daily("BE/ME")
        section = self._find_section("Value Weighted Average of BE")
        return self._access(section, "y", scale=False, name="Book-to-Market Value-Weighted (Annual)")

    def bm_agg(self) -> Self:
        self._require_not_daily("BE/ME")
        section = self._find_section("Sum of BE")
        return self._access(section, "y", scale=False, name="Book-to-Market Aggregate (Annual)")

    # INTERNALS
    def _read(self, data: bytes) -> pa.Table:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            inner = z.namelist()[0]
            text = z.read(inner).decode("latin-1")  # never fails

        self._sections = _parse_ff_txt(text)
        if not self._sections:
            raise ModelNotFoundError(
                f"No sections parsed from {self.url}. "
                "The file format may have changed."
            )

        # Return the *raw* default view — Base.load passes it through
        # _process_table to populate self._data.
        section = self._find_section(
            "Value-Weighted", "Returns", _FREQ_LABEL[self.frequency]
        )
        return self._finalize(section, self.frequency, scale=True)

    def _get_sections(self) -> dict[str, pa.Table]:
        if self._sections is None:
            _ = self.data  # triggers load() → _read() → populates _sections
        if self._sections is None:
            raise RuntimeError("Sections not loaded; we shouldn't be here.")
        return self._sections

    def _find_section(self, *needles: str) -> pa.Table:
        sections = self._get_sections()
        normalized = [
            n.lower().replace("-", "_").replace(" ", "_") for n in needles
        ]
        for name, tbl in sections.items():
            lname = name.lower()
            if all(n in lname for n in normalized):
                return tbl
        raise ModelNotFoundError(
            f"No section matching {needles!r} in {self.url}. "
            f"Available sections: {list(sections.keys())}"
        )

    def _finalize(
        self, table: pa.Table, freq: str, *, scale: bool
    ) -> pa.Table:
        """Offset dates to end-of-period, optionally scale, rename cols."""
        table = offset_period_eom(table, freq)
        if scale:
            table = scale_to_decimal(table)
            
        renames = [
            "date" if n.strip().lower() == "date" 
            else n.strip().replace(" ", "_").replace("-", "_").upper()
            for n in table.column_names
        ]
        return table.rename_columns(renames).combine_chunks()

    def _access(
        self, table: pa.Table, freq: str, *, scale: bool, name: str
    ) -> Self:
        table = self._finalize(table, freq, scale=scale)
        table = filter_table_by_date(table, self._start_date, self._end_date)
        return self._clone_with_table(table, new_freq=freq, new_name=name)

    def _require_not_daily(self, t: str) -> None:
        if self.frequency == "d":
            raise ModelNotFoundError(
                f"{t} is not published for daily industry portfolios."
            )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"industries={self.industries}, "
            f"frequency={self.frequency!r}, "
            f"dividends={self.dividends})"
        )
