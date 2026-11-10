# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
from __future__ import annotations
import io
import zipfile
from typing import Any, Final
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
from getfactormodels.models.base import Base
from getfactormodels.utils.date_utils import offset_period_eom


class FamaFrenchBreakpoints(Base):
    """Fama-French breakpoint dataset loader.

    Downloads the CSV for a single breakpoint factor from Ken
    French's data library and returns the parsed percentile table.

    Each factor has one frequency. size (`me`) and prior returns 
    (`prior`/`mom`) are monthly, others are annual. Passing an 
    unsupported frequency raises `ValueError`.
    
    NOTE: annual data is aligned to EOY (12-31) not end of fiscal year.
    """

    @property
    def _precision(self) -> int:
        return 4

    # id: (slug, name)
    BP_META: Final[dict[str, tuple[str, str]]] = {
        "me": ("ME", "Size"),
        "beme": ("BE-ME", "Book-to-Market"),
        "bm": ("BE-ME", "Book-to-Market"),  # alias of beme
        "op": ("OP", "Operating Profitability"),
        "inv": ("INV", "Investment"),
        "ep": ("E-P", "Earnings-to-Price"),
        "cfp": ("CF-P", "Cash Flow-to-Price"),
        "dp": ("D-P", "Dividend Yield"),
        "prior": ("Prior_2-12", "Prior Returns"),
        "mom": ("Prior_2-12", "Prior Returns"),  # alias of prior
    }

    _MONTH_ONLY: Final[frozenset[str]] = frozenset({"me", "prior", "mom"})

    def __init__(self, factor: str = "me", **kwargs: Any) -> None:
        """Initialise the breakpoint loader.

        Args:
            factor: Breakpoint identifier. Must be one of
                `BP_META.keys()`. Defaults to `me`.
            **kwargs: Forwarded to `Base`. If ``frequency`` is
                omitted it is set to the factor's only supported
                value.

        Raises:
            ValueError: If `factor` is unrecognised, or if an
                explicit ``frequency`` is not the value supported by
                the factor.
        """
        self.factor = factor.strip().lower()
        if self.factor not in self.BP_META:
            raise ValueError(
                f"Invalid breakpoint factor '{factor}'. "
                f"Options: {list(self.BP_META.keys())}"
            )

        # Use the factor's frequency when frequency hasn't be specified.
        expected = "m" if self.factor in self._MONTH_ONLY else "y"
        kwargs.setdefault("frequency", expected)
        super().__init__(**kwargs)

        if self.frequency != expected:
            raise ValueError(
                f"Factor '{self.factor}' only supports "
                f"frequency='{expected}'; got '{self.frequency}'."
            )

    @property
    def name(self) -> str:
        _, display_name = self.BP_META[self.factor]
        return f"Fama/French - Breakpoints - {display_name}"

    @property
    def frequencies(self) -> list[str]:
        """Supported frequencies for this breakpoint factor."""
        return ["m"] if self.factor in self._MONTH_ONLY else ["y"]

    @property
    def url(self) -> str:
        """Download URL for the current factor and frequency."""
        base_url = (
            "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"
        )
        slug, _ = self.BP_META[self.factor]
        suffix = "_Daily_CSV.zip" if self.frequency == "d" else "_CSV.zip"
        return f"{base_url}/{slug}_Breakpoints{suffix}"

    @property
    def schema(self) -> pa.Schema:
        """Arrow schema for the parsed table.

        Factors whose sort variable can be < 0 have two count cols.
        """
        if self._data is not None:
            return self._data.schema

        percentiles = [str(x) for x in range(5, 101, 5)]

        # Sort variables that can be <= 0 get a split count row.
        if self.factor in ("bm", "beme", "ep", "dp", "cfp"):
            raw_fields = [
                ("n_le_0", pa.int64()),
                ("n_gt_0", pa.int64()),
            ]
        else:
            raw_fields = [("n", pa.int64())]

        raw_fields.extend([(pct, pa.float64()) for pct in percentiles])

        fields = [("date", pa.date32())] + [
            (f"{self.factor}_{name}", type) for name, type in raw_fields
        ]
        return pa.schema(fields)

    def _read(self, data: bytes) -> pa.Table:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            with z.open(z.namelist()[0]) as f:
                raw_text = f.read().decode("utf-8", errors="ignore")

        # skip, capture from the first date-like line
        # until blank line or the copyright footer.
        clean_lines: list[str] = []
        capturing = False
        for line in raw_text.splitlines():
            stripped = line.strip()
            if not capturing:
                if stripped and stripped[0].isdigit() and "," in stripped:
                    capturing = True
            if capturing:
                if not stripped or "Copyright" in line:
                    break
                clean_lines.append(line)

        cleaned_csv = "\n".join(clean_lines)

        # 22 (one 'n'), or 23 (<=0, >0) cols
        first_row = clean_lines[0].split(",")
        percentiles = [str(x) for x in range(5, 101, 5)]
        if len(first_row) == 23:
            raw_cols = ["n_le_0", "n_gt_0"] + percentiles
        else:
            raw_cols = ["n"] + percentiles

        column_names = ["date"] + [f"{self.factor}_{c}" for c in raw_cols]

        read_options = pv.ReadOptions(
            autogenerate_column_names=False,
            column_names=column_names,
        )
        parse_options = pv.ParseOptions(
            delimiter=",",
            ignore_empty_lines=True,
            invalid_row_handler=lambda row: "skip",
        )

        # counts are integers, everything else is float.
        # the final .cast(self.schema) normalizes int32 to int64.
        col_types: dict[str, pa.DataType] = {"date": pa.string()}
        for c in column_names[1:]:
            col_types[c] = (
                pa.int32() if c.endswith("_n") or "_n_" in c else pa.float64()
            )

        convert_options = pv.ConvertOptions(column_types=col_types)

        table = pv.read_csv(
            io.BytesIO(cleaned_csv.encode("utf-8")),
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )

        # drop malformed/null rows
        valid_mask = pc.is_valid(table[column_names[1]])
        table = table.filter(valid_mask)

        # NOTE: some annual periods are June-June, so offset_period_eom
        # rolls '2020' to 2020-12-31 rather than ~2020-06-30.
        # TODO: possible param on offset (fiscal_year bool or a month int)
        table = offset_period_eom(table, self.frequency)

        return table.cast(self.schema).combine_chunks()
