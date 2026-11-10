# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
"""This module provides access to JKP Global Factor data."""

import zipfile
from collections.abc import Sequence
from io import BytesIO
from typing import Any, Literal
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
from getfactormodels.models.base import FactorModel, RegionMixin
from getfactormodels.registry import JKP_FACTORS, JKP_REGIONS
from getfactormodels.utils.arrow_utils import join_on_date


class JKPFactors(RegionMixin, FactorModel):
    """Jensen, Kelly, Pedersen's Global Factor data."""
    regions = list(JKP_REGIONS) #tuple
    def __init__(
            self,
            factor: str | Sequence[str] | None = None,
            frequency: Literal['d', 'm'] = 'm',
            weight: Literal['vw_cap', 'vw', 'ew'] = 'vw_cap',
            region: str | Sequence[str] | None = 'usa',
            **kwargs: Any,
    ) -> None:
        """WIP.

        Args:
            factor (str):
            frequency (str):
            weight (str):
            region (str):
            **kwargs:
        Universe not implemented yet.
        """
        # Validation
        if isinstance(factor, str):
            self.factors = [factor.strip().lower()]
        elif isinstance(factor, Sequence):
            self.factors = [f.strip().lower() for f in factor]
        else:
            raise ValueError("Must specify either 'factor' or a list of factors.")

        # Check registry
        for f in self.factors:
            if f not in JKP_FACTORS:
                raise ValueError(f"Unknown factor '{f}'.")

        if region is None:
            raw_regions = ['usa']
        elif isinstance(region, str):
            raw_regions = [region]
        else:
            raw_regions = list(region)

        self._requested_regions = [self._resolve_region(r) for r in raw_regions]
        self.weight = weight.lower()

        super().__init__(frequency=frequency, **kwargs)

    def _resolve_region(self, region: str) -> str:
        """Resolve a single region alias."""
        val = str(region).strip().lower()
        resolved = val if val in self.regions else self._aliases.get(val, val)
        if resolved not in self.regions:
            raise ValueError(
                f"Region '{region}' not supported for {self.__class__.__name__}. "
                f"Available: {self.regions}"
            )
        return resolved

    def _resolve_theme(self, theme: str) -> str | list[str]:
        """Resolves a theme to its constituent factors."""
        ... #TODO:FIXME:

    @property
    def frequencies(self) -> list[str]:
        return ['d', 'm']

    @property
    def url(self) -> str | dict[str, str]:
        """Endpoint URLs for every requested (factor, region) pair."""
        freq_str = 'monthly' if self.frequency == 'm' else 'daily'
        url_map: dict[str, str] = {}

        for factor in self.factors:
            for region in self._requested_regions:
                key = f"{factor}_{region}"
                url_map[key] = (
                    "https://jkpfactors-data.s3.amazonaws.com/public/"
                    f"[{region}]_[{factor}]_[{freq_str}]_[{self.weight}].zip"
                )

        if len(url_map) == 1:
            return next(iter(url_map.values()))
        return url_map

    @property
    def schema(self) -> pa.Schema:
        """The strictly enforced schema for JKP datasets."""
        return pa.schema([
            ("location", pa.string()),
            ("name", pa.string()),
            ("freq", pa.string()),
            ("weighting", pa.string()),
            ("direction", pa.int8()),
            ("n_stocks", pa.int16()),
            ("n_stocks_min", pa.int16()),
            ("date", pa.date32()),
            ("ret", pa.float64()),
        ])

    def _read(self, data: bytes | dict[str, bytes]) -> pa.Table:
        """Turn raw ZIP bytes into a single, date‑aligned PyArrow table."""
        if isinstance(data, dict):
            tables: list[pa.Table] = []

            for key, zip_bytes in data.items():
                table = self._parse_zip_bytes(zip_bytes)
                # name is the factor label 
                factor_label = table.column("name")[0].as_py()

                # More than one region appends _{region} to 
                # col name.
                if len(self._requested_regions) > 1:
                    region = key.rsplit("_", 1)[-1]
                    col_name = f"{factor_label}_{region}"
                else:
                    col_name = factor_label

                projected = (
                    table.select(["date", "ret"])
                    .rename_columns(["date", col_name])
                )
                tables.append(projected)

            merged = tables[0]
            for next_table in tables[1:]:
                merged = join_on_date(
                    merged, next_table, join_type="full outer"
                )

            sorted_indices = pc.sort_indices(
                merged, sort_keys=[("date", "ascending")]
            )

            return merged.take(sorted_indices)

        # single factor, region
        table = self._parse_zip_bytes(data)
        factor_label = table.column("name")[0].as_py()
        return (
            table.select(["date", "ret"])
            .rename_columns(["date", factor_label])
        )

    def _parse_zip_bytes(self, zip_bytes: bytes) -> pa.Table:
        """Extract the first CSV from a ZIP archive and read it."""
        with zipfile.ZipFile(BytesIO(zip_bytes)) as z:
            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise FileNotFoundError("No CSV file found in JKP factor zip archive.")
            csv_content = z.read(csv_files[0])

        return pv.read_csv(
            BytesIO(csv_content),
            convert_options=pv.ConvertOptions(
                column_types=self.schema
            ),
        )
