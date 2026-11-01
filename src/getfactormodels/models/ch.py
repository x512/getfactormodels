# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
"""Size and Value in China (Liu, Stambaugh & Yuan, 2019)."""
import io
from typing import ClassVar, override
import httpx
import pyarrow as pa
from python_calamine import CalamineWorkbook
from getfactormodels.models.base import FactorModel, RegionMixin
from getfactormodels.utils.date_utils import offset_period_eom


class CHFactors(FactorModel, RegionMixin):
    """Size and Value in China of Liu, Stambaugh & Yuan (2019).

    Monthly and daily data for the CH-3 (SMB, VMG) and CH-4 
    (SMB, VMG, PMO) models, with market excess returns and risk-free rate
    for China.

    References:
        - Liu, J., Stambaugh, R. F., & Yuan, Y. (2019). Size and value in 
          China. Journal of Financial Economics, 134(1), 48–69.
    """
    regions: ClassVar[list[str]] = ["china", "ch", "cn"]

    def __init__(
        self,
        model: int | str = "4",
        frequency: str = "m",
        *,
        region: str | None = "cn",
        **kwargs,
    ):
        """Initialize a Size and Value in China model.

        Args:
            model (int | str): Model variant, either '3' or '4'. Default: '4'.
            frequency (str): Data frequency, 'm' (monthly) or 'd' (daily). Default: 'm'.
            region (str, optional): Region code, restricted to China.
            **kwargs: Additional parameters passed to base model.
        """
        model_str = "".join(c for c in str(model).lower() if c.isdigit())
        if model_str not in ["3", "4"]:
            raise ValueError(f"Invalid model ('{model}'), must be 3 or 4.")

        super().__init__(model=model_str, frequency=frequency, **kwargs)
        self.region = region or "cn"

    @property
    def frequencies(self) -> list[str]:
        """Available frequencies for Size & Value in China."""
        return ["d", "m"]

    @property
    def _precision(self) -> int:
        return 4

    @property
    def schema(self) -> pa.Schema:
        """Schema for Size and Value in China model."""
        is_daily = self.frequency == "d"

        fields = [
            ("date" if is_daily else "mnthdt", pa.string()),
            ("mktrf", pa.float64()),
            ("rf_dly" if is_daily else "rf_mon", pa.float64()),
            ("SMB", pa.float64()),
            ("VMG", pa.float64()),
        ]

        if self.model == "4":
            fields.append(("PMO", pa.float64()))

        return pa.schema(fields)

    @property
    def url(self) -> str:
        """Size and Value in China source URL."""
        base_url = "https://en.mingshiim.com/static/"
        _freq = "monthly" if self.frequency == "m" else "daily"
        return f"{base_url}CH{self.model}_factors_{_freq}_202608.xlsx"

    # FIXME: SSL bypass temporarily required for Mingshi site
    @override
    def _download(self, client=None) -> bytes:
        """Download using httpx with SSL bypass."""
        self.log.warning("CHFactors requires SSL bypass.")

        url = self.url
        self.log.info("Downloading CHFactors from %s", url)

        with httpx.Client(verify=False, follow_redirects=True) as http_client:
            resp = http_client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            return resp.content

    def _read(self, data: bytes | dict[str, bytes]) -> pa.Table:
        if isinstance(data, dict): # only for pyright
            raise TypeError("bytes expected. Received a dict.")

        workbook = CalamineWorkbook.from_filelike(io.BytesIO(data))
        rows = workbook.get_sheet_by_index(0).to_python()
        if not rows:
            raise ValueError("Excel sheet is empty.")

        # parse headers (preserve index alignment even if None)
        raw_headers = rows[0]
        headers = [str(h).strip() if h is not None else f"col_{i}" for i, h in enumerate(raw_headers)]
        data_rows = rows[1:]

        table_data = {}
        for i, name in enumerate(headers):
            col_values = []
            for row in data_rows:
                val = row[i] if i < len(row) else None
                # Normalize empty values to None
                col_values.append(None if val in ("", "None", None) else val)
            table_data[name] = col_values

        table = pa.Table.from_pydict(table_data)
        table = table.select(self.schema.names).cast(self.schema)
        table = offset_period_eom(table, self.frequency)

        rename_map = {
            "mktrf": "MKTRF_CH",
            "mnthdt": "date",
            "rf_mon": "RF_CH",
            "rf_dly": "RF_CH",
            "rf": "RF_CH",
        }
        new_names = [rename_map.get(name.lower(), name) for name in table.column_names]
        table = table.rename_columns(new_names)

        if "RF_CH" in table.column_names:
            cols = [c for c in table.column_names if c != "RF_CH"] + ["RF_CH"]
            table = table.select(cols)

        return table.combine_chunks()
