# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
"""Fama-French (and Carhart) factor models implementation."""

import io
import zipfile
from abc import ABC, abstractmethod
from typing import Any
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
from getfactormodels.models.base import FactorModel, PortfolioBase, RegionMixin
from getfactormodels.utils.arrow_utils import (
    round_to_precision,
    scale_to_decimal,
)
from getfactormodels.utils.date_utils import offset_period_eom

_FF_BASE_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"

class FamaFrenchFactors(FactorModel, RegionMixin):
    """Download Fama-French (and Carhart) factor models.

    Downloads the 3-factor (1993), 5-factor (2015), or 6-factor (2018)
    model of Fama & French, or Carhart's (1997) 4-factor model.

    Weekly data only available for the 3-factor model.

    Args:
        model (str | int): model to return. '3' '4' '5' '6' (default: '3')
        frequency (str, optional): frequency of the data. 'd' 'm' 'y' 'w'
            (default: 'm').
        start_date (str, optional): start date of the data. YYYY[-MM-DD].
        end_date (str, optional): end date of the data. YYYY[-MM-DD].
        cache_ttl (int, optional): cached download time-to-live in seconds 
            (default: 86400).
        region (str, optional): return an international/emerging market 
            model.

    Reference:
        - E. F. Fama and K. R., 1993. French. Common risk factors in the returns 
        on stocks and bonds. Journal of Financial Economics, vol. 33, no. 1,
        pp. 3–56.
        
        - E. F. Fama and K. R. French, 2015. A five-factor asset pricing model. 
        Journal of Financial Economics, vol. 116, no. 1, pp. 1–22.
        
        - E. F. Fama and K. R. French, 2018. Choosing factors. Journal of 
        Financial Economics, vol. 128, no. 2, pp. 234–252, 2018.
    """
    @property
    def frequencies(self) -> list[str]:
        return ['d', 'w', 'm', 'y']

    @property
    def _precision(self) -> int:
        return 6

    regions = [
            'us', 'emerging', 'developed', 'ex-us', 'europe',
            'japan', 'asia-pacific-ex-japan', 'north-america',
        ]

    def __init__(
        self,
        frequency: str = 'm',
        model: int | str = '3',
        region: str | None = 'us',
        **kwargs: Any,
    ) -> None:
        model_str = str(model).lower().replace('ff', '').strip()
        kwargs.pop('model', None)
        super().__init__(model=model_str, frequency=frequency, **kwargs)

        self._model = model_str
        self.region = region
        self._validate_ff_input()

    @property
    def schema(self) -> pa.Schema:
        cols = [
            ("date", pa.string()),
            ("Mkt-RF", pa.float64()),
            ("SMB", pa.float64()),
            ("HML", pa.float64()),
        ]

        if self.model in ["5", "6"]:
            cols += [("RMW", pa.float64()), ("CMA", pa.float64())]

        if self.model in ["4", "6"]:
            is_intl = self.region not in ["us", None]
            mom_name = "WML" if is_intl else ("UMD" if self.model == "6" else "MOM")
            cols.append((mom_name, pa.float64()))
        cols.append(("RF", pa.float64()))

        return pa.schema(cols)

    @property
    def _mom_schema(self) -> pa.Schema:
        return pa.schema([("date", pa.string()), ("val", pa.float64())])

    @property
    def model(self) -> str:
        """Return Fama/French model identifier."""
        return self._model

    @model.setter
    def model(self, value: int | str) -> None:
        """Set and normalize the Fama-French model identifier."""
        val = str(value).lower().replace("ff", "").strip()
        if val not in ["3", "4", "5", "6"]:
            raise ValueError(f"Invalid Fama-French model '{value}'. Options: 3, 4, 5, 6")

        if hasattr(self, "_model") and val != self._model:
            self.log.info("Model changed from %s to %s", self._model, val)
            self._data = None
        self._model = val

    @property
    def _ff_region_map(self) -> dict[str, str]:
        return {
            'us': 'US',
            'emerging': 'Emerging',
            'developed': 'Developed',
            'ex-us': 'Developed_ex_US',
            'europe': 'Europe',
            'japan': 'Japan',
            'asia-pacific-ex-japan': 'Asia_Pacific_ex_Japan',
            'north-america': 'North_America',
        }

    def _validate_ff_input(self) -> None:
        if self.region != 'us' and self.frequency == 'w':
            raise ValueError(f"Weekly frequency not available for {self.region}.")
        if self.region == "emerging" and self.frequency != 'm':
            raise ValueError("Emerging markets only available in monthly.")
        if self.frequency == 'w' and self.model != "3":
            raise ValueError("Weekly Fama-French data is only available for the 3-factor model.")

    @property
    def url(self) -> dict[str, str]:
        url_list = {}
        ff_url_name = self._ff_region_map.get(self.region)

        if ff_url_name == 'Emerging':
            filename = "Emerging_5_Factors_CSV.zip"
        elif ff_url_name == 'US':
            base_model = "F-F_Research_Data_5_Factors_2x3" if self.model in {"5", "6"} else "F-F_Research_Data_Factors"
            suffix = {'d': '_daily', 'w': '_weekly'}.get(self.frequency, "")
            filename = f"{base_model}{suffix}_CSV.zip"
        else:
            base_model = '3' if self.model in ['3', '4'] else '5'
            daily = '_Daily' if self.frequency == 'd' else ''
            filename = f"{ff_url_name}_{base_model}_Factors{daily}_CSV.zip"

        url_list['factors'] = f"{_FF_BASE_URL}/{filename}"

        if self.model in ["4", "6"]:
            freq_suffix = "_daily" if self.frequency == 'd' else ""
            mom_filename = (
                f"F-F_Momentum_Factor{freq_suffix}_CSV.zip"
                if self.region in ['us', None]
                else f"{self.region}_Mom_Factor{freq_suffix.title()}_CSV.zip"
            )
            url_list['momentum'] = f"{_FF_BASE_URL}/{mom_filename}"

        return url_list

    def _read_zip(self, _data: bytes, use_schema: pa.Schema | None = None) -> pa.Table:
        _schema = use_schema if use_schema else self.schema

        with zipfile.ZipFile(io.BytesIO(_data)) as z:
            filename = z.namelist()[0]
            with z.open(filename) as f:
                lines = f.read().decode('utf-8').splitlines()

        is_mom = 'momentum' in filename.lower() or '_mom_' in filename.lower()
        annual_marker = next((i for i, l in enumerate(lines) if "annual factors:" in l.lower()), None)

        if self.frequency == 'y':
            raw_content = lines[annual_marker + 1:] if annual_marker is not None else lines
        else:
            _start = 6 if self.region == 'emerging' else (12 if is_mom else 4)
            _stop = annual_marker if annual_marker else len(lines)
            raw_content = lines[_start:_stop]

        content = [",".join(_schema.names)]
        for line in raw_content:
            clean = line.strip()
            if not clean or "copyright" in clean.lower():
                if "copyright" in clean.lower():
                    self.copyright = clean
                continue

            parts = [p.strip() for p in clean.split(',')]
            if parts and parts[0].isdigit():
                content.append(",".join(parts))

        return pv.read_csv(
            io.BytesIO("\n".join(content).encode('utf-8')),
            convert_options=pv.ConvertOptions(
                null_values=["-99.99", "-999", "-99.990", "-0.9999"],
                strings_can_be_null=True,
                column_types=_schema,
                include_columns=_schema.names,
            ),
        )

    def _read(self, data: dict[str, bytes]) -> pa.Table:
        main_cols = [f for f in self.schema if f.name not in ["UMD", "MOM", "WML"]]

        if self.region == 'emerging' and self.model in ["3", "4"]:
            main_cols.insert(4, pa.field("RMW", pa.float64()))
            main_cols.insert(5, pa.field("CMA", pa.float64()))

        main_schema = pa.schema(main_cols)
        table = self._read_zip(data['factors'], use_schema=main_schema)

        if 'momentum' in data:
            mom_table = self._read_zip(data['momentum'], use_schema=self._mom_schema)
            mom_key = next(k for k in ["UMD", "MOM", "WML"] if k in self.schema.names)
            mom_table = mom_table.rename_columns(["date", mom_key])
            table = table.join(mom_table, keys="date", join_type="inner").combine_chunks()

        table = table.set_column(0, "date", table.column(0).cast(pa.string()))
        table = offset_period_eom(table, self.frequency)
        table = scale_to_decimal(table)
        table = round_to_precision(table, self._precision)

        return table.select(self.schema.names).combine_chunks()

# FF Portfolio Factory Function
def get_ff_portfolios(
    factors: str | None = None,
    sort: str | None = None,
    industries: int | None = None,
    weights: str = 'vw',
    frequency: str = 'm',
    dividends: bool = True,
    **kwargs: Any,
):
    from .ff_industries import FamaFrenchIndustryPortfolios
    from .ff_portfolios import FamaFrenchSorts

    if industries is not None:
        return FamaFrenchIndustryPortfolios(
            industries=industries,
            frequency=frequency, 
            dividends=dividends,
            **kwargs,
        )
        
    return FamaFrenchSorts(
        factors=factors or "size",
        sort=sort or "10",
        frequency=frequency, 
        weights=weights, 
        dividends=dividends,
        **kwargs,
    )
