# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
"""AQR Capital Management factor models.

This module provides access to AQR Capital Management's public datasets. These 
models parse multi-sheet Excel workbooks using calamine, and provide regional 
data using the `RegionMixin()`.

Models: HMLDevilFactors, QMJFactors, BABFactors, VMEFactors, AQR6Factors.

"""
import io
from abc import ABC, abstractmethod
import pyarrow as pa
from python_calamine import CalamineWorkbook
from typing_extensions import override
from getfactormodels.models.base import Composite, FactorModel, RegionMixin
from getfactormodels.utils.arrow_utils import (
    round_to_precision,
    select_table_columns,
)
from getfactormodels.utils.date_utils import offset_period_eom
from getfactormodels.utils.http_client import _HttpClient

_AQR_REGIONS: list[str] = [
    'aus', 'aut', 'bel', 'can', 'che', 'deu', 'dnk', 'esp', 
    'fin', 'fra', 'gbr', 'grc', 'hkg', 'irl', 'isr', 'ita', 
    'jpn', 'nld', 'nor', 'nzl', 'prt', 'sgp', 'swe', 'usa',
    'europe', 'north america', 'pacific', 'global', 'global ex usa',
]

class _AQRModel(FactorModel, RegionMixin):
    """Base class for AQR factor models.

    Notes:
    - These models are slow to download. Daily datasets are 20-30 MB each,
    and the download is rate limited.
    """
    @property
    def frequencies(self) -> list[str]: return ["d", "m"]

    @property
    def _precision(self) -> int: return 8

    @property
    def regions(self) -> list[str]:
        """List of supported AQR countries/regions."""
        return _AQR_REGIONS

    def __init__(self, frequency: str = 'm', cache_ttl: int = 14400, 
                 region: str = 'usa', **kwargs):
        """Initialize a AQR FactorModel.

        Args:
            frequency (str): The frequency of the data. M, D (default: M)
            start_date (str, optional): The start date of the data, YYYY-MM-DD.
            end_date (str, optional): The end date of the data, YYYY-MM-DD.
            region (str, optional): region or country to return data for 
                excluding RF.
            output_file (str, optional): The filepath to save the output data.
            cache_ttl (str): cache time-to-live in seconds.
        """
        self.cache_ttl = cache_ttl
        super().__init__(frequency=frequency, cache_ttl=cache_ttl, **kwargs)
        self.region = region

    def _aqr_dt_fix(self, d) -> str:
        if hasattr(d, 'strftime'):
            return d.strftime("%Y%m%d")
        if isinstance(d, str) and '/' in d:
            m, day, y = d.split('/')
            return f"{y}{m.zfill(2)}{day.zfill(2)}"
        return str(d)

    def _get_header(self, rows: list[list]) -> int:
        for i, row in enumerate(rows):
            if row and str(row[0]).strip().upper() == 'DATE':
                return i
        raise ValueError(f"Header row 'DATE' not found in {self.__class__.__name__}")

    @override
    def _read(self, data: bytes | dict[str, bytes]) -> pa.Table:
        if isinstance(data, dict):
            raise TypeError(f"{self.__class__.__name__} expects bytes, received dict.")

        wb = CalamineWorkbook.from_filelike(io.BytesIO(data))
        tables = []

        for sheet_name, factor_name in self._sheet_map.items():
            rows = wb.get_sheet_by_name(sheet_name).to_python()
            header_row = self._get_header(rows)
            headers = [str(h).strip().upper() for h in rows[header_row]]

            col_idx = 1 if sheet_name == "RF" else headers.index(self.region.upper())

            dates, values = [], []
            for r in rows[header_row + 1:]:
                if not r or r[0] is None or r[col_idx] == '':
                    continue
                try:
                    dates.append(self._aqr_dt_fix(r[0]))
                    values.append(float(r[col_idx]))
                except (ValueError, TypeError):
                    continue

            col_key = 'RF_AQR' if factor_name == 'RF' else factor_name
            t = pa.Table.from_pydict({"date": dates, col_key: values})
            tables.append(offset_period_eom(t, self.frequency))

        result = tables[0]
        for next_t in tables[1:]:
            result = result.join(next_t, keys='date', join_type='left outer')

        return round_to_precision(result, self._precision).combine_chunks()


    @property
    def url(self) -> str:
        f = 'daily' if self.frequency == 'd' else 'Monthly'
        return f'https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/{self._file_slug}-{f}.xlsx'

    @property
    def schema(self) -> pa.Schema:
        """Dynamically build schema from date + sheet map values."""
        fields = [('date', pa.string())]
        for sheet_name, factor_name in self._sheet_map.items():
            col_key = 'RF_AQR' if factor_name == 'RF' else factor_name
            fields.append((col_key, pa.float64()))
        return pa.schema(fields)

    @property
    @abstractmethod
    def _sheet_map(self) -> dict[str, str]: pass

    _file_slug: str = ""


class HMLDevilFactors(_AQRModel):
    """HML Devil factors, Asness and Frazzini (2013)."""
    _file_slug = 'The-Devil-in-HMLs-Details-Factors'

    @property
    def _sheet_map(self) -> dict[str, str]:
        return {
            'HML Devil': 'HML_Devil',
            'MKT': 'Mkt-RF',
            'SMB': 'SMB',
            'UMD': 'UMD',
            'RF': 'RF',
        }


class QMJFactors(_AQRModel):
    """Quality Minus Junk (QMJ), Asness, Frazzini & Pedersen (2019)."""
    _file_slug = 'Quality-Minus-Junk-Factors'

    @property
    def _sheet_map(self) -> dict[str, str]:
        return {
            'QMJ Factors': 'QMJ',
            'MKT': 'Mkt-RF',
            'SMB': 'SMB',
            'HML FF': 'HML',
            'UMD': 'UMD',
            'RF': 'RF',
        }


class BABFactors(_AQRModel):
    """Betting Against Beta (BAB) factors, Frazzini and Pedersen (2014)."""
    _file_slug = 'Betting-Against-Beta-Equity-Factors'

    @property
    def _sheet_map(self) -> dict[str, str]:
        return {
            'BAB Factors': 'BAB',
            'MKT': 'Mkt-RF',
            'SMB': 'SMB',
            'UMD': 'UMD',
            'HML FF': 'HML',
            'RF': 'RF_AQR',
        }


class AQR6Factors(Composite, RegionMixin):
    """AQR 6-Factor Model, Frazzini, Kabiller & Pedersen (2018)."""
    def __init__(self, frequency: str = 'm', region: str = 'usa', **kwargs):
        super().__init__(frequency=frequency, **kwargs)
        self.region = region

    @property
    def components(self) -> list:
        return [
            BABFactors(frequency=self.frequency, region=self.region),
            QMJFactors(frequency=self.frequency, region=self.region),
        ]

    @property
    def regions(self) -> list[str]:
        return _AQR_REGIONS

    @property
    def frequencies(self) -> list[str]:
        return ['m', 'd']

    @override
    def _construct(self, client: _HttpClient) -> pa.Table:
        bab_inst, qmj_inst = self.components

        bab_inst.load(client=client)
        qmj_inst.load(client=client)

        bab = select_table_columns(bab_inst.data, ['BAB', 'Mkt-RF', 'SMB'])
        qmj = select_table_columns(qmj_inst.data, ['QMJ', 'HML', 'UMD', 'RF_AQR'])

        qmj = qmj.rename_columns([
            "HML_FF" if col == "HML" else col
            for col in qmj.column_names
        ])

        table = bab.join(qmj, keys='date').combine_chunks()
        return table.select(self.schema.names).cast(self.schema)

    @property
    def schema(self) -> pa.Schema:
        return pa.schema([
            ('date', pa.string()),
            ('Mkt-RF', pa.float64()),
            ('SMB', pa.float64()),
            ('HML_FF', pa.float64()),
            ('UMD', pa.float64()),
            ('BAB', pa.float64()),
            ('QMJ', pa.float64()),
            ('RF_AQR', pa.float64()),
        ])


class VMEFactors(_AQRModel):
    """Value and Momentum Everywhere: Asness, Moskowitz, and Pedersen (2013)."""
    _file_slug = 'Value-and-Momentum-Everywhere-Factors'
    _VME_COL_MAP = {
        'everywhere': 1, 'all_equities': 3, 'all_other': 5,
        'usa': 7, 'uk': 9, 'europe': 11, 'japan': 13,
    }

    @property
    def frequencies(self) -> list[str]:
        return ["m"]

    @property
    def regions(self) -> list[str]:
        return list(self._VME_COL_MAP.keys())

    @property
    def _sheet_map(self) -> dict[str, str]:
        return {'VME Factors': 'VME'}

    @property
    def schema(self) -> pa.Schema:
        return pa.schema([
            ('date', pa.string()),
            ('VAL', pa.float64()),
            ('MOM', pa.float64()),
        ])

    @override
    def _read(self, data: bytes | dict[str, bytes]) -> pa.Table:
        if isinstance(data, dict):
            raise TypeError(f"{self.__class__.__name__} expects bytes, received dict.")

        wb = CalamineWorkbook.from_filelike(io.BytesIO(data))
        rows = wb.get_sheet_by_name("VME Factors").to_python()
        header_idx = self._get_header(rows)
        start_idx = self._VME_COL_MAP.get(self.region.lower(), 7)

        dates, val, mom = [], [], []
        for r in rows[header_idx + 1:]:
            if not r or r[0] is None or r[start_idx] == '':
                continue
            dates.append(self._aqr_dt_fix(r[0]))
            val.append(float(r[start_idx]))
            mom.append(float(r[start_idx + 1]))

        table = pa.Table.from_pydict({"date": dates, "VAL": val, "MOM": mom})
        table = offset_period_eom(table, "m")

        return table.cast(self.schema).combine_chunks()

"""
class _AQRPortfolios(_AQRModel):
    @property
    @override
    def _frequencies(self) -> list[str]: return ["m"]

    @property
    @override
    def _regions(self) -> list[str]: return [ "us", "global"]

    def __init__(self, 
                 frequency: str = 'm', 
                 region: str = 'usa', 
                 sort: str | None = None #None, quality, size,quality, 3x5 
                 **kwargs):
            if sort == None:
                self.sort = 10
        if self.sort in ['3x5', '6']: #or contains 'size or me, and quality'
                self.sort = 6
        # if self.sort != 6 or 10, we have a problem...
        super().__init__(frequency=frequency, region=region, weights='vw', **kwargs)


"""

"""
## TEST 
class _QMJPortfolios(_AQRPortfolios):

    @property
    def _sheet_map(self):
    if self.sort == 10:
        return {'10 Portfolios Formed on Quality': 'QMJ_Portfolios'}
    return {'Size x Quality (2 x3)': 'QMJ_Portfolios'} 

    def _get_url(self):
        if self.sort == 6:
            https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/Quality-Minus-Junk-Six-Portfolios-Formed-on-Size-and-Quality-Monthly.xlsx
        return f'https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/Quality-Minus-Junk-10-QualitySorted-Portfolios-Monthly.xlsx'

"""
