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
from typing import override
import pyarrow as pa
from python_calamine import CalamineWorkbook
from getfactormodels.models.base import (
    CompositeModel,
    FactorModel,
    RegionMixin,
)
from getfactormodels.utils.arrow_utils import (
    rearrange_columns,
    round_to_precision,
    select_table_columns,
)
from getfactormodels.utils.date_utils import offset_period_eom
from getfactormodels.utils.http_client import _HttpClient

_AQR_REGIONS = [
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
    def _frequencies(self) -> list[str]: return ["d", "m"]
    
    @property
    def _precision(self) -> int: return 8

    @property
    def _regions(self) -> list[str]:
        """List of supported AQR countries/regions."""
        return _AQR_REGIONS

    def __init__(self, frequency: str = 'm', cache_ttl: int = 14400, 
                 region: str = 'usa', **kwargs):
        """Initialize a AQR FactorModel.

        Args:
            frequency (str): The frequency of the data. M, D (default: M)
            start_date (str, optional): The start date of the data, YYYY-MM-DD.
            end_date (str, optional): The end date of the data, YYYY-MM-DD.
            output_file (str, optional): The filepath to save the output data.
            cache_ttl (str): cache time-to-live in seconds.
        """
        self.cache_ttl = cache_ttl
        super().__init__(frequency=frequency, cache_ttl=cache_ttl, **kwargs)
        self.frequency = frequency
        self.region = region


    @override
    def _download(self, client: _HttpClient | None = None) -> bytes:
        url = self._get_url()
        
        if client is not None:
            return client.stream(url, self.cache_ttl, model_name=self.__class__.__name__)
        # direct calls
        with _HttpClient(timeout=15.0) as new_client:
            return new_client.stream(url, self.cache_ttl, model_name=self.__class__.__name__)


    def _aqr_dt_fix(self, d) -> str:
        """Private helper to fix AQR's MM/DD/YYYY format."""
        # dt objects from calamine
        if hasattr(d, 'strftime'): 
            return d.strftime("%Y%m%d")
        
        if isinstance(d, str) and '/' in d:
            m, day, y = d.split('/')
            return f"{y}{m.zfill(2)}{day.zfill(2)}"
        
        return str(d)


    # New, let VME use it, and modularizes _process_sheet
    def _get_header_idx(self, rows: list[list]) -> int:
        """Finds row index containing 'DATE'."""
        for i, row in enumerate(rows):
            if row and str(row[0]).strip().upper() == 'DATE':
                return i
        raise ValueError(f"Could not find a header row containing 'DATE' in {self.__class__.__name__}")


    def _process_sheet(self, sheet_name: str, rows: list[list], header_row: int) -> pa.Table:
        """Extracts date and factor values from a single sheet."""
        headers = [str(h).strip().upper() for h in rows[header_row]]
        
        if sheet_name == 'RF':
            col_idx = 1
        else:
            try: # map region to its col in the sheet
                col_idx = headers.index(self.region.upper())
            except ValueError:
                msg = f"Region '{self.region}' not found in sheet '{sheet_name}'. Available: {headers}"
                raise ValueError(msg)

        dates, values = [], []
        for r in rows[header_row + 1:]:
            # Skip empty rows or footers 
            if not r or r[0] is None or r[col_idx] == '': 
                continue
            try:
                dates.append(self._aqr_dt_fix(r[0]))
                values.append(float(r[col_idx]))
            except (ValueError, TypeError):
                continue

        # Maps sheet name to the factor name
        name = self._sheet_map.get(sheet_name, sheet_name)
        col_name = 'RF_AQR' if name == 'RF' else name
        
        return pa.Table.from_pydict({"date": dates, col_name: values})


    def _read(self, data: bytes) -> pa.Table:
        wb = CalamineWorkbook.from_filelike(io.BytesIO(data))
        tables = []

        for sheet_name in self._sheet_map:
            rows = wb.get_sheet_by_name(sheet_name).to_python()
            headers = self._get_header_idx(rows)
            
            t = self._process_sheet(sheet_name, rows, headers)
            tables.append(offset_period_eom(t, self.frequency))
        
        # using left outer join on these models. Uses the factor the 
        #  model's named after. Gets full data for that factor, and only 
        #  filters that factors NaNs. 
        result = tables[0]
        for next_t in tables[1:]:
            result = result.join(next_t, keys='date', join_type='left outer')

        #table = rearrange_columns(result)   let base handle?
        return round_to_precision(result, self._precision).combine_chunks()

    @property
    @abstractmethod
    def _sheet_map(self) -> dict:
        pass


class HMLDevilFactors(_AQRModel):
    """HML Devil factors, Asness and Frazzini (2013).

    References:
        C. Asness and A. Frazzini (2013). The Devil in HML’s Details. 
        The Journal of Portfolio Management, vol. 39, pp. 49–68.
    """
    @property
    def _sheet_map(self):
        return {'HML Devil': 'HML_Devil',
                'MKT': 'Mkt-RF',
                'SMB': 'SMB',
                'UMD': 'UMD',
                'RF': 'RF'}

    def _get_url(self):
        f = 'daily' if self.frequency == 'd' else 'monthly'
        return f'https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/The-Devil-in-HMLs-Details-Factors-{f}.xlsx'

    @property
    def schema(self) -> pa.Schema:
        """Schema for HML Devil."""
        return pa.schema([  
            ('date', pa.string()),
            ('HML Devil', pa.float64()),
            ('MKT', pa.float64()),
            ('SMB', pa.float64()),
            ('UMD', pa.float64()),
            ('RF', pa.float64()),
        ])


class QMJFactors(_AQRModel):
    """Quality Minus Junk (QMJ), Asness, Frazzini & Pedersen (2019).
    
    References:
        C. Asness, A. Frazzini and L. Pedersen, 2019. Quality Minus Junk.
        Review of Accounting Studies 24, no. 1 (2019): 34-112.
    """
    @property
    def schema(self) -> pa.Schema:
        """Schema for QMJ."""
        return pa.schema([  
            ('date', pa.string()),
            ('QMJ Factors', pa.float64()),
            ('MKT', pa.float64()),
            ('SMB', pa.float64()),
            ('HML FF', pa.float64()),
            ('UMD', pa.float64()),
            ('RF', pa.float64()),
        ])

    @property
    def _sheet_map(self):
        return {'QMJ Factors': 'QMJ',
                'MKT': 'Mkt-RF',
                'SMB': 'SMB',
                'HML FF': 'HML',
                'UMD': 'UMD',
                'RF': 'RF'}

    def _get_url(self):
        f = 'Daily' if self.frequency == 'd' else 'Monthly'
        return f'https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/Quality-Minus-Junk-Factors-{f}.xlsx'


class BABFactors(_AQRModel):
    """Betting Against Beta (BAB) factors, Frazzini and Pedersen (2014).

    References:
        Frazzini, A. and Pedersen, L. (2014). Betting against beta. 
        Journal of Financial Economics, 111(1), 1-25.
    """
    @property
    def schema(self) -> pa.Schema:
        return pa.schema([  
            ('date', pa.string()),
            ('BAB Factors', pa.float64()),
            ('MKT', pa.float64()),
            ('SMB', pa.float64()),
            ('HML FF', pa.float64()),
            ('UMD', pa.float64()),
            ('RF', pa.float64()),
        ])
    
    @property
    def _sheet_map(self):
        return {'BAB Factors': 'BAB',
                'MKT': 'Mkt-RF', 
                'SMB': 'SMB',           # SMB_AQR?
                'UMD': 'UMD',
                'HML FF': 'HML',
                'RF': 'RF_AQR'} 

    def _get_url(self):
        f = 'Daily' if self.frequency == 'd' else 'Monthly'
        return f'https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/Betting-Against-Beta-Equity-Factors-{f}.xlsx'


# NEW: CompositeModel, not a subclass of _AQRModel. --------------------------------- #
class AQR6Factors(CompositeModel, RegionMixin):
    """AQR 6-Factor Model, Frazzini, Kabiller & Pederson (2018).

    This is the model used 'Buffett's Alpha'. The factors 
    are: Mkt-RF, SMB, HML, UMD, BAB, QMJ.

    References:
        Frazzini, Kabiller & Pedersen (2018). Buffett’s Alpha. 
        Financial Analysts Journal, 74:4, 35-55.
    """
    @property
    def _regions(self) -> list[str]: return _AQR_REGIONS

    @property
    def _frequencies(self) -> list[str]: return ['m', 'd']

    def __init__(self, frequency: str = 'm', region: str = 'usa', **kwargs):
        """Initialize the AQR 6-Factor Model."""
        super().__init__(frequency=frequency, **kwargs)
        self.region = region

    def _construct(self, client: _HttpClient) -> pa.Table:
        bab_t = BABFactors(frequency=self.frequency, region=self.region).load(client=client)
        qmj_t = QMJFactors(frequency=self.frequency, region=self.region).load(client=client)

        bab = select_table_columns(bab_t.data, ['BAB', 'Mkt-RF', 'SMB'])
        qmj = select_table_columns(qmj_t.data, ['QMJ', 'HML', 'UMD'])  # We use 'HML FF' on BAB and QMJ, renamed HML. AQR's data.

        # Renaming HML to HML_FF: user doesn't know if it's HML Devil otherwise.
        qmj = qmj.rename_columns([
            "HML_FF" if col == "HML" else col for col in qmj.column_names
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
        ])


# NEW/WIP/TESTING/ETC: Value and Momentum Everywhere. --------------------------------- #
# Returns the 'everywhere' VAL and MOM by defult.
class VMEFactors(_AQRModel):
    """Value and Momentum Everywhere: Asness, Moskowitz, and Pedersen (2013)."""
    @property
    @override # only AQR model not avail in daily.
    def _frequencies(self) -> list[str]:
        return ["m"]

    @property
    @override # different regions to other models.
    def _regions(self) -> list[str]:
        """Regions specific to the VME Excel layout."""
        return [ 
            'everywhere', 'all_equities', 'all_other', 
            'usa', 'uk', 'europe', 'japan',
        ]  # all_equities by default? us?


    @override  # workbook is different to the other models.
    def _read(self, data: bytes) -> pa.Table:
        wb = CalamineWorkbook.from_filelike(io.BytesIO(data))
        rows = wb.get_sheet_by_name("VME Factors").to_python()
        header_idx = self._get_header_idx(rows)
        _vme_map = {
            'everywhere': 1, 'all_equities': 3, 'all_other': 5,
            'usa': 7, 'uk': 9, 'europe': 11, 'japan': 13,
        }
        
        start_idx = _vme_map.get(self.region.lower(), 7)
        
        dates, val, mom = [], [], []
        # Use header_idx + 1 (or +2 if there are sub-headers)
        for r in rows[header_idx + 1:]:
            if not r or r[0] is None or r[start_idx] == '':
                continue
            
            dates.append(self._aqr_dt_fix(r[0]))
            val.append(float(r[start_idx]))
            mom.append(float(r[start_idx + 1]))

        table = pa.Table.from_pydict({"date": dates, "VAL": val, "MOM": mom})
        table = offset_period_eom(table, "m")

        return table.cast(self.schema).combine_chunks()


    @property
    def schema(self) -> pa.Schema:
        """Schema for Value and Momentum Everywhere (VME)."""
        return pa.schema([  
            ('date', pa.string()),  # forcing 'DATE' to lower here
            ('VAL', pa.float64()),  # note: actual column names: VALLS_VME_US90
            ('MOM', pa.float64()),  #                            MOMLS_VME_UK90
        ])
    
    @property
    def _sheet_map(self) -> dict: return {'VME Factors': 'VME'}

    def _get_url(self) -> str:
        f = 'Monthly'
        return f'https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/Value-and-Momentum-Everywhere-Factors-{f}.xlsx'
