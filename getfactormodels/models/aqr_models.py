#!/usr/bin/env python3
# getfactormodels: A Python package to retrieve financial factor model data.
# Copyright (C) 2025 S. Martin <x512@pm.me>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""Module that provides a base class for AQR models.

- HMLDevilFactors() - HML Devil
- BABFactors() - Betting against beta
- QMJFactors() - Quality Minus Junk
"""
import io
from abc import ABC, abstractmethod
from typing import override
import pyarrow as pa
from python_calamine import CalamineWorkbook
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.arrow_utils import (
    rearrange_columns,
    round_to_precision,
)
from getfactormodels.utils.date_utils import offset_period_eom
from getfactormodels.utils.http_client import _HttpClient


class _AQRModel(FactorModel):
    """Abstract base class for AQR's factor models.

    This subclass handles parsing the AQR Excel workbook with calamine, 
    validates the country param, and has a getter/setter for country.

    - Models using this base: BABFactors, HMLDevilFactors, QMJFactors.

    Notes:
    - These models are slow to download. Daily datasets are 20-30 MB each,
    and the download is rate limited.

    """
    # TODO: cache_ttl improved for AQR, use file's last modified date in header. 
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m"]

    def __init__(self, frequency: str = 'm', *, cache_ttl: int = 86400, 
                 country: str = 'usa', **kwargs):
        self.cache_ttl = cache_ttl
        self.country = country
        self._validate_country(country) #will fix casing
        super().__init__(frequency=frequency, cache_ttl=cache_ttl, **kwargs)
        self.frequency = frequency

    @property
    def _precision(self) -> int:
        return 8

    @property
    def country(self) -> str:
        return self._country
    @country.setter
    def country(self, value: str):
        c = self._validate_country(value)
        if not hasattr(self, '_country') or self._country != c:
            self._country = c
            self._data = None  # Reset caches
            self._df = None
    
    @override
    def _download(self) -> bytes:
        with _HttpClient(timeout=15.0) as client:
            return client.stream(self._get_url(), self.cache_ttl, model_name=self.__class__.__name__)

    # NEW. TODO: FIXME: good enough for now.
    @classmethod
    def list_countries(cls) -> list[str]:
        """Returns the list of supported AQR countries/regions."""
        return [
            'AUS', 'AUT', 'BEL', 'CAN', 'CHE', 'DEU', 'DNK', 'ESP', 
            'FIN', 'FRA', 'GBR', 'GRC', 'HKG', 'IRL', 'ISR', 'ITA', 
            'JPN', 'NLD', 'NOR', 'NZL', 'PRT', 'SGP', 'SWE', 'USA',
            'EUROPE', 'NORTH AMERICA', 'PACIFIC', 'GLOBAL', 'GLOBAL EX USA',
        ]


    def _validate_country(self, value: str) -> str:
        """Standardizes and validates country input for AQR models."""
        if value is None or str(value).strip().lower() in ['', 'none', 'us']:
            return 'USA'

        requested = str(value).strip().upper()
        valid = self.list_countries() #class method

        if requested in valid:
            return requested

        msg = f"Unsupported country/region: '{value}'. \nMust be one of: {valid}"
        self.log.error(msg)
        raise ValueError(msg)


    def _aqr_dt_fix(self, d) -> str:
        """Fixes AQR's 'MM/DD/YYYY' format."""
        if isinstance(d, str) and '/' in d:
            m, day, y = d.split('/')
            return f"{y}{m.zfill(2)}{day.zfill(2)}"
        if hasattr(d, 'strftime'):
            # for the dt objects from Calamine
            return d.strftime("%Y%m%d")
        return str(d)


    def _process_sheet(self, sheet_name: str, wb: CalamineWorkbook) -> pa.Table:
        rows = wb.get_sheet_by_name(sheet_name).to_python()
        header_row = None
       
        # Finds the header row (contains 'DATE'). One row was 17, others 18.
        for i, row in enumerate(rows):
            if row and str(row[0]).strip().upper() == 'DATE':
                header_row = i
                break

        if header_row is None:
            msg = f"Error: Couldn't find a header row for sheet: '{sheet_name}')"
            self.log.error(msg)
            raise ValueError(msg)
        
        headers = [str(h).strip().upper() for h in rows[header_row]]
        data_rows = rows[header_row + 1:]

        if sheet_name == 'RF':
            col_idx = 1
        else:
            country_key = self.country.upper() if self.country else 'USA'
            
            if country_key not in headers:
                msg = f"'{country_key}' not found in {sheet_name} headers. Available: {headers}"
                self.log.error(msg)
                raise ValueError(msg)

            col_idx = headers.index(country_key)

        dates, values = [], []
        for r in data_rows:
            if not r or r[0] is None or r[col_idx] == '': 
                continue
                
            dates.append(self._aqr_dt_fix(r[0]))
            values.append(float(r[col_idx]))

        clean_factor_name = self._sheet_map.get(sheet_name, sheet_name)

        if clean_factor_name in ['RF', 'RF_AQR']:
            final_col_name = 'RF_AQR'
        elif self.country == 'USA':
            final_col_name = clean_factor_name
        else:
            # prepend all but RF with country
            final_col_name = f"{self.country}_{clean_factor_name}"

        return pa.Table.from_pydict({"date": dates, final_col_name: values})

    def _read(self, data: bytes) -> pa.Table:
        wb = CalamineWorkbook.from_filelike(io.BytesIO(data))
        tables = []

        for sheet in self._sheet_map.keys():
            t = self._process_sheet(sheet, wb)
            t = offset_period_eom(t, self.frequency)
            tables.append(t)
        # using left outer join on these models. Uses the factor the 
        #  model's named after. Gets full data for that factor, and only 
        #  filters that factors NaNs. 
        result = tables[0]
        for next_t in tables[1:]:
            result = result.join(next_t, keys='date', join_type='left outer')

        table = rearrange_columns(result)
        table = round_to_precision(table, self._precision)
        return table.combine_chunks()
    
    @property
    @abstractmethod
    def _sheet_map(self) -> dict:
        """Mapping sheet names to factor names."""
        pass


class HMLDevilFactors(_AQRModel):
    """Download the HML Devil factors from AQR.com.

    HML Devil factors of C. Asness and A. Frazzini (2013)

    Args:
        frequency (str): The frequency of the data. M, D (default: M)
        start_date (str, optional): The start date of the data, YYYY-MM-DD.
        end_date (str, optional): The end date of the data, YYYY-MM-DD.
        output_file (str, optional): The filepath to save the output data.

    References:
    - C. Asness and A. Frazzini, ‘The Devil in HML’s Details’, The Journal of Portfolio 
    Management, vol. 39, pp. 49–68, 2013.
    ---

    Notes:
    - Mkt-RF, SMB_AQR and UMD all start in ~1990. HML_Devil 
        starts 1926-07, and RF begins 1926-08-02.
    """
    #TODO: smarter caching!
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
    """Quality Minus Junk: Asness, Frazzini & Pedersen (2017).
    
    References:
    - Asness, Cliff S. and Frazzini, Andrea and Pedersen, Lasse Heje, 
      Quality Minus Junk (June 5, 2017). http://dx.doi.org/10.2139/ssrn.2312432
     
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
    """Betting Against Beta: A. Frazzini, L. Pedersen (2014).

    References:
    - Frazzini, A. and Pedersen, L. Betting against beta,
      Journal of Financial Economics, 111, issue 1, p. 1-25, 2014.

    """
    @property
    def schema(self) -> pa.Schema:
        """Schema for Betting Against Beta factor model.
        
        - see: Asness & Frazzini (2013): BAB model uses FF's 
        HML, SMB and UMD.
        """
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
                'MKT': 'Mkt-RF', 'SMB': 'SMB', 'HML FF': 'HML', 'RF': 'RF_AQR'}  #SMB_AQR?

    def _get_url(self):
        f = 'Daily' if self.frequency == 'd' else 'Monthly'
        return f'https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/Betting-Against-Beta-Equity-Factors-{f}.xlsx'

# NOTE: Countries that == FF regions: JPN, USA 
# Regions that match FF regions: global, Global Ex USA, Europe, North America, 
#Pacific if ex japan
#Add error handling/validations
#Output to CLI then needs titles, FF and AQR specifics...
