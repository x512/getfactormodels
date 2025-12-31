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
- BABFactors - Betting against beta
- QMJFactors - Quality Minus Junk
"""

import io
from abc import ABC, abstractmethod
import pyarrow as pa
from python_calamine import CalamineWorkbook
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.data_utils import (
    aqr_dt_fix,
    offset_period_eom,
    rearrange_columns,
    round_to_precision,
)


class _AQRModel(FactorModel):
    """
    Abstract base class for AQR's factor models.
    
    This subclass handles parsing the AQR Excel workbook with calamine, 
    validates the country param, and has a getter/setter for country.
    
    - Models using this base: BABFactors, HMLDevilFactors, QMJFactors.
    
    Notes 
    - These models are slow to download. Daily datasets are 20-30 MB each,
    and the download is rate limited.
    - AQR datasets are provided as Excel workbooks. Each factor is found on 
    a different sheet. All sheets except 'RF' contain 24 countries and 5 
    aggregate equity portfolios.
    
    """
   # TODO: Progress bar for AQR models!
    def __init__(self, frequency: str = 'm', *, cache_ttl: int = 86400, country: str = 'usa', **kwargs):
        self.cache_ttl = cache_ttl
        self.country = country
        self._validate_country(country) #will fix casing
        super().__init__(frequency=frequency, cache_ttl=cache_ttl, **kwargs)

    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m"]

    @property
    def _precision(self) -> int:
        return 8

    @property
    @abstractmethod
    def sheet_map(self) -> dict:
        """Mapping of Excel sheet names to internal factor names."""
        pass
    
    # country getter/setter: TODO: region for FF, when decided if just 'region' param...
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

    def _validate_country(self, value: str) -> str:
        """Validates and returns the standardized country string."""
        if value is None:
            return 'USA'
            
        requested = str(value).strip().upper()
        valid = [
            'AUS', 'AUT', 'BEL', 'CAN', 'CHE', 'DEU', 'DNK', 'ESP', 
            'FIN', 'FRA', 'GBR', 'GRC', 'HKG', 'IRL', 'ISR', 'ITA', 
            'JPN', 'NLD', 'NOR', 'NZL', 'PRT', 'SGP', 'SWE', 'USA',
            'EUROPE', 'NORTH AMERICA', 'PACIFIC', 'GLOBAL', 'GLOBAL EX USA'
        ]

        if requested in valid:
            return requested
        
        raise ValueError(f"Unsupported country/region: '{value}'. "
                         f"Must be one of: {valid}")

    
    def _process_sheet(self, sheet_name: str, wb: CalamineWorkbook) -> pa.Table:
        rows = wb.get_sheet_by_name(sheet_name).to_python()
        header_row = None
       
        # Finds the header row (contains 'DATE'). One row was 17, others 18.
        for i, row in enumerate(rows):
            if row and str(row[0]).strip().upper() == 'DATE':
                header_row = i
                break

        if header_row is None:
            raise ValueError(f"Couldn't find the header row")
        
        headers = [str(h).strip().upper() for h in rows[header_row]]
        data_rows = rows[header_row + 1:]

        if sheet_name == 'RF':
            col_idx = 1
        else:
            country_col = self.country.upper() if self.country not in [None, 'USA'] else 'USA'
            
            if country_col in headers:
                col_idx = headers.index(country_col)
            else: # fix: Don't return col 1 (AUS in factors!!)... raise error instead.
                raise ValueError(f"'{country_col}' not found.")

        dates, values = [], []
        for r in data_rows:
            if not r or r[0] is None or r[col_idx] == '': 
                continue # skip empty rows/rows without a date 
                
            dt_val = aqr_dt_fix(r[0])
            val = float(r[col_idx])

            dates.append(dt_val)
            values.append(val) # NOTE: EVERY FACTOR GETS PREPENDED WITH {country}_

        return pa.Table.from_pydict({"date": dates, sheet_name: values})


    def _read(self, data: bytes) -> pa.Table:
        wb = CalamineWorkbook.from_filelike(io.BytesIO(data))
        tables = []
        prefix = f"{self.country}_" if self.country != 'USA' else ""

        for sheet, col_name in self.sheet_map.items():
            t = self._process_sheet(sheet, wb)
            t = offset_period_eom(t, self.frequency)
            
            # Handle naming: date/RF stay same, others get country prefix
            final_col = col_name if col_name in ['RF', 'AQR_RF'] else f"{prefix}{col_name}"
            tables.append(t.rename_columns(['date', final_col]))

        # Joins all sheets on 'date'
        result = tables[0]
        for next_t in tables[1:]:
            # left outer because it uses the factor each model's named for, retreives
            # its full data, and doesn't filter out any other NaNs.
            result = result.join(next_t, keys='date', join_type='left outer')

        _table = rearrange_columns(result)
        table = round_to_precision(_table, self._precision)
            
        return table.combine_chunks()


class HMLDevilFactors(_AQRModel):
    """Download the HML Devil factors from AQR.com.

    HML Devil factors of C. Asness and A. Frazzini (2013)

    Args
        frequency (str): The frequency of the data. M, D (default: M)
        start_date (str, optional): The start date of the data, YYYY-MM-DD.
        end_date (str, optional): The end date of the data, YYYY-MM-DD.
        output_file (str, optional): The filepath to save the output data.

    References
    - C. Asness and A. Frazzini, ‘The Devil in HML’s Details’, The Journal of Portfolio 
    Management, vol. 39, pp. 49–68, 2013.
    ---
    NOTES:
    - Data contains leading NaNs.
    - Mkt-RF, SMB_AQR and UMD all start in ~1990. HML_Devil 
        starts 1926-07, and RF begins 1926-08-02.
    TODO: smarter caching for HML Devil download.
    TODO: progress bar for AQR (defeated)

   """
    @property
    def sheet_map(self):
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


# NEW
class QMJFactors(_AQRModel):
    """Quality Minus Junk: Asness, Frazzini & Pedersen (2017).
    
    References
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
            ('UMD', pa.float64()),
            ('RF', pa.float64()),
        ])


    @property
    def sheet_map(self):
        return {'QMJ Factors': 'QMJ',
                'MKT': 'Mkt-RF',
                'SMB': 'SMB',
                'RF': 'RF'}

    def _get_url(self):
        f = 'Daily' if self.frequency == 'd' else 'Monthly'
        return f'https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/Quality-Minus-Junk-Factors-{f}.xlsx'

# NEW
class BABFactors(_AQRModel):
    """Betting Against Beta: A. Frazzini, L. Pedersen (2014)

    References
    - Frazzini, A. and Pedersen, L. Betting against beta,
      Journal of Financial Economics, 111, issue 1, p. 1-25, 2014.
    """
    @property
    def schema(self) -> pa.Schema:
        """Schema for HML Devil."""
        return pa.schema([  
            ('date', pa.string()),
            ('BAB Factors', pa.float64()),
            ('MKT', pa.float64()),
            ('SMB', pa.float64()),
            ('UMD', pa.float64()),
            ('RF', pa.float64()),
        ])
    
    @property
    def sheet_map(self):
        return {'BAB Factors': 'BAB',
                'MKT': 'Mkt-RF', 'SMB': 'SMB', 'RF': 'RF_AQR'}  #SMB_AQR?

    def _get_url(self):
        f = 'Daily' if self.frequency == 'd' else 'Monthly'
        return f'https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/Betting-Against-Beta-Equity-Factors-{f}.xlsx'


#aqr utils: 
# - aqr_dt_fix
# - need a progress bar for AQR d/ls only. 
# NOTE: Countries that == FF regions: JPN, USA 
# Regions that match FF regions: 
# global 
# Global Ex USA
#Europe 
#North America 
#Pacific #ex Japan? APEJ?
# No RF's per country provided
#List countries func needed
#Add error handling/validations
#Output to CLI then needs titles, FF and AQR specifics...

# TODO: return UMD, HML Devil (or HML FF) on all? Depends what models use.
