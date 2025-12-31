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
# TODO: break this all up into models/*.py !
# TODO: httpx, a client class, model classes... (done done and done)
import io
from unittest import result
import pyarrow as pa
import pyarrow.compute as pc
from python_calamine import CalamineWorkbook
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.data_utils import (
    offset_period_eom,
    round_to_precision, rearrange_columns
)


class HMLDevilFactors(FactorModel):
    """Download the HML Devil factors from AQR.com.

    HML Devil factors of C. Asness and A. Frazzini (2013)

    Notes:
    - Slow. If download isn't cached, it takes a while.
    I see why I had a boolean return as series...
    full model for daily with no nans starts 1990-07-02, monthly 1990-07

    Parameters:
        frequency (str): The frequency of the data. M, D (default: M)
        start_date (str, optional): The start date of the data, YYYY-MM-DD.
        end_date (str, optional): The end date of the data, YYYY-MM-DD.
        output_file (str, optional): The filepath to save the output data.

    References:
        C. Asness and A. Frazzini, ‘The Devil in HML’s Details’, The Journal of Portfolio 
        Management, vol. 39, pp. 49–68, 2013.
    Data source: https//aqr.com
    ---
    NOTES:
    - Slow. If download hasn't been cached, it can take a while.
    - Data contains leading NaNs.
    - Mkt-RF, SMB_AQR and UMD all start in ~1990. HML_Devil 
        starts 1926-07, and RF begins 1926-08-02.
    TODO: smarter caching for HML Devil download.
    TODO: progress bar for AQR (defeated)
    TODO: check source...
    TODO: country code to region
    """
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m"]  # TODO: aqr d/m only? 

    def __init__(self, frequency: str = 'm', *, cache_ttl: int = 43200, country: str = 'usa', **kwargs):
        self.country = country.upper()
        self.cache_ttl = cache_ttl
        
        # TESTING, ROUGH
        self.country = country 
        self._validate_country() #will fix casing
        super().__init__(frequency=frequency, cache_ttl=cache_ttl, **kwargs) 

    @property 
    def _precision(self) -> int:
        return 8 

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


    def _get_url(self) -> str:
        file = 'daily' if self.frequency == 'd' else 'monthly'
        base_url = 'https://www.aqr.com/-/media/AQR/Documents/Insights/'
        return f'{base_url}Data-Sets/The-Devil-in-HMLs-Details-Factors-{file}.xlsx'
    
    def _validate_country(self):
        """Checks if the requested country/region is supported by AQR."""
        requested = str(self.country).strip().upper()
        valid = [
            # countries
            'AUS', 'AUT', 'BEL', 'CAN', 'CHE', 'DEU', 'DNK', 'ESP', 
            'FIN', 'FRA', 'GBR', 'GRC', 'HKG', 'IRL', 'ISR', 'ITA', 
            'JPN', 'NLD', 'NOR', 'NZL', 'PRT', 'SGP', 'SWE', 'USA',
            # regions. TODO: Merge with FF regions... 
            'EUROPE', 'NORTH AMERICA', 'PACIFIC', 'GLOBAL', 
            'GLOBAL EX USA'    
        ]

        if requested in valid:
            self.country = requested
        elif not self.country:
            self.country = 'USA'  #fallback to default

        else:
            raise ValueError(f"Unsupported country/region: '{self.country}'. "
                f"Must be one of: {valid}")
            

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
                
            dt_val = self._aqr_dt_fix(r[0])
            val = float(r[col_idx])

            dates.append(dt_val)
            values.append(val) # NOTE: EVERY FACTOR GETS PREPENDED WITH {country}_

        return pa.Table.from_pydict({"date": dates, sheet_name: values})
    

    def _read(self, data: bytes) -> pa.Table:
        wb = CalamineWorkbook.from_filelike(io.BytesIO(data))

        # sheet name : output name 
        sheet_map = {
            'HML Devil': 'HML_Devil',
            'MKT': 'Mkt-RF',           
            'SMB': 'SMB',          
            'UMD': 'UMD',          
            'RF': 'AQR_RF',                
        }

        tables = []
        prefix = f"{self.country}_" if self.country not in [None, 'USA'] else ""

        for sheet, col_name in sheet_map.items():
            try:
                t = self._process_sheet(sheet, wb)
                t = offset_period_eom(t, self.frequency)

                # prepend country to factors, not 'date' or 'RF' cols.
                if col_name in ['AQR_RF', 'RF', 'date']:
                    factor_name = col_name
                else:
                    factor_name = f"{prefix}{col_name}"
                
                t = t.rename_columns(['date', factor_name])                
                tables.append(t)
            
            except Exception as e:
                print(f"Warning: Could not process sheet {sheet}: {e}")

        # Join all tables on the 'date' column
        result_table = tables[0]
        for next_table in tables[1:]:
            result_table = result_table.join(next_table, keys='date', join_type='left outer') # left outer, uses HML Devil col
        
        # Sort and return
        _table = result_table.combine_chunks()

        # Schema ENFORCED?...
        table = rearrange_columns(_table)

        return table   #.sort_by([("date", "ascending")])

"""
Fama French Regions, AQR countries/aggregate equity portfolios
Global	
Global Ex USA	
Europe
North America	
Pacific
---
EQUITIES (Regions in FF):
USA
JPN
---
AUS	AUT	BEL	CAN	CHE	DEU	DNK	ESP	FIN	FRA	GBR	GRC	HKG	IRL	ISR	ITA	JPN	NLD	NOR	NZL	PRT	SGP	SWE	USA


No RF's per country provided
List countries needed
Add error handling/validations
Output to CLI then needs titles, FF and AQR specifics...
"""
