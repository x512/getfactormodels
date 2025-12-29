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
import pyarrow as pa
import pyarrow.compute as pc
from python_calamine import CalamineWorkbook
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.data_utils import offset_period_eom


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

    def __init__(self, frequency: str = 'm', *, cache_ttl: int = 43200, country_code: str = 'usa', **kwargs):
        self.country_code = country_code.upper()
        self.cache_ttl = cache_ttl
        super().__init__(frequency=frequency, cache_ttl=cache_ttl, **kwargs) 
        # TESTING, ROUGH
        self.country_code = country_code.upper() # 'USA', 'AUS', 'JPN', etc.

    @property 
    def _precision(self) -> int:
        return 10 

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
        headers = [str(h).strip() for h in rows[18]]
        data_rows = rows[19:]
        
        try:
            col_idx = headers.index(self.country_code)
        except ValueError:
            # Fallback to USA or raise error if the region doesn't exist in this tab
            col_idx = self.country_code.index('USA') if 'USA' in headers else 1

        dates, values = [], []
        # TODO: countries
        # Looking at the dataset, can extend FF's region param fully into HML Devil!!
        # AUS  AUT	BEL	CAN	CHE	DEU	DNK	ESP	FIN	FRA	GBR	GRC	HKG	IRL	
        #   ISR	ITA	JPN	NLD	NOR	NZL	PRT	SGP	SWE	USA
        #
        # v rough, implemented, must be one of above, not user friendly at all. 
        # Aggregates won't work as uppercase... 
        # TODO: PROPERLY! with ff's regions
        for r in data_rows:
            if not r or r[0] is None or r[col_idx] == '': 
                continue # skip rows where USA has no data? Nans?
                
            dates.append(self._aqr_dt_fix(r[0]))
            values.append(float(r[col_idx]))  # NOTE: EVERY FACTOR GETS PREPENDED WITH {country_code}_ AND RF BECOMES 1M_US_TBILL (KEEP AS RF FOR NOW)

        return pa.Table.from_pydict({"date": dates, sheet_name: values})
    
    def _read(self, data: bytes) -> pa.Table:
        wb = CalamineWorkbook.from_filelike(io.BytesIO(data))
        
        sheet_map = {
            'HML Devil': 'HML_Devil',
            'MKT': 'Mkt-RF',
            'SMB': 'SMB_AQR',
            'UMD': 'AQR_UMD',
            'RF': 'AQR_RF',
        }

        # Extract and align
        tables = []
        for sheet in sheet_map:
            t = self._process_sheet(sheet, wb)
            t = offset_period_eom(t, self.frequency)
            tables.append(t)

        # 'left outer' join using HML Devil col 
        table = tables[0]
        for t in tables[1:]:
            table = table.join(t, keys='date', join_type='left outer')
            # consolidate memory after join
            table = table.combine_chunks() 

        #  sort after outer joins
        table = table.sort_by([(table.schema.names[0], "ascending")])

        # Rename and prefix if country_code
        is_intl = self.country_code not in ['US', 'USA', 'us', 'usa', None]
        prefix = f"{self.country_code}_" if is_intl else ""
        
        new_names = []
        for col in table.column_names:
            if col == 'date':
                new_names.append('date')
                continue
            
            base_name = sheet_map.get(col, col)

            if base_name in ['RF', 'AQR_RF']:
                new_names.append(base_name)
            else:
                new_names.append(f"{prefix}{base_name}")

        table = table.rename_columns(new_names)

        # Rounding here 
        # fix: _precision for factors, but 4 for RF.
        for col_name in table.column_names:
            if col_name == 'date':
                continue
            
            idx = table.schema.get_field_index(col_name)
            prec = 4 if col_name in ['RF', 'AQR_RF'] else self._precision
            
            rounded_col = pc.round(table.column(idx), prec)
            table = table.set_column(idx, col_name, rounded_col)

        return table
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
