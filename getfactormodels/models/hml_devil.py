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
from typing import Any
import pandas as pd
from getfactormodels.models.base import FactorModel
import pyarrow as pa
import pyarrow.compute as pc
from python_calamine import CalamineWorkbook
from getfactormodels.utils.utils import _offset_period_eom


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

    Returns:
        pd.DataFrame: the HML Devil model data indexed by date.

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
    """
    @property
    def _frequencies(self) -> list[str]:
        return ["d", "m"]  # TODO: aqr d/m only? 

    def __init__(self, frequency: str = 'm', cache_ttl: int = 43200, **kwargs: Any) -> None:
        self.cache_ttl = cache_ttl
        super().__init__(frequency=frequency, cache_ttl=cache_ttl, **kwargs)

    def _get_url(self) -> str:
        base_url = 'https://www.aqr.com/-/media/AQR/Documents/Insights/'
        file = 'daily' if self.frequency == 'd' else 'monthly'

        return f'{base_url}Data-Sets/The-Devil-in-HMLs-Details-Factors-{file}.xlsx'

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

    def _read(self, data: bytes) -> pa.Table:
        """Reads the HML Devil XLSX"""
        workbook = CalamineWorkbook.from_filelike(io.BytesIO(data))
        sheets = {0: 'HML Devil', 4: 'MKT', 5: 'SMB', 7: 'UMD', 8: 'RF'}

        table_list = []

        for idx, sheet_name in sheets.items():
            rows = workbook.get_sheet_by_name(sheet_name).to_python()

            # AQR headers: 18, data: 19-
            headers = [str(h).strip() for h in rows[18]]
            data_rows = rows[19:]

            col_idx = headers.index('USA') if 'USA' in headers else 1

            dates, values = [], []
            for row in data_rows:
                if not row or row[0] is None: continue

                # AQR date format = MM/DD/YYYY... 
                # Makes a YYYYMMDD string to _offset_period_eom
                d = row[0]
                if isinstance(d, str) and '/' in d:
                    m, day, y = d.split('/')
                    date_val = f"{y}{m.zfill(2)}{day.zfill(2)}"
                elif hasattr(d, 'strftime'):
                    date_val = d.strftime("%Y%m%d")
                else:
                    date_val = str(d)

                # normalization
                val = row[col_idx]
                if not isinstance(val, (int, float)): continue

                dates.append(date_val)
                values.append(float(val))

            temp_table = pa.Table.from_pydict({"date": dates, sheet_name: values})

            temp_table = _offset_period_eom(temp_table, self.frequency)

            table_list.append(temp_table)

        final_table = table_list[0]

        for next_t in table_list[1:]:
            final_table = final_table.join(next_t, keys="date")

        final_table = final_table.cast(self.schema)

        rename_map = {'MKT': 'Mkt-RF', 
                      'HML Devil': 'HML_Devil', 
                      'SMB': 'SMB_AQR'}

        renames = ['date' if n == 'Date' else rename_map.get(n, n) for n in final_table.column_names]

        table = final_table.rename_columns(renames)
        print(table)

        # TODO: allow returning nulls? allow returning series?
        # HML Devil factor goes back to 1926? Return NaNs then for all others...
        #final_table = pc.drop_null(final_table)

        #return table 

        df = table.to_pandas().set_index('date')

        # --------------------------------------------------------------------------------#
        # Base will do this shortly...
        df.index = pd.to_datetime(df.index)
        #leading_nans_count = (~df.notna()).cumprod(axis=0).sum(axis=0)
        #print("DataFrame:")
        #print(df)
        #print("\nNumber of leading NaNs per row before leaving class:")
        #print(leading_nans_count)
        precision = 8 if self.frequency == 'd' else 4
        return df.round(precision)

        # From func - cache stuff -- OLD CACHE STUFF
        #self.cache_dir = Path('~/.cache/getfactormodels/aqr/hml_devil').expanduser()
        #self.cache_dir.mkdir(parents=True, exist_ok=True)
        #self.cache = dc.Cache(str(self.cache_dir)) # diskcache requires a string path
        # TODO: can see the last modified date in debug log; possibly set cache
        # according to this
