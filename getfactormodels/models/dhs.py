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
import io
from typing import Any
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pv
from getfactormodels.models.base import FactorModel
from getfactormodels.models.fama_french import FamaFrenchFactors
from getfactormodels.utils.utils import _process


class DHSFactors(FactorModel):
    # roughing in infos, not approp for docstr but need TODO a reliable
    # way of getting and setting these when more models are redone. Most
    # importantly the copyright/attribution info! TODO
    """Download the DHS Behavioural Factors.

    Downloads the Behavioural Factors of Kent Daniel, David Hirshleifer, and 
    Lin Sun (DHS). Data from 1972-07-01 until end of 2023.

    Args:
        `frequency` (`str`): The frequency of the data. `m` or `d` (default: `m`)
        `start_date` (`str, optional`): The start date of the data, `YYYY-MM-DD`
        `end_date` (`str, optional`): The end date of the data, `YYYY-MM-DD`
        `output_file` (`str, optional`): The filepath to save the output data.

    References:
    - Short and Long Horizon Behavioral Factors," Kent Daniel, David 
    Hirshleifer and Lin Sun, Review of Financial Studies, 2020, 33 (4):
    1673-1736.
    
    Data source: https://sites.google.com/view/linsunhome/
    """
    @property
    def _frequencies(self) -> list[str]:
        return ['d', 'm']

    SCHEMA = pa.schema([
        ('Date', pa.timestamp('ms')),
        ('FIN', pa.float64()),
        ('PEAD', pa.float64()),
    ])

    def __init__(self, frequency: str = 'm', **kwargs: Any) -> None:
        #self.frequency = frequency
        super().__init__(frequency=frequency, **kwargs)


    def _get_url(self) -> str:
        """Construct the Google Sheet URL for monthly or daily."""
        base_url = 'https://docs.google.com/spreadsheets/d/'

        if self.frequency == 'd':
            gsheet_id = '1lWaNCuHeOE-nYlB7GA1Z2-QQa3Gt8UJC'
            #info_id =
        else:
            gsheet_id = '1VwQcowFb5c0x3-0sQVf1RfIcUpetHK46'
            #info_sheet_id = '#gid=96292754'

        return  f'{base_url}{gsheet_id}/export?format=csv'


    def _read(self, data):
        """Reads the Mispricing factors CSV."""
        try:
            if self.frequency == 'd':
                col_names = ['Date', 'Year', 'Month', 'Day', 'FIN', 'PEAD']
            else:
                col_names = ['Date', 'PEAD', 'FIN'] #note FIN PEAD swap in m and d 
            # TEST: reading CSV with PyArrow
            # will be a CSV reader
            read_opts = pv.ReadOptions(skip_rows=1, 
                                       column_names=col_names)

            parse_opts = pv.ParseOptions(delimiter=',',
                                         ignore_empty_lines=True)

            convert_opts = pv.ConvertOptions(
                column_types=self.SCHEMA,
                timestamp_parsers=["%m/%d/%Y", "%Y%m"],
                include_columns=['Date', 'PEAD', 'FIN'], 
            )

            table = pv.read_csv(
                io.BytesIO(data),
                read_options=read_opts,
                parse_options=parse_opts,
                convert_options=convert_opts,
            )

            table = table.rename_columns(['date', 'PEAD', 'FIN'])

            # for debug, testing 
            initial_rows = table.num_rows
            table = table.drop_null()

            final_rows = table.num_rows
            rows_dropped = initial_rows - final_rows

            if rows_dropped > 0:
                msg = ("{rows_dropped} NaN rows dropped." # check
                    f"{initial_rows} -> {final_rows}).")
                self.log.debug(msg)
            else:
                msg = f"No NaNs detected, {final_rows} rows."
                self.log.debug(msg)


            # pandas line --------------------------------------------------- #
            df = table.to_pandas()  # LOSS OF DATA/ROUNDING ERROR TODO (sheet has more than 4 decimals)

            df = df.set_index('date')
            df.index.name = 'date' # Set the index name
            
            data = df * 0.01    # TODO: Decimal types possibly
            data = data.round(8) 
            # TODO: check every model, does it need decimalization?

            if self.frequency == "m":
                data.index = data.index + pd.offsets.MonthEnd(0) 
                # bad TODO, start of month, THEN get data THEN offset. TODO: After fama french

            # Move this into a function
            try:
                ffdata = FamaFrenchFactors(model="3", frequency=self.frequency,
                                           start_date=self.start_date, end_date=self.end_date)
                ff = ffdata.download()
                #ff = ff.round(4)
                data = pd.concat([ff["Mkt-RF"], data, ff["RF"]], axis=1)
                data = data.dropna(how="any")

            except NameError:
                print("Warning: _get_ff_factors function not found. Skipping FF merge.")

            return _process(data, self.start_date,
                            self.end_date, filepath=self.output_file)

        except (pa.ArrowIOError, pa.ArrowInvalid) as e:
            self.log.error("Reading or parsing failed: %s", e)
            empty_df = pd.DataFrame(columns=['Date', 'PEAD', 'FIN'], # type: ignore [reportArgumentType] 
                                    index=pd.DatetimeIndex([], name='date'))
            return empty_df
