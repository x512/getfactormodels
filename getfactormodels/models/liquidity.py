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
import pandas as pd
from getfactormodels.utils.utils import _process
from getfactormodels.models.base import FactorModel
from typing import Optional

class LiquidityFactors(FactorModel):
    """Download the Pastor-Stambaugh Liquidity factors from Robert F. Stambaugh's website.

    * Only available in monthly data.

    params:
        frequency (str): The data frequency. 'm'
        start_date (str, datetime):
        end_date (str, datetime):
        output_file
        
    Source:
    - L. Pastor and R. Stambaugh, ‘Liquidity Risk and Expected Stock Returns’, 
      Journal of Political Economy, vol. 111, no. 3, pp. 642–685, 2003.
    - Data source: https://finance.wharton.upenn.edu/~stambaug/liq_data_1962_2024.txt

    """
    def __init__(self, frequency: str = 'm', **kwargs: Any) -> None:

        if frequency.lower() != 'm':
            err_msg = f"Invalid frequency '{frequency}': Liquidity only available in monthly 'm'."
            raise ValueError(err_msg)

        super().__init__(frequency=frequency, **kwargs)

        if self.frequency != 'm':
            err_msg = ("Invalid frequency: Liquidity factors are only available in monthly.")
            raise ValueError(err_msg)
 
    def _get_url(self) -> str:
        #TODO: Backup data sources: https://research.chicagobooth.edu/-/media/research/famamiller/data/liq_data_1962_2024.txt')
        return 'https://finance.wharton.upenn.edu/~stambaug/liq_data_1962_2024.txt'

    def download(self):
        """Get the Liquidity factors"""
        _data = self._download() #in base_model
        data = self._read_csv(_data)
    
        return data 

    # Still old func stuff below here. Need to move some to base, but when consistent across models. 
    def _read_csv(self, data) -> pd.DataFrame:
        _data = data.decode('utf-8')
        data = io.StringIO(_data)

        # Note: headers are the last commented line in header.
        headers = [line[1:].strip().split('\t')
            for line in data.readlines() if line.startswith('%')][-1]

        # Fix: was losing first line of data
        data.seek(0)

        # read .csv
        data = pd.read_csv(data, sep='\\s+', names=headers, 
                           comment='%', index_col=0)

        data.index.name = 'date'  # Should make it all DATE
        data.index = data.index.astype(str)

        data = data.rename(columns={'Agg Liq.': 'AGG_LIQ',
                                    'Innov Liq (eq8)': 'INNOV_LIQ',
                                    'Traded Liq (LIQ_V)': 'TRADED_LIQ'})

        # -99.000... floats to NaN? Return the source data -99? (consistent with ff?)
        data['TRADED_LIQ'] = data['TRADED_LIQ'].replace(-99.000000, -999)
        #numpy for NaN handling?

        data.index = pd.to_datetime(data.index, 
                                    format='%Y%m') + pd.offsets.MonthEnd(0)  

        # some things...
        data = data.round(6)
        # Need to either consistently return start or end of month across all 
        #  models, and decide whether or not to use BDay start/end (depending 
        #  on what the source data does. Don't think any models have a 
        #  weekend/non bday? maybe?..)

        # Check for nans, warn nd return 

        return _process(data, self.start_date,
                        self.end_date, filepath=self.output_file)


