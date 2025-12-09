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
from getfactormodels.http_client import HttpClient
from getfactormodels.utils.utils import _process

### NOTE: ISSUE: TODO: FIXME: the first 65 NaN values are 0.00
class LiquidityFactors:
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
    def __init__(self, frequency='m', start_date=None, end_date=None,
                 output_file=None, cache_ttl=86400): #monthly data, daily cache for now (need util to find if its near end of month etc
        self.frequency = frequency.lower()

        if self.frequency != 'm':
            err_msg = "Frequency must be 'm'."
            print('Liquidity factors are only available for monthly frequency.')
            raise ValueError(err_msg)

        self.start_date = start_date
        self.end_date = end_date
        self.output_file = output_file
        #self.url = 'https://research.chicagobooth.edu/-/media/research/famamiller/data/liq_data_1962_2024.txt'
        self.url = 'https://finance.wharton.upenn.edu/~stambaug/liq_data_1962_2024.txt'
        self.cache_ttl = cache_ttl   #test

    def download(self):
        return self._download(self.start_date, self.end_date, self.output_file)

        
    def _download(self, start_date, end_date, output_file):
        with HttpClient(timeout=5.0) as client:
            #TODO make timeout Optional
            _data = client.download(self.url, self.cache_ttl)

        # simple validate data returned TODO
        data = _data.decode('utf-8')
        data = io.StringIO(data)
        
        # Headers are last commented line
        headers = [line[1:].strip().split('\t')
                   for line in data.readlines() if line.startswith('%')][-1]

        # Fix: was losing first line of data
        data.seek(0)
        
        # ...read .csv here
        data = pd.read_csv(data, sep='\\s+', names=headers, comment='%', index_col=0)

        data.index.name = 'date'
        data.index = data.index.astype(str)

        data = data.rename(columns={'Agg Liq.': 'AGG_LIQ',
                                'Innov Liq (eq8)': 'INNOV_LIQ',
                                'Traded Liq (LIQ_V)': 'TRADED_LIQ'})

        # The first 65 values in the traded liquidity series are -99.000000.
        data['TRADED_LIQ'] = data['TRADED_LIQ'].replace(-99.000000, 0)

        if self.frequency.lower() == 'm':
            data.index = pd.to_datetime(data.index, format='%Y%m') + pd.offsets.MonthEnd(0)

        return _process(data, start_date, end_date, filepath=output_file)

