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
import zipfile
from typing import Any
import pandas as pd
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.http_client import HttpClient
from getfactormodels.utils.utils import _slice_dates

# TODO: yearly data isn't inclusive of end yr
#  but start_date includes partial start year/month !!
# eg start_date=1968-12-25, end_date=1970-01-01 with frequency='y' 
## will return yearly data for all of 1968 and none of 1970.
#
# Potentially warn user if month and day is passed to year? 
# Warn the returned data includes the full year, or not the last year because they specifed date...
#  just read YYYY?


class FamaFrenchFactors(FactorModel):
    # will do proper docstr later, this from this func
    # Class is an implementation of the function, a little less spaghetti.
    # utils and getting every model with pyarrow and pushing pandas to
    # boundaries to pretty print will help.
    """Download Fama-French (and Carhart) factor models.

    Downloads the 3-factor (1993), 5-factor (2015), or 6-factor (2018)
    model of Fama & French, or Carhart's (1997) 4-factor model. Data
    is available in daily, weekly, monthly, and annual frequencies.
    Saves data to file if `output_file` is specified.

    Args:
        `model (str | int)`: model to return. `3`, `4`, `5` or `6` (default: 3)
        `frequency` (`str`): the frequency of the data. `d`, `m` `y` or `w`
        (default: `m`). Only the 3-Factor model is available for weekly freq.
        `start_date` (`str, optional`): the start date of the data, as 
            YYYY-MM-DD.
        `end_date` (`str, optional`): the end date of the data, as YYYY-MM-DD.
        `cache_ttl` (`int, optional`): Cached download time-to-live in seconds 
            (default: `86400`).
        `emerging` (`bool, optional`): if true, returns the emerging market 
        dataset for the model. Only available for monthly frquency.

    Returns:
        `pd.Dataframe`: time series of returned data.

    References:
    - E. F. Fama and K. R. French, ‘Common risk factors in the returns on stocks 
    and bonds’, Journal of Financial Economics, vol. 33, no. 1, pp. 3–56, 1993.
    - E. F. Fama and K. R. French, ‘A five-factor asset pricing model’, Journal 
    of Financial Economics, vol. 116, no. 1, pp. 1–22, 2015.
    - E. F. Fama and K. R. French, ‘Choosing factors’, Journal of Financial 
    Economics, vol. 128, no. 2, pp. 234–252, 2018.
    """
    @property
    def _frequencies(self) -> list[str]:
        return ['d', 'w', 'm', 'y']

    def __init__(self, frequency: str = 'm', model: int|str = '3', emerging: bool = False,  **kwargs: Any) -> None:
        self.frequency = frequency
        self.model = str(model) #lost this somewhere...
        self.emerging = emerging

        self._validate_ff_input()

        super().__init__(frequency=frequency, model=model, **kwargs)


    def _validate_ff_input(self):
        if self.model not in ["3", "4", "5", "6"]:
            raise ValueError(
                f"Invalid model '{self.model}': must be '3' '4' '5' or '6'"
            )

        if self.frequency == 'w' and self.model not in {"3", "4"}:
            raise ValueError(
                "Weekly data is only available for Fama-French 3 and 4 factor (Carhart) models"
            )

        if self.frequency != 'm' and self.emerging is True:
            raise ValueError(
                "Emerging Markets data is only available in monthly frequency."
            )

    
    def _get_url(self) -> str:
        """Construct the URL for downloading Fama-French data."""
        base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"

        if self.emerging == True:  # 2 EM files (5 factor and a MOM series)
            basefilename = 'Emerging_5_Factors_CSV.zip'
            return f"{base_url}/{basefilename}"

        if self.model in {"3", "4"}:
            basefilename = "F-F_Research_Data_Factors"
        elif self.model in {"5", "6"}:
            basefilename = "F-F_Research_Data_5_Factors_2x3"
        else:
            basefilename = "F-F_Research_Data_Factors"

        _freq = ""
        if self.frequency == 'd':
            _freq = "_daily"
        elif self.frequency == 'w':
            _freq = "_weekly"

        file_name = f"{basefilename}{_freq}_CSV.zip"
        return f"{base_url}/{file_name}"


    def _get_mom_url(self, frequency, emerging):
        """Constructs the URL for momentum factors."""
        base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"

        if emerging == False and frequency == 'd':
            file = "F-F_Momentum_Factor_daily_CSV.zip"
        elif emerging is True:
            file = "Emerging_MOM_Factor_CSV.zip"
        else:
            file = "F-F_Momentum_Factor_CSV.zip"

        return f"{base_url}/{file}"


    # ------------------------------------------------------------------------- #
    @staticmethod
    def _read_zip(_data, frequency, emerging: bool = False):
        """Download and read CSV from zip file."""
        # old function stuff....
        try:
            with zipfile.ZipFile(io.BytesIO(_data)) as zip_file:
                filename = zip_file.namelist()[0]

                with zip_file.open(filename) as file:
                    content = file.read().decode('utf-8')

                skip_rows = 12 if 'momentum' in filename.lower() else 4

                if emerging == True:
                    skip_rows = 6

                # Monthly files contain the annual data, under this line
                annual_marker = " Annual Factors: January-December"
                marker_pos = content.find(annual_marker)

                if frequency in ['y', 'm'] and marker_pos != -1:
                    if frequency == 'm':
                        # monthly = content before marker
                        content = content[:marker_pos]
                    else:  # freq == 'y'
                        # annual = find the annual section and take its header/data
                        all_lines = content[marker_pos:].split('\n')
                        # [0] Annual Factors: January-December
                        # [1] Header Line 
                        # [2] First Data Line
                        if len(all_lines) > 2:
                            # Join lines starting from the Header Line
                            content = '\n'.join(all_lines[1:])
                            skip_rows = 0

                df = pd.read_csv(
                    io.StringIO(content),
                    skiprows=skip_rows,
                    index_col=[0],
                    header=0,
                    parse_dates=False,
                    skipfooter=1,
                    engine="python",
                    na_values=[-99.99, '-99.99'],  #stated in emerging, momentum; every model? 
                    na_filter=True,
                    on_bad_lines='error')

            df.index = df.index.astype(str)
            df.index = df.index.str.strip()
            df.index.name = "date"
            df = df.dropna()
            return df

        except Exception as e:
            print(f"Error parsing zip file: {e}")
            return pd.DataFrame()

   
    # parse_date=True prob handles this now: keeping for now.
    def _parse_dates(self, df): #TODO: types everywhere.
        """Parse the date index based on frequency."""
        if df.empty:
            return df

        if self.frequency == 'm':
            # Monthly data YYYYMM (6 chars)
            mask = df.index.str.len() == 6
            df = df[mask]
            if not df.empty:
                df.index = pd.to_datetime(df.index, format='%Y%m') + pd.offsets.MonthEnd(0)

        elif self.frequency == 'y':
            mask = df.index.str.len() == 4
            df = df[mask]
            if not df.empty:
                df.index = pd.to_datetime(df.index, format='%Y') + pd.offsets.YearEnd(month=12)

        elif self.frequency in {'d', 'w'}:
            # Day/week data shares YYYYMMDD format
            mask = df.index.str.len() == 8
            df = df[mask]
            if not df.empty:
                df.index = pd.to_datetime(df.index, format='%Y%m%d')

        df.index.name = "date"
        return df


    def _download_mom_data(self, frequency, emerging):
        url = self._get_mom_url(frequency, emerging)
        try:
            with HttpClient(timeout=10.0) as client:
                _zip = client.download(url)
            return self._read_zip(_zip, frequency)
        except Exception as e: #todo exception
            raise RuntimeError(f"Failed to download momentum data from {url}.") from e


    def _add_momentum_factor(self, df):
        if self.model not in {"4", "6"}:
            return df 
        
        try:
            mom_data = self._download_mom_data(self.frequency, self.emerging)
        
        except RuntimeError as e:
            self.log.warning(f"Momentum factor download failure: {e}")
            return df
        
        if mom_data.empty:
            print("Empty df.")
            return df
        
        mom_data = self._parse_dates(mom_data)

        if self.emerging is True:
            mom_name = "WML"  # winner minus loser in the EM data
        elif self.model == "6":    # 6 factor call it Up Minus Down
            mom_name = "UMD"
        elif self.model == "4":   # and MOM in 4-factor (TODO: name MOM for just MOM?)
            mom_name = "MOM"
 
        if len(mom_data.columns) > 0:    
            mom_series = (
                mom_data.iloc[:, 0]  # Extracts first col,
                .rename(mom_name) # Renames it
                .reindex(df.index)
            )

            df = df.join(mom_series)
            self.log.info(f"Added momentum factor: {mom_name}")
            
        else:        
            self.log.warning("Mom data contained no columns after parsing...")
        return df 


    def _read(self, data) -> pd.DataFrame:
        try:
            df = self._read_zip(data, self.frequency)

            if df.empty:
                print("Warning: No data downloaded")
                return pd.DataFrame()

            # Parse dates and add momentum if needed
            df = self._parse_dates(df)
            df = self._add_momentum_factor(df)
            df = _slice_dates(df, self.start_date, self.end_date)
            df = df.sort_index()
            # Just for now until rewrite... later.
            if self.emerging == True:
                if self.model == '3':
                    df = df.get(["Mkt-RF", "SMB", "HML", "RF"], df)
                if self.model == '4':
                    df = df.get(["Mkt-RF", "SMB", "HML", "WML", "RF"], df)
                if self.model == '5':
                    df = df.get(["Mkt-RF", "SMB", "HML", "RMW", "CMA", "RF"], df)
                #if 6 then it's all here...

            # Decimalize. Keep here for now. Finicky.
            df = df.astype(float)
            df = df * 0.01
            # TODO: drop nans/nulls, after adding MOM, and for barillas shanken, etc.
            return df

        except Exception as e:
            print(f"Error downloading Fama-French data: {e}")
            raise

# Welllll its using the base class at least... TODO FIXME FIXME 
