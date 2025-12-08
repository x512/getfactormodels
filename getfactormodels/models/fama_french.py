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
#from __future__ import annotations
import io
import logging  # TODO: FF logging! FIXME
import zipfile
import pandas as pd
from getfactormodels.http_client import HttpClient


class FamaFrenchFactors:
    # will do proper docstr later, this from this func
    # Class is an implementation of the function, a little less spaghetti.
    # utils and getting every model with pyarrow and pushing pandas to
    # boundaries to pretty print will help.
    """Download Fama-French (and Carhart) factor models.

    Downloads the 3-factor (1993), 5-factor (2015), or 6-factor (2018)
    model of Fama & French, or Carhart's (1997) 4-factor model. Data
    is available in daily, weekly, monthly, and annual frequencies.
    Saves data to file if output_file is specified.

    params :
    - model (str, int): model to return. 3, 4, 5 or 6 (default: 3).
    - frequency (str): the frequency of the data. d m y or w (default: m)
    - start_date (str, optional): the start date of the data, as YYYY-MM-DD.
    - end_date (str, optional): the end date of the data, as YYYY-MM-DD.

   NOTES (DEV): no output_file at the moment. Need Writer class to do it
    as the function aimed to do.
    """
    def __init__(self, frequency='m', start_date=None, end_date=None,
                 output_file=None, model='3'):
        self.frequency = frequency.lower()
        self.start_date = start_date if start_date else None
        self.end_date = end_date if start_date else None
        self.model = str(model) #allow str, '3', or int, 3, for ff model numbers

        self._validate()
        self.url = self._construct_url()

    def _validate(self) -> None:
        """Validates ff input parameters."""
        if self.frequency.lower() not in ["d", "m", "y", "w"]:
            raise ValueError("Fama-French factors frequencies: d m y w.")

        if self.model not in ["3", "4", "5", "6"]:
            raise ValueError(
                f"Invalid model '{self.model}'. "
                "  - Must be one of: '3' '4' '5' '6'"
            )

        if self.frequency == 'w' and self.model not in {"3", "4"}:
            raise ValueError(
                "Weekly data is only available for Fama-French 3 and 4 factor(Carhart) models"
            )


    def _construct_url(self) -> str:
        """Construct the URL for downloading Fama-French data."""
        base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"

        if self.model in {"3", "4"}:
            basefilename = "F-F_Research_Data_Factors"
        elif self.model in {"5", "6"}:
            basefilename = "F-F_Research_Data_5_Factors_2x3"
        else:
            basefilename = "F-F_Research_Data_Factors"  # Default fallback

        _freq = "" #daily, weekly file name suffixes

        if self.frequency == 'd':
            _freq = "_daily"
        elif self.frequency == 'w':
            _freq = "_weekly"

        file_name = f"{basefilename}{_freq}_CSV.zip"

        return f"{base_url}/{file_name}"


    @staticmethod
    def _download_and_read_zip(url: str, freq: str = None):
        """Download and read CSV from zip file."""
        with HttpClient(timeout=10.0) as client:
            _data = client.download(url)

        #data = io.StringIO(_data.decode('utf-8'))

        try:
            with zipfile.ZipFile(io.BytesIO(_data)) as zip_file:
                filename = zip_file.namelist()[0]

                with zip_file.open(filename) as file:
                    content = file.read().decode('utf-8')

                skip_rows = 12 if 'momentum' in filename.lower() else 4

                # Monthly files contain the annual data, under this line
                annual_marker = " Annual Factors: January-December"
                marker_pos = content.find(annual_marker)

                if freq in ['y', 'm'] and marker_pos != -1:
                    if freq == 'm':
                        # monthly = content before marker
                        content = content[:marker_pos]
                    else:  # freq == 'y'
                        # annual = skip marker and header
                        # finds end of marker line and skip it
                        lines = content[marker_pos:].split('\n', 3)
                        if len(lines) >= 4:
                            content = lines[2]  # Data starts after marker and header
                            skip_rows = 0

                df = pd.read_csv(
                    io.StringIO(content),
                    skiprows=skip_rows,
                    index_col=[0],
                    header=0,
                    parse_dates=False,
                    skipfooter=1,
                    engine="python",
                    na_filter=False,    #this was causing problems, couldnt change dtype
                    on_bad_lines='error')
                # note verbose=True (deprecation warning) was showing "1 NA
                # filled" preventing conversion to float, until na_filter=False.

            df.index = df.index.astype(str)
            df.index = df.index.str.strip()
            df.index.name = "date"
            df = df.dropna()
            return df

        except Exception as e:
            print(f"Error reading file from {url}: {e}")
            return pd.DataFrame()


    @classmethod
    def _download_mom_data(cls, frequency: str) -> pd.DataFrame:
        """Download and process momentum factor data."""
        base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"
        file = "F-F_Momentum_Factor_CSV.zip"

        if frequency.lower() == "d":
            file = "F-F_Momentum_Factor_daily_CSV.zip"

        url = f"{base_url}/{file}"

        try:
            df = cls._download_and_read_zip(url)
            df.columns = ["MOM"]
            return df
        except Exception as e:
            print(f"Warning: Could not download momentum data: {e}")
            return pd.DataFrame()


    # parse_date=True prob handles this now.
    # keeping for now.
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


    def _momentum_factor(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add momentum factor to the DataFrame if required by model."""
        if self.model in {"4", "6"}:
            mom_data = self._download_mom_data(self.frequency)

            if not mom_data.empty:
                mom_data = self._parse_dates(mom_data)
                # align indices, and join
                mom_data = mom_data.reindex(df.index)
                df = df.join(mom_data)

                # rename column for 6 factor model
                if self.model == "6" and "MOM" in df.columns:
                    df = df.rename(columns={"MOM": "UMD"})

        return df


    def download(self):
        return self._download(self.url)

    def _download(self, url) -> pd.DataFrame:
        try:
            df = self._download_and_read_zip(url)

            if df.empty:
                print("Warning: No data downloaded")
                return pd.DataFrame()

            # Parse dates and add momentum if needed
            df = self._parse_dates(df)
            df = self._momentum_factor(df)
            df = self._date_range_mask(df)

            # Sort by date
            df = df.sort_index()

            # TODO: drop nans/nulls, after adding MOM, and for barillas shanken,
            # and for aqr...: hmld will go back to 1926 full of nans, momentum
            # fills with nans til 1926 with the carhart 4, etc. atm. TODO.

            # Decimalize  -- make helper. Keep last step here. Finicky.
            df = df.astype(float)
            df = df * 0.01

            return df

        except Exception as e:
            print(f"Error downloading Fama-French data: {e}")
            raise


    def _date_range_mask(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter DataFrame by start and end dates."""
        if df.empty:
            return df

        if self.start_date is not None or self.end_date is not None:
            mask = pd.Series(True, index=df.index)

            if self.start_date is not None:
                mask = mask & (df.index >= self.start_date)

            if self.end_date is not None:
                mask = mask & (df.index <= self.end_date)

            df = df[mask]

        return df
