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

# TODO: yearly data isn't inclusive of end yr
#  but start_date includes partial start year/month !!
# eg start_date=1968-12-25, end_date=1970-01-01 with frequency='y' 
## will return yearly data for all of 1968 and none of 1970.
#
# Potentially warn user if month and day is passed to year? 
# Warn the returned data includes the full year, or not the last year because they specifed date...
#  just read YYYY?


class FamaFrenchFactors(FactorModel):
    # will do proper docstr later. TODO: utils and getting every model with pyarrow and pushing pandas to
    # boundaries.
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
        `region` (`str, optional`): returns the region (intl/developed/emerging markets)
         model. New. Testing.

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

    FF_REGION_MAP = {
        'us': 'us',
        'emerging': 'Emerging',
        'developed': 'Developed',
        'developed ex us': 'Developed_ex_US',
        'ex us': 'Developed_ex_US',
        'ex-us': 'Developed_ex_US',
        'europe': 'Europe',
        'japan': 'Japan',
        'asia pacific ex japan': 'Asia_Pacific_ex_Japan',
        'north_america': 'North_America',
        'na': 'North_America',
    } # TODO: Clean this up, and region map usage throughout (after factorextractor gets removed)

    def __init__(self,
                 frequency: str = 'm',
                 model: int|str = '3',
                 region: str | None = 'us',
                 **kwargs: Any) -> None:
        self.model = str(model)
        # eg: japan -> Japan [FIXME, lower everywhere]
        self.region = None if region is None else self.FF_REGION_MAP.get(region.lower(), None)

        super().__init__(frequency=frequency, model=model, **kwargs)

        self._validate_ff_input()
    def _validate_ff_input(self):
        if self.model not in ["3", "4", "5", "6"]:
            raise ValueError(
                f"Invalid model '{self.model}': must be '3' '4' '5' or '6'",
            )
        if self.frequency == 'w' and self.model not in {"3"}: #no 4 weekly mom
            raise ValueError(f"Fama French weekly data is only available for the 3 Factor model ({self.model})")

        if self.frequency != 'm' and self.region == "Emerging":
            raise ValueError(
                "Emerging Markets data is only available in monthly frequency.",
            )
        if self.frequency not in ['d', 'm', 'y'] and self.region not in ['US', 'us', None]:
            raise ValueError(
                "Region data is only available in daily, monthly and yearly frequency.",
            )       
        # self.region is None if invalid region, or it is mapped! (e.g., 'Japan', 'us').
        valid_mapped_regions = set(self.FF_REGION_MAP.values()) | {None}

        if self.region not in valid_mapped_regions:
            valid_region_keys = ', '.join(f'`{k}`' for k in self.FF_REGION_MAP)
            raise ValueError(
                f"Invalid region. Must be one of: {valid_region_keys}",
            )


    def _get_url(self) -> str:
        """Constructs the URL for downloading Fama-French data based on region, model, and frequency."""
        base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"
        region = self.region
        model = self.model
        frequency = self.frequency.lower()

        file_name = None
        basefilename = None # Initialize basefilename here for clarity

        if region in ['us', None]:
            if model in {"3", "4"}:
                basefilename = "F-F_Research_Data_Factors"
            elif model in {"5", "6"}:
                basefilename = "F-F_Research_Data_5_Factors_2x3"
            else:
                raise ValueError(f"Invalid model '{model}' for US factors. Must be 3, 4, 5, or 6.")

            freq_suffix = ""
            if frequency == 'd':
                freq_suffix = "_daily"
            elif frequency == 'w':
                freq_suffix = "_weekly"

            file_name = f"{basefilename}{freq_suffix}_CSV.zip"
            self.log.debug(f"filename: {file_name}")
            self.log.debug(f"full url: {base_url}/{file_name}")
            return f"{base_url}/{file_name}"

        if model in ['3', '4', '5', '6']:
            base_ff_model = '3' if model in ['3', '4'] else '5'
        else:
            raise ValueError(f"Invalid model '{model}' for regional factors in region '{region}'.")
        freq_suffix = '_Daily' if frequency == 'd' else ''

        if region == 'Emerging' and frequency == 'm':
            file_name = 'Emerging_5_Factors_CSV.zip'
        elif region is not None:
            file_name = f'{region}_{base_ff_model}_Factors{freq_suffix}_CSV.zip'

        return f'{base_url}/{file_name}'

    def _get_mom_url(self, frequency, region):
        """Constructs the URL for momentum factors."""
        base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"

        if region not in ["us", None]: # Note: region is mapped 'Emerging', not 'emerging'
            if self.model in ['4', '6']:
                freq_suffix = "_Daily" if frequency == 'd' else ""
                file_name = f'{region}_Mom_Factor{freq_suffix}_CSV.zip' 
                return f"{base_url}/{file_name}"

       # if region == 'Emerging':
       #     file = "Emerging_MOM_Factor_TXT.zip" #only m; forgot why TXT 

        elif frequency == 'd':
            file = "F-F_Momentum_Factor_daily_CSV.zip"

        else:
            file = "F-F_Momentum_Factor_CSV.zip"

        return f"{base_url}/{file}"

    # ------------------------------------------------------------------------- #

    @staticmethod
    def _read_zip(_data, frequency, region):
        """Download and read CSV from zip file."""
        # old function stuff....
        
        try:
            with zipfile.ZipFile(io.BytesIO(_data)) as zip_file:
                filename = zip_file.namelist()[0]

                with zip_file.open(filename) as file:
                    content = file.read().decode('utf-8')
                if region == 'Emerging':
                    skip_rows = 6
                else:
                    skip_rows = 12 if 'momentum' in filename.lower() else 4

                # Monthly files contain the annual data, under this line
                annual_marker = " Annual Factors: January-December"
                marker_pos = content.find(annual_marker)
                
                if frequency in ['y', 'm'] and marker_pos != -1:
                    if frequency == 'm':
                        content = content[:marker_pos]
                    else:  # freq == 'y'
                        all_lines = content[marker_pos:].split('\n')
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
                    na_filter=True,   #fix to mom data  # NANS ARE RETURNING AS -0.99999 coz decimalizing
                    on_bad_lines='error')

            df.index = df.index.astype(str)
            df.index = df.index.str.strip()
            df.index.name = "date"
            #df = df.dropna(how='all')
            return df

        except Exception as e:
            print(f"Error parsing zip file: {e}")
            return pd.DataFrame()

   
    # parse_date=True prob handles this now: keeping for now.   #TODO: remove
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


    def _download_mom_data(self, frequency):
        url = self._get_mom_url(frequency, self.region)
        try:
            with HttpClient(timeout=10.0) as client:
                _zip = client.download(url)
            return self._read_zip(_zip, frequency, self.region)
        except Exception as e: #todo exception
            raise RuntimeError(f"Failed to download momentum data from {url}.") from e


    def _add_momentum_factor(self, df):
        if self.model not in {"4", "6"}:
            return df 
        
        try:
            mom_data = self._download_mom_data(self.frequency)
        
        except RuntimeError as e:
            self.log.warning(f"Momentum factor download failure: {e}")
            return df
        
        if mom_data.empty:
            print("Empty df.")
            return df
        
        mom_data = self._parse_dates(mom_data)

        if self.region not in ['us', None]:
            mom_name = "WML"  # winner minus loser in the EM and intl data
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

            if self.region == 'us':
                df = df.dropna(how='any')  # Drop NaNs after MOM added; trims models to MOM size 
            else:
                # eg, emerging RMW/CMA begins with NaNs -- keepin
                # eg, asia pacific ex japan starts with 4 months of NaNs in WML (4 in monthly) -- droppin
                # but all factors go back to 1990-07-02. 
                #If momentum model, then we drop the nan WML rows...
                if self.model in ['4', '6']:
                    df = df.dropna(how='any')
                else:
                    df = df.dropna(how='all')  # in dev/emerging/intl markets the mom factor if often shorter than the dataset. (But if models are momentum models... might just trim to size as well)
        else:        
            self.log.warning("Mom data contained no columns after parsing...")
        return df 


    def _read(self, data) -> pd.DataFrame:
        try:
            df = self._read_zip(data, self.frequency, self.region)

            if df.empty:
                print("Warning: No data downloaded")
                return pd.DataFrame()

            # Parse dates and add momentum if needed
            df = self._parse_dates(df)
            df = self._add_momentum_factor(df)
            #df = _slice_dates(df, self.start_date, self.end_date)
            df = df.sort_index()
            # Just for now until rewrite... later.
            if self.region == 'Emerging':
                if self.model == '3':
                    df = df.get(["Mkt-RF", "SMB", "HML", "RF"], df)
                if self.model == '4':
                    df = df.get(["Mkt-RF", "SMB", "HML", "WML", "RF"], df)
                if self.model == '5':
                    df = df.get(["Mkt-RF", "SMB", "HML", "RMW", "CMA", "RF"], df)
                #if 6 then it's all here...

            # Decimalize. Keep here for now. Finicky.
            df = df.astype(float)
            return df / 100
            # TODO: drop nans/nulls, after adding MOM, and for barillas shanken, etc.

        except Exception as e:
            print(f"Error downloading Fama-French data: {e}")
            raise

# Welllll its using the base class at least... TODO FIXME FIXME 
