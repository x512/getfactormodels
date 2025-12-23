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
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
###import pandas as pd
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.http_client import _HttpClient
from getfactormodels.utils.utils import _offset_period_eom, _decimalize


#TODO: break up _read_zip
class FamaFrenchFactors(FactorModel):
    # will do proper docstr later. TODO: utils and getting every model with pyarrow and pushing pandas to
    # boundaries.
    """Download Fama-French (and Carhart) factor models.

    Downloads the 3-factor (1993), 5-factor (2015), or 6-factor (2018)
    model of Fama & French, or Carhart's (1997) 4-factor model. Data
    is available in daily, weekly, monthly, and annual frequencies.
    Saves data to file if `output_file` is specified.

    Args:
        model (str | int): model to return. '3' '4' '5' '6' (default: '3')
        frequency (str, optional): frequency of the data. 'd' 'm' 'y' 'w'
            (default: 'm'). Weekly only available for the 3-factor model.
        start_date (str, optional): start date of the data. YYYY[-MM-DD].
        end_date (str, optional): end date of the data. YYYY[-MM-DD].
        cache_ttl (int, optional`): cached download time-to-live in seconds 
            (default: 86400).
        region (str, optional): return a region-specific (emerging/developed)
            factor model.

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

    @property
    def schema(self) -> pa.Schema:
        """Fama-French schema for specific model/frequency/region."""
        # all models
        cols = [("date", pa.string()), 
                ("Mkt-RF", pa.float64()), 
                ("SMB", pa.float64()), 
                ("HML", pa.float64())]
        
        # 5 and 6-factor models: RMW and CMA
        if self.model in ["5", "6"]:
            cols += [("RMW", pa.float64()), 
                     ("CMA", pa.float64())]

        # momentum models
        if self.model in ["4", "6"]:
            _intl = self.region not in ["us", None]
            
            # dev/emerging: WML. US 6 factor: UMD. US 4: MOM. [missing something??]
            mom_name = "WML" if _intl else ("UMD" if self.model == "6" else "MOM")
            cols.append((mom_name, pa.float64()))
        cols.append(("RF", pa.float64()))
        
        return pa.schema(cols)

    @property
    def _mom_schema(self) -> pa.Schema:
        """Private helper: schema for momentum files.
        'val' is a placeholder for the specific (model, region)
          momentum factor, it gets renamed during the join process.
        """
        return pa.schema([("date", pa.string()), 
                          ("val", pa.float64())])

    @property  # new: now property
    def _ff_region_map(self) -> dict[str, str]:
        """Private: map input to region"""
        return {
            'us': 'us',
            'emerging': 'Emerging',
            'developed': 'Developed',
            'ex-us': 'Developed_ex_US',
            'ex us': 'Developed_ex_US',
            'europe': 'Europe',
            'japan': 'Japan',
            'asia pacific ex japan': 'Asia_Pacific_ex_Japan',
            'na': 'North_America',
            'north_america': 'North_America',
        }
    @property 
    def _precision(self) -> int:
        return 8

    def __init__(self, frequency: str = 'm', 
                 model: int | str = '3', 
                 region: str | None = 'us', 
                 **kwargs: Any) -> None:
        self.model = str(model)
        self.region = None if region is None else self._ff_region_map.get(region.lower(), None)
        super().__init__(frequency=frequency, 
                         model=model, 
                         **kwargs)
        
        self._validate_ff_input()

    def _validate_ff_input(self) -> None:
        if self.model not in ["3", "4", "5", "6"]:
            raise ValueError(f"Invalid model '{self.model}'")

        if self.frequency == 'w' and self.model != "3":
            raise ValueError("Weekly FF is only 3-Factor.")

        if self.region not in ['us', None] and self.frequency == 'w':
            raise ValueError(f"Weekly frequency not available for {self.region}.")

        if self.region == "Emerging" and self.frequency != 'm':
            raise ValueError("Emerging markets only available in monthly.")


    def _get_url(self) -> str:
        """Constructs the URL for downloading Fama-French data."""
        base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"
        # Default: region=US
        if self.region in ['us', None]:
            _model = "F-F_Research_Data_5_Factors_2x3" if self.model in {"5", "6"} else "F-F_Research_Data_Factors"
            freq_map = {'d': '_daily', 'w': '_weekly'}
            suffix = freq_map.get(self.frequency, "")
            
            return f"{base_url}/{_model}{suffix}_CSV.zip"

        # Emerging: only monthly 5-factors avail
        if self.region == 'Emerging':
            return f"{base_url}/Emerging_5_Factors_CSV.zip"

        # region: 3 and 4 use 3 factors data, 5 and 6 use 5.
        base_model = '3' if self.model in ['3', '4'] else '5'
        _daily = '_Daily' if self.frequency == 'd' else ''
        region_filename = f"{self.region}_{base_model}_Factors{_daily}_CSV.zip"

        return f"{base_url}/{region_filename}"


    def _get_mom_url(self) -> str:
        """Constructs URL for the required mom file."""
        base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"
        freq_suffix = "_daily" if self.frequency == 'd' else ""

        if self.region in ['us', None]:
            filename = f"F-F_Momentum_Factor{freq_suffix}_CSV.zip"
        else:
            # regional, e.g., 'Europe_Mom_Factor_Daily_CSV.zip'
            filename = f"{self.region}_Mom_Factor{freq_suffix.title()}_CSV.zip"

        return f"{base_url}/{filename}"


    def _read_zip(self, _data: bytes, use_schema: pa.Schema = None) -> pa.Table:
        """Download and read CSV from zip file."""
        _schema = use_schema if use_schema else self.schema
        
        with zipfile.ZipFile(io.BytesIO(_data)) as z:
            filename = z.namelist()[0]
            with z.open(filename) as f:
                lines = f.read().decode('utf-8').splitlines()

        # Gotta break this up

        # skip lines/where the data starts 
        is_mom = 'momentum' in filename.lower() or '_mom_' in filename.lower()
        start_idx = 6 if self.region == 'Emerging' else (12 if is_mom else 4)
       
        # TESTING: copyright info
        if is_mom:
            last_line = lines[-2].strip() if lines else ""
        else:
            last_line = lines[-1].strip() if lines else ""
        if "copyright" in last_line.lower():
            if last_line not in self.copyright:  # will extend to metadata TODO
                self.copyright = f"{self.copyright} | {last_line}".strip(" | ")
        
        # filter content
        content = []
        for line in lines[start_idx:]:
            _lower = line.lower()
            # stop on annual marker or copyright footer:
            if "annual factors" in _lower or "copyright" in _lower:
                break
            if line.strip():
                content.append(line)

        # pa to read
        table = pv.read_csv(
            io.BytesIO("\n".join(content).encode('utf-8')),
            convert_options=pv.ConvertOptions(
                null_values=['-99.99', '-999', ' -99.99', ' -999'],
                column_types=_schema, 
            ),
        )
        
        return table.rename_columns(["date"] + table.column_names[1:])
    

    def _read(self, data: bytes) -> pa.Table:
        table = self._read_zip(data, use_schema=self.schema)

        if self.model in {"4", "6"}:
            with _HttpClient(timeout=15.0) as client:
                mom_bytes = client.download(self._get_mom_url(), self.cache_ttl)
            
            mom_table = self._read_zip(mom_bytes, use_schema=self._mom_schema)
            mom_key = (set(self.schema.names) & {"UMD", "MOM", "WML"}).pop()
            mom_table = mom_table.rename_columns(["date", mom_key])
            table = table.join(mom_table, keys="date", join_type="inner")
        
        table = table.set_column(0, "date", table.column(0).cast(pa.string()))
        
        table = _offset_period_eom(table, self.frequency)
        table = _decimalize(table, self.schema, self._precision)
        
        # cast schema/validate against it
        table = table.select(self.schema.names).filter(pc.is_valid(table.column(1)))
        
        table.validate(full=True)
        
        return table
