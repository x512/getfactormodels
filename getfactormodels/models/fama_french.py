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
import pyarrow.csv as pv
from getfactormodels.models.base import FactorModel
from getfactormodels.utils.data_utils import (
    offset_period_eom,
    scale_to_decimal,
)
from getfactormodels.utils.http_client import _HttpClient


#TODO: break up _read_zip
class FamaFrenchFactors(FactorModel):
    """Download Fama-French (and Carhart) factor models.

    Downloads the 3-factor (1993), 5-factor (2015), or 6-factor (2018)
    model of Fama & French, or Carhart's (1997) 4-factor model.

    Weekly data only available for the 3-factor model.

    Args
        model (str | int): model to return. '3' '4' '5' '6' (default: '3')
        frequency (str, optional): frequency of the data. 'd' 'm' 'y' 'w'
            (default: 'm').
        start_date (str, optional): start date of the data. YYYY[-MM-DD].
        end_date (str, optional): end date of the data. YYYY[-MM-DD].
        cache_ttl (int, optional`): cached download time-to-live in seconds 
            (default: 86400).
        region (str, optional): return an international/emerging market 
            model.

    References
    - E. F. Fama and K. R. French, ‘Common risk factors in the returns 
      on stocks and bonds’, Journal of Financial Economics, vol. 33, 
      no. 1, pp. 3–56, 1993.
    - E. F. Fama and K. R. French, ‘A five-factor asset pricing model’, 
      Journal of Financial Economics, vol. 116, no. 1, pp. 1–22, 2015.
    - E. F. Fama and K. R. French, ‘Choosing factors’, Journal of 
      Financial Economics, vol. 128, no. 2, pp. 234–252, 2018.

    Note: "-0.9999" should be NaNs! [TODO: FIXME]

    """
    # TODO: NaNs in FamaFrench models!

    @property
    def _frequencies(self) -> list[str]:
        return ['d', 'w', 'm', 'y']

    @property 
    def _precision(self) -> int:
        return 6  

    def __init__(self, 
                 frequency: str = 'm', 
                 model: int | str = '3', 
                 region: str | None = 'us', 
                 **kwargs: Any) -> None:
        """Initialize the Fama-French factor model."""
        self.model = str(model)
        self.region = region.lower() if region else 'us'
        super().__init__(frequency=frequency, model=model, **kwargs)
        self._validate_ff_input()

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
            
            # dev/emerging: WML. US 6 factor: UMD. US 4: MOM.
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
    
    @classmethod
    def list_regions(cls) -> list[str]:
        """Returns the list of supported Fama-French regions (clean keys)."""
        return [
            'us', 'emerging', 'developed', 'ex-us', 
            'europe', 'japan', 'asia-pacific-ex-japan', 'north-america'
        ]
    
    @property
    def _ff_region_map(self) -> dict[str, str]:
        """Private: maps region input to region URL str"""
        return {
            'us': 'US',
            'emerging': 'Emerging',
            'developed': 'Developed',
            'ex-us': 'Developed_ex_US',
            'europe': 'Europe',
            'japan': 'Japan',
            'asia-pacific-ex-japan': 'Asia_Pacific_ex_Japan',
            'north-america': 'North_America',
        }

    def _validate_ff_input(self) -> None:
        """Validates input for Fama-French models."""
        valid_regions = self.list_regions()

        if self.region and self.region not in valid_regions:
            raise ValueError(f"Invalid region '{self.region}'. Supported: {valid_regions}")

        if self.region != 'us' and self.frequency == 'w':
            raise ValueError(f"Weekly frequency not available for {self.region}.")

        if self.region == "Emerging" and self.frequency != 'm':
            raise ValueError("Emerging markets only available in monthly.")

        if self.model not in ["3", "4", "5", "6"]:
            raise ValueError(f"Invalid model '{self.model}'")

        if self.frequency == 'w' and self.model != "3":
            raise ValueError("Weekly Fama-French data is only available for the 3-factor model.")
 

    def _get_url(self) -> str:
        """Constructs the URL for downloading Fama-French data."""
        base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"
        ff_url_name = self._ff_region_map.get(self.region)

        # Emerging: only monthly 5-factors avail
        if ff_url_name == 'Emerging':
            return f"{base_url}/Emerging_5_Factors_CSV.zip"

        if ff_url_name == 'US':
            _model = "F-F_Research_Data_5_Factors_2x3" if self.model in {"5", "6"} else "F-F_Research_Data_Factors"
            freq_map = {'d': '_daily', 'w': '_weekly'}
            suffix = freq_map.get(self.frequency, "")
            
            filename = f"{_model}{suffix}_CSV.zip"

        else:
            # regions: 3 and 4 use 3. 5 and 6 use the 5-factor file.
            base_model = '3' if self.model in ['3', '4'] else '5'
            daily = '_Daily' if self.frequency == 'd' else ''
            filename = f"{ff_url_name}_{base_model}_Factors{daily}_CSV.zip"

        return f"{base_url}/{filename}"


    def _get_mom_url(self) -> str:
        """Constructs the URL for the required momentum file."""
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

        is_mom = 'momentum' in filename.lower() or '_mom_' in filename.lower()
        annual_marker = next((i for i, l in enumerate(lines) if "annual factors:" in l.lower()), None)

        if self.frequency == 'y':
            raw_content = lines[annual_marker + 1:] if annual_marker is not None else lines
        else:
            _start = 6 if self.region == 'Emerging' else (12 if is_mom else 4)
            _stop = annual_marker if annual_marker else len(lines)
            raw_content = lines[_start:_stop]

        content = [",".join(_schema.names)]  # Force clean header
        for line in raw_content:
            clean = line.strip()
            if not clean or "copyright" in clean.lower():
                if "copyright" in clean.lower(): self.copyright = clean
                continue
            
            # fix: keep lines starting with a number
            first_part = clean.replace(',', ' ').split()
            if first_part and first_part[0].isdigit():
                content.append(",".join(first_part))

        return pv.read_csv(
            io.BytesIO("\n".join(content).encode('utf-8')),
            convert_options=pv.ConvertOptions(
                null_values=["-99.99", "-999", "-99.990", "-0.9999"],
                strings_can_be_null=True,
                column_types=_schema,
            ),
        )   
    

    def _add_momentum(self, table: pa.Table) -> pa.Table:
        """Private helper to download and join the specific momentum
        factor required."""

        with _HttpClient(timeout=15.0) as client:
            mom_zip = client.download(self._get_mom_url(), self.cache_ttl)

        mom_table = self._read_zip(mom_zip, use_schema=self._mom_schema)
        
        # WML for intl, UMD for US 6, MOM for US 4
        mom_key = next(k for k in ["UMD", "MOM", "WML"] if k in self.schema.names)
        mom_table = mom_table.rename_columns(["date", mom_key])

        return table.join(mom_table, keys="date", join_type="inner").combine_chunks()


    def _read(self, data: bytes) -> pa.Table:
        # Temporary schema without the momentum factor for the first file
        main_cols = [f for f in self.schema if f.name not in ["UMD", "MOM", "WML"]]
        main_schema = pa.schema(main_cols)

        table = self._read_zip(data, use_schema=main_schema)

        if self.model in {"4", "6"}:
            table = self._add_momentum(table)

        table = table.set_column(0, "date", table.column(0).cast(pa.string()))
        table = offset_period_eom(table, self.frequency)
        table = scale_to_decimal(table)
        return table.select(self.schema.names).combine_chunks()
