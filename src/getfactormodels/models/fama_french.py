# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import io
import zipfile
from typing_extensions import override
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
from getfactormodels.models.base import FactorModel, RegionMixin
from getfactormodels.utils.arrow_utils import (
    print_table_preview,
    round_to_precision,
    scale_to_decimal,
)
from getfactormodels.utils.date_utils import offset_period_eom
from abc import ABC, abstractmethod

#TODO: break up _read_zip
class FamaFrenchFactors(FactorModel, RegionMixin):
    """Download Fama-French (and Carhart) factor models.

    Downloads the 3-factor (1993), 5-factor (2015), or 6-factor (2018)
    model of Fama & French, or Carhart's (1997) 4-factor model.

    Weekly data only available for the 3-factor model.

    Args:
        model (str | int): model to return. '3' '4' '5' '6' (default: '3')
        frequency (str, optional): frequency of the data. 'd' 'm' 'y' 'w'
            (default: 'm').
        start_date (str, optional): start date of the data. YYYY[-MM-DD].
        end_date (str, optional): end date of the data. YYYY[-MM-DD].
        cache_ttl (int, optional`): cached download time-to-live in seconds 
            (default: 86400).
        region (str, optional): return an international/emerging market 
            model.

    References:
    - E. F. Fama and K. R., 1993. French. Common risk factors in the returns 
      on stocks and bonds. Journal of Financial Economics, vol. 33, no. 1,
      pp. 3–56.
    - E. F. Fama and K. R. French, 2015. A five-factor asset pricing model. 
      Journal of Financial Economics, vol. 116, no. 1, pp. 1–22.
    - E. F. Fama and K. R. French, 2018. Choosing factors. Journal of 
      Financial Economics, vol. 128, no. 2, pp. 234–252, 2018.

    """
    # TODO: NaNs in FamaFrench models! check
    #Note: "-0.9999" should be NaNs! [TODO: FIXME]
    @property
    def _frequencies(self) -> list[str]: return ['d', 'w', 'm', 'y']
    
    @property 
    def _precision(self) -> int: return 6

    @property 
    def _regions(self) -> list[str]:
        return [
            'us', 'emerging', 'developed', 'ex-us', 'europe',
            'japan', 'asia-pacific-ex-japan', 'north-america', 
        ]

    def __init__(self, 
                 frequency: str = 'm', 
                 model: int | str = '3', 
                 region: str | None = 'us', 
                 **kwargs) -> None:
        """Initialize a Fama-French factor model."""
        self.model = str(model)
        super().__init__(frequency=frequency, model=model, **kwargs)
        
        self.region = region

        self._validate_ff_input()
    

    @property
    def schema(self) -> pa.Schema:
        """Fama-French schema for specific model/frequency/region."""
        cols = [("date", pa.string()), 
                ("Mkt-RF", pa.float64()), 
                ("SMB", pa.float64()), 
                ("HML", pa.float64())]
        
        # 5 and 6-factor models: RMW and CMA
        if self.model in ["5", "6"]:
            cols += [("RMW", pa.float64()), 
                     ("CMA", pa.float64())]

        # mom models (4/6): dev/emerging: WML. US 6 factor: UMD. US 4: MOM.
        if self.model in ["4", "6"]:
            is_intl = self.region not in ["us", None]
            mom_name = "WML" if is_intl else ("UMD" if self.model == "6" else "MOM")
            cols.append((mom_name, pa.float64()))
        cols.append(("RF", pa.float64()))
        
        return pa.schema(cols)

    @property
    def _mom_schema(self) -> pa.Schema:
        """Private helper: schema for momentum files. 'val' is placeholder."""
        return pa.schema([("date", pa.string()), 
                          ("val", pa.float64())])
    @property
    def model(self) -> str:
        return self._model
    @model.setter
    def model(self, value: int | str):
        val = str(value)
        if val not in ["3", "4", "5", "6"]:
            raise ValueError(f"Invalid model '{val}'. Options: 3, 4, 5, 6")
        
        if hasattr(self, '_model') and val != self._model:
            self.log.info(f"Model changed from {self._model} to {val}")
            self._data = None
            self._view = None
        self._model = val 

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
        """Validates input for Fama-French models for things getters/setters dont take care of."""
        if self.region != 'us' and self.frequency == 'w':
            raise ValueError(f"Weekly frequency not available for {self.region}.")

        if self.region == "emerging" and self.frequency != 'm':
            raise ValueError("Emerging markets only available in monthly.")

        if self.frequency == 'w' and self.model != "3":
            raise ValueError("Weekly Fama-French data is only available for the 3-factor model.")
 

    def _get_url(self) -> dict[str, str]:
        """Constructs the URLs for downloading Fama-French data."""
        base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"
        url_list = {}

        ff_url_name = self._ff_region_map.get(self.region)
        
        if ff_url_name == 'Emerging':
            # Emerging: only monthly 5-factors avail
            filename = "Emerging_5_Factors_CSV.zip"
        elif ff_url_name == 'US':
            _model = "F-F_Research_Data_5_Factors_2x3" if self.model in {"5", "6"} else "F-F_Research_Data_Factors"
            freq_map = {'d': '_daily', 'w': '_weekly'}
            suffix = freq_map.get(self.frequency, "")
            filename = f"{_model}{suffix}_CSV.zip"
        else:
            # International regions
            base_model = '3' if self.model in ['3', '4'] else '5'
            daily = '_Daily' if self.frequency == 'd' else ''
            filename = f"{ff_url_name}_{base_model}_Factors{daily}_CSV.zip"
            
        url_list['factors'] = f"{base_url}/{filename}"
        
        # Construct the momentum URL if needed.
        if self.model in ["4", "6"]:
            freq_suffix = "_daily" if self.frequency == 'd' else ""
            if self.region in ['us', None]:
                mom_filename = f"F-F_Momentum_Factor{freq_suffix}_CSV.zip"
            else:
                mom_filename = f"{self.region}_Mom_Factor{freq_suffix.title()}_CSV.zip"
            
            url_list['momentum'] = f"{base_url}/{mom_filename}"

        return url_list


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
            _start = 6 if self.region == 'emerging' else (12 if is_mom else 4)
            _stop = annual_marker if annual_marker else len(lines)
            raw_content = lines[_start:_stop]

        content = [",".join(_schema.names)]
        for line in raw_content:
            clean = line.strip()
            if not clean or "copyright" in clean.lower():
                if "copyright" in clean.lower(): self.copyright = clean
                continue
            
            # Use commas for split, not spaces!
            parts = [p.strip() for p in clean.split(',')]
            
            # only keeps lines that start with a date
            if parts and parts[0].isdigit():
                content.append(",".join(parts))

        return pv.read_csv(
            io.BytesIO("\n".join(content).encode('utf-8')),
            convert_options=pv.ConvertOptions(
                null_values=["-99.99", "-999", "-99.990", "-0.9999"],
                strings_can_be_null=True,
                column_types=_schema,
                include_columns=_schema.names,
            ),
        )   
    

    def _read(self, data: dict[str, bytes]) -> pa.Table:
        main_cols = [f for f in self.schema if f.name not in ["UMD", "MOM", "WML"]]
        
        # Emerging Markets: base file is 5 factors (even for models 3/4)
        if self.region == 'emerging' and self.model in ["3", "4"]:
            main_cols.insert(4, pa.field("RMW", pa.float64()))
            main_cols.insert(5, pa.field("CMA", pa.float64()))

        main_schema = pa.schema(main_cols)
        # data['factors']
        table = self._read_zip(data['factors'], use_schema=main_schema)

        if 'momentum' in data:
            mom_table = self._read_zip(data['momentum'], use_schema=self._mom_schema)
            
            # momentum column name based on schema
            mom_key = next(k for k in ["UMD", "MOM", "WML"] if k in self.schema.names)
            mom_table = mom_table.rename_columns(["date", mom_key])
            
            table = table.join(mom_table, keys="date", join_type="inner").combine_chunks()

        table = table.set_column(0, "date", table.column(0).cast(pa.string()))
        table = offset_period_eom(table, self.frequency)
        table = scale_to_decimal(table)
        
        table = round_to_precision(table, self._precision)

        return table.select(self.schema.names).combine_chunks()


# Idea, testing something
#----------------------------------------------------------------------------
# Extremely roughed in
# Rough implementation of Fama-French portfolio returns. Lots of redundancies
# btw Industry portfolios and these. Need TODO: FactorModel to BaseModel. Just... 
# it works right now.

class _FFPortfolioBase(FactorModel, ABC):
    """A base class for Fama French portfolio return data."""
    @property
    def _precision(self) -> int: return 6

    @abstractmethod
    def _get_ff_table(self, lines: list[str]) -> str:
        """Abstract method to select and clean portfolio files"""
        ...

    def __init__(self,
                 frequency: str = 'm',
                 weights: str = 'vw',
                 *,
                 dividends: bool = True,
                 **kwargs) -> None:
        super().__init__(frequency=frequency, **kwargs)

        self.dividends = dividends
        self.weights = weights.lower()

        if self.weights not in ('vw', 'ew'):
            raise ValueError(
                f"`weights` must be 'vw' or 'ew', got '{self.weights}'"
            )

    def _fix_ff_nulls(self, table: pa.Table) -> pa.Table:
        """Helper to transform Fama-French values representing NaNs to nulls.

        Converts -99.99, -999 values to null.
        """
        # TODO: FIXME: pyarrow.csv should be able to parse the nulls properly. Isn't.
        #   As unlikely as it may be, there's >1.0 values (e.g., Insurance industry,
        #   annual, 1927), so this could convert a legitmate 0.9999 to null.
        ff_nans = [-99.99, -999.0, -99.9] # fix: use before scale_to_decimal. Will NaN a 0.999% or 9.99% return.

        for i, name in enumerate(table.column_names):
            if name.lower() == "date": continue
            col = table.column(name)
            
            # Create a boolean mask which yields True if ff_nans is_in a col of floats.
            mask = pc.is_in(col, value_set=pa.array(ff_nans, type=pa.float64()))
            null_scalar = pa.scalar(None, type=col.type)
            new_col = pc.if_else(mask, null_scalar, col)
            table = table.set_column(i, name, new_col)

        return table


    def _clean_column_names(self, table: pa.Table) -> pa.Table:
        """Standardize column names across all portfolio types."""
        renames = [
            name.strip().lower().replace(" ", "_").replace("-", "_")
            for name in table.column_names
        ]
        return table.rename_columns(renames)


    def _read(self, data: bytes) -> pa.Table:
        """Common _read for all Fama-French portfolios."""
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            with z.open(z.namelist()[0]) as f:
                lines = f.read().decode('utf-8').splitlines()

        clean_csv = self._get_ff_table(lines)

        table = pv.read_csv(
            io.BytesIO(clean_csv.encode('utf-8')),
            parse_options=pv.ParseOptions(),
            convert_options=pv.ConvertOptions(
                null_values=["-99.99", "-999", "-99.9"],
                column_types={"date": pa.string()},
            ),
        )
        table = self._fix_ff_nulls(table)

        table = offset_period_eom(table, self.frequency)
        table = scale_to_decimal(table)
        table = self._clean_column_names(table)

        return table.combine_chunks()

    @property
    def schema(self) -> pa.Schema:
        """Fama-French portfolio schema. Returns actual schema if loaded."""
        if self._data is not None:
            return self._data.schema
        # Date only if not loaded? 
        return pa.schema([pa.field("date", pa.date32())])


class _FamaFrenchFactorPortfolios(_FFPortfolioBase):
    """Fama-French factor-sorted portfolios (univariate, bivariate, three-way)."""
    @property
    def _frequencies(self) -> list[str]: return ['d', 'w', 'm', 'y']
    def __init__(self,
                 frequency: str = 'm',
                 formed_on: str | list[str] = 'size',
                 weights: str = 'vw',
                 sort: str | int | None = None,
                 *,
                 dividends: bool = True,
                 **kwargs) -> None:

        super().__init__(frequency=frequency, weights=weights,
                         dividends=dividends, **kwargs)

        if isinstance(formed_on, str):
            self.formed_on = [formed_on.lower()]
        else:
            self.formed_on = sorted([f.lower() for f in formed_on])

        self.is_multivariate = len(self.formed_on) > 1

        if sort is None:
            if len(self.formed_on) == 1:
                sort = 'decile'
            elif len(self.formed_on) == 2:
                if any(f in {'ep', 'cfp', 'dp'} for f in self.formed_on):
                    sort = '2x3' # these are only avail as 2x3 sorts
                else:
                    sort = '5x5' 
            else:
                sort = '2x4x4'

        self.sort = str(sort).lower()

        sort_to_n = {
            'tertile': 3, 'quintile': 5, 'decile': 10,
            '3': 3, '5': 5, '10': 10,
            '2x3': 6, '6': 6,
            '5x5': 25, '25': 25,
            '10x10': 100, '100': 100,
            '2x4x4': 32, '32': 32,
        }

        if self.sort not in sort_to_n:
            raise ValueError(
                f"sort must be one of {list(sort_to_n.keys())}, got '{self.sort}'"
            )

        self.n_portfolios = sort_to_n[self.sort]
        self._validate_params()


    def _validate_params(self) -> None:
        valid_factors = {
            'size', 'bm', 'op', 'inv', 'ep', 'cfp', 'dp', 'mom',
            'st_rev', 'lt_rev', 'ac', 'beta', 'net_shares', 'var', 'resvar'
        }

        for f in self.formed_on:
            if f not in valid_factors:
                raise ValueError(
                f"Invalid factor: '{self.formed_on}'. "
                f"Valid factors: {sorted(valid_factors)}"
            )
        if self.is_multivariate:
            if self.n_portfolios not in {6, 25, 100, 32}:
                raise ValueError(
                    f"Multivariate sorts (factors: {self.formed_on}) only support "
                    f"sorts for 2x3, 5x5, 10x10 (6, 25, 100). Got {self.n_portfolios}."
                )
        else:
            if self.n_portfolios not in {3, 5, 10}:
                raise ValueError(
                    f"Univariate sorts (factor: {self.formed_on[0]}) "
                    f"must be 3, 5, or 10. Got {self.n_portfolios}."
                ) 

        month_year_only = {'ep', 'cfp', 'dp', 'beta', 'ac',
                           'net_shares', 'var', 'resvar'}
        if (self.frequency == 'd' and
                any(f in month_year_only for f in self.formed_on)):
            raise ValueError(
                f"Daily data not available for {self.formed_on}. "
                f"Use frequency='m' or 'y'."
            )

        # Sorts on prior returns are decile only
        prior_return_factors = {'mom', 'st_rev', 'lt_rev'}
        if (any(f in prior_return_factors for f in self.formed_on) and
                self.n_portfolios != 10):
            raise ValueError(
                f'{self.formed_on} is only available in deciles.'
            )

    def _get_url(self) -> str:
        """Construct Fama-French portfolio URL."""
        if self.is_multivariate:
            return self._multivariate_url()
        return self._univariate_url()


    def _univariate_url(self) -> str:
        """Helper to map input to slug in URL"""
        base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"
        # Maps input to a slug for URL construction. Comments are the sub-
        # headers on the page.
        mapping = {
            # Univariate sorts
            'size': 'Portfolios_Formed_on_ME',
            'bm': 'Portfolios_Formed_on_BE-ME',
            'op': 'Portfolios_Formed_on_OP',
            'inv': 'Portfolios_Formed_on_INV',
            # Univariate sorts on E/P, CF/P, D/P. [Month/year only]
            'ep': 'Portfolios_Formed_on_E-P',
            'cfp': 'Portfolios_Formed_on_CF-P',
            'dp': 'Portfolios_Formed_on_D-P',
            # Sorts involving prior returns
            'mom': '10_Portfolios_Prior_12_2',
            'st_rev': '10_Portfolios_Prior_1_0',
            'lt_rev': '10_Portfolios_Prior_60_13',
            # Sorts involving Accruals[...] [month/year only]
            'ac': 'Portfolios_Formed_on_AC',
            'beta': 'Portfolios_Formed_on_BETA',
            'ni': 'Portfolios_Formed_on_NI', # Net share issues
            'var': 'Portfolios_Formed_on_VAR',       # variance
            'resvar': 'Portfolios_Formed_on_RESVAR', # residual variance
        }

        factor = self.formed_on[0]
        base_name = mapping.get(factor)

        if not base_name:
            raise ValueError(f"Unknown factor: {factor}")

        if factor in {'mom', 'lt_rev', 'st_rev'} and not self.dividends:
            self.log.warning(
                "Fama-French does not provide ex-div momentum portfolios. "
                "Ignoring dividends=False."
            )
            suffix = "_CSV.zip"
        elif self.frequency == 'd':
            suffix = "_Daily_CSV.zip"
        elif not self.dividends:
            suffix = "_Wout_Div_CSV.zip"
        else:
            suffix = "_CSV.zip"

        return f"{base_url}/{base_name}{suffix}"


    def _multivariate_url(self) -> str:
        """Construct URL for multivariate sorts."""
        base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"
        factors = frozenset(self.formed_on)
        n = self.n_portfolios

        three_way_map = {
            # "Three-way sorts on Size, B/M, OP, and Inv" 
            frozenset(['size', 'bm', 'op']): "32_Portfolios_ME_BEME_OP_2x4x4",
            frozenset(['size', 'bm', 'inv']): "32_Portfolios_ME_BEME_INV_2x4x4",
            frozenset(['size', 'op', 'inv']): "32_Portfolios_ME_OP_INV_2x4x4",
        }

        if len(factors) == 3:
            slug = three_way_map.get(factors)
            if not slug:
                raise ValueError(f"3-way sort {self.formed_on} not supported.")
        else:
            bivariate_map = {
                # All: "Bivariate sorts on Size, B/M, OP and Inv"
                frozenset(['size', 'bm']): "",
                frozenset(['size', 'op']): "ME_OP",
                frozenset(['size', 'inv']): "ME_INV",
                # n=25 only
                frozenset(['bm', 'op']): "BEME_OP",
                frozenset(['bm', 'inv']): "BEME_INV",
                frozenset(['op', 'inv']): "OP_INV",
                # All "Bivariate sorts on Size, E/P, CF/P, and D/P"  n=6 only
                frozenset(['size', 'ep']): "ME_EP",
                frozenset(['size', 'cfp']): "ME_CFP",
                frozenset(['size', 'dp']): "ME_DP",
                # All bivariate sorts in "Sorts involving Accruals, Market Beta,
                # Net Share Issues, Daily Variance, and Daily Residual Variance"
                # n = 25 only
                frozenset(['size', 'ac']): "ME_AC",     # accruals
                frozenset(['size', 'beta']): "ME_BETA",
                frozenset(['size', 'ni']): "ME_NI",      # net share issues
                frozenset(['size', 'var']): "ME_VAR",
                frozenset(['size', 'resvar']): "ME_RESVAR",   # residual variance
                # Sorts involving Prior Returns - no option for excluding divs.
                frozenset(['size', 'mom']): "ME_Prior_12_2",
                frozenset(['size', 'st_rev']): "ME_Prior_1_0",
                frozenset(['size', 'lt_rev']): "ME_Prior_60_13",
            }
            slug = bivariate_map.get(factors)
            if slug is None:
                raise ValueError(f"Multivariate combination '{self.formed_on}' not supported.")

        if self.frequency == 'd':
            file_ext = "Daily_CSV.zip"
        elif not self.dividends:
            file_ext = "Wout_Div_CSV.zip"
        else:
            file_ext = "CSV.zip"

        # 3-way sorts
        if len(factors) == 3:
            return f"{base_url}/{slug}_{file_ext}"
        
        grid_parts = {
            6: ("6_Portfolios", "2x3"),
            25: ("25_Portfolios", "5x5"),
            100: ("100_Portfolios", "10x10"),
            32: ("32_Portfolios", "2x4x4"),
        }
        prefix, suffix = grid_parts[n]

        # Bivariate sorts
        if not slug:
            return f"{base_url}/{prefix}_{suffix}_{file_ext}"
        if "Prior" in slug: # prior rets
            return f"{base_url}/{prefix}_{slug}_{file_ext}"
        
        # Univariate
        return f"{base_url}/{prefix}_{slug}_{suffix}_{file_ext}"


    def _get_ff_table(self, lines: list[str]) -> str:
        """Select correct table and clean whitespace for CSV parsing."""
        weight_ln = "Value" if self.weights == 'vw' else "Equal"
        freq_map = {'y': 'Annual', 'd': 'Daily', 'm': 'Monthly', 'w': 'Weekly'}
        freq_str = freq_map.get(self.frequency, "Monthly")

        table_start = None
        for i, ln in enumerate(lines):
            if (weight_ln in ln and freq_str in ln and
                    ("Returns" in ln or "Prior" in ln)):
                table_start = i
                break

        if table_start is None:
            raise ValueError(
                f"Could not find `{weight_ln}` section for `{freq_str}`"
            )

        # header line
        header_line = table_start + 1
        while header_line < len(lines) and not lines[header_line].strip():
            header_line += 1

        if header_line >= len(lines):
            raise ValueError("No header found in table")

        headers = ("date " + " ".join(lines[header_line].split())).lower()
        cleaned = [headers]

        # Extract data
        for line in lines[header_line + 1:]:
            clean = line.strip()
            if not clean or not clean[0].isdigit():
                break
            cleaned.append("  ".join(clean.split()))

        return "\n".join(cleaned)


    @override
    def _read(self, data: bytes) -> pa.Table:
        table = super()._read(data) # base _read, then slice

        if (not self.is_multivariate and
                self.formed_on[0] not in {'mom', 'st_rev', 'lt_rev'}):

            # fix(?): col handling when <=0 in cols.
            has_negative_col = any("<=" in name for name in table.column_names[:2])
            mapping = {
                3: slice(2, 5) if has_negative_col else slice(1, 4),
                5: slice(5, 10) if has_negative_col else slice(4, 9),
                10: slice(10, 20) if has_negative_col else slice(9, 19),
            }
            
            target_slice = mapping.get(self.n_portfolios)

            # 'prior returns' portfolios are decile only. Only slicing 
            # if the table has deciles/quintiles/tertiles.
            if target_slice and table.num_columns > 16: #or == 21
                indices = [0] + list(range(target_slice.start, target_slice.stop))
                indices = [i for i in indices if i < table.num_columns]
                table = table.select(indices)

        return table.combine_chunks()


class _FFIndustryPortfolios(_FFPortfolioBase):
    """Fama-French industry portfolios."""
    _INDUSTRY_PORTFOLIO_SIZES = {5, 10, 12, 17, 30, 38, 48, 49}

    @property
    def _frequencies(self) -> list[str]: return ['d', 'm', 'y']
    
    #@property
    #def _regions(self) -> list[str]: return ['us']

    def __init__(self,
                 n_portfolios: int = 5,
                 frequency: str = 'm',
                 weights: str = 'vw',
                 *,
                 dividends: bool = True,
                 **kwargs) -> None:

        if n_portfolios not in self._INDUSTRY_PORTFOLIO_SIZES:
            raise ValueError(
                f"n_portfolios must be one of {sorted(self._INDUSTRY_PORTFOLIO_SIZES)}, "
                f"got {n_portfolios}"
            )

        self.n_portfolios = n_portfolios
        super().__init__(frequency=frequency, weights=weights,
                         dividends=dividends, **kwargs)


    def _get_url(self) -> str:
        """Get industry portfolio URL."""
        base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"

        if self.frequency == 'd':
            suffix = "daily_CSV.zip"
        elif not self.dividends:
            suffix = "Wout_Div_CSV.zip"
        else:
            suffix = "CSV.zip"

        filename = f"{self.n_portfolios}_Industry_Portfolios_{suffix}"
        return f"{base_url}/{filename}"


    def _get_ff_table(self, lines: list[str]) -> str:
        """Select correct industry portfolio table and clean whitespace."""
        w_str = "Value Weighted" if self.weights == 'vw' else "Equal Weighted"
        freq_map = {'y': 'Annual', 'd': 'Daily', 'm': 'Monthly'}
        freq_str = freq_map.get(self.frequency, "Monthly")

        start_line = f"Average {w_str} Returns -- {freq_str}"

        idx = next((i for i, ln in enumerate(lines) if start_line in ln), None)
        if idx is None:
            raise ValueError(f"Could not find section: {start_line}")

        cleaned = []
        headers = ("date " + " ".join(lines[idx + 1].split())
                   ).lower().replace("-", "_")
        
        cleaned.append(headers)

        # Extract data
        for line in lines[idx + 2:]:
            clean = line.strip()
            if not clean or not clean[0].isdigit():
                break
            cleaned.append("  ".join(clean.split()))

        return "\n".join(cleaned)


# Public, prob. to main, here for now.
def FamaFrenchPortfolios(
        formed_on: str | list[str] = 'size',
        sort: str | int | None = None,
        industry: int | None = None,
        frequency: str = 'm',
        weights: str = 'vw',
        **kwargs) -> pa.Table:
    # TODO: This docstr properly. really properly.
    """Factory for Fama French portfolios.

    Args:
        frequency (str): 'd', 'w', 'm', 'y'
        formed_on (str | list[str] | set | tuple): 
        sort (str, int): 'decile', 'quintile', 'tertile', '2x3', '5x5'
            '10x10', '2x2x4'. Accepts int: 10, 5, 3, 6, 25, 100, 32.
        industry (int): for Fama French Industry Portfolios. Specify 
            n portfolios, industry=12
        weights: 'vw', 'ew'

    Examples:
        ff = FamaFrenchPortfolios(industry=12)
        ff = FamaFrenchPortfolios(formed_on='mom', sort='decile')
        ff = FamaFrenchPortfolios(frequency = 'd', formed_on=['op', 'size'], sort='2x3')
        ff = FamaFrenchPortfolios(formed_on='size', sort=6, weights='ew')
        ff = FamaFrenchPortfolios(industry=30, frequency='m', weights='vw')
    """
    # we industry?
    if industry is not None:
        return _FFIndustryPortfolios(
            frequency=frequency, weights=weights, n_portfolios=industry, **kwargs
        )

    # keep formed_on None, let class handle it.
    return _FamaFrenchFactorPortfolios(
        frequency=frequency, weights=weights, formed_on=formed_on, sort=sort, **kwargs
    )




## TODO: LT_rev, ST_rev...
