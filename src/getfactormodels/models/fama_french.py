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
from getfactormodels.models.base import FactorModel, RegionMixin, PortfolioBase
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


# WIP ---------------------------------------------------------------------- #
# TODO: FactorModel to BaseModel.
class _FFPortfolioBase(PortfolioBase, ABC):
    """Base class for Fama-French portfolio return data."""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @abstractmethod
    def _find_table_start(self) -> str:
        """A pattern to identify the specific table's start."""
        ...

    def _get_ff_table(self, lines: list[str]) -> str:
        weight_ln = "Value" if self.weights == 'vw' else "Equal"
        
        # FIX: daily size/op CSVs incorrectly have '-- Monthly' in the headers, the TXT files 
        # are correct. Not using freq now..
        table_start = None
        for i, ln in enumerate(lines):
            # weights and rets, no freq
            if weight_ln in ln and ("Returns" in ln or "Prior" in ln):
                table_start = i
                break

        if table_start is None:
            raise ValueError(
                f"Could not find table header: `{weight_ln}`"
            )

        # Skip title line, find first non empty line
        header_idx = table_start + 1
        while header_idx < len(lines) and not lines[header_idx].strip():
            header_idx += 1

        if header_idx >= len(lines):
            raise ValueError("Couldn't find table header (found table title).")

        headers = ("date " + " ".join(lines[header_idx].split())).lower().replace("-", "_")
        cleaned = [headers]

        # Extract lines
        for line in lines[header_idx + 1:]:
            clean = line.strip()
            # Table's over if line doesn't start with a number
            if not clean or not clean[0].isdigit():
                break
            cleaned.append("  ".join(clean.split()))

        return "\n".join(cleaned)


    def _read(self, data: bytes) -> pa.Table:
        """FF portfolio read."""
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            with z.open(z.namelist()[0]) as f:
                lines = f.read().decode('utf-8').splitlines()

        clean_csv = self._get_ff_table(lines)

        table = pv.read_csv(
            io.BytesIO(clean_csv.encode('utf-8')),
            convert_options=pv.ConvertOptions(
                null_values=["-99.99", "-999", "-99.9"],
                column_types={"date": pa.string()},
            ),
        )

        table = self._fix_ff_nulls(table)
        table = offset_period_eom(table, self.frequency)
        table = scale_to_decimal(table)

        renames = [n.strip().lower().replace(" ", "_").replace("-", "_") for n in table.column_names]
        table = table.rename_columns(renames)

        return table.combine_chunks()


    def _fix_ff_nulls(self, table: pa.Table) -> pa.Table:
        ff_nans = pa.array([-99.99, -999.0, -99.9], type=pa.float64())
        for i, name in enumerate(table.column_names):
            if name.lower() == "date": continue
            col = table.column(name)
            mask = pc.is_in(col, value_set=ff_nans)
            table = table.set_column(i, name, pc.if_else(mask, pa.scalar(None, type=col.type), col))
        return table


    @property
    def schema(self) -> pa.Schema:
        """Fama-French portfolio schema."""
        if self._data is not None:
            return self._data.schema
        return pa.schema([pa.field("date", pa.date32())])
    
    @property
    def _precision(self) -> int: return 6

class _FamaFrenchSorts(_FFPortfolioBase):
    """Fama-French factor-sorted portfolios (Univariate and Multivariate)."""

    @property
    def _frequencies(self) -> list[str]:
        return ['d', 'w', 'm', 'y']

    def __init__(self,
                 formed_on: str | list[str] = 'size',
                 sort: str | int | None = None,
                 **kwargs):
        super().__init__(**kwargs)
        # TODO: REGIONS

        # formed_on: sorted list of lowercase strings
        if isinstance(formed_on, str):
            self.formed_on = [formed_on.lower()]
        else:
            self.formed_on = sorted([f.lower() for f in formed_on])

        self.sort = str(sort).lower() if sort is not None else self._get_default_sort()
        self.n_portfolios = self._map_sort_to_count()
        self.is_multivariate = len(self.formed_on) > 1

        self._validate_params()


    def _find_table_start(self) -> str:
        """Identify the table section based on weights."""
        return "Equal Weighted Returns" if self.weights == 'ew' else "Value Weighted Returns"


    def _map_sort_to_count(self) -> int:
        """Map sort string/int to total portfolio count."""
        mapping = {
            'tertile': 3, '3': 3,
            'quintile': 5, '5': 5,
            'decile': 10, '10': 10,
            '2x3': 6, '6': 6,
            '5x5': 25, '25': 25,
            '10x10': 100, '100': 100,
            '2x4x4': 32, '32': 32,
        }
        if self.sort not in mapping:
            raise ValueError(f"Sort '{self.sort}' not recognized. Try 'decile', '5x5', etc.")
        return mapping[self.sort]


    def _get_default_sort(self) -> str:
        """Assigns a default sort if none provided."""
        if len(self.formed_on) == 1:
            return 'decile'
        if len(self.formed_on) == 2:
            if any(f in {'ep', 'cfp', 'dp'} for f in self.formed_on):
                return '2x3'
            return '5x5'
        return '2x4x4'


    def _validate_params(self):
        """Validates factors, frequency and sort types."""
        valid = {'size', 'bm', 'op', 'inv', 'ep', 'cfp', 'dp', 'mom',
                 'st_rev', 'lt_rev', 'ac', 'beta', 'ni', 'var', 'resvar'}
        for f in self.formed_on:
            if f not in valid:
                raise ValueError(f"Invalid factor: {f}. Must be one of {sorted(valid)}")

        no_daily = {'ep', 'cfp', 'dp', 'beta', 'ac', 'ni', 'var', 'resvar'}
        if self.frequency == 'd' and any(f in no_daily for f in self.formed_on):
            raise ValueError(f"Daily data is not available for sorts involving {self.formed_on}")

        prior_rets = {'mom', 'st_rev', 'lt_rev'}
        if not self.is_multivariate:
            # Univariate sorts only support specific cuts (3, 5, 10)
            # This fixes eg, "-p 2x3 --on op" returning the full table.
            if self.n_portfolios not in {3, 5, 10}:
                 raise ValueError(
                     f"Sort '{self.sort}' ({self.n_portfolios}) is not a valid univariate sort. "
                     f"Use 'tertile', 'quintile', 'decile' (or 3, 5 or 10)."
                 )

            if self.formed_on[0] in prior_rets: #noqa
                if self.n_portfolios != 10:
                    raise ValueError(f"{self.formed_on[0]} is only available in deciles.")
    
    @override
    def _read(self, data: bytes) -> pa.Table:
        table = super()._read(data)
        
        # fix: drop the "<= 0" column here (bivariates can contain it)
        neg_cols = [name for name in table.column_names if "<=" in name]
        if neg_cols:
            table = table.drop(neg_cols) #

        # Sorts on prior rets are decile only, the rest are a table 
        # of date [+ <= 0] + tertiles, quintiles, deciles.
        if not self.is_multivariate and self.formed_on[0] not in {'mom', 'st_rev', 'lt_rev'}:
            table = self._slice_univariate(table)
        
        return table.combine_chunks()


    def _slice_univariate(self, table: pa.Table) -> pa.Table:
        # No "< =" col now
        mapping = {
            3: slice(1, 4),
            5: slice(4, 9),
            10: slice(9, 19),
        }
        
        target_slice = mapping.get(self.n_portfolios)

        # Not allowing NxN for univariates, or decile for multivariates etc.
        if target_slice is None:
             raise ValueError(f"Cannot slice univariate table: no mapping for {self.n_portfolios} portfolios.")

        # Only slice if tertile/quintile/decile. (Sorts on prior rets are decile only)
        if target_slice and table.num_columns > 16:
            indices = [0] + list(range(target_slice.start, target_slice.stop))
            return table.select([i for i in indices if i < table.num_columns])

        return table


    def _get_url(self) -> str:
        """Constructs a URL for a portfolio .zip on the Ken French library."""
        base = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"

        if self.frequency == 'd':
            suffix = "_Daily_CSV.zip"
        elif not self.dividends:
            suffix = "_Wout_Div_CSV.zip"
        else:
            suffix = "_CSV.zip"

        # Univariate sorts
        if not self.is_multivariate:
            mapping = {
                'size': 'Portfolios_Formed_on_ME',
                'bm': 'Portfolios_Formed_on_BE-ME',
                'op': 'Portfolios_Formed_on_OP',
                'inv': 'Portfolios_Formed_on_INV',
                'ep': 'Portfolios_Formed_on_E-P',
                'cfp': 'Portfolios_Formed_on_CF-P',
                'dp': 'Portfolios_Formed_on_D-P',
                'mom': '10_Portfolios_Prior_12_2',
                'st_rev': '10_Portfolios_Prior_1_0',
                'lt_rev': '10_Portfolios_Prior_60_13',
                'ac': 'Portfolios_Formed_on_AC',
                'beta': 'Portfolios_Formed_on_BETA',
                'ni': 'Portfolios_Formed_on_NI',
                'var': 'Portfolios_Formed_on_VAR',
                'resvar': 'Portfolios_Formed_on_RESVAR',
            }
            return f"{base}/{mapping[self.formed_on[0]]}{suffix}"

        # Multivariate sorts
        factors = frozenset(self.formed_on)

        # 3-way sorts  # NOTE REGIONAL AVAIL.
        if len(factors) == 3:
            three_way = {
                frozenset(['size', 'bm', 'op']): "32_Portfolios_ME_BEME_OP_2x4x4",
                frozenset(['size', 'bm', 'inv']): "32_Portfolios_ME_BEME_INV_2x4x4",
                frozenset(['size', 'op', 'inv']): "32_Portfolios_ME_OP_INV_2x4x4",
            }
            slug = three_way.get(factors)
            if not slug: 
                raise ValueError("3-way sort not supported.")

            return f"{base}/{slug}{suffix}"

        # Bivariate sorts
        bivariate_map = {
            frozenset(['size', 'bm']): "",
            frozenset(['size', 'op']): "ME_OP",
            frozenset(['size', 'inv']): "ME_INV",
            frozenset(['bm', 'op']): "BEME_OP",
            frozenset(['bm', 'inv']): "BEME_INV",
            frozenset(['op', 'inv']): "OP_INV",
            frozenset(['size', 'ep']): "ME_EP",
            frozenset(['size', 'cfp']): "ME_CFP",
            frozenset(['size', 'dp']): "ME_DP",
            frozenset(['size', 'ac']): "ME_AC",
            frozenset(['size', 'beta']): "ME_BETA",
            frozenset(['size', 'ni']): "ME_NI",
            frozenset(['size', 'var']): "ME_VAR",
            frozenset(['size', 'resvar']): "ME_RESVAR",
            frozenset(['size', 'mom']): "ME_Prior_12_2",
            frozenset(['size', 'st_rev']): "ME_Prior_1_0",
            frozenset(['size', 'lt_rev']): "ME_Prior_60_13",
        }

        slug_part = bivariate_map.get(factors)
        if slug_part is None:
            raise ValueError(f"Bivariate combination {self.formed_on} not supported.")

        grid_map = {6: ("6_Portfolios", "2x3"),
                    25: ("25_Portfolios", "5x5"),
                    100: ("100_Portfolios", "10x10")}
        prefix, grid = grid_map.get(self.n_portfolios, ("25_Portfolios", "5x5"))

        if not slug_part: # bivariate
            return f"{base}/{prefix}_{grid}{suffix}"
        if "Prior" in slug_part: # bivariate sorts on prior returns
            return f"{base}/{prefix}_{slug_part}{suffix}"

        return f"{base}/{prefix}_{slug_part}_{grid}{suffix}"


class _FamaFrenchIndustryPortfolios(_FFPortfolioBase):
    """Fama-French industry portfolios."""
    _SIZES = {5, 10, 12, 17, 30, 38, 48, 49}

    @property
    def _frequencies(self) -> list[str]: return ['d', 'm', 'y']

    def __init__(self, n_portfolios: int | str = 5, **kwargs):
        n_portfolios = int(n_portfolios) # allow str through
        if n_portfolios not in self._SIZES:
            raise ValueError(f"Industry count must be one of {self._SIZES}")
        self.n_portfolios = n_portfolios
        super().__init__(**kwargs)


    def _find_table_start(self) -> str:
        w_str = "Value Weighted" if self.weights == 'vw' else "Equal Weighted"
        freq_map = {'y': 'Annual', 'd': 'Daily', 'm': 'Monthly'}

        return f"Average {w_str} Returns -- {freq_map.get(self.frequency, 'Monthly')}"


    def _get_url(self) -> str:
        base = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"

        if self.frequency == 'd':
            suffix = "daily_CSV.zip"
        else:
            suffix = "Wout_Div_CSV.zip" if not self.dividends else "CSV.zip"

        return f"{base}/{self.n_portfolios}_Industry_Portfolios_{suffix}"


def _get_ff_portfolios(
    formed_on: str | list[str] = 'size', #on=, sorted_on=, might rename
    sort: str | int | None = None, #2x3, decile, 25
    industry: int | None = None, #industries= ? allow str through, "12" 
    weights: str = 'vw',
    frequency: str = 'm',
    **kwargs) -> pa.Table:
    """Internal helper: factory to return.

    Factory for Fama French portfolios. Either `industry` or 
    `formed_on` required.
    """
    # we industry?
    if industry is not None:
        return _FamaFrenchIndustryPortfolios(
            frequency=frequency, weights=weights, n_portfolios=industry, **kwargs
        )

    # keep formed_on None, let class handle it.
    return _FamaFrenchSorts(
        frequency=frequency, weights=weights, formed_on=formed_on, sort=sort, **kwargs
    )

# TODO: setters/getters, regions, tests, aggregates for q? prob not. 
