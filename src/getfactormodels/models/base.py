# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
from typing import Literal, override
import pyarrow as pa
from getfactormodels.utils.arrow_utils import (
    filter_table_by_date,
    print_table_preview,
    rearrange_columns,
    round_to_precision,
    select_table_columns,
)
from getfactormodels.utils.date_utils import (
    _validate_input_date,
    validate_date_range,
)
from getfactormodels.utils.http_client import _HttpClient
from getfactormodels.utils.utils import _save_to_file

"""An abstract base class for model implementations.

- FactorModel: abstract base class. Provides common data handling, 
  caching, and date-filtering logic implemented by all factor models.
- RegionMixin: A mixin for models supporting international data.
- CompositeModel: base class for models built from other models.
- ModelCollection: for returning multiple models in a combined table.
"""
logger = logging.getLogger(__name__)


class FactorModel(ABC):
    """Abstract Base Class used by all factor model implementations."""
    def __init__(self, frequency: str | None = 'm',
                 start_date: str | None  = None,
                 end_date: str | None = None,
                 output_file: str | None = None,
                 cache_ttl: int = 86400,
                 **kwargs: Any):
        """Initialize the factor model instance.

        Args:
            frequency (str): the frequency of the data. Default: 'm'.
            start_date (str, opt): The start date YYYY[-MM-DD]
            end_date (str, opt): The end date YYYY[-MM-DD]
            output_file (str): optional path to save data to.
            cache_ttl (int): Cache time-to-live in seconds. Default: 86400.
            **kwargs: some models have additional params.
        """
        self.log = logger

        self._data: pa.Table | None = None
        self._start_date = None
        self._end_date = None
        self._frequency = None

        self.frequency = frequency 
        self.start_date = start_date 
        self.end_date = end_date
        self.output_file = output_file
        self.cache_ttl = cache_ttl

        self.copyright: str = ""  # NEW, TEST. fix: Carhart erroring with FF with copyright
        self._selected_factors: list[str] | None = None  # for eg drop/extract  # changing
        super().__init__()

    def __len__(self) -> int:
        """Length of the pa.Table after filtering."""
        return len(self.data)

    def __str__(self) -> str:
        if self.data is None:
            return self.__repr__()
        
       # table = self.data

        if isinstance(self, RegionMixin):
            region_label = f" ({self.region})"
        else:
            region_label = ""

        header = f"{self.__class__.__name__}{region_label}\n"
        return header + print_table_preview(self.data)

    def __repr__(self) -> str:
        params = []
        attrs = [
            'model', 'frequency', 'region', 'start_date', 'end_date',
            'country', 'classic', 'output_file',
        ]

        for attr in attrs:
            val = getattr(self, attr, None)

            if val is not None and val is not False and val != "":
                # format the str with quotes
                repr_val = f"'{val}'" if isinstance(val, str) else val
                params.append(f"{attr}={repr_val}")

        return f"{self.__class__.__name__}({', '.join(params)})"

    def __getitem__(self, key: str | list[str]) -> pa.Table:
        """Returns a pa.Table of date + selected factors."""
        return select_table_columns(self.data, key)

    def _repr_html_(self) -> str:
        """HTML repr for nice IPython/Jupyter outputs."""
        style = (
            "font-family: monospace; "
                "font-size: 0.9em; "
                "line-height: 1.4; "
                "background-color: transparent; "
                "white-space: pre; "
                "overflow-x: auto; "
                "display: block;"
        )

        # str(self) calls print_table_preview, wrap in <pre>
        return f'<pre style="{style}">{str(self)}</pre>'

    @property
    def start_date(self) -> str | None:
        return self._start_date
    @start_date.setter
    def start_date(self, value: Any):
        valid = _validate_input_date(value, is_end=False)
        self._start_date, self._end_date = validate_date_range(valid, self._end_date)

    @property
    def end_date(self) -> str | None:
        return self._end_date
    @end_date.setter
    def end_date(self, value: Any):
        valid = _validate_input_date(value, is_end=True)
        self._start_date, self._end_date = validate_date_range(self._start_date, valid)

    @property
    def frequency(self) -> str | None: 
        return self._frequency
    @frequency.setter
    def frequency(self, value: str | None):
        if value is None:
            self._frequency = None
            return

        val = value.lower()
        if val not in self._frequencies:
            raise ValueError(f"Invalid '{val}'. Options: {self._frequencies}") 

        if val != self._frequency:
            if self._frequency is not None:
                msg = f"Freq. changed from {self._frequency} to {val}." 
                self.log.info(msg)
            self._frequency = val
            self._data = None


    @property
    def data(self) -> pa.Table:
        """Returns a pa.Table with requested data (sliced)."""
        if self._data is None:
            self.load()   # Don't know if good idea
            
        table = self._data

        # drop/extract handled here.
        if self._selected_factors is not None:
            selection = list(dict.fromkeys(['date'] + self._selected_factors))
            table = table.select(selection)

        return filter_table_by_date(table, self._start_date, self._end_date)


    @property
    def shape(self) -> tuple[int, int]:
        """(rows, columns), like Pandas/Numpy."""
        return self.data.shape


    def extract(self, factor: str | list[str]) -> "FactorModel":   #Self
        """Select specific factors from the model. Str or list[str]."""
        table = select_table_columns(self.load(), factor)
        self._selected_factors = [f for f in table.column_names if f != 'date']
        return self


    def drop(self, factor: str | list[str]) -> "FactorModel": #Self
        """Remove specific factors from the model. Str or list[str]."""
        t_cols = self.load().column_names
        to_drop = {f.lower() for f in ([factor] if isinstance(factor, str) else factor)}

        # select: cols (lowercase) not in the to_drop set
        selection = [c for c in t_cols if c.lower() not in to_drop and c != 'date']

        if not selection:
            raise ValueError("Cannot drop all factors from the model.")

        self._selected_factors = selection
        return self


    def to_file(self, filepath: str | Path | None = None) -> None:
        """Save data to a file.

        Supports .parquet, .ipc, .feather, .csv, .txt, .pkl, .md
        
        Args:
            filepath (str | Path, optional): filepath to save data to.
        """
        target = filepath if filepath else self.output_file
        if not target:
            self.log.error("No filepath provided and no default output_file set.")
            return

        # self.data does get_table, selection, and date slicing.
        table = self.data

        if table.num_rows == 0:
            self.log.warning("No data available to save.")
            return

        _save_to_file(table, target, model_instance=self)


    def to_pandas(self) -> "pd.DataFrame":
        """Convert model to a pandas DataFrame.

        - Triggers the download if not loaded.
        """
        try:
            import pandas as pd  # check for if user has pd?
            df = self.data.to_pandas()
            if "date" in df.columns:
                df = df.set_index("date")
            return df
        except ImportError:
            raise ImportError("Requires Pandas. Try `pip install pandas`") from None


    def to_polars(self) -> "pl.DataFrame":
        """Convert model to a polars DataFrame.

        - Wrapper around Polars' `from_arrow()`. 
        - Triggers the download if not loaded.
        """
        try:
            import polars as pl
            return pl.from_arrow(self.data)
        except ImportError:
            raise ImportError("Requires Polars. Try `pip install polars`") from None
    # maybe
    #def preview(self, n: int = 4):
    #    """Prints the formatted table preview."""
    #    print(print_table_preview(self.data, n_rows=n))

    # RENAME: load, was _get_table
    def load(self, client: _HttpClient | None = None) -> pa.Table:
        """Trigger download or construction."""
        if self._data is not None:
            return self._data

        if hasattr(self, '_construct'):
            if client is None:
                with _HttpClient() as shared_client:
                    table = self._construct(shared_client)
            else:
                table = self._construct(client)
        else:
            raw_bytes = self._download(client=client)
            table = self._read(raw_bytes)

        # move this out probably ---------------------
        if "date" in table.column_names:
            table = table.sort_by([("date", "ascending")])

        table = round_to_precision(table, self._precision)

        # CompositeModel: if drop_null=True, drop
        if getattr(self, 'drop_null', False):
            table = table.drop_null()

        table.validate(full=True)
        self._data = rearrange_columns(table=table).combine_chunks()
        # -------------------------------------------
        return self


    def _download(self, client: _HttpClient | None = None) -> bytes | dict[str, bytes]:
        urls = self._get_url()
        self.log.info(f"Downloading from: {urls}")
        def _download_method(client: _HttpClient, urls: str | dict):
            if isinstance(urls, str):
                # A client was given. Using it.
                return client.download(urls, self.cache_ttl)
            return {k: client.download(v, self.cache_ttl) for k, v in urls.items()}
        try:
            if client:
                return _download_method(client, urls)
            with _HttpClient() as new_client:
                return _download_method(new_client, urls)
        except Exception as e:
            self.log.error(f"Download failed: {e}")
            raise RuntimeError(f"Could not retrieve data for {self.__class__.__name__}") from e


    # might move to utils
    def __dataframe__(self, *, nan_as_null: bool = False):
        """Dataframe interchange protocol support.

        Casts date32 to ns.

        Args:
            nan_as_null: converts NaN to null

        Examples:
            model = FamaFrenchFactors(model='3')
            df = model.to_pandas()

            import polars as pl
            df = pl.from_arrow(model.data)

            import pandas as pd
            df = pd.api.interchange.from_dataframe(model.data)
        """
        table = self.data

        if table.column_names[0] == "date":
            col_type = table.schema.field(0).type

        if pa.types.is_date(col_type):
            date_ns = table.column(0).cast(pa.timestamp("ns")).combine_chunks()
            table = table.set_column(0, "date", date_ns)

        return table.combine_chunks().__dataframe__(nan_as_null=nan_as_null, allow_copy=True)


    @property
    def _precision(self) -> int: return 8

    @property
    @abstractmethod
    def schema(self) -> pa.Schema:
        pass

    @property
    @abstractmethod
    def _frequencies(self) -> list[str]:
        pass

    @abstractmethod 
    def _get_url(self) -> str | dict[str, str]:
        """Build the unique data source URL."""
        pass

    @abstractmethod
    def _read(self, data: bytes) -> pa.Table:
        """Read bytes into a pa.Table."""
        pass


class CompositeModel(FactorModel):
    """Base for models constructed from other models."""
    def __init__(self, frequency: str = 'm', *, drop_null: bool = True, **kwargs):
        self.drop_null = drop_null
        super().__init__(frequency=frequency, **kwargs)

    @abstractmethod
    def _construct(self, client: _HttpClient) -> pa.Table:
        pass
    
    def _get_url(self) -> str:
        raise NotImplementedError("CompositeModel: no remote source.")
    
    def _read(self, data: bytes) -> pa.Table:
        raise NotImplementedError("CompositeModel: _read called on a composite models.")


class ModelCollection(CompositeModel):
    """A ModelCollection holds multiple models of the same frequencies."""
    def __init__(self, model_keys: list[str], **kwargs):
        from getfactormodels.main import model

        self.model_keys = model_keys
        self.instances = [model(m, **kwargs) for m in model_keys]

        regional_inst = [i for i in self.instances if isinstance(i, RegionMixin)]
        
        if regional_inst and len(regional_inst) < len(self.instances):
            req_region = kwargs.get('region')
            if req_region and req_region.lower() not in ['us', 'usa']: #allow USA through, as all models are US models. 
                raise ValueError("Cannot combine regional models with non-regional models for non-US regions.")
        super().__init__(**kwargs)


    def __str__(self) -> str:
        if self._data is None:
            return self.__repr__()

        region_label = ""
        regional_inst = next((i for i in self.instances if isinstance(i, RegionMixin)), None)
        if regional_inst:
            region_label = f" ({regional_inst.region})"

        models_str = ", ".join(f"'{k}'" for k in self.model_keys)
        header = f"{self.__class__.__name__}: {models_str} {region_label}\n"
        return header + print_table_preview(self.data)

    @property
    def _frequencies(self) -> list[str]:
        # the intersection of frequencies btw models
        freqs = set(self.instances[0]._frequencies)
        for inst in self.instances[1:]:
            freqs &= set(inst._frequencies)
        return list(freqs)

    @property
    def schema(self) -> pa.Schema:
        seen = {'date'}
        fields = [pa.field('date', pa.date32())]
        for inst in self.instances:
            for field in inst.schema:
                if field.name not in seen:
                    fields.append(field)
                    seen.add(field.name)
        return pa.schema(fields)
    

    def _construct(self, client: _HttpClient) -> pa.Table:
        tables = []
        for inst in self.instances:
            inst.load(client=client)
            table = inst.data
            table = table.set_column(0, "date", table.column("date").cast(pa.date32()))
            tables.append(table)

        # start with the first table
        final_table = tables[0]
        
        for i, next_table in enumerate(tables[1:], 1):
            # finding (probable) duplicate columns (not 'date') TODO: rename some factors...
            overlaps = set(final_table.column_names) & set(next_table.column_names)
            overlaps.discard('date')
            
            keep_cols = [c for c in next_table.column_names 
                                  if c not in overlaps and c != 'date']

            if keep_cols:
                cols_to_join = ['date'] + keep_cols
                table_to_join = next_table.select(cols_to_join)

                final_table = final_table.join(
                    table_to_join, 
                    keys="date", 
                    join_type="left outer",
                )
            
        return final_table.sort_by([("date", "ascending")])


class RegionMixin:
    """Mixin for models that support international regions/countries."""
    #TODO: map properly!
    _aliases = {
        'uk': 'gbr', 'ger': 'deu', 'sg': 'sgp',
        'hk': 'hkg', 'nl': 'nld', 'fr': 'fra',

        # FF and AQR (not VME) - shared countries
        'usa': 'us', 'us': 'usa',
        'jpn': 'japan', 'japan': 'jpn',

        # FF and AQR (not VME) - shared regions : aggregate portfolios
        'ex-us': 'global ex usa',
        'north-america': 'north america', 'north america': 'north-america',

        # FF regions : AQR Aggregate Portfolios. 
        # Note: AQR's 'pacific' includes japan.
        #for VME
        'global_stocks': 'all_equities',
        
        # Aus
        'australia': 'aus', 'au': 'aus',
    }

    @property
    @abstractmethod
    def _regions(self) -> list[str]:
        pass

    @property
    def region(self) -> str:
        if hasattr(self, "_region") and self._region is not None:
            return self._region
        if not self._regions:
            return ""

        # set default: usa, else first region if no usa in model's regions... 
        if 'us' in self._regions:
            return 'us'
        if 'usa' in self._regions:
            return 'usa'
        return self._regions[0]
            
    @region.setter
    def region(self, value: str | None):
        if value is None:
            self._region = None
            return

        val = str(value).strip().lower()
        # Try a direct match, then alias, else keep (to fail)
        resolved = val if val in self._regions else self._aliases.get(val, val)
        
        if resolved not in self._regions:
            raise ValueError(f"Region '{value}' not supported for {self.__class__.__name__}. "
                             f"Available: {self._regions}")

        current = getattr(self, "_region", None)
        if current is not None and current != resolved:
            self._data = None
            if hasattr(self, "log"):
                self.log.info(f"Region changed to {resolved}. Cache reset.")

        self._region = resolved
    
    @classmethod
    def list_regions(cls) -> list[str]:
        """List available regions without instantiation."""
        if isinstance(cls._regions, property):
            # reach into the fget of the property
            return cls._regions.fget(cls) 
        return cls._regions


class PortfolioBase(FactorModel, ABC):
    """Base class for portfolio return data."""
    def __init__(self, frequency: str = 'm',
                 weights: Literal['vw', 'ew'] = 'vw',
                 *,
                 dividends: bool = True,
                 **kwargs):
        super().__init__(frequency=frequency, **kwargs)

        self.dividends = dividends
        self.weights = weights.lower()

        if self.weights not in ('vw', 'ew'):
            msg = f'weights should be either "vw" or "ew", not {weights}'
            raise ValueError(msg)

    @property
    def _precision(self) -> int: return 6
