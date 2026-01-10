#!/usr/bin/env python3
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
import logging
from abc import ABC, abstractmethod
from typing import override
from pathlib import Path
from typing import Any
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
        try:
            table = self.data
        except Exception:
            return self.__repr__()

        if isinstance(self, RegionMixin):
            region_label = f" ({self.region})"
        else:
            region_label = ""

        header = f"{self.__class__.__name__}{region_label}\n"
        return header + print_table_preview(table)

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

        # str(self) calls get_table_preview, wrap in <pre>
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
        table = self._get_table()

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
        """Select specific factors from the model. Str or list[str].

        Stateful: Sets the view to only these factors.
        """
        table = select_table_columns(self._get_table(), factor)
        self._selected_factors = [f for f in table.column_names if f != 'date']
        return self


    def drop(self, factor: str | list[str]) -> "FactorModel": #Self
        """Remove specific factors from the model. Str or list[str].

        Stateful: Removes these factors from the view.
        """
        t_cols = self._get_table().column_names
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
        Args
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

        - Wrapper around Arrow's `to_pandas()`.
        - Triggers the download if not loaded.
        """
        try:
            import pandas as pd  # check for if user has pd?
            df = self.data.to_pandas()
            if "date" in df.columns:
                df = df.set_index("date")
            return df
        except ImportError:
            raise ImportError("Requires Pandas. `pip install pandas`") from None


    def to_polars(self) -> "pl.DataFrame":
        """Convert model to a polars DataFrame.

        - Wrapper around Polars' `from_arrow()`. 
        - Triggers the download if not loaded.
        """
        try:
            import polars as pl
            return pl.from_arrow(self.data)
        except ImportError:
            raise ImportError("Requires Polars. `pip install polars`") from None
    # maybe
    #def preview(self, n: int = 4):
    #    """Prints the formatted table preview."""
    #    print(print_table_preview(self.data, n_rows=n))


    def _get_table(self, client: _HttpClient | None = None) -> pa.Table: # New: pass client to it
        """Internal: triggers download if cache empty.

        Returns the full table/data.
        """
        if self._data is None:
            raw_bytes = self._download(client=client)
            table = self._read(raw_bytes)

            # fix: slice of data for AQR models with country (order isn't guarenteed after a join)
            if "date" in table.column_names:
                table = table.sort_by([("date", "ascending")])

            table = round_to_precision(table, self._precision)
            table.validate(full=True)
            self._data = rearrange_columns(table=table).combine_chunks()

        return self._data


    # New: allows a dict or str, multi-dl using dict comprehension
    # New: pass a client in.
    def _download(self, client: _HttpClient | None = None) -> bytes | dict[str, bytes]:
        urls = self._get_url()
        self.log.info(f"Downloading data from: {urls}")
        # will do properly later...
        try:
            if client is not None:
                # A client was given. Using it.
                if isinstance(urls, str):
                    return client.download(urls, self.cache_ttl)
                return {k: client.download(v, self.cache_ttl) for k, v in urls.items()}
            #else:
            with _HttpClient() as _client:
                if isinstance(urls, str):
                    return _client.download(urls, self.cache_ttl)
                return {k: _client.download(v, self.cache_ttl) for k, v in urls.items()}

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
            df = pl.from_dataframe(model.data)

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
    def _precision(self) -> int:
        return 8

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



# ---------------------------------------------------------------------
# New: regional mixin (this unifies country/region, and removes the region 
# property from AQR/FF models). Adds list_regions, a regions property, 
# getter/setter. 
class RegionMixin:
    """Mixin for models that support international regions/countries."""
    # just here for now... removes some friction
    # TODO: long region names
    _aliases = {
        #AQR (not VME), FF
        'usa': 'us', 'us': 'usa',
        'jpn': 'japan', 'japan': 'jpn',
        'uk': 'gbr', 
        'ger': 'deu',
        # FF regions : AQR Aggregate Portfolios. 
        # Note: AQR's 'pacific' includes japan.
        # 'europe', 'north america', 'pacific', 'global', 'global ex usa',
        'ex-us': 'global ex usa',
        'north-america': 'north america', 'north america': 'north-america',
        #for VME
        'developed': 'everywhere',
        'global': 'everywhere',
        'global_stocks': 'all_equities',
        'macro': 'all_other',
    }

    @property
    @abstractmethod
    def _regions(self) -> list[str]:
        pass

    @property
    def region(self) -> str:
        # default set to 'us' or 'usa' (depends whats in _region!)
        if not hasattr(self, "_region"):
            return 'us' if 'us' in self._regions else 'usa'
        return self._region
    @region.setter
    def region(self, value: str | None):
        val = str(value).strip().lower() if value else self.region

        resolved = None
        if val in self._regions:
            resolved = val
        elif self._aliases.get(val) in self._regions:
            resolved = self._aliases.get(val)
        if not resolved:
            raise ValueError(f"Invalid region '{value}'. Supported: {self._regions}")
        if hasattr(self, "_region") and resolved != self._region:
            msg = f"Region changed to {resolved}. Cache reset."
            # Mixin can use self.log:
            #   the child class that mixed it in inherited it.
            self.log.info(msg)
            self._data = None
            #self._selected_factors = None # Reset selected factors when region changes?
        self._region = resolved

    @classmethod
    def list_regions(cls) -> list[str]:
        """List available regions."""
        # List regions without instantiation
        if isinstance(cls._regions, property):
            # if a property on an uninstantiated class, reach into the fget
            return cls._regions.fget(cls) 
        return cls._regions


# NEW: CHILD CLASS FOR CONSTRUCTED MODELS
class CompositeModel(FactorModel):
    """Base for models constructed by joining multiple other FactorModels.

    Ensures a single `_HttpClient` session is shared across all models 
    used for construction.
    """
    def __init__(self, frequency: str = 'm', **kwargs):
        super().__init__(frequency=frequency, **kwargs)

    @abstractmethod
    def _construct(self, client: _HttpClient) -> pa.Table:
        pass
    
    @override
    def _get_table(self, *, client: _HttpClient | None = None, keep_nulls: bool = False) -> pa.Table:
        """Internal: triggers download if cache empty, returning the full dataset.

        Args:
            client (obj): an instance of _HttpClient. (None by default)
            keep_nulls (bool): if True, drops rows with nulls.
        
        Note:
            Overrides FactorModel._get_table
        """
        # TODO: keep_nulls needs a util that drops only continuous nans from boundaries.
        if self._data is not None:
            return self._data

        if client is None:
            with _HttpClient() as shared_client:
                self._data = self._construct(shared_client)
        else:
            self._data = self._construct(client)
        
        table = self._data

        table = table.sort_by([("date", "ascending")])
        table = round_to_precision(table, self._precision)
        table = rearrange_columns(table=table)

        if not keep_nulls:
            table = table.drop_null()

        self._data = table.combine_chunks()
        self._data.validate(full=True) 

        return self._data

    def _get_url(self) -> str:
        raise NotImplementedError("CompositeModel: no remote source.")
    def _read(self, data: bytes) -> pa.Table:
        raise NotImplementedError("CompositeModel: _read called on a composite models.")
