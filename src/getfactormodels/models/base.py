# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal, Any
import pyarrow as pa
from getfactormodels.utils.arrow_utils import (
    _merge_schemas,
    filter_table_by_date,
    print_table_preview,
    rearrange_columns,
    round_to_precision,
    select_table_columns,
    scale_to_percent,
)
from getfactormodels.utils.date_utils import (
    _validate_input_date,
    validate_date_range,
)
from getfactormodels.utils.http_client import _HttpClient
from getfactormodels.utils.utils import _save_to_file

"""Abstract base classes, mixins, and containers for factor data.

Classes:
    - FactorModel: abstract base class. Provides common data handling, 
      caching, and date-filtering logic implemented by all factor models.
    - RegionMixin: A mixin for models supporting international data.
    - CompositeModel: base class for models built from other models.
    - ModelCollection: for returning multiple models in a combined table.
    - PortfolioBase: Abstract base class specialized for portfolio return 
          datasets (sorting, weighting, and dividend handling).
"""
logger = logging.getLogger(__name__)


class FactorModel(ABC):
    """Abstract Base Class used by all factor model implementations.

    Provides common data handling, caching, validation, and date-filtering
    logic implemented by all factor models.

    Note:
        Subclasses must inherit from `FactorModel` and implement the
        following abstract members:

        - `schema` (property): The PyArrow schema defining the table structure.
        - `_frequencies` (property): List of supported frequencies.
        - `_get_url()`: Method returning endpoint URL(s) as a string or dict.
        - `_read(data)`: Method parsing raw downloaded bytes into a `pa.Table`.
    """
    """
    # NEW TESTING FOR FOOTERS
    _dataset_author: str | None = None
    _dataset_repository: str | None = None
    _dataset_year: str | None = None
    _dataset_title: str | None = None
    """
    def __init__(self,
                 frequency: str = 'm', # str | None removed
                 start_date: str | None  = None,
                 end_date: str | None = None,
                 output_file: str | None = None,
                 cache_ttl: int = 86400,
                 #force: bool = False,  # NEW force redownload
                 *,
                 as_percent: bool = False,
                 **kwargs):
        """Initialize a model instance.

        Args:
            frequency (str): the frequency of the data. Default: m.
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
        self.as_percent = as_percent
        self.cache_ttl = cache_ttl
        # self.force = force # not impl
        self.model = str(kwargs.get("model", self.__class__.__name__))
        self.copyright: str = ""  # NEW, TEST. fix: Carhart erroring with FF with copyright
        self._selected_factors: list[str] | None = None  # for eg drop/extract  # changing
        
        super().__init__()

    def clear_cache(self) -> int:
        """Clears local cache entries matching this instance's resolved model key(s)."""
        from platformdirs import user_cache_path
        from getfactormodels.utils.cache import _Cache

        cache_dir = user_cache_path(appname="getfactormodels", appauthor="x512", ensure_exists=True)

        # Handle ModelCollection/CompositeModel vs standard FactorModel
        keys_to_evict = getattr(self, "model_keys", self.model)

        with _Cache(cache_dir) as cache:
            count = cache.evict_model(keys_to_evict)

        self._data = None
        return count

    def __len__(self) -> int:
        """Length of the pa.Table after filtering."""
        return len(self.data)

    def __str__(self) -> str:
        if self.data is None:
            return self.__repr__()

        if isinstance(self, RegionMixin):
            region_label = f" ({self.region})"
        else:
            region_label = ""

        header = f"{self.__class__.__name__}{region_label}\n"
        return header + print_table_preview(self.data)

    #def __str__(self) -> str:
    #    return f"{self.preview}"

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
                "font-size: 0.88em; "
                "line-height: 1.2; "
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
    def start_date(self, value):
        valid = _validate_input_date(value, is_end=False)
        self._start_date, self._end_date = validate_date_range(valid, self._end_date)

    @property
    def end_date(self) -> str | None:
        return self._end_date
    @end_date.setter
    def end_date(self, value):
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
        """Return a table with requested data."""
        if self._data is None:
            self.load()

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


    def join(self, other: "FactorModel") -> "ModelCollection":
        """Joins this model with another model or portfolio."""
        from getfactormodels.models.base import ModelCollection
        return ModelCollection(instances=[self, other])


    def extract(self, factor: str | list[str]) -> "FactorModel":   #Self
        """Select specific factors from the model. Str or list[str]."""
        table = select_table_columns(self.data, factor)
        self._selected_factors = [f for f in table.column_names if f != 'date']
        return self


    def drop(self, factor: str | list[str]) -> "FactorModel": #Self
        """Remove specific factors from the model. Str or list[str]."""
        t_cols = self.data.column_names
        to_drop = {f.lower() for f in ([factor] if isinstance(factor, str) else factor)}

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

        Triggers a data download if the model has not yet been loaded.
        Sets the 'date' column as the index.

        Returns:
            pandas.DataFrame: indexed by date.

        Raises:
            ImportError: If the `pandas` package is not installed.
        """
        try:
            import pandas as pd
            df = self.data.to_pandas()
            if "date" in df.columns:
                df = df.set_index("date")
            return df
        except ImportError:
            raise ImportError("Requires Pandas. Try `pip install pandas`") from None


    def to_polars(self) -> "pl.DataFrame":
        """Convert model to a polars DataFrame.

        A wrapper around Polars `from_arrow()`. Triggers
        download if not already loaded.

        Returns:
            polars.DataFrame

        Raises:
            ImportError: If the `polars` package is not installed.
        """
        try:
            import polars as pl
            return pl.from_arrow(self.data)
        except ImportError:
            raise ImportError("Requires Polars. Try `pip install polars`") from None

    # maybe
    def preview(self, n_rows: int = 6, n_cols: int = 5) -> str | None:
        if self.data is None:
            return self.__repr__()

        if isinstance(self, RegionMixin):
            region_label = f" ({self.region})"
        else:
            region_label = ""

        header = f"{self.__class__.__name__}{region_label}\n"
        table_str = print_table_preview(self.data, n_rows=n_rows, n_cols=n_cols)

        if table_str is None:
            return header + "Empty Table"

        return header + table_str


    def load(self, client: _HttpClient | None = None) -> "FactorModel":
        if self._data is not None: # and not force
            return self

        raw_table = self._get_data(client)
        self._data = self._process_table(raw_table)
        return self


    def _get_data(self, client: _HttpClient | None = None) -> pa.Table:
        if hasattr(self, '_construct'):
            if client is None:
                with _HttpClient() as shared_client:
                    return self._construct(shared_client)
            return self._construct(client)

        raw_bytes = self._download(client=client)
        return self._read(raw_bytes)


    def _process_table(self, table: pa.Table) -> pa.Table:
        if "date" in table.column_names:
            if table.column("date").type != pa.date32():
                idx = table.column_names.index("date")
                table = table.set_column(idx, "date", table.column("date").cast(pa.date32()))
            table = table.sort_by([("date", "ascending")])

        if self.as_percent:
            table = scale_to_percent(table)

        table = round_to_precision(table, self._precision)

        if getattr(self, 'drop_null', False):
            table = table.drop_null()

        table.validate(full=True)

        return rearrange_columns(table=table).combine_chunks()


    def _download(self, client: _HttpClient | None = None) -> bytes | dict[str, bytes]:
        urls = self._get_url()
        self.log.info(f"Downloading from: {urls}")

        def _download_method(http_client: _HttpClient, target_urls: str | dict):
            if isinstance(target_urls, str):
                return http_client.download(
                    target_urls,
                    tag=self.model,
                    model_name=self.__class__.__name__,
                    cache_ttl=self.cache_ttl,
                    #force=self.force,
                )
            return {
                k: http_client.download(
                    v,
                    tag=self.model,
                    model_name=self.__class__.__name__,
                    cache_ttl=self.cache_ttl,
                    #force=self.force,
                )
                for k, v in target_urls.items()
            }

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
        col_type = None

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
        """The model's schema."""
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

    @property
    def url(self) -> str | dict[str, Any]:
        """Returns the endpoint URL(s) for the model data."""
        return self._get_url()


class CompositeModel(FactorModel):
    """Base for models constructed from other models."""
    def __init__(self,
                 frequency: str = 'm',
                 *,
                 drop_null: bool = True,
                 **kwargs):
        self.drop_null = drop_null
        super().__init__(frequency=frequency, **kwargs)

    @abstractmethod
    def _construct(self, client: _HttpClient) -> pa.Table:
        pass

    @property
    def components(self) -> list[FactorModel]:
        """Sub-models that make up this composite model. Override in subclasses if needed."""
        return getattr(self, "instances", [])

    def _get_url(self) -> dict[str, str | dict]:
        """Returns URLs of all underlying component models."""
        if self.components:
            return {
                getattr(inst, 'model', inst.__class__.__name__): inst.url
                for inst in self.components
            }
        return {}

    def _read(self, data: bytes) -> pa.Table:
        raise NotImplementedError("CompositeModel: _read called on a composite models.")


class ModelCollection(FactorModel):
    """Container holding multiple joined models, portfolios, or datasets."""
    def __init__(self,
                 model_keys: list[str] | None = None,
                 instances: list[FactorModel] | None = None,
                 **kwargs):
        """Initialize ModelCollection instance.

        Args:
            model_keys (list[str], optional): List of model names to instantiate and combine.
            instances (list[FactorModel], optional): Pre-instantiated factor model instances.
            **kwargs: Additional keyword arguments forwarded to component models.
        """
        if instances:
            self.instances = instances
            self.model_keys = [i.__class__.__name__ for i in instances]
        elif model_keys:
            #temp FIXME
            import getfactormodels.models as factor_models
            from getfactormodels.utils.registry import (
                get_model_class,
                get_model_key,
            )

            self.model_keys = model_keys
            self.instances = []
            for m in model_keys:
                key = get_model_key(m)
                class_name = get_model_class(key)
                model_class = getattr(factor_models, class_name)
                self.instances.append(model_class(model=key, **kwargs))
        else:
            raise ValueError("model_keys or instances required.")

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
        freqs = set(self.instances[0]._frequencies)
        for inst in self.instances[1:]:
            freqs &= set(inst._frequencies)
        return list(freqs)

    @property
    def schema(self) -> pa.Schema:   #TODO: FIXME: all schemas match source. Apply new schema from cols or just rework the entire schema thing...?
        return _merge_schemas([inst.schema for inst in self.instances])


    def _construct(self, client: _HttpClient) -> pa.Table:
        for inst in self.instances:
            inst.load(client=client)

        if not self.instances:
            raise ValueError("ModelCollection has no instances to construct.")

        final_table = self._cast_dates(self.instances[0].data)

        is_portfolio_join = any(isinstance(i, PortfolioBase) for i in self.instances)
        join_type = "inner" if is_portfolio_join else "left outer"

        for inst in self.instances[1:]:
            next_table = self._cast_dates(inst.data)
            final_table = self._join_method(final_table, next_table, join_type=join_type)

        return final_table.sort_by([("date", "ascending")])

    @staticmethod
    def _cast_dates(table: pa.Table) -> pa.Table:
        # ensure date is date32, don't offset
        if "date" in table.column_names and table.column("date").type != pa.date32():
            idx = table.column_names.index("date")
            table = table.set_column(idx, "date", table.column("date").cast(pa.date32()))
        return table

    @staticmethod
    def _join_method(base_table: pa.Table, next_table: pa.Table, join_type: str) -> pa.Table:
        overlaps = set(base_table.column_names) & set(next_table.column_names)
        overlaps.discard('date')

        uniq_cols = [c for c in next_table.column_names if c not in overlaps]
        if "date" not in uniq_cols:
            uniq_cols.insert(0, "date")

        table_to_join = next_table.select(uniq_cols)

        return base_table.join(
            table_to_join,
            keys="date",
            join_type=join_type,
        ).combine_chunks()

    def _get_url(self) -> dict[str, str | dict]:
        """Returns URLs of all underlying component models."""
        if getattr(self, "instances", None):
            return {
                getattr(inst, 'model', inst.__class__.__name__): inst.url
                for inst in self.instances
            }
        return {}

    def _read(self, data: bytes) -> pa.Table:
        raise NotImplementedError("ModelCollection: _read called on a collection.")


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
    """Abstract base class for portfolio return datasets."""
    def __init__(
            self,
            frequency: str = 'm',
            sort: str | list[str] | None = None,
            grid: str | None = None,
            count: int | None = None,
            weights: Literal['vw', 'ew'] = 'vw',
            *,
            dividends: bool = True,  # tot ret by default
            **kwargs,
    ):
        """Initialize the portfolio model instance.

        Args:
            frequency (str, optional): Data frequency ('m' or 'd'). Default is 'm'.
            sort (str | list[str], optional): Sorting variable(s) for portfolios.
            grid (str, optional):
            count (int, optional): Number of portfolios.
            weights (str, optional): Weighting scheme ('vw' for value-weighted, 'ew' for equal-weighted). Default is 'vw'.
            dividends (bool, optional): Include dividends in total returns. Default is True.
            **kwargs: Additional keyword arguments passed to base models.
        """
        super().__init__(frequency=frequency, **kwargs)

        #_aliases = {'size': 'me',}
        self.dividends = dividends
        self.weights = weights.lower()
        self.sort = sort
        self.grid = grid
        self.count = count

        if self.weights not in ('vw', 'ew'):
            msg = f'weights should be either "vw" or "ew", not {weights}'
            raise ValueError(msg)

    @property
    def _precision(self) -> int: return 6


#class AnomalyBase(PortfolioBase):
#    """Base class for anomaly datasets."""
#
#    def __init__(self, anomaly: str | list[str], **kwargs):
#        self.anomaly = [anomaly] if isinstance(anomaly, str) else anomaly
#        super().__init__(**kwargs)
#

