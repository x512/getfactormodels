# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
"""Abstract base classes, mixins, and containers for factor data."""
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Literal, Self
import pyarrow as pa
from getfactormodels.utils.arrow_utils import (
    _merge_schemas,
    filter_table_by_date,
    print_table_preview,
    rearrange_columns,
    round_to_precision,
    scale_to_percent,
    select_table_columns,
)
from getfactormodels.utils.date_utils import (
    _validate_input_date,
    validate_date_range,
)
from getfactormodels.utils.http_client import _HttpClient
from getfactormodels.utils.utils import _save_to_file

logger = logging.getLogger(__name__)

class Base(ABC):    
    """Abstract Base Class used by all implementations."""
    def __init__(
        self,
        frequency: str = 'm',
        start_date: str | None = None,
        end_date: str | None = None,
        *,
        as_percentage: bool = False,
        drop_nulls: bool = False,
        cache_ttl: int = 86400,
        force: bool = False,
        output_file: str | None = None,
        show_url: bool = False,
        **kwargs,
    ):
        """Initialize base.

        Args:
            frequency (str): Data frequency. m, d, w, q, y, w2w. Default: m.
            start_date (str, opt): Start date, YYYY[-MM-DD]
            end_date (str, opt): End date, YYYY[-MM-DD]
            as_percentage (bool, opt): Scale to percentage values. Default: False.
            drop_nulls (bool, opt): Drop rows containing null values. Default: False.
            cache_ttl (int, opt): Cache time-to-live in seconds. Default: 86400.
            force (bool, opt): Bypass local disk/memory cache. Default: False.
            output_file (str, opt): output file or path.
            show_url (bool, opt): Print target download URL(s) to stdout on load. Default: False.
            **kwargs: additional keyword args.
        """
        super().__init__()

        self.log = logger

        if kwargs:
            raise TypeError(
                f"Unexpected keyword argument(s) passed to "
                    f"{self.__class__.__name__}: {list(kwargs.keys())}",
            )

        self._data: pa.Table | None = None
        self._start_date: str | None = None
        self._end_date: str | None = None
        self._frequency: str | None = None

        self.frequency = frequency
        self.start_date = start_date
        self.end_date = end_date
        self.as_percentage = as_percentage
        self.drop_nulls = drop_nulls
        self.cache_ttl = cache_ttl
        self.no_cache = force
        self.output_file = output_file
        self.show_url = show_url
        self._selected_factors: list[str] | None = None

    @property
    @abstractmethod
    def url(self) -> str | dict[str, Any]:
        """Endpoint URL of the dataset."""
        pass

    @property
    @abstractmethod
    def schema(self) -> pa.Schema:
        """Expected PyArrow schema of the dataset."""
        pass

    @property
    @abstractmethod
    def frequencies(self) -> list[str]:
        """List of available frequencies."""
        pass

    @property
    def _precision(self) -> int:
        """Decimal precision of the values."""
        return 8

    @property
    def name(self) -> str:
        """Name of the model or dataset."""
        from getfactormodels.utils.registry import _MODEL_REGISTRY

        model_key = getattr(self, "model", None)
        if model_key in _MODEL_REGISTRY:
            return _MODEL_REGISTRY[model_key]["name"]
        # Fallback to class name
        return self.__class__.__name__


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
        if val not in self.frequencies:
            raise ValueError(f"Invalid frequency '{val}'. Options: {self.frequencies}")

        if val != self._frequency:
            if self._frequency is not None:
                self.log.info("Frequency changed from %s to %s.", self._frequency, val)
            self._frequency = val
            self._data = None

    @property
    def data(self) -> pa.Table:
        """Return the processed and date-filtered pa.Table."""
        if self._data is None:
            self.load()

        if self._data is None:
            raise RuntimeError("Failed to load table data.")

        table = self._data

        if self._selected_factors is not None:
            selection = list(dict.fromkeys(['date'] + self._selected_factors))
            table = table.select(selection)

        return filter_table_by_date(table, self._start_date, self._end_date)

    def load(self, client: _HttpClient | None = None) -> Self:
        """Download or load data from cache and run processing pipeline."""
        if self._data is not None and not self.no_cache:
            return self

        if self.show_url:
            print(f"URL: {self.url}")

        raw_table = self._get_data(client=client)
        self._data = self._process_table(raw_table)
        return self

    def _get_data(self, client: _HttpClient | None = None) -> pa.Table:
        """Route table construction or standard download."""
        if hasattr(self, "_construct"):
            # safer access. hasattr passed; type ignore because Base lacks _construct
            construct_fn = self._construct  # type: ignore[attr-defined]
            if client is None:
                with _HttpClient() as shared_client:
                    return construct_fn(shared_client)
            return construct_fn(client)

        raw_data = self._download(client=client)
        return self._read(raw_data)

    def _download(self, client: _HttpClient | None = None) -> bytes | dict[str, bytes]:
        target_urls = self.url
        self.log.info("Downloading from: %s", target_urls)

        def _fetch(http_client: _HttpClient, urls: str | dict):
            if isinstance(urls, str):
                return http_client.download(
                    urls,
                    tag=getattr(self, "model", self.__class__.__name__),
                    model_name=self.__class__.__name__,
                    cache_ttl=self.cache_ttl,
                    force=self.no_cache,
                )
            return {
                k: http_client.download(
                    v,
                    tag=getattr(self, "model", self.__class__.__name__),
                    model_name=self.__class__.__name__,
                    cache_ttl=self.cache_ttl,
                    force=self.no_cache,
                )
                for k, v in urls.items()
            }

        try:
            if client:
                return _fetch(client, target_urls)
            with _HttpClient() as new_client:
                return _fetch(new_client, target_urls)
        except Exception as e:
            self.log.error("Download failed: %s", e)
            raise RuntimeError(f"Could not retrieve data for {self.__class__.__name__}") from e

    def _process_table(self, table: pa.Table) -> pa.Table:
        if "date" in table.column_names:
            if table.column("date").type != pa.date32():
                idx = table.column_names.index("date")
                table = table.set_column(idx, "date", table.column("date").cast(pa.date32()))
            table = table.sort_by([("date", "ascending")])

        if self.as_percentage:
            table = scale_to_percent(table)

        table = round_to_precision(table, self._precision)

        if self.drop_nulls:
            table = table.drop_null()

        table.validate(full=True)
        return rearrange_columns(table=table).combine_chunks()

    @abstractmethod 
    def _read(self, data: bytes | dict[str, bytes]) -> pa.Table:
        """Parse raw downloaded bytes into a Table. Subclasses must impl."""
        pass

    def extract(self, factor: str | list[str]) -> Self:
        """Select specific factors from the model."""
        table = select_table_columns(self.data, factor)
        self._selected_factors = [f for f in table.column_names if f != 'date']
        return self

    def drop(self, factor: str | list[str]) -> Self:
        """Remove specific factors from the model."""
        t_cols = self.data.column_names
        to_drop = {f.lower() for f in ([factor] if isinstance(factor, str) else factor)}
        selection = [c for c in t_cols if c.lower() not in to_drop and c != 'date']
        if not selection:
            raise ValueError("Cannot drop all factors from the model.")
        self._selected_factors = selection
        return self

    @classmethod
    def clear_cache(cls, key: str | list[str] | None = None) -> int:
        """Evict cached files for a model or instance.

        Can be called as a class method or on an instance.

        Args:
            key (str | list[str], opt): Specific model key or list of keys to evict.
                If omitted on a class, defaults to the class's model attribute or name.

        Returns:
            int: Number of evicted cache entries.
        """
        if not isinstance(cls, type):
            # instance (self)
            keys_to_evict = getattr(cls, "model_keys", getattr(cls, "model", cls.__class__.__name__))
            count = cls._evict_cache_keys(keys_to_evict)
            if hasattr(cls, "log"):
                cls.log.info("Removed %s cached entry/entries for %s.", count, keys_to_evict)
            cls._data = None
            return count

        # classmethod
        target_keys = key if key is not None else getattr(cls, "model", cls.__name__)
        return cls._evict_cache_keys(target_keys)

    @staticmethod
    def _evict_cache_keys(keys: str | list[str]) -> int:
        from platformdirs import user_cache_path
        from getfactormodels.utils.cache import _Cache

        cache_dir = user_cache_path(appname="getfactormodels", appauthor="x512", ensure_exists=True)
        with _Cache(cache_dir) as cache:
            return cache.evict_model(keys)

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
        table = self.data
        if table.num_rows == 0:
            self.log.warning("No data available to save.")
            return
        _save_to_file(table, target, model_instance=self)

    def to_pandas(self):
        """Convert model to a pandas DataFrame.

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

    def to_polars(self):
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

    def __len__(self) -> int:
        return len(self.data)

    def __str__(self) -> str:
        if self._data is None:
            return self.__repr__()

        region_label = f" ({self.region})" if hasattr(self, "region") and self.region else ""
        header = f"{self.name}{region_label}\n"
        return header + print_table_preview(self.data)

    def __getitem__(self, key: str | list[str]) -> pa.Table:
        return select_table_columns(self.data, key)

    def __repr__(self) -> str:
        params = []
        for attr in ['model', 'frequency', 'region', 'start_date', 'end_date', 'output_file']:
            val = getattr(self, attr, None)
            if val is not None and val is not False and val != "":
                repr_val = f"'{val}'" if isinstance(val, str) else val
                params.append(f"{attr}={repr_val}")
        return f"{self.__class__.__name__}({', '.join(params)})"

    def _repr_html_(self) -> str:
        style = (
            "font-family: monospace; "
                "font-size: 0.88em; "
                "line-height: 1.2; "
                "background-color: transparent; "
                "white-space: pre; "
                "overflow-x: auto; "
                "display: block;"
        )
        return f'<pre style="{style}">{str(self)}</pre>'

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

        if table.column_names and table.column_names[0] == "date":
            col_type = table.schema.field(0).type

        if pa.types.is_date(col_type):
            date_ns = table.column(0).cast(pa.timestamp("ns")).combine_chunks()
            table = table.set_column(0, "date", date_ns)

        return table.combine_chunks().__dataframe__(nan_as_null=nan_as_null, allow_copy=True)


class FactorModel(Base):
    """Standard factor model base class."""
    def __init__(self, model: str | None = None, **kwargs):
        """Initialize a model instance.

        Args:
            model (str, optional): id of the factor model.
            **kwargs: Additional operational parameters.
        """
        resolved_model = model or kwargs.get("model", self.__class__.__name__)
        self.model = str(resolved_model)

        super().__init__(**kwargs)

    @property
    def frequencies(self) -> list[str]:
        """Supported data frequencies for this model."""
        return ["m"]


class PortfolioBase(Base):
    """Base class for portfolio sorts."""
    def __init__(
        self,
        sort: str | list[str] | None = None,  # op, inv, bm, etc.
        count: int | None = None,  # n portfolios (e.g., 10 for deciles)
        grid: str | None = None,   # e.g., '2x3', '3x5', '5x5', '2x3x3'
        *,
        weights: Literal['vw', 'ew'] = 'vw',
        dividends: bool = True,
        **kwargs,
    ):
        """Initialize the portfolio model instance.

        Args:
            sort (str | list[str]): Sorting variables ('size', 'op', 'bm', etc.).
            weights (Literal['vw', 'ew']): Weights, value ('vw') or equal ('ew') weighted.
            count (int, opt): Number of portfolios to return.
            grid (str, opt): 2x3 etc. grid sorts.
            dividends (bool): False for ex-div, true for total return (default).
            **kwargs: extra args.
        """
        self.sort = sort
        self.weights = (weights or 'vw').lower()
        self.count = count
        self.grid = grid.lower().strip() if grid else None
        self.dividends = dividends

        if self.weights not in ('vw', 'ew'):
            raise ValueError(f"Weights must be either 'vw' or 'ew', not '{weights}'.")

        super().__init__(**kwargs)

    @property
    def _precision(self) -> int:
        return 6

    @property
    def frequencies(self) -> list[str]:
        return ["m"]


class Anomalies(Base):
    """Base class for anomaly data from global-q."""
    def __init__(
            self,
            anomaly: str | list[str],
            *,
            bivariate: bool = False,  # False for deciles, True for 3x5
            **kwargs,
    ):
        """Base for anomalies.

        Args:
            anomaly (str, list[str]): anomaly id.
            bivariate (bool): 3x5, 2-way sort (on size) when true (default).
            **kwargs: extra args.
        """
        self.anomalies: list[str] = [anomaly] if isinstance(anomaly, str) else anomaly
        self.bivariate: bool = bivariate

        self.sort_type: Literal['decile', '3x5'] = '3x5' if bivariate else 'decile'

        super().__init__(**kwargs)

    @property
    def _precision(self) -> int:
        return 6

    @property
    def _frequencies(self) -> list[str]:
        return ["m", "y", "w", "d", "q"]


class Composite(Base):
    """Base class for models constructed from others."""
    def __init__(self, drop_nulls: bool = True, **kwargs):
        super().__init__(drop_nulls=drop_nulls, **kwargs)

    @property
    def components(self) -> list[Base]:
        return getattr(self, "instances", [])

    @property
    def url(self) -> dict[str, str | dict]:
        """URLs."""
        if self.components:
            return {
                getattr(inst, 'model', inst.__class__.__name__): inst.url 
                for inst in self.components
            }
        return {}

    @abstractmethod
    def _construct(self, client) -> pa.Table:
        pass

    def _read(self, data: bytes | dict[str, bytes]) -> pa.Table:
        raise NotImplementedError("Composites/collections should use `_construct`.")


class Collection(Composite):
    """Container holding multiple joined models or portfolios of the same frequency."""
    def __init__(
        self,
        instances: list[Base] | None = None,
        model_keys: list[str] | None = None,
        frequency: str = 'm',
        region: str | None = 'us',
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs,
    ):
        """Initialize collection of models or portfolios.

        Args:
            instances (list[Base], optional): Pre-instantiated model/portfolio objects.
            model_keys (list[str], optional): List of model keys to instantiate automatically.
            frequency (str): Data frequency.
            region (str, optional): Geographic region.
            start_date (str, optional): Start date.
            end_date (str, optional): End date.
            **kwargs: Additional operational parameters passed to instantiated models.
        """
        if instances and model_keys:
            raise ValueError("Provide either 'instances' or 'model_keys', not both.")

        if model_keys:
            import getfactormodels.models as factor_models
            from getfactormodels.utils.registry import (
                get_model_class,
                get_model_key,
            )

            resolved_instances = []
            for m in model_keys:
                key = get_model_key(m)
                class_name = get_model_class(key)
                model_class = getattr(factor_models, class_name)
                
                model_kwargs = {
                    "frequency": frequency,
                    "start_date": start_date,
                    "end_date": end_date,
                    **kwargs,
                }

                # Handle regional validation & parameters
                if issubclass(model_class, RegionMixin):
                    model_kwargs["region"] = region
                elif region is not None and str(region).strip().lower() not in ['usa', 'us']:
                    raise ValueError(
                        f"Model '{class_name}' in collection does not support regional data (US only). "
                        f"Region '{region}' was requested."
                    )

                resolved_instances.append(model_class(**model_kwargs))
            
            instances = resolved_instances

        if not instances:
            raise ValueError("Collection requires either 'instances' or 'model_keys'.")

        self.instances = instances
        self.model_keys = [getattr(i, 'model', i.__class__.__name__) for i in instances]
        super().__init__(frequency=frequency, start_date=start_date, end_date=end_date, **kwargs)


    @property
    def frequencies(self) -> list[str]:
        """Common frequencies."""
        if not self.instances:
            return []
        freqs = set(self.instances[0].frequencies)
        for inst in self.instances[1:]:
            freqs &= set(inst.frequencies)
        return list(freqs)

    @property
    def schema(self) -> pa.Schema:
        """Expected pa.Schema of the Collection."""
        return _merge_schemas([inst.schema for inst in self.instances])

    def _construct(self, client) -> pa.Table:
        if not self.instances:
            raise ValueError("Collection has no instances to construct.")

        for inst in self.instances:
            inst.load(client=client)

        final_table = self._cast_dates(self.instances[0].data)
        portfolio_join = any(isinstance(i, PortfolioBase) for i in self.instances)
        join_type = "inner" if portfolio_join else "left outer"

        for inst in self.instances[1:]:
            next_table = self._cast_dates(inst.data)
            final_table = self._join_method(final_table, next_table, join_type=join_type)

        return final_table.sort_by([("date", "ascending")])

    # temp 
    @staticmethod
    def _cast_dates(table: pa.Table) -> pa.Table:
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


class RegionMixin(ABC):
    """Mixin for models that support international regions/countries."""
    _aliases = {
        'uk': 'gbr', 'ger': 'deu', 'sg': 'sgp',
        'hk': 'hkg', 'nl': 'nld', 'fr': 'fra',
        'usa': 'us', 'us': 'usa',
        'jpn': 'japan', 'japan': 'jpn',
        'ex-us': 'global ex usa',
        'north-america': 'north america', 'north america': 'north-america',
        'global_stocks': 'all_equities',
        'australia': 'aus', 'au': 'aus',
    }

    @property
    @abstractmethod
    def regions(self) -> list[str]:
        """List of supported region codes for the model."""
        pass

    @property
    def region(self) -> str:
        if hasattr(self, "_region") and self._region is not None:
            return self._region
        if not self.regions:
            return ""

        if 'us' in self.regions:
            return 'us'
        if 'usa' in self.regions:
            return 'usa'
        return self.regions[0]

    @region.setter
    def region(self, value: str | None):
        if value is None:
            self._region = None
            return

        val = str(value).strip().lower()
        resolved = val if val in self.regions else self._aliases.get(val, val)

        if resolved not in self.regions:
            raise ValueError(
                f"Region '{value}' not supported for {self.__class__.__name__}. "
                    f"Available: {self.regions}"
            )

        current = getattr(self, "_region", None)
        if current is not None and current != resolved:
            self._data = None
            if hasattr(self, "log"):
                self.log.info(f"Region changed to {resolved}. Cache reset.")

        self._region = resolved

    @classmethod
    def list_regions(cls) -> list[str]:
        """List available regions without instantiation."""
        regions_attr = getattr(cls, 'regions', None)
        if isinstance(regions_attr, property):
            return regions_attr.fget(cls)
        return regions_attr or []

