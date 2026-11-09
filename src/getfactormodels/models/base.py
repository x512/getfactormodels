# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
"""Abstract base classes, mixins, and containers for factor data."""

import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Any, ClassVar, Literal, Self, override, Final
import pyarrow as pa
from getfactormodels.utils.arrow_utils import (
    _merge_schemas,
    cast_and_sort_dates,
    filter_table_by_date,
    join_on_date,
    print_table_preview,
    rearrange_columns,
    round_to_precision,
    scale_to_percent,
    select_table_columns,
    drop_null_rows,
)
from getfactormodels.utils.cache import evict_model_cache
from getfactormodels.utils.date_utils import (
    _validate_input_date,
    validate_date_range,
)
from getfactormodels.utils.http_client import _HttpClient
from getfactormodels.utils.utils import _save_to_file

logger = logging.getLogger(__name__)

class Base(ABC):    # eventually split into base, and processor. 
    """Abstract Base Class used by all implementations."""
    def __init__(
        self,
        frequency: Literal['d', 'w', 'w2w', 'm', 'q', 'y', 'a'] = 'm',
        start_date: str | None = None,
        end_date: str | None = None,
        *,
        as_percentage: bool = False,
        drop_nulls: bool = False,
        cache_ttl: int = 86400,
        force: bool = False,
        output_file: str | Path | None = None,
        show_url: bool = False,
    ) -> None:
        """Initialize base."""
        super().__init__()

        self.log = logger

        self._data: pa.Table | None = None
        self._start_date: str | None = None
        self._end_date: str | None = None
        self._frequency: str | None = None
        
        self.drop_nulls = drop_nulls
        
        self.frequency = frequency
        self.start_date = start_date
        self.end_date = end_date
        self.as_percentage = as_percentage
        self.cache_ttl = cache_ttl
        self.no_cache = force
        self.output_file = output_file
        self.show_url = show_url
        self._selected_factors: list[str] | None = None

    @property
    @abstractmethod
    def url(self) -> str | list[str] | dict[str, Any]:
        """Endpoint URL(s) of the dataset."""
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
        from getfactormodels.registry.model_registry import get_model_name
        model_key = getattr(self, "model", None)
        
        if isinstance(model_key, str):
            name = get_model_name(model_key)
            if name:
                return name
                
        return self.__class__.__name__

    @property
    def start_date(self) -> str | None:
        return self._start_date

    @start_date.setter
    def start_date(self, value: str | None) -> None:
        valid = _validate_input_date(value, is_end=False)
        self._start_date, self._end_date = validate_date_range(valid, self._end_date)

    @property
    def end_date(self) -> str | None:
        return self._end_date

    @end_date.setter
    def end_date(self, value: str | None) -> None:
        valid = _validate_input_date(value, is_end=True)
        self._start_date, self._end_date = validate_date_range(self._start_date, valid)

    @property
    def frequency(self) -> str | None:
        return self._frequency

    @frequency.setter
    def frequency(self, value: str | None) -> None:
        if value is None:
            self._frequency = None
            return

        val = value.lower()

        if val == 'a':
            val = 'y'

        if val not in self.frequencies:
            raise ValueError(f"Invalid frequency '{val}'. Options: {self.frequencies}")

        if val != self._frequency:
            if self._frequency is not None:
                self.log.info("Frequency changed from %s to %s.", self._frequency, val)
            self._frequency = val
            self._data = None

    @property
    def data(self) -> pa.Table:
        """Return the processed and date-filtered table."""
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
        
        raw_table = self._get_raw(client=client)   # direct call
        self._data = self._process_table(raw_table)
        return self

    def _get_raw(self, client: _HttpClient | None = None) -> pa.Table:
        """Download raw data."""
        raw_data = self._download(client=client)
        return self._read(raw_data)

    def _download(self, client=None) -> bytes | dict[str, bytes]:
        tag = getattr(self, "model", self.__class__.__name__)
        try:
            if client:
                return client.download_any(self.url, tag=tag, model_name=self.__class__.__name__,
                                           cache_ttl=self.cache_ttl, force=self.no_cache)
            with _HttpClient() as c:
                return c.download_any(self.url, tag=tag, model_name=self.__class__.__name__,
                                      cache_ttl=self.cache_ttl, force=self.no_cache)
        except Exception as e:
            self.log.error("Download failed: %s", e)
            raise RuntimeError(f"Could not retrieve data for {self.__class__.__name__}") from e

    def _process_table(self, table: pa.Table) -> pa.Table:
        table = cast_and_sort_dates(table)
        if self.as_percentage:
            table = scale_to_percent(table)
        table = round_to_precision(table, self._precision)
        if self.drop_nulls:
            table = table.drop_null()
        table.validate(full=True)
        return rearrange_columns(table=table).combine_chunks()

    #@abstractmethod  -- UNabstracted
    def _read(self, data: bytes | dict[str, bytes]) -> pa.Table:
        """Parse raw downloaded bytes into a Table. Subclasses must impl."""
        raise NotImplementedError("`_read` not implemented.")

    def extract(self, factor: str | Sequence[str]) -> Self:
        """Select specific factors from the model."""
        factors = [factor] if isinstance(factor, str) else list(factor)
        table = select_table_columns(self.data, factors)
        self._selected_factors = [f for f in table.column_names if f != 'date']
        return self

    def drop(self, factor: str | Sequence[str]) -> Self:
        """Remove specific factors from the model."""
        t_cols = self.data.column_names
        to_drop = {f.lower() for f in ([factor] if isinstance(factor, str) else factor)}
        selection = [c for c in t_cols if c.lower() not in to_drop and c != 'date']
        if not selection:
            raise ValueError("Cannot drop all factors from the model.")
        self._selected_factors = selection
        return self

    def dropna(
        self, 
        subset: str | Sequence[str] | None = None, 
        how: Literal['any', 'all'] = 'any'
    ) -> Self:
        """Drop rows containing null values.

        Behaves like Pandas/Polars `dropna()`. Calls `.load()`
        if needed.

        Args:
            subset: Column(s) to check. `None` = all columns.
            how: `any` (default) drops if any checked column is null;
                `all` drops only if every checked column is null.
        """
        if self._data is None:
            self.load()
        if self._data is not None:
            self._data = drop_null_rows(self._data, subset, how)
        return self

    def clear_cache(self) -> int:
        """Evict cached files for this instance.

        Returns:
            Number of cache entries evicted.
        """
        tag = getattr(self, "model", self.__class__.__name__)
        return evict_model_cache(tag)

    def to_file(self, filepath: str | Path | None = None) -> None:
        """Save the processed data to a file.

        `filepath` can be a directory, a filename, or a full path. The file format 
        is inferred from the extension. If no extension is provided, it defaults 
        to CSV. If a directory is passed, a default filename is generated.

        Note: NaNs are preserved in the output unless `drop_nulls=True` 
        was passed during initialization.

        Supported extensions:
            .parquet, .ipc, .feather, .csv, .txt, .pkl, .md

        Args:
            filepath: Destination path. If None, falls back to the `output_file` 
                parameter provided during initialization.

        Raises:
            ValueError: If the file extension is not supported.
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

    def to_pandas(self) -> "pd.DataFrame":
        """Convert the data to a pandas DataFrame.

        Triggers a download if the data is not already loaded.
        The 'date' column is automatically set as the DataFrame index.

        Returns:
            pandas.DataFrame indexed by date.

        Raises:
            ImportError: If the `pandas` package is not installed.

        Example:
            >>> model = getfactormodels.model("ff3")
            >>> df = model.to_pandas()
            >>> df.head()
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
        """Convert the data to a polars DataFrame.

        Triggers a download if the data is not already loaded.
        Wrapper around Polars `pl.from_arrow()`.

        Returns:
            polars.DataFrame

        Raises:
            ImportError: If the `polars` package is not installed.

        Example:
            >>> model = getfactormodels.model("ff3")
            >>> df = model.to_polars()
            >>> df.head()
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

        region = getattr(self, "region", None)
        region_label = f" ({region})" if region else ""
        header = f"{self.name}{region_label}\n"
        return header + print_table_preview(self.data, n_rows=5) #cols dynamic depending on term width

    def __getitem__(self, key: str | Sequence[str]) -> pa.Table:
        keys = [key] if isinstance(key, str) else list(key)
        return select_table_columns(self.data, keys)

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

    def __dataframe__(self, *, nan_as_null: bool = False) -> object:
        """Dataframe interchange protocol support.

        Casts date32 to ns.

        Examples:
            import polars as pl
            df = pl.from_arrow(model.data)

            import pandas as pd
            df = pd.api.interchange.from_dataframe(model.data)
        """
        table = self.data
        col_type = None

        if table.column_names and table.column_names[0] == "date":
            col_type = table.schema.field(0).type

        if col_type is not None and pa.types.is_date(col_type):
            date_ns = table.column(0).cast(pa.timestamp("ns")).combine_chunks()
            table = table.set_column(0, "date", date_ns)

        return table.combine_chunks().__dataframe__(nan_as_null=nan_as_null, allow_copy=True)


class FactorModel(Base):
    """Standard factor model base class."""
    def __init__(self, model: str | None = None, **kwargs) -> None:
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


def _normalize_to_str(chars: str | Sequence[str]) -> tuple[str, ...]:
    if isinstance(chars, str):
        chars = [chars]
    out = []
    for c in chars:
        out.extend(part.strip().lower() for part in c.split(",") if part.strip())
    return tuple(out)


class PortfolioBase(Base):
    """Base class for multi-bucket cross-sectional portfolio sorts."""
    VALID_SORTS: Final = MappingProxyType({
        'ff': frozenset({None, '2x3', '2x4x4', '5x5', '10x10', 'bivariate', '3', '5', "10"}), #decile?
        'q': frozenset({None, '2x3', '2x3x3', '3x5', 'decile', 'bivariate'}), #10?
    })
    def __init__(
        self,
        source: Literal['ff', 'q'],
        factor: str | list[str] | None = None,  # Sorting 
        *,
        #weights: Literal['vw', 'ew'] = 'vw', # Value-weighted ('vw') or equal-weighted ('ew')
        sort: str | None = None,
        dividends: bool = True,              # Include dividends in total return calculation
        **kwargs,
    ) -> None:
        """Initialize the portfolio model instance.

        Args:
            source: provider src
            factor: factor/anomaly IDs ('size', 'inv')
            sort: Sorting variables or anomaly identifiers.
            weights: Weighting scheme ('vw' or 'ew').
            dividends: Total return flag.
            **kwargs: Additional parameters passed to Base.
        """
        if source not in self.VALID_SORTS:
            raise ValueError(f"Invalid data source: '{source}'. "
                             f"Must be one of {list(self.VALID_SORTS.keys())}.")

        if sort not in self.VALID_SORTS[source]:
            raise ValueError(f"Invalid sort '{sort}' for source '{source}'.")

        self.source = source
        self.dividends = dividends
        self.sort = sort
        self.factor = _normalize_to_str(factor)
        super().__init__(**kwargs)

    @property
    def _precision(self) -> int:
        """Portfolio returns require 6 decimal places of precision."""
        return 6

    @property
    def frequencies(self) -> list[str]:
        """Supported frequencies for cross-sectional portfolio sorts."""
        return ["m"]


class Composite(Base):
    """Base class for models constructed by combining other models."""
    def __init__(self, *, drop_nulls: bool = True, **kwargs) -> None:
        super().__init__(drop_nulls=drop_nulls, **kwargs)

    @property
    def components(self) -> list[Base]:
        """Return the list of child model instances."""
        return getattr(self, "instances", [])

    @property
    def url(self) -> dict[str, str | list[str] | dict[str, Any]]:
        """Return a mapping of component names to URLs."""
        return {
            getattr(inst, 'model', inst.__class__.__name__): inst.url 
            for inst in self.components
        }

    @property
    def frequencies(self) -> list[str]:
        """Return the intersection of frequencies across all components."""
        if not self.components:
            return []
        freqs = set(self.components[0].frequencies)
        for inst in self.components[1:]:
            freqs &= set(inst.frequencies)
        return list(freqs)

    @property
    def schema(self) -> pa.Schema:
        """Return the merged schema of all components."""
        if not self.components:
            return pa.schema([])
        return _merge_schemas([inst.schema for inst in self.components])

    @abstractmethod
    def _construct(self, client) -> pa.Table:
        """Combine component tables into a single table."""
        pass

    @override
    def _get_raw(self, client: _HttpClient | None = None) -> pa.Table:
        if client is None:
            with _HttpClient() as shared_client:
                return self._construct(shared_client)
        return self._construct(client)

    @override
    def clear_cache(self) -> int:
        """Clear the cache for all child components."""
        total = 0
        for inst in self.components:
            total += inst.clear_cache()
        return total


class Collection(Composite):
    """Container holding multiple models or portfolios joined horizontally."""
    def __init__(
        self,
        instances: list[Base] | None = None,
        model_keys: list[str] | None = None,
        frequency: str = 'm',
        region: str | None = 'us',
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs,
    ) -> None:
        """Initialize a collection.

        Args:
            instances: Pre-instantiated models.
            model_keys: Registry keys to instantiate automatically.
            frequency: Data frequency passed to uninstantiated models.
            region: Region passed to uninstantiated models.
            start_date: Start date filter.
            end_date: End date filter.
            **kwargs: Extra keyword arguments.
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
            # FIX: scaling to percentage happens twice 
            # TEMP: strip formatting flags. Children need to return 
            #     raw decimals. 
            child_kwargs = {
                k: v for k, v in kwargs.items() 
                if k not in ("as_percentage", "drop_nulls")
            }
            for m in model_keys:
                key = get_model_key(m)
                class_name = get_model_class(key)
                model_class = getattr(factor_models, class_name)

                model_kwargs = {
                    "frequency": frequency,
                    "start_date": start_date,
                    "end_date": end_date,
                    **child_kwargs,  #TEMP:FIX:
                }

                if issubclass(model_class, RegionMixin):
                    model_kwargs["region"] = region
                elif region is not None and str(region).strip().lower() not in ['usa', 'us']:
                    raise ValueError(
                        f"Model '{class_name}' in collection does not support regional data (US only). "
                            f"Region '{region}' was requested.",
                    )

                resolved_instances.append(model_class(**model_kwargs))

            instances = resolved_instances

        if not instances:
            raise ValueError("Collection requires either 'instances' or 'model_keys'.")

        self.instances = instances
        self.model_keys = [getattr(i, 'model', i.__class__.__name__) for i in instances]

        super().__init__(
            frequency=frequency, 
            start_date=start_date, 
            end_date=end_date, 
            **kwargs
        )

    def _construct(self, client) -> pa.Table:
        if not self.instances:
            raise ValueError("Collection has no instances to construct.")

        for inst in self.instances:
            inst.load(client=client)

        final_table = self.instances[0].data

        portfolio_join = any(isinstance(i, PortfolioBase) for i in self.instances)
        join_type = "inner" if portfolio_join else "left outer"

        for inst in self.instances[1:]:
            final_table = join_on_date(final_table, inst.data, join_type=join_type)

        return final_table


class RegionMixin(ABC):
    """Mixin for models that support international regions/countries."""
    _region: str | None = None

    regions: ClassVar[list[str]]

    _aliases: ClassVar[dict[str, str]] = {
        'uk': 'gbr', 'ger': 'deu', 'sg': 'sgp',
        'hk': 'hkg', 'nl': 'nld', 'fr': 'fra',
        'usa': 'us', 'us': 'usa',
        'jpn': 'japan', 'japan': 'jpn',
        'ex-us': 'global ex usa',
        'north-america': 'north america', 'north america': 'north-america',
        'global_stocks': 'all_equities',
        'australia': 'aus', 'au': 'aus',
    }

    def __init__(self, region: str | None = None, **kwargs: Any) -> None:
        """Initialize the region mixin explicitly."""
        super().__init__(**kwargs)
        self.region = region

    @property
    def region(self) -> str:
        if self._region is not None:
            return self._region        
        if not self.regions:
            return ""

        if 'us' in self.regions:
            return 'us'
        if 'usa' in self.regions:
            return 'usa'
        return self.regions[0]

    @region.setter
    def region(self, value: str | None) -> None:
        if value is None:
            self._region = None
            return

        val = str(value).strip().lower()
        resolved = val if val in self.regions else self._aliases.get(val, val)

        if resolved not in self.regions:
            raise ValueError(
                f"Region '{value}' not supported for {self.__class__.__name__}. "
                    f"Available: {self.regions}",
            )

        current = getattr(self, "_region", None)
        if current is not None and current != resolved:
            self._data = None

            log = getattr(self, "log", None)
            if log is not None:
                log.info("Region changed to %s. Cache reset.", resolved)
        self._region = resolved

    @classmethod
    def list_regions(cls) -> list[str]:
        """List available regions without instantiation."""
        return cls.regions
