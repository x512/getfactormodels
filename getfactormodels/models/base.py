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
from pathlib import Path
from typing import Any
import pyarrow as pa
from getfactormodels.utils.data_utils import (
    filter_table_by_date,
    print_table_preview,
    rearrange_columns,
    round_to_precision,
    select_table_columns,
    validate_date_range,
)
from getfactormodels.utils.http_client import _HttpClient
from getfactormodels.utils.utils import _save_to_file, _validate_input_date


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
            start_date (str, opt):
            end_date (str, opt):
            output_file (str | Path, opt):
            cache_ttl (int): Cache time-to-live in seconds. Default: 86400.
            **kwargs: some models have additional params.
        """
        logger_name = f"{self.__module__}.{self.__class__.__name__}"
        self.log = logging.getLogger(logger_name)

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
        return len(self.data) # length of the table (after slicing)

    def __str__(self) -> str:
        if self._data is not None:
            region = getattr(self, 'region', 'US')
            header = f"{self.__class__.__name__} ({region})\n"
            return header + print_table_preview(self.data)
        return self.__repr__()

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

        if self._start_date != valid:
            self._start_date = valid
            validate_date_range(self.start_date, self.end_date)

    @property
    def end_date(self) -> str | None:
        return self._end_date
    @end_date.setter
    def end_date(self, value: Any):
        valid = _validate_input_date(value, is_end=True)
        if self._end_date != valid:
            self._end_date = valid  # set 
            validate_date_range(self.start_date, self.end_date)

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

        return filter_table_by_date(
            table, 
            self.start_date, 
            self.end_date,
        )


    @property
    def shape(self) -> tuple[int, int]:
        """(rows, columns), like Pandas/Numpy."""
        return self.data.shape


    def extract(self, factor: str | list[str]) -> "FactorModel":   #Self
        """Select specific factors from the model. Str or list[str]. Case-sensitive.

        Stateful: Sets the view to only these factors.
        """
        table = self._get_table()
        select = select_table_columns(table, factor)
        
        self._selected_factors = [f for f in select.column_names if f != 'date']
        return self


    def drop(self, factor: str | list[str]) -> "FactorModel": #Self
        """Remove specific factors from the model. Str or list[str].
                
        Stateful: Removes these factors from the view. Case-sensitive.
        """
        to_drop = [factor] if isinstance(factor, str) else factor
        all_cols = self._get_table().column_names

        selection = [c for c in all_cols if c not in to_drop and c != 'date']

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
        """Convert model to a pandas DataFrame. Wrapper around Arrow's `to_pandas()`."""
        try:
            import pandas as pd  # not needed, but if user doesn't have pandas we can err
            df = self.data.to_pandas()
            if "date" in df.columns:
                df = df.set_index("date")
            return df
        except ImportError:
            raise ImportError("Requires Pandas. `pip install pandas`") from None


    def to_polars(self) -> "pl.DataFrame":
        """Convert model to a polars DataFrame.

        Wrapper around Polars' `from_arrow()`. Triggers the download if 
        not loaded.
        """
        try:
            import polars as pl
            return pl.from_arrow(self.data)
        except ImportError:
            raise ImportError("Requires Polars. `pip install polars`") from None


    def _extract_as_table(self, factor: str | list[str]) -> pa.Table:
        """Internal helper: Extracts factors (columns) and returns a pa.Table."""
        table = self._get_table()
        factor_cols = select_table_columns(table, factor)
        
        if factor_cols.num_columns < 2: 
            raise ValueError("Extraction must include at least one factor.")

        return filter_table_by_date(factor_cols, self.start_date, self.end_date)
    

    def _get_table(self) -> pa.Table:
        """Internal: triggers download if cache empty.

        Returns the full table/data.
        """
        if self._data is None:
            raw_bytes = self._download()
            table = self._read(raw_bytes)

            # order isn't guarenteed after a join.
            # fix: was messing up AQR models with a country param once pd was removed!
            if "date" in table.column_names:
                table = table.sort_by([("date", "ascending")])

            table = round_to_precision(table, self._precision)
            table.validate(full=True)
            self._data = rearrange_columns(table=table).combine_chunks()

        return self._data


    # TODO: ff requires two files... allow list passed in
    def _download(self) -> bytes:
        url = self._get_url()
        log_msg = f"Downloading data from: {url}"
        self.log.info(log_msg)
        try:
            with _HttpClient(timeout=15.0) as client:
                return client.download(url, self.cache_ttl)
        except Exception as e:
            self.log.error(f"Failed to download from {url}: {e}")
            raise RuntimeError(f"Download failed for {url}.") from e


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
    def _get_url(self) -> str | list[str]:
        """Build the unique data source URL."""
        #TODO: change to accept a list (e.g., for FF mom models, backup URLs)
        pass

    @abstractmethod
    def _read(self, data: bytes) -> pa.Table:
        """Read bytes into a pa.Table."""
        pass
