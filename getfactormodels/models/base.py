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
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from getfactormodels.utils.http_client import _HttpClient
from getfactormodels.utils.utils import _save_to_file, _validate_date

# Pandas display opts...
pd.set_option('display.float_format', lambda x: f'{x:.8f}')

class FactorModel(ABC):
    """Abstract Base Class used by all factor model implementations."""
    def __init__(self, frequency: str | None = 'm',
                 start_date: str | None  = None,
                 end_date: str | None = None,
                 output_file: str | None = None,
                 cache_ttl: int = 86400,
                 **kwargs: Any):
        """ TODO """

        logger_name = f"{self.__module__}.{self.__class__.__name__}"
        self.log = logging.getLogger(logger_name)

        self._data: pa.Table | None = None     #pd.DataFrame | pa.Table | None = None # Internal storage for processed data 
        self._df: pd.DataFrame | None = None  # new caching the df
        
        self._start_date = None
        self._end_date = None
        self._frequency = None

        self.frequency = frequency 
        self.start_date = start_date 
        self.end_date = end_date
        self.output_file = output_file
        self.cache_ttl = cache_ttl
        self.copyright: str = ""  # NEW, TEST. fix: Carhart erroring with FF with copyright
        
        super().__init__()
    
    def __repr__(self) -> str:
        params = [
            f"frequency='{self.frequency}'",
            f"start_date='{self.start_date}'",
            f"end_date='{self.end_date}'",
        ]
        
        if hasattr(self, 'region') and self.region:
            params.append(f"region='{self.region}'")
        
        if self.output_file:
            params.append(f"output_file='{self.output_file}'")

        return f"{self.__class__.__name__}({', '.join(params)})"
   
    def __len__(self) -> int:
        return len(self.data) # length of the df (after slicing)


    @property
    def start_date(self) -> str | None:
        return self._start_date
    @start_date.setter
    def start_date(self, value: Any):
        valid = _validate_date(value, is_end=False)
        if self._start_date != valid:
            self._start_date = valid
            self._df = None # make .data reslice the table

    @property
    def end_date(self) -> str | None:
        return self._end_date
    @end_date.setter
    def end_date(self, value: Any):
        valid = _validate_date(value, is_end=True)
        if self._end_date != valid:
            self._end_date = valid  # set 
            self._df = None
    
    @property
    def frequency(self) -> str | None: #added none for typehint TODO check
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
                self.log.info(f"Freq. changed from {self._frequency} to {val}.")
            self._frequency = val
            self._data = None
            self._df = None

    @property
    def data(self) -> pd.DataFrame:
        """Returns the sliced Pandas df."""
        if self._df is not None:
            return self._df

        table = self._get_table()
        
        sliced_table = self._slice_to_range(table)
        
        # pd containment zone ------------------------------ # 
        df = sliced_table.to_pandas(date_as_object=False)
        if 'date' in df.columns:
            df = df.set_index('date')
            df.index = pd.to_datetime(df.index)     
        self._df = df
        return self._df
        # -------------------------------------------------- #






    def extract(self, factor: str | list[str]) -> pa.Table | pa.ChunkedArray:
        """Select specific factors from the model.

        Args:
            factor (str | list[str]): The column name(s) to extract. 
                Matches are case-sensitive.

        Returns:
            pa.Series | pa.ChunkedArray: The subset of requested factors.
        """
        table = self._get_table()

        _sliced = self._slice_to_range(table)
        _factors = self._check_factors_exist(_sliced, factor)
        
        selection = list(set(['date'] + _factors))
        
        self._data = _sliced.select(selection) #overwrite with the subset
        self._df = None
        
        table = self._data

        if isinstance(factor, str):  #array if one col
            return table.column(selection.index(factor))
        
        return table


    def drop(self, factor: str | list[str]) -> pa.Table:
        """Remove specific factors from the model.

        Args:
            factor (str | list[str]): The column name(s) to remove.
                Matches are case-sensitive.

        Returns:
            pd.DataFrame: The dataset excluding the specified factors.
        """
        table = self._get_table()
        _sliced = self._slice_to_range(table)
        
        _factors_to_drop = self._check_factors_exist(_sliced, factor)
        
        drop_set = {f for f in _factors_to_drop if f != 'date'}
        cols = [c for c in _sliced.column_names if c not in drop_set]
        
        if len(cols) <= 1: #check before update...
            raise ValueError("Cannot drop all factors from the model.")
        
        self._data = _sliced.select(cols) 
        self._df = None

        return self._data


    # Generic public save to file method. # TODO: better saving util
    def to_file(self, filepath: str | Path | None = None) -> None:
        """Save data to a file.

        If filepath is None, data is saved to user's CWD with generated filename.

        Args:
            filepath (str | Path | None): the filepath to save data to. 
                Supported extensions: .pkl .csv .txt. .parquet. 

        Example:
            `.to_file()` or `.to_file('custom_name.pkl')`
        ---
        TODO: instead of using cwd, use XDG_USER_CACHE/getfactormodels/data/file.csv
        """
        # if self.data is None or self.data.empty:
        #     self.log.warning("No data to save.")
        #     return

        target = filepath if filepath else self.output_file

        if not target:
            self.log.error("No filepath provided and no default output_file set.")
            return

        df = self.data

        if df is None or df.empty:
            self.log.warning("No data available to save.")
            return

        _save_to_file(df, target, model_instance=self)
        self.log.info(f"Data saved: {target}")

    
    def download(self) -> pd.DataFrame:
        """Download and return the data.

        Will download the latest data or read from cache. If output_file is set on 
        the model instance, the result is saved. 
        ---
        TODO: pyarrow returns.
        """
        df = self.data
    
        # if path exists, savin
        if self.output_file:
            # to_file() logs the save
            self.to_file()  # using public method now

        return df


    def _get_table(self) -> pa.Table:
        """Internal: always returns the full dataset."""
        if self._data is not None:
            return self._data

        raw_bytes = self._download()

        table = self._read(raw_bytes) 
        self._data = self._rearrange_columns(table)
        
        return self._data 


    # TODO: Move utils out, if need in base then have helper in here
    def _slice_to_range(self, table: pa.Table) -> pa.Table:
        """Vectorized slicing using PyArrow compute expressions."""
        start, end = self.start_date, self.end_date
        if start is None and end is None:
            return table

        # target the date col and its type
        date_col = 'date' if 'date' in table.column_names else table.column_names[0]
        target_type = table.schema.field(date_col).type
        expr = pc.field(date_col)

        # Build the combined expression
        mask = None
        if start:
            mask = (expr >= pc.scalar(start).cast(target_type))
        
        if end:
            end_mask = (expr <= pc.scalar(end).cast(target_type))
            mask = (mask & end_mask) if mask is not None else end_mask

        return table.filter(mask) if mask is not None else table


    def _rearrange_columns(self, table: pa.Table) -> pa.Table:
        """Standardize column order between models. 
        
        - For the columns that exist in the model, the order is always: 
        date, Mkt-RF, [FACTORS...], RF.
        """
        cols = table.column_names

        if 'date' not in cols:
            # if 'date' isn't col 0 by now the subclass/model is bad. It's not the user's input.
            raise RuntimeError(
                f"Error: '{self.__class__.__name__}' did not create a 'date' column! (cols: {cols})"
            )
        
        front = [c for c in ['date', 'Mkt-RF'] if c in cols]
        back = [c for c in ['RF'] if c in cols]
        
        boundaries = set(front + back)
        mid = [c for c in cols if c not in boundaries]
        
        new_order = front + mid + back
        
        if new_order == cols:
            return table
            
        return table.select(new_order)


    def _check_factors_exist(self, 
                            table: pa.Table, 
                            factors: str | list[str] | None) -> list[str]:
        """
        Checks if factors exist in a pyarrow Table. 
        Returns the list of valid factors or raises if missing.
        """
        if table.num_rows == 0:
            raise RuntimeError("Table is empty.")
        
        if not factors: #silently return if nothing to do 
            return []

        _factors = [factors] if isinstance(factors, str) else list(factors)
        
        # Validation against table schema
        table_cols = set(table.column_names)
        missing = [f for f in _factors if f not in table_cols]

        if missing:
            raise ValueError(f"Factors not in Table: {missing}")

        return _factors    
    
    # TODO: ff requires two files, allow list passed in, or it's two connections
    def _download(self) -> bytes:
        url = self._get_url()
        log_msg = f"Downloading data from: {url}"
        self.log.info(log_msg)
        try:
            with _HttpClient(timeout=15.0) as client:
                return client.download(url, self.cache_ttl)
        except Exception as e:
            self.log.error(f"Failed to download from {url}: {e}")
            # crash fast on download failure
            raise RuntimeError(f"Download failed for {url}.") from e


    @property # new default
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
    def _get_url(self) -> str | list[str]:  #TODO, backup URLs with client... (list[str])
        """Build the unique data source URL."""
        pass

    @abstractmethod
    def _read(self, data: bytes) -> pa.Table:
        """Read bytes into a pa.Table."""
        pass

