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
from typing import Any
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from getfactormodels.utils.http_client import HttpClient
from getfactormodels.utils.utils import _validate_date, _save_to_file
from pathlib import Path


class FactorModel(ABC):
    """Abstract Base Class used by all factor model implementations.""" 
    def __init__(self, frequency: str | None = 'm',
                 start_date: str | None  = None,
                 end_date: str | None = None,
                 output_file: str | None = None,
                 cache_ttl: int = 86400,
                 **kwargs: Any):

        logger_name = f"{self.__module__}.{self.__class__.__name__}"
        self.log = logging.getLogger(logger_name)

        self._data: pd.DataFrame | pa.Table | None = None # Internal storage for processed data 
        self._start_date = None
        self._end_date = None
        self._frequency = None

        self.frequency = frequency 
        self.start_date = start_date 
        self.end_date = end_date
        self.output_file = output_file
        self.cache_ttl = cache_ttl
        
        super().__init__()
    
    def __repr__(self) -> str:
        params = [
            f"frequency='{self.frequency}'",
            f"start_date='{self.start_date}'",
            f"end_date='{self.end_date}'"
        ]
        
        if hasattr(self, 'region') and self.region:
            params.append(f"region='{self.region}'")
        
        if self.output_file:
            params.append(f"output_file='{self.output_file}'")

        return f"{self.__class__.__name__}({', '.join(params)})"
   
    def __len__(self) -> int:
        if self._data is None:
            return 0
        return len(self._data)


    @property
    def start_date(self) -> str | None:
        return self._start_date
    @start_date.setter
    def start_date(self, value: Any):
        valid = _validate_date(value, is_end=False)
        if self._start_date != valid:
            self._data = None
            self._start_date = valid

    @property
    def end_date(self) -> str | None:
        return self._end_date
    @end_date.setter
    def end_date(self, value: Any):
        valid = _validate_date(value, is_end=True)
        if self._end_date != valid:
            self._data = None
            self._end_date = valid

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
            raise ValueError(f"Invalid frequency '{val}'. Options: {self._frequencies}") 
        
        if val != self._frequency:
            if self._frequency is not None:
                self.log.info(f"Frequency changed from {self._frequency} to {val}.")
            self._frequency = val
            self._data = None 


    @property
    def data(self) -> pd.DataFrame: # Note: currently guarantees a pd.DataFrame
        """Access the processed dataset.

        This property acts as the primary access point to model data.

        Returns:
            pd.DataFrame: The processed factor data.
        ---
        TODO: Support returning pa.Table directly without forced conversion to pandas.
        """
        # return if cached
        if self._data is not None:
            return self._data 

        file = self._download() 
        df = self._read(file)

        _ordered = self._rearrange_columns(df)  #rearranges pa.Table or pd.DataFrame
        _sliced = self._slice_to_range(_ordered)

        if isinstance(_sliced, pa.Table):
            _df = _sliced.to_pandas(date_as_object=False)
        elif isinstance(_sliced, pd.DataFrame):
            _df = _sliced
        else:
            raise TypeError(f"Unexpected data type after slicing: {type(_sliced)}")

        self._data = _df
        return _df


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


    def extract(self, factor: str | list[str]) -> pd.Series | pd.DataFrame:
        """Select specific factors from the model.

        Args:
            factor (str | list[str]): The column name(s) to extract. 
                Matches are case-sensitive.

        Returns:
            pd.Series | pd.DataFrame: The subset of requested factors.

        ---
        TODO: extract for pa.Table.
        """
        data = self.data #was download()!
        _factors = self._check_factors_exist(data, factor)
        #return data[_factors] # if not _factors else data[factor]
        result = data[_factors]
        # squeeze df to Series if 1 col
        return result.squeeze() if isinstance(factor, str) else result  # type: ignore [reportReturnType]


    def drop(self, factor: str | list[str]) -> pd.Series | pd.DataFrame | list[str]: #will seperate vlidation when blocked in
        """Remove specific factors from the model.

        Args:
            factor (str | list[str]): The column name(s) to remove.
                Matches are case-sensitive.

        Returns:
            pd.DataFrame: The dataset excluding the specified factors.
        ---
        TODO: drop for pa.Table.
        """
        data = self.data
        _factors = self._check_factors_exist(data, factor)

        if len(set(_factors)) >= len(data.columns):
            self.log.error(f"Attempted to drop all columns: {_factors}")
            raise ValueError("Can't drop all columns from a model.")        
        
        return data.drop(columns=factor) #errors=raise


    # Generic public save to file method. # TODO: better saving util
    def to_file(self, filepath: str | Path | None = None) -> None:
        """Save data to a file.

        If `filepath` is None, data is saved to user's CWD with generated filename.

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



    def _slice_to_range(self, data: pa.Table | pd.DataFrame) -> pa.Table | pd.DataFrame:
        """
        Slice a dataset to a requested date range.

        Args:
            data (pa.Table | pd.DataFrame): The dataset to slice.  

        Returns:
            pa.Table | pd.DataFrame: the filtered dataset.

        Raises:
            ValueError: if the Pandas index is not a DatetimeIndex.
            TypeError: if 'data' isn't a pd.DataFrame or a pa.Table.
            pa.ArrowInvalid: error occured during the PyArrow compute filter.
            KeyError: if the 'date' column is missing from the PyArrow Table.

        ---
        NOTES:
        - When data is a `pa.Table`, the first column must be named 'date'.
        - Data should be pre-sorted to avoid the overhead of sorting here. 
        
        TODO: 
        Enforce sorting. Can switch PyArrow to binary search slicing 
        (with `pc.search_sorted` + `.slice()`).
        """
        start, end = self.start_date, self.end_date 

        if isinstance(data, pd.DataFrame):
            if not isinstance(data.index, pd.DatetimeIndex):
                raise ValueError("Pandas DataFrame index must be a DatetimeIndex.")
            
            if not data.index.is_monotonic_increasing:
                data = data.sort_index()
                
            return data.loc[start : end]
        
        elif isinstance(data, pa.Table):
            date_col = 'date'  # can get from model schema, but all are date...
            
            if date_col not in data.column_names:
                raise KeyError(f"'{date_col}' not found in the PyArrow Table.")

            try:  #if definitely sorted: pc.search_sorted + .slice()
                mask = (pc.field(date_col) >= pc.scalar(start)) & \
                   (pc.field(date_col) <= pc.scalar(end))
            
                return data.filter(mask)
            except Exception as e:
                    raise pa.ArrowInvalid(f"Failed to filter PyArrow Table: {e}")
        else:
            raise TypeError(f"Unsupported data type: {type(data)}. Expected pd.DataFrame or pa.Table.")


    def _rearrange_columns(self, data: pd.DataFrame | pa.Table) -> pd.DataFrame | pa.Table:
        """Rearrange columns to a standard order.

        Private method to move 'Mkt-RF' to the first column position, 'RF' to 
        the last column. If the columns aren't present, the original order is 
        preserved.

        If a `pd.Series` is given, the series is returned unchanged.

        Args:
          data (`pd.DataFrame | pa.Table`): The input data object.

        Returns:
          `pd.DataFrame | pa.Table`: The data with rearranged columns.
        """
        if isinstance(data, pd.Series):
            return data
        
        # now working for both tables and dfs!
        #cols = list(data.columns) if isinstance(data, pd.DataFrame) else data.column_names
        cols = list(data.columns) if hasattr(data, "columns") else data.column_names
        
        for factor in ['RF', 'Mkt-RF']:
            if factor in cols:
                cols.remove(factor)
                if factor == 'Mkt-RF':
                    cols.insert(0, factor)  # to front
                else:
                    cols.append(factor)     # to back
        
        return data[cols] if hasattr(data, "columns") else data.select(cols)


    # added data param = single point of access for drop/extract. But data should be safe to call anyway.
    def _check_factors_exist(self, data: pd.DataFrame, factors: Any) -> list[str]:  # make boolean for list[str] | str, and make _if_factors_exist
        """private helper: check a df for cols existing"""
        if data.empty:
            raise RuntimeError("DataFrame empty: no factors.")
        if not factors:
            self.log.info("method called with empty factor list.")
            return []   #returns indexed empty.df

        _factors = [factors] if isinstance(factors, str) else list(factors)
        if not _factors:
            return []

        missing = [f for f in _factors if f not in data.columns]

        if missing:
            self.log.error(f"{missing} not in model.")
            raise ValueError(f"Factors not in model: {missing}.")
        return _factors


    def _download(self) -> bytes:
        url = self._get_url()
        log_msg = f"Downloading data from: {url}"
        self.log.info(log_msg)
        try:
            with HttpClient(timeout=15.0) as client:
                return client.download(url, self.cache_ttl)
        except Exception as e:
            self.log.error(f"Failed to download from {url}: {e}")
            # crash fast on download failure
            raise RuntimeError(f"Download failed for {url}.") from e


    @property
    @abstractmethod
    def _frequencies(self) -> list[str]:
        pass

    @abstractmethod
    def _get_url(self) -> str:
        """Build the unique data source URL."""
        pass

    @abstractmethod
    def _read(self, data: bytes) -> pd.DataFrame | pa.Table:
        """Convert bytes into a pd.DataFrame or pa.Table."""
        pass

#feat branch: precision rf 
#feat 
#
