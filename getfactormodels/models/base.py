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
from getfactormodels.utils.http_client import HttpClient
from pathlib import Path

class FactorModel(ABC):
    """base model used by all factor models."""
    @property
    @abstractmethod
    def _frequencies(self) -> list[str]:
        pass

    @abstractmethod
    def _get_url(self) -> str:
        """builds the unique data source URL."""
        pass

    @abstractmethod
    def _read(self, data: bytes) -> pd.DataFrame | pa.Table:
        """convert bytes into a pd.DataFrame or pa.Table."""
        pass

    def __init__(self, frequency: str | None = 'm',
                 start_date: str | None  = None,
                 end_date: str | None = None,
                 output_file: str | None = None,
                 cache_ttl: int = 86400,
                 **kwargs: Any):

        logger_name = f"{self.__module__}.{self.__class__.__name__}"
        self.log = logging.getLogger(logger_name)
        
        # Init to None... 
        self._data: pd.DataFrame | None = None # Internal storage for processed data 
        self._start_date = None
        self._end_date = None
        self._frequency = None
        
        # ...use setters
        self.frequency = frequency 
        self.start_date = start_date 
        self.end_date = end_date
        
        self.output_file = output_file
        self.cache_ttl = cache_ttl

        super().__init__()
    
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
        
        # now handles the initial set AND subsequent changes
        if val != self._frequency:
            if self._frequency is not None:
                self.log.info(f"Frequency changed from {self._frequency} to {val}.")
            self._frequency = val
            self._data = None 

    @property
    def start_date(self) -> str | None:
        return self._start_date

    @start_date.setter
    def start_date(self, value: str | None):
        if getattr(self, "_start_date", None) != value:
            self._data = None
            self._start_date = value


    @property
    def end_date(self) -> str | None:
        return self._end_date

    @end_date.setter
    def end_date(self, value: str | None):
        if getattr(self, "_end_date", None) != value:
            self._data = None
            self._end_date = value


    @property
    def data(self) -> pd.DataFrame:
        """public: access the data. Checks for data, calls download()."""
        # return if cached
        if self._data is not None:
            return self._data 

        file = self._download() 
        df = self._read(file)
        
        if isinstance(df, pa.Table):
            df = df.to_pandas(date_as_object=False)
            
        self._data = self._slice_to_range(df)
        return self._data

        
    def download(self) -> pd.DataFrame:
        """Public method to download and return the data. 
        Calls .data property to fetch and store
        """
        df = self.data
    
        # if path exists, savin
        if self.output_file:
            self._export(df, self.output_file)

        return df


    def extract(self, factor: str | list[str]) -> pd.Series | pd.DataFrame:
        """Return only the specified factors."""
        data = self.data #was download()!
        _factors = self._check_for_factors(data, factor)
        return data[_factors] # if not _factors else data[factor]


    def drop(self, factor: str | list[str]) -> pd.Series | pd.DataFrame | list[str]: #will seperate vlidation when blocked in
        """Drop the specified factors. Case-sensitive."""
        data = self.data
        _factors = self._check_for_factors(data, factor)

        if len(set(_factors)) >= len(data.columns):
            self.log.error(f"Attempted to drop all columns: {_factors}")
            raise ValueError("Can't drop all columns from a model.")        
        
        return data.drop(columns=factor) #errors=raise

    # TODO: a) user can't extract the index: good. Empty str? Not a col, that's good.
    #       b) user can drop every column, leaving the index with an empty df: that's bad. [FIXED - check len]
    #       c) user can't drop the index: that's good...
    #       d) user can pass duplicates though: drop(["HML", "HML"]); doesn't cause a problem though.  [FIXED - use set]
    #       e) user can pass through empties... [] None "", drop([]) drop([None, "", ""])    [FIXED]
 

    @property
    def _url(self) -> str:
        """Internal property: data source URL. 
        - Note: for Fama-French this is the "base" model: "3" for 3 and 4-factor models, 
          "5" for the 5 and 6-factor models.)
        """
        return self._get_url()


    def _slice_to_range(self, df: pd.DataFrame) -> pd.DataFrame:
        """The one method to rule them all."""
        # Ensure index is sorted (crucial for slicing)
        if not df.index.is_monotonic_increasing:
            df = df.sort_index()
        # TODO: pa.Tables need to do this soon 
        return df.loc[self.start_date : self.end_date]


    # added data param = single point of access for drop/extract. But data should be safe to call anyway.
    def _check_for_factors(self, data: pd.DataFrame, factors: Any) -> list[str]:
        """private helper: check a df for cols existing"""
        if data.empty:
            raise RuntimeError("DataFrame empty: no factors.")
        if not factors:
            self.log.info("method called with empty factor list.")
            return []   #returns indexed empty.df

        _factors = [factors] if isinstance(factors, str) else list(factors)

        missing = [f for f in _factors if f not in data.columns]

        if missing:
            self.log.error(f"{missing} not in model.")
            raise ValueError(f"Factors not in model: {missing}.")
        return _factors


    #renamed _download from _download_from_url, removed old _download into data
    def _download(self) -> bytes:
        url = self._url
        log_msg = f"Downloading data from: {url}"
        self.log.info(log_msg)
        try:
            with HttpClient(timeout=15.0) as client:
                return client.download(url, self.cache_ttl)
        except Exception as e:
            self.log.error(f"Failed to download from {url}: {e}")
            # crash fast on download failure
            raise RuntimeError(f"Download failed for {url}.") from e


  # TODO: Remove FactorExtractor
    #def _drop_rf():
    #if rf=0
    #def _drop_mktrf(): 
    #if mktrf = 0
    # set flags.
    #let utils handle it. (_pd_rearrange_cols, if no_rf = 1, then drop it)

    # Making download concrete, and moved the abstractmethod to _read!
    #def _download(self) -> pd.DataFrame:
    #    """Private template method: called by `data` property when self._data is none.
    #    * Should not be called directly with subclasses.
    #    """
    #    # Don't need to check for data here
    #    raw_data = self._download_from_url()
    #    data = self._read(raw_data)
#
 #       if isinstance(data, pa.Table):
  #          data = data.to_pandas()
   #     
    #    # Storage
     #   self._data = data
#
 #       return data


    def to_file(self, filepath: str | Path | None = None) -> None:
        """ save to file 
        Usage: m.to_file() or m.to_file('custom_name.csv')
        """
        if self.data is None or self.data.empty:
            self.log.warning("Nothing to save. Did you run download()?")
            return

        # If the user passes a filepath here, we temporarily use it.
        # Otherwise, we use whatever is already stored in self.output_file.
        target = filepath if filepath else self.output_file
        
        # Now call the internal worker
        self._export(self.data, target)

    def _export(self, df: pd.DataFrame, target_path: str | Path | None) -> None:
        if not self.output_file:
            return
        from getfactormodels.utils.utils import _save_to_file
        _save_to_file(df, target_path, model_instance=self)
        
        self.log.info(f"Data saved: {self.output_file}")
