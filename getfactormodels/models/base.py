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

    def __init__(self, frequency: str = 'm',
                 start_date: str | None  = None,
                 end_date: str | None = None,
                 output_file: str | None = None,
                 cache_ttl: int = 86400,
                 **kwargs: Any):

        logger_name = f"{self.__module__}.{self.__class__.__name__}"
        self.log = logging.getLogger(logger_name)

        #self.frequency = frequency.lower()  #
        self.start_date = start_date
        self.end_date = end_date
        self.output_file = output_file
        self.cache_ttl = cache_ttl
        self._data: pd.DataFrame | None = None # Internal storage for processed data

        self.frequency = frequency #setter below

        if self.frequency not in self._frequencies:
            raise ValueError(f"Invalid frequency {frequency}. Valid options: {self._frequencies}")
        super().__init__()

    @property
    def frequency(self) -> str:
        """The user-facing chosen frequency."""
        return self._set_frequency

    @frequency.setter
    def frequency(self, value: str):
        val = value.lower()
        # check subclasses _frequencies
        if val not in self._frequencies:
            raise ValueError(f"Invalid frequency '{val}'. Options: {self._frequencies}") 
        # if a change, set it
        if hasattr(self, '_set_frequency') and val != self._set_frequency:
            self.log.info(f"Frequency changed to {val}.")
            self._data = None
            
        self._set_frequency = val

    @property
    def data(self) -> pd.DataFrame:
        """public: access the data. Checks for data, calls download()."""
        # Check if data is stored
        if self._data is not None:
            return self._data 
        # Download, not stored
        return self._download()


    def download(self) -> pd.DataFrame:
        """Public method to download and return the data. 
        Calls .data property to fetch and store
        """
        return self.data


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
 
    # added data param = single point of access for drop/extract. But data should be safe to call anyway.
    def _check_for_factors(self, data: pd.DataFrame, factors: Any) -> list[str]:
        """private helper: check a df for cols existing"""
        if data.empty:
            raise RuntimeError(f"DataFrame empty: no factors.")
        if not factors:
            self.log.info("method called with empty factor list.")
            return []   #returns indexed empty.df

        _factors = [factors] if isinstance(factors, str) else list(factors)

        missing = [f for f in _factors if f not in self.data.columns]
        
        if missing:
            self.log.error(f"{missing} not in model.")
            raise ValueError(f"Factors not in model: {missing}.")
        else:
            return _factors

    # TODO: Remove FactorExtractor
    #def _drop_rf():
    #if rf=0
    #def _drop_mktrf(): 
    #if mktrf = 0
    # set flags.
    #let utils handle it. (_pd_rearrange_cols, if no_rf = 1, then drop it)

    # Making download concrete, and moved the abstractmethod to _read!
    def _download(self) -> pd.DataFrame:
        """Private template method: called by `data` property when self._data is none.
        * Should not be called directly with subclasses.
        """
        # Don't need to check for data here
        raw_data = self._download_from_url()
        data = self._read(raw_data)

        if isinstance(data, pa.Table):
            data = data.to_pandas()
        
        # Storage
        self._data = data

        return data


    @property
    def _url(self) -> str:
        """Internal property: data source URL.
        Fama French returns the underlying model url (4 factor (3+MOM) returns the 3 factor url)
        """
        # subclasses implement _get_url which this uses.
        return self._get_url()

    def _download_from_url(self) -> bytes:
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

