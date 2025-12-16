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

        self.frequency = frequency.lower()
        self.start_date = start_date
        self.end_date = end_date
        self.output_file = output_file
        self.cache_ttl = cache_ttl
        self._data: pd.DataFrame | None = None # Internal storage for processed data

        if self.frequency not in self._frequencies:
            raise ValueError(f"Invalid frequency {frequency}. Valid options: {self._frequencies}")
        super().__init__()

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
        """Retrieves a single factor (column) from the dataset."""
        data = self.download()

        if data.empty:
            self.log.error("DataFrame is empty.")
            raise RuntimeError("DataFrame empty: can not extract a factor.")

        if isinstance(factor, str):
            if factor not in data.columns:
                available = list(data.columns)
                self.log.error(f"Factor '{factor}' not found in model. Available: {available}")
                raise ValueError(f"Factor '{factor}' not available.")       
            return data[factor]

        elif isinstance(factor, list):
            return data[factor] 


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

