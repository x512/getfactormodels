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
from typing import Any, Optional
from getfactormodels.utils.http_client import HttpClient
import pandas as pd

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
    def _read(self, data: bytes) -> pd.DataFrame:
        """converts the bytes into a DataFrame."""
        pass
        
    def __init__(self, frequency: str = 'm',
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None,
                 output_file: Optional[str] = None,
                 cache_ttl: int = 86400,
                 **kwargs: Any):

        logger_name = f"{self.__module__}.{self.__class__.__name__}"
        self.log = logging.getLogger(logger_name)

        self.frequency = frequency.lower()
        self.start_date = start_date
        self.end_date = end_date
        self.output_file = output_file
        self.cache_ttl = cache_ttl

        self._data: Optional[pd.DataFrame] = None # Internal storage for processed data

        # Validate input frequency
        if self.frequency not in self._frequencies:
            raise ValueError(f"Invalid frequency {frequency}. Valid options: {self._frequencies}")
        super().__init__()

    @property
    def data(self) -> pd.DataFrame:
        """public: access the data. Calls download()."""
        return self.download()

    # Making download concrete, and moved the abstractmethod to _read!
    def download(self) -> pd.DataFrame:
        if self._data is not None:
            self.log.debug("Data loaded. Returning stored DataFrame.")
            return self._data
        
        raw_data = self._http_download() 
        data = self._read(raw_data)

        self._data = data
        return self._data

    @property
    def url(self) -> str:
        """data source URL"""
        return self._get_url()

    def _http_download(self) -> bytes:
        url = self.url
        self.log.info(f"Downloading data from: {url}")

        with HttpClient(timeout=15.0) as client:
            # Uses the cache_ttl set in __init__
            return client.download(url, self.cache_ttl)

