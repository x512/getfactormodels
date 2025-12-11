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
from getfactormodels.http_client import HttpClient


class FactorModel(ABC):
    """base model used by all factor models."""
    @property
    @abstractmethod
    def _frequencies(self) -> list[str]:
        pass

    def __init__(self, frequency: str = 'm',
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None,
                 output_file: Optional[str] = None,
                 cache_ttl: int = 86400,
                 **kwargs: Any): #ff has models, qfactors have classic boolean, no RF etc

        logger_name = f"{self.__module__}.{self.__class__.__name__}"
        self.log = logging.getLogger(logger_name)

        self.frequency = frequency.lower()
        self.start_date = start_date
        self.end_date = end_date
        self.output_file = output_file
        self.cache_ttl = cache_ttl

        self.log.debug(f"FactorModel initialized with frequency='{self.frequency}'")

        # Validate input frequency for model
        if self.frequency not in self._frequencies: 
            raise ValueError(f"Invalid frequency {frequency}. Valid options: {self._frequencies}")
        super().__init__()


    @abstractmethod
    def download(self) -> Any:  #TODO: type hints!
        pass

    @abstractmethod
    def _get_url(self) -> str:
        """build url based on freq etc"""
        pass 

    @property
    def url(self) -> str:
        """data source URL"""
        return self._get_url()

    def _download(self) -> bytes:
        url = self.url

        msg = f"Downloading data from: {url}"
        self.log.info(msg)

        with HttpClient(timeout=15.0) as client:
            return client.download(url, self.cache_ttl)

