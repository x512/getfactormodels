from abc import ABC, abstractmethod
from getfactormodels.http_client import HttpClient
import logging
from typing import Optional, Any

class FactorModel(ABC):
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

        msg = f"FactorModel initialized"
        self.log.debug(msg)

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

#    def _validate_frequency
#        ...

#    def _rearrange_cols
#        ...
