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
import hashlib
import logging
import ssl
import certifi
import httpx
from platformdirs import user_cache_path
from .cache import _Cache

log = logging.getLogger(__name__)
# TODO: errors

class HttpClient:
    """Simple http client: wrapper around httpx.Client with caching."""
    
    APP_NAME = "getfactormodels"  #platformdirs for xdg cache 

    def __init__(self, timeout: float | int = 15.0,
                 cache_dir: str | None = None, # None by default!
                 default_cache_ttl: int = 86400): # 1 day default
        self.timeout = timeout
        self.default_cache_ttl = default_cache_ttl
        self._client = None # initializes client as None

        # XDG path
        if cache_dir is None:
            _cache_path = user_cache_path(appname=self.APP_NAME, 
                                       ensure_exists=True)
            self.cache_dir = str(_cache_path.resolve())
            
        else:
            # user explicitly passed a path, use it...
            self.cache_dir = cache_dir

        msg = f"cache directory: {self.cache_dir}"
        log.debug(msg)

        self.cache = _Cache(self.cache_dir, default_timeout=default_cache_ttl)

    def __enter__(self):
        if self._client is None:
            # fix: tox depre warning: create SSL context
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            self._client = httpx.Client(
                verify=ssl_context,
                timeout=self.timeout,
                follow_redirects=True,
                max_redirects=3,
            )

        return self 
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        msg = f"closed: {self._client.__class__.__name__}"
        log.debug(msg)

    def close(self) -> None:
        if self._client is not None:
            msg = f'closing {self._client.__class__.__name__}'
            log.debug(msg)  # No print in log messages, ruff
            self._client.close()
            self._client = None

        msg =f'closing {self.cache.__class__.__name__}'
        log.debug(msg)
        self.cache.close()

    def _generate_cache_key(self, url: str) -> str:
        """Generates a cache key/hash for the URL."""
        return hashlib.sha256(url.encode('utf-8')).hexdigest()

    def download(self, url: str, cache_ttl: int | None = None) -> bytes:
        """Downloads content from a given URL.
           cache_ttl: int, secs
        """
        # To prevent non-context manager usage?
        if self._client is None:
            raise ClientNotOpenError(
                "HttpClient is not open. It must be used within a 'with HttpClient(...) as client:' block."
            )

        cache_key = self._generate_cache_key(url)
        cached_data = self.cache.get(cache_key)

        if cached_data is not None:
            return cached_data

        try:
            msg = f'Connecting: {url[:30]}...'
            log.info(msg)

            response = self._client.get(url)
            response.raise_for_status()
            data = response.content  # Always bytes

            # Store in cache: need to verify it
            self.cache.set(cache_key, data, expire_secs=cache_ttl)
            msg = f"{len(data)} bytes cached."
            log.info(msg)

            return data
        except httpx.HTTPStatusError as e:
            err = f"HTTP error {e.response.status_code} for {url}"
            log.error(err)
            raise ConnectionError(f"HTTP error: {e.response.status_code}")
        except httpx.RequestError as e:
            err = f"Network error for {url}: {e}"
            log.error(err)
            raise ConnectionError(f"Request error: {e}")

    def check_connection(self, url: str):
        """Simple url ping. Boolean."""
        check_timeout = 4.0

        if self._client is None:
            raise ClientNotOpenError("HttpClient is not open. Use in a `with` block.")

        try:
            msg = f"Attempting HEAD: {url}..."
            log.info(msg)

            response = self._client.head(url, timeout=check_timeout)

            if response.is_success:
                msg = f"URL:{url}\nstatus: {response.status_code}"
                log.info(msg)
                return True

            msg = "Falling back to GET..."
            log.info(msg)

            response = self._client.get(url, timeout=check_timeout)

            if response.is_success:
                return True

            msg = "Couldn't establish connection."
            log.info(msg)
        
        except httpx.RequestError: 
            return False

        return False

    # TODO: user needs to acces this. force, or clear cache?
    def _clear_cache(self) -> None:
        self.cache.clear()
        log.debug("cache cleared by HttpClient")

# TODO: Exception handling...
class ClientNotOpenError(Exception):
    """Raised when HttpClient is used outside of a 'with' block."""
    pass
