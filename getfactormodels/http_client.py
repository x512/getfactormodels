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
import httpx    # changing from requests, testing first.
import certifi  # adding certifi for CA with httpx
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

class HttpClient:
    """Simple http client: wrapper around httpx.Client."""
    # retries and backoff factor might be good later... TODO.
    def __init__(self, timeout: 15.0):  #TODO: fix type hint on timeout
        self.timeout = timeout,
        self._client = httpx.Client(
            verify=certifi.where(),
            timeout=self.timeout,       # Same err
            follow_redirects=True,
            max_redirects=3,
            #headers
        )
        #
        # TODO: http2
        #
    def close(self) -> None:
        log.debug("closing connection.")
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    def download(self, url: str) -> str:
        """Downloads text content from a given URL."""
        try:
            log.info(f"Connecting: {url}...")
            response = self._client.get(url)
            response.raise_for_status()
            # TODO: cache resp TODO
            return response.text

        except httpx.HTTPStatusError as e:
            log.error(f"HTTP error {e.response.status_code} for {url}")
            raise ConnectionError(f"HTTP error: {e.response.status_code}")

        except httpx.RequestError as e:
            log.error(f"Network error for {url}: {e}")
            raise ConnectionError(f"Request error: {e}")

    def check_connection(self, url: str) -> bool:
        """Simple url ping."""
        #
        # TODO: improve check (try HEAD, check status code value. then try GET)
        #    if file url isn't avail, check if base url is good, etc...
        #    handle errors
        check_timeout = 4.0

        try:
            response = self._client.head(url, timeout=check_timeout)

            if response.is_success:
                log.info(f"URL:{url}\nstatus: {response.status_code}")
                return True

            log.info("Falling back to try GET...")
            response = self._client.get(url, timeout=check_timeout)

            if response.is_success:
                return True

        except Exception:
            return False

