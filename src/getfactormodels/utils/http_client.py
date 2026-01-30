# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import hashlib
import logging
import ssl
import sys
import time
from io import BytesIO
import certifi
import httpx
from platformdirs import user_cache_path
from .cache import _Cache

log = logging.getLogger(__name__)

# TODO: cleanup debug messages!

# TODO: FIXME: opens, checks, closes, re-opens to d/l. Need to open - check, download if needed - close. 
# 
class _HttpClient:
    """Internal HTTP client with caching.

    Wrapper around httpx.Client with SSL context creation and 
    XDG-compliant caching.
    """
    APP_NAME = "getfactormodels"
    APP_AUTHOR = "x512"

    def __init__(self, timeout: float | int = 15.0,
                 cache_dir: str | None = None, # None by default!
                 default_cache_ttl: int = 86400):
        """Initialize the internal client.

        Args:
            timeout (str | float): max time to wait for a network response.
            cache_dir (str, optional): Path to store cached files. Defaults to
              standard user cache directory (~/.cache/getfactormodels).
            default_cache_ttl (int): cache ttl in seconds (default: 86400, one day)
        """
        self.timeout = timeout
        self.default_cache_ttl = default_cache_ttl
        self._client = None

        # XDG path
        if cache_dir is None:
            _cache_path = user_cache_path(appname=self.APP_NAME, 
                                          appauthor=self.APP_AUTHOR, 
                                          ensure_exists=True)
            self.cache_dir = str(_cache_path.resolve())

        else: # user explicitly passed a path, use it
            self.cache_dir = cache_dir
        
        self.cache = _Cache(self.cache_dir, default_timeout=default_cache_ttl)


    def __enter__(self):
        if self._client is None:

            ssl_context = ssl.create_default_context(cafile=certifi.where())
            self._client = httpx.Client(
                verify=ssl_context,
                timeout=self.timeout,
                follow_redirects=True,
                max_redirects=3,
            )
            msg = f"http_client ENTER: (client: {self._client.__class__.__name__})"
            log.debug(msg)
        return self


    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
        self.cache.close()


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


    def _generate_cache_key(self, url: str) -> str:
        """Generate a cache key for the URL."""
        return hashlib.sha256(url.encode('utf-8')).hexdigest()


    def download(self, url: str, model_name: str = "Data", cache_ttl: int | None = None) -> bytes:
        """Downloads content, automatically choosing between standard GET and streaming with progress."""
        key, data, expired = self._check_for_update(url)
        if not expired:
            return data

        if self._client is None:
            raise ClientNotOpenError("HttpClient is not open. Use within a 'with' block.")

        #try:
        # stream request here to get headers without downloading body yet
        with self._client.stream("GET", url) as resp:
            resp.raise_for_status()
            
            total = int(resp.headers.get("Content-Length", 0))
            # progress bar if over 1MB
            if total > 1_048_576:
                log.debug(f"File size: {total} bytes. Streaming...")
                new_data = self._progress_bar(resp, model_name)
            else:
                # Small file: just read it directly from the existing stream
                new_data = resp.read()

        #except httpx.HTTPStatusError as e:
        #    raise NetworkError(f"Server returned {e.response.status_code} for {url}") from e
        #except httpx.RequestError as e:
        #    raise NetworkError(f"Connection failed for {url}") from e

        ttl = cache_ttl or self.default_cache_ttl
        meta = {
            "etag": resp.headers.get("ETag"),
            "last_modified": resp.headers.get("Last-Modified"),
            "expires_at": time.time() + ttl,
        }
        
        self.cache.set(key, new_data, metadata=meta)
        return new_data

    def stream(self, url: str, cache_ttl: int, model_name="Model") -> bytes:
        """Wrapper around Httpx's stream.

        - Uses the _progress_bar helper. 
        - This is used by AQR models.
        """
        key, data, expired = self._check_for_update(url)
        if not expired: return data

        with self._client.stream("GET", url) as resp:
            resp.raise_for_status()
            new_data = self._progress_bar(resp, model_name)

            ttl = cache_ttl or self.default_cache_ttl
            meta = {
                "etag": resp.headers.get("ETag"),
                "last_modified": resp.headers.get("Last-Modified"),
                "expires_at": time.time() + ttl,
            }
            
            self.cache.set(key, new_data, metadata=meta)
            return new_data


    def _progress_bar(self, response, model_name: str = "Model") -> bytes:
        """A progress bar for downloads."""
        buffer = BytesIO()
        total = int(response.headers.get("Content-Length", 0))
        downloaded = 0
        start_time = time.time()
        label = f"({model_name}) Downloading data"

        chunk_size = 128 * 1024
        for chunk in response.iter_bytes(chunk_size=chunk_size):
            buffer.write(chunk)
            downloaded += len(chunk)

            if total > 0:
                percent = (downloaded / total) * 100
                elapsed = time.time() - start_time
                speed = (downloaded / 1024) / elapsed if elapsed > 0 else 0

                bar = ('#' * int(percent // 5)).ljust(20, '.')

                sys.stderr.write(f"\r{label}: [{bar}] {percent:3.0f}% ({speed:3.2f} kb/s) ")
                sys.stderr.flush()

        sys.stderr.write("\n")
        return buffer.getvalue() 
    

    # New
    def _get_metadata(self, url: str) -> dict:
        try:
            resp = self._client.head(url, timeout=5.0)
            resp.raise_for_status() 
            
            return {
                "etag": resp.headers.get("ETag"),
                "last_modified": resp.headers.get("Last-Modified"),
            }
        except Exception as e:
            # If fail (405/404, timeout), return empty, so can just do a normal download 
            log.debug(f"Err?: Unable to get metadata from {url}: {e}")
            return {}

    # New
    def _refresh_ttl(self, key, data, meta):
        """Helper to update cache's ttl without re-downloading."""
        meta["expires_at"] = time.time() + self.default_cache_ttl
        self.cache.set(key, data, metadata=meta)


    def _check_for_update(self, url: str) -> tuple[str, bytes | None, bool]:
        cache_key = self._generate_cache_key(url)
        
        cached_data, cached_meta = self.cache.get(cache_key)
        if not cached_data or not cached_meta:
            return cache_key, None, True # expired = True

        expires_at = cached_meta.get("expires_at", 0)
        if time.time() < expires_at:
            log.debug(f"CACHE HIT: {url[:30]}... ({int(expires_at - time.time())}s remaining)")
            return cache_key, cached_data, False # expired = False

        log.debug("CACHE STALE: Checking server...")
        remote_meta = self._get_metadata(url)
        
        if remote_meta.get("etag") and remote_meta.get("etag") == cached_meta.get("etag"):
            log.debug("SYNC: ETag match.")
            self._refresh_ttl(cache_key, cached_data, cached_meta)
            return cache_key, cached_data, False

        if remote_meta.get("last_modified") and remote_meta.get("last_modified") == cached_meta.get("last_modified"):
            log.debug("SYNC: Date match.")
            self._refresh_ttl(cache_key, cached_data, cached_meta)
            return cache_key, cached_data, False

        log.debug("CACHE EXPIRED: Metadata mismatch or unavailable.")
        return cache_key, None, True


    def check_connection(self, url: str) -> bool:
        if self._client is None: 
            return False
        try:
            return self._client.head(url, timeout=5.0).is_success
        except:
            return False

    # TODO: user needs to acces this. force, or clear cache?
    def _clear_cache(self) -> None:
        self.cache.clear()
        log.debug("CACHE: cleared by HttpClient")

# TODO: Exception handling...
class ClientNotOpenError(Exception):
    """Raised when HttpClient is used outside of a 'with' block."""
    pass
