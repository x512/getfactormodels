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


class ClientNotOpenError(Exception):
    """Raised when HttpClient is used outside of a 'with' block."""
    pass


class _HttpClient:
    """Internal HTTP client with caching.

    Wrapper around httpx.Client with SSL context creation and 
    XDG-compliant caching.
    """
    APP_NAME = "getfactormodels"
    APP_AUTHOR = "x512"

    def __init__(
        self,
        timeout: float | int = 15.0,
        cache_dir: str | None = None,
        default_cache_ttl: int = 86400,
    ):
        self.timeout = timeout
        self.default_cache_ttl = default_cache_ttl
        self._client = None

        if cache_dir is None:
            _cache_path = user_cache_path(
                appname=self.APP_NAME, 
                appauthor=self.APP_AUTHOR, 
                ensure_exists=True,
            )
            self.cache_dir = str(_cache_path.resolve())
        else:
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

    def download(
        self,
        url: str,
        tag: str = "Data",
        model_name: str = "Data",
        cache_ttl: int | None = None,
        force: bool = False,
    ) -> bytes:
        """Downloads content, automatically choosing between standard GET and streaming with progress."""
        cache_key = self._generate_cache_key(url)

        # Check cache hit unless force=True
        if not force:
            _, data, expired = self._check_for_update(url, tag=tag)
            if not expired and data is not None:
                return data

        if self._client is None:
            raise ClientNotOpenError("HttpClient is not open. Use within a 'with' block.")

        with self._client.stream("GET", url) as resp:
            resp.raise_for_status()

            total = int(resp.headers.get("Content-Length", 0))
            if total > 1_048_576:
                log.debug(f"File size: {total} bytes. Streaming...")
                new_data = self._progress_bar(resp, model_name)
            else:
                new_data = resp.read()

        ttl = cache_ttl or self.default_cache_ttl
        meta = {
            "etag": resp.headers.get("ETag"),
            "last_modified": resp.headers.get("Last-Modified"),
            "expires_at": time.time() + ttl,
        }

        self.cache.set(
            key=cache_key,
            data=new_data,
            tag=tag,
            metadata=meta,
            expire_secs=ttl,
        )
        return new_data

    def stream(
        self,
        url: str,
        cache_ttl: int,
        tag: str = "Model",
        model_name: str = "Model",
        force: bool = False,
    ) -> bytes:
        """Wrapper around Httpx's stream."""
        cache_key = self._generate_cache_key(url)

        if not force:
            _, data, expired = self._check_for_update(url, tag=tag)
            if not expired and data is not None:
                return data

        if self._client is None:
            raise ClientNotOpenError("HttpClient is not open. Use within a 'with' block.")

        with self._client.stream("GET", url) as resp:
            resp.raise_for_status()
            new_data = self._progress_bar(resp, model_name)

            ttl = cache_ttl or self.default_cache_ttl
            meta = {
                "etag": resp.headers.get("ETag"),
                "last_modified": resp.headers.get("Last-Modified"),
                "expires_at": time.time() + ttl,
            }

            self.cache.set(
                key=cache_key,
                data=new_data,
                tag=tag,
                metadata=meta,
                expire_secs=ttl,
            )
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

    def _get_metadata(self, url: str) -> dict:
        try:
            resp = self._client.head(url, timeout=5.0)
            resp.raise_for_status()

            return {
                "etag": resp.headers.get("ETag"),
                "last_modified": resp.headers.get("Last-Modified"),
            }
        except httpx.HTTPError as e:
            log.debug(f"Unable to get metadata from {url}: {e}")
            return {}

    def _refresh_ttl(self, key: str, data: bytes, meta: dict, tag: str = "Data"):
        """Helper to update cache's ttl without re-downloading."""
        meta["expires_at"] = time.time() + self.default_cache_ttl
        self.cache.set(key, data, tag=tag, metadata=meta)

    def _check_for_update(self, url: str, tag: str = "Data") -> tuple[str, bytes | None, bool]:
        cache_key = self._generate_cache_key(url)

        cached_data, cached_meta = self.cache.get(cache_key)
        if not cached_data or not cached_meta:
            return cache_key, None, True

        expires_at = cached_meta.get("expires_at", 0)
        if time.time() < expires_at:
            log.debug(f"CACHE HIT: {url[:30]}... ({int(expires_at - time.time())}s remaining)")
            return cache_key, cached_data, False

        log.debug("CACHE STALE: Checking server...")
        remote_meta = self._get_metadata(url)

        if remote_meta.get("etag") and remote_meta.get("etag") == cached_meta.get("etag"):
            log.debug("SYNC: ETag match.")
            self._refresh_ttl(cache_key, cached_data, cached_meta, tag=tag)
            return cache_key, cached_data, False

        if remote_meta.get("last_modified") and remote_meta.get("last_modified") == cached_meta.get("last_modified"):
            log.debug("SYNC: Date match.")
            self._refresh_ttl(cache_key, cached_data, cached_meta, tag=tag)
            return cache_key, cached_data, False

        log.debug("CACHE EXPIRED: Metadata mismatch or unavailable.")
        return cache_key, None, True

    def check_connection(self, url: str) -> bool:
        """Returns True if the URL exists and returns a 2xx status code."""
        if self._client is None:
            return False
        try:
            resp = self._client.head(url, timeout=5.0)
            return resp.is_success
        except httpx.HTTPError as e:
            log.debug(f"Connection check failed for {url}: {e}")
            return False
