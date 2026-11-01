# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
# getfactormodels/utils/http_client.py
"""HTTP client."""

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
        self._client: httpx.Client | None = None

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
            log.debug(
                "http_client ENTER: (client: %s)",
                self._client.__class__.__name__,
            )
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
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def download(
        self,
        url: str,
        tag: str = "Data",
        model_name: str = "Data",
        cache_ttl: int | None = None,
        *,
        force: bool = False,
        force_progress: bool = False,
    ) -> bytes:
        """Download content, utilizing cache when possible."""
        # Check cache
        if not force:
            cached_data = self._get_cached_data(url, tag=tag)
            if cached_data is not None:
                return cached_data

        # Check client is open
        if self._client is None:
            raise ClientNotOpenError(
                "HttpClient is not open. Use within a 'with' block."
            )

        # Download
        with self._client.stream("GET", url) as resp:
            resp.raise_for_status()
            new_data = self._read_with_progress(
                resp, model_name, force=force_progress,
            )

        # Cache
        ttl = cache_ttl or self.default_cache_ttl
        meta = {
            "etag": resp.headers.get("ETag"),
            "last_modified": resp.headers.get("Last-Modified"),
            "expires_at": time.time() + ttl,
        }

        self.cache.set(
            key=self._generate_cache_key(url),
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
        *,
        force: bool = False,
    ) -> bytes:
        """Wrapper to force streaming with a progress bar."""
        return self.download(
            url,
            tag=tag,
            model_name=model_name,
            cache_ttl=cache_ttl,
            force=force,
            force_progress=True,
        )

    def download_any(
        self, 
        target: str | list[str] | dict[str, str], 
        tag: str, 
        model_name: str, 
        cache_ttl: int | None,
        *,
        force: bool,
    ) -> bytes | dict[str, bytes]:
        """Download a single URL, a list of fallback URLs, or a dictionary of URLs."""
        
        if isinstance(target, str):
            return self.download(target, tag=tag, model_name=model_name, cache_ttl=cache_ttl, force=force)
            
        if isinstance(target, list):
            last_exc = None
            for url in target:
                try:
                    return self.download(url, tag=tag, model_name=model_name, cache_ttl=cache_ttl, force=force)
                except Exception as e:
                    last_exc = e
                    log.warning("Failed to download %s, trying fallback URL...", url)
            raise DownloadError(f"Failed to download {model_name} from any fallback URL.") from last_exc
            
        return {
            k: self.download(v, tag=tag, model_name=model_name, cache_ttl=cache_ttl, force=force)
            for k, v in target.items()
        }

    def check_connection(self, url: str) -> bool:
        """Returns True if the URL exists and returns a 2xx status code."""
        if self._client is None:
            return False
        try:
            resp = self._client.head(url, timeout=5.0)
            return resp.is_success
        except httpx.HTTPError as e:
            log.debug("Connection check failed for %s: %s", url, e)
            return False

    @staticmethod
    def _format_speed(bytes_per_sec: float) -> str:
        """Convert raw bytes/sec into a human-readable string."""
        if bytes_per_sec >= 1024 * 1024:
            return f"{bytes_per_sec / (1024 * 1024):.1f} MB/s"
        if bytes_per_sec >= 1024:
            return f"{bytes_per_sec / 1024:.1f} KB/s"
        return f"{bytes_per_sec:.0f} B/s"

    def _read_with_progress(
        self,
        response: httpx.Response,
        model_name: str = "Model",
        *,
        force: bool = False,
    ) -> bytes:
        """Read response bytes, displaying a progress bar if in an interactive TTY."""
        total = int(response.headers.get("Content-Length", 0))

        is_tty = sys.stderr.isatty()
        if not is_tty or (0 < total < 100_000 and not force):
            return response.read()

        buffer = BytesIO()
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
                bar = ("#" * int(percent // 5)).ljust(20, ".")

                if elapsed > 0.3:
                    speed_bps = downloaded / elapsed
                    speed_str = self._format_speed(speed_bps)
                else:
                    speed_str = "---"

                sys.stderr.write(
                    f"\r{label}: [{bar}] {percent:3.0f}% ({speed_str}) "
                )
                sys.stderr.flush()

        sys.stderr.write("\n")
        return buffer.getvalue()

    def _get_cached_data(self, url: str, tag: str = "Data") -> bytes | None:
        """Returns cached data if valid, otherwise refreshes TTL or returns None."""
        cache_key = self._generate_cache_key(url)
        cached_data, cached_meta = self.cache.get(cache_key)

        if not cached_data or not cached_meta:
            return None

        expires_at = cached_meta.get("expires_at", 0)
        if time.time() < expires_at:
            return cached_data

        remote_meta = self._get_metadata(url)

        if remote_meta.get("etag") and remote_meta.get(
            "etag"
        ) == cached_meta.get("etag"):
            self._refresh_ttl(cache_key, cached_data, cached_meta, tag=tag)
            return cached_data

        if remote_meta.get("last_modified") and remote_meta.get(
            "last_modified"
        ) == cached_meta.get("last_modified"):
            self._refresh_ttl(cache_key, cached_data, cached_meta, tag=tag)
            return cached_data

        return None

    def _get_metadata(self, url: str) -> dict:
        """Send a HEAD request to fetch ETag/Last-Modified headers."""
        if self._client is None:
            return {}
        try:
            resp = self._client.head(url, timeout=5.0)
            resp.raise_for_status()
            return {
                "etag": resp.headers.get("ETag"),
                "last_modified": resp.headers.get("Last-Modified"),
            }
        except httpx.HTTPError:
            return {}

    def _refresh_ttl(
        self, key: str, data: bytes, meta: dict, tag: str = "Data"
    ) -> None:
        """Update cache TTL without re-downloading."""
        meta["expires_at"] = time.time() + self.default_cache_ttl
        self.cache.set(key, data, tag=tag, metadata=meta)
