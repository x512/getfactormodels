#!/usr/bin/env python3
import logging
from pathlib import Path
from typing import Optional
from diskcache import Cache

logging.basicConfig(level=logging.ERROR)
log = logging.getLogger(__name__)

class _Cache:
    def __init__(self, cache_dir='~/.getfactormodels_cache',
                 default_timeout=86400):  # TODO: XDG cache

        cache_path = Path(cache_dir).expanduser()

        self.cache = Cache(str(cache_path))
        self.default_timeout = default_timeout

        log.debug(f"Cache initialized at: {cache_path}")

    @property
    def directory(self) -> str:
        return self.cache.directory

    def get(self, key: str) -> Optional[bytes]:
        """Retrieves data from cache if valid, otherwise returns None."""
        data = self.cache.get(key)
        if data is not None:
            log.debug(f"Cache hit, key: {key}")
        else:
            log.debug(f"Cache miss, key: {key}")
        return data

    def set(self, key: str, data: bytes, expire_secs: Optional[int] = None):
        """Save data to the cache expiration time."""
        timeout = expire_secs if expire_secs is not None else self.default_timeout
        try:
            self.cache.set(key, data, expire=timeout)
            log.debug(f"Data saved to cache for key: {key}, expires in {timeout}s")
        except Exception as e:
            log.error(f"Failed to write to cache for key {key}: {e}")

    def close(self):
        log.debug("Closing cache.")
        self.cache.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
