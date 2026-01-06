#!/usr/bin/env python3
import logging
from pathlib import Path
#from typing import Optional
from diskcache import Cache

logging.basicConfig(level=logging.ERROR)
log = logging.getLogger(__name__)

class _Cache:
    def __init__(self, cache_dir: str, default_timeout=86400):

        cache_path = Path(cache_dir) 
        
        self.cache = Cache(str(cache_path))

        self.default_timeout = default_timeout

        msg = f"Cache initialized: {cache_path}"
        log.debug(msg)

    @property
    def directory(self) -> str:
        return self.cache.directory

    def get(self, key: str) -> (bytes | None): #type err as bytes|None TODO FXME
        """Retrieves data from cache if valid, otherwise returns None."""
        data = self.cache.get(key)
        msg = f"key: {key}"
        log.debug(msg)
        if data is not None:
            log.debug("hit")
        else:
            log.debug("miss")
        return data

    def set(self, key: str, data: bytes, expire_secs: int | None = None):
        """Save data to the cache expiration time."""
        timeout = expire_secs if expire_secs is not None else self.default_timeout
        try:
            self.cache.set(key, data, expire=timeout)
            log.debug(f"data written to cache ({key[:8]}...): expiry: {timeout}s")
        except Exception as e:
            msg = f"Failed to write to cache: {key}:\n{e}"
            log.error(msg)

    # new: TODO: impl clear cache, optionally by model
    def remove(self, key: str):
        """Removes a specific key from the cache."""
        if self.cache.delete(key):
            log.info(f"Cache entry removed: {key}")
        else:
            log.debug(f"Cache entry not found (no action): {key}")

    def close(self):
        self.cache.close()
        msg = f'closed {self.cache.__class__.__name__}'
        log.debug(msg)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def clear(self):
        """Removes all data entries from the cache directory."""
        self.cache.clear()
        
        msg = f"cache cleared: (dir: {self.directory})"
        log.info(msg)
