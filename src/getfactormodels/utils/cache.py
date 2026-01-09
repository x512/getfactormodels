#!/usr/bin/env python3
import logging
import time
from pathlib import Path
from diskcache import Cache

logging.basicConfig(level=logging.ERROR)
log = logging.getLogger(__name__)
# TODO: remove/clean up debug messages

class _Cache:
    def __init__(self, cache_dir: str, default_timeout=14400): #4 hour default now with head check.
        cache_path = Path(cache_dir)                           #note: some models going to ttl 1 day,
        self.cache = Cache(str(cache_path))                    #     others until eom. TODO.
        self.default_timeout = default_timeout

        msg = f"CACHE INIT: {cache_path}"
        log.debug(msg)

    @property
    def directory(self) -> str:
        return self.cache.directory

    def get(self, key: str) -> tuple[bytes | None, dict | None]:
        """Retrieves data from cache if valid, otherwise returns None."""
        entry = self.cache.get(key)
        if entry:
            log.debug(f"CACHE HIT: {key[:8]}...")
            if isinstance(entry, dict) and "data" in entry:
                metadata = entry.get("metadata")
                log.debug(f"CACHE DATA: ETag: {metadata.get('etag') if metadata else None}")
                return entry["data"], metadata

        log.debug(f"CACHE MISS: {key[:8]}...")
        return None, None


    def set(self, key: str, data: bytes, metadata: dict | None = None, expire_secs: int | None = None):
        """Saves data (bytes) and etag/last_modified metadata as a single entry."""
        timeout = expire_secs or self.default_timeout
        entry = {
            "data": data,
            "metadata": metadata,
            "expires_at": time.time() + self.default_timeout,
        }
        try:
            self.cache.set(key, entry, expire=timeout)
            log.debug(f"CACHE WRITE: {key[:8]}... (expiry: {timeout}s)")
        except Exception as e:
            log.error(f"Failed to write cache entry: {e}")


    # new: TODO: impl clear cache, optionally by model
    def remove(self, key: str):
        """Removes a specific key from the cache."""
        if self.cache.delete(key):
            log.info(f"Cache entry removed: {key}")
        else:
            log.debug(f"Cache entry not found: {key}")


    def close(self):
        self.cache.close()
        msg = f'CLOSED: {self.cache.__class__.__name__}'
        log.debug(msg)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def clear(self):
        """Removes all data entries from the cache directory."""
        self.cache.clear()

        msg = f"CLEARED: (dir: {self.directory})"
        log.info(msg)
