# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
from __future__ import annotations  #dont want, fixme
import logging
from pathlib import Path
from diskcache import Cache


log = logging.getLogger(__name__)


class _Cache:
    """diskcache wrapper for data storage and tag-based eviction."""

    def __init__(self, cache_dir: str | Path, default_timeout: int = 14400):
        self.cache_path = Path(cache_dir).resolve()
        self.cache = Cache(str(self.cache_path))
        self.default_timeout = default_timeout

        log.debug(f"CACHE INIT: {self.cache_path}")

    @property
    def directory(self) -> str:
        return self.cache.directory


    def get(self, key: str) -> tuple[bytes | None, dict | None]:
        """Retrieves data bytes and metadata from cache if key exists and is unexpired."""
        entry = self.cache.get(key)
        if entry and isinstance(entry, dict):
            log.debug(f"CACHE HIT: {key[:8]}...")
            return entry.get("data"), entry.get("metadata")

        log.debug(f"CACHE MISS: {key[:8]}...")
        return None, None


    def set(
        self,
        key: str,
        data: bytes,
        tag: str,  # Required: binds entry to a model tag for eviction
        metadata: dict | None = None,
        expire_secs: int | None = None,
    ) -> None:
        """Saves data bytes, metadata, and binds entry to a required eviction tag."""
        timeout = self.default_timeout if expire_secs is None else expire_secs
        entry = {"data": data, "metadata": metadata}

        try:
            self.cache.set(key, entry, expire=timeout, tag=tag)
            log.debug(f"CACHE WRITE: {key[:8]}... (tag: {tag}, expiry: {timeout}s)")
        except Exception as e:
            log.error(f"Failed to write cache entry '{key[:8]}': {e}")


    def evict_model(self, tag: str | list[str] | set[str]) -> None:
        """Removes all cache entries associated with one or more model tags."""
        tags = [tag] if isinstance(tag, str) else tag

        for t in tags:
            self.cache.evict(tag=t)
            log.info(f"Evicted cache entries for model '{t}'.")


    def remove_key(self, key: str) -> bool:
        """Removes a specific storage key entry from the cache."""
        deleted = self.cache.delete(key)
        if deleted:
            log.info(f"Cache entry removed: {key[:8]}...")
        else:
            log.debug(f"Cache entry not found for removal: {key[:8]}...")
        return bool(deleted)


    def clear_all(self) -> int:
        """Destructive: Wipes all cache entries across all models in this directory."""
        count = len(self.cache)
        self.cache.clear()
        log.info(f"PURGED ALL CACHE ENTRIES: {count} item(s)")
        return count


    def close(self) -> None:
        self.cache.close()
        log.debug(f"CLOSED: {self.cache.__class__.__name__}")    
   

    def __contains__(self, key: str) -> bool:
        """Allows 'if key in cache' checks without deserializing payloads."""
        return key in self.cache
    
    def __len__(self) -> int:
        """Returns total count of stored items in the cache engine."""
        return len(self.cache)

    def __repr__(self) -> str:
        return f"<_Cache path='{self.cache_path}' items={len(self)}>"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

