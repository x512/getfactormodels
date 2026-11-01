# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
from __future__ import annotations  # safe to delete — see note below

import logging
from pathlib import Path
from diskcache import Cache
from platformdirs import user_cache_path

log = logging.getLogger(__name__)


def default_cache_dir() -> Path:
    """Directory used for the package's default on-disk cache."""
    return user_cache_path(
        appname="getfactormodels",
        appauthor="x512",
        ensure_exists=True,
    )


class _Cache:
    """diskcache wrapper for data storage and tag-based eviction."""

    def __init__(self, cache_dir: str | Path, default_timeout: int = 14400):
        self.cache_path = Path(cache_dir).resolve()
        self.cache = Cache(str(self.cache_path))
        self.default_timeout = default_timeout
        log.debug("CACHE INIT: %s", self.cache_path)

    @property
    def directory(self) -> str:
        return self.cache.directory

    def get(self, key: str) -> tuple[bytes | None, dict | None]:
        entry = self.cache.get(key)
        if entry and isinstance(entry, dict):
            log.debug("CACHE HIT: %s...", key[:8])
            return entry.get("data"), entry.get("metadata")
        log.debug("CACHE MISS: %s...", key[:8])
        return None, None

    def set(
        self,
        key: str,
        data: bytes,
        tag: str,
        metadata: dict | None = None,
        expire_secs: int | None = None,
    ) -> None:
        timeout = self.default_timeout if expire_secs is None else expire_secs
        entry = {"data": data, "metadata": metadata}
        try:
            self.cache.set(key, entry, expire=timeout, tag=tag)
            log.debug("CACHE WRITE: %s... (tag: %s, expiry: %ss)", key[:8], tag, timeout)
        except Exception as e:
            log.error("Failed to write cache entry '%s': %s", key[:8], e)

    def evict_model(self, tag: str | list[str] | set[str]) -> int:
        """Remove all cache entries for one or more model tags.

        Returns:
            Total number of entries evicted across all tags.
        """
        tags = [tag] if isinstance(tag, str) else tag
        total = 0
        for t in tags:
            n = self.cache.evict(tag=t)
            total += n
            log.info("Evicted %s cache entries for model '%s'.", n, t)
        return total

    def remove_key(self, key: str) -> bool:
        deleted = self.cache.delete(key)
        if deleted:
            log.info("Cache entry removed: %s...", key[:8])
        else:
            log.debug("Cache entry not found for removal: %s...", key[:8])
        return bool(deleted)

    def clear_all(self) -> int:
        count = len(self.cache)
        self.cache.clear()
        log.info("PURGED ALL CACHE ENTRIES: %s item(s)", count)
        return count

    def close(self) -> None:
        self.cache.close()
        log.debug("CLOSED: %s", self.cache.__class__.__name__)

    def __contains__(self, key: str) -> bool:
        return key in self.cache

    def __len__(self) -> int:
        return len(self.cache)

    def __repr__(self) -> str:
        return f"<_Cache path='{self.cache_path}' items={len(self)}>"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def evict_model_cache(keys: str | list[str] | set[str]) -> int:
    """Evict cache entries for the given model key(s) from the default cache."""
    with _Cache(default_cache_dir()) as cache:
        return cache.evict_model(keys)

def clear_cache(key: str | list[str] | None = None) -> int:
    """Evict cached entries for specific model key(s), or the entire cache.

    Args:
        key: Model key(s) to clear. If None, wipes the entire disk cache.
    """
    if key is None:
        with _Cache(default_cache_dir()) as cache:
            return cache.clear_all()
    return evict_model_cache(key)

