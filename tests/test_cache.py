# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import pytest
from pathlib import Path
from getfactormodels.utils.cache import (
    _Cache,
    default_cache_dir,
    evict_model_cache,
    clear_cache,
)
from unittest.mock import patch


def test_cache_init_and_repr(tmp_path):
    with _Cache(tmp_path) as cache:
        assert cache.directory == str(tmp_path.resolve())
        assert "path=" in repr(cache)
        assert len(cache) == 0


def test_cache_set_and_get(tmp_path):
    with _Cache(tmp_path) as cache:
        key = "test_key_12345678"
        data = b"hello world"
        meta = {"source": "test"}

        # Cache miss initially
        d, m = cache.get(key)
        assert d is None
        assert m is None
        assert key not in cache

        # Set entry
        cache.set(key, data, tag="ff3", metadata=meta)
        assert key in cache
        assert len(cache) == 1

        # Cache hit
        d, m = cache.get(key)
        assert d == data
        assert m == meta


def test_cache_evict_model(tmp_path):
    with _Cache(tmp_path) as cache:
        cache.set("k1", b"d1", tag="ff3")
        cache.set("k2", b"d2", tag="ff4")
        cache.set("k3", b"d3", tag="q")

        assert len(cache) == 3

        # Evict single string tag
        cache.evict_model("ff3")
        assert "k1" not in cache
        assert len(cache) == 2

        # Evict list/set tags
        cache.evict_model(["ff4", "q"])
        assert len(cache) == 0


def test_cache_remove_and_clear(tmp_path):
    with _Cache(tmp_path) as cache:
        cache.set("k1", b"d1", tag="ff3")
        cache.set("k2", b"d2", tag="ff3")

        # Remove specific key (success and missing cases)
        assert cache.remove_key("k1") is True
        assert cache.remove_key("nonexistent") is False
        assert len(cache) == 1

        # Clear all
        count = cache.clear_all()
        assert count == 1
        assert len(cache) == 0


def test_cache_set_exception_handling(tmp_path):
    """Test that cache write failures are caught and logged without crashing."""
    with _Cache(tmp_path) as cache:
        # Mock the underlying diskcache Cache.set to raise an error
        with patch.object(cache.cache, "set", side_effect=Exception("Disk full")):
            # Should catch the exception, log it via log.error, and not raise
            cache.set("bad_key_12345", b"data", tag="ff3")

def test_default_cache_dir():
    """Test that the path resolver returns a valid Path."""
    path = default_cache_dir()
    assert isinstance(path, Path)
    assert "getfactormodels" in str(path)


def test_empty_cache_logs(tmp_path):
    """Test the 'else' branches in evict_model and clear_all when cache is empty."""
    with _Cache(tmp_path) as cache:
        # Evicting a tag that doesnt exist, log.debug
        assert cache.evict_model("nonexistent") == 0
        
        assert cache.clear_all() == 0


def test_global_helpers(tmp_path, monkeypatch):
    """Test the module-level evict and clear functions safely."""
    monkeypatch.setattr(
        "getfactormodels.utils.cache.default_cache_dir", 
        lambda: tmp_path
    )

    # populate mock cache
    with _Cache(tmp_path) as cache:
        cache.set("key1", b"data1", tag="ff3")
        cache.set("key2", b"data2", tag="ff4")

    # Test evict_model_cache (one key)
    assert evict_model_cache("ff3") == 1
    with _Cache(tmp_path) as cache:
        assert "key1" not in cache
        assert "key2" in cache

    # Test clear_cache with a key
    assert clear_cache("ff4") == 1
    with _Cache(tmp_path) as cache:
        assert len(cache) == 0

    # clear_cache with NO argument (clear all)
    with _Cache(tmp_path) as cache:
        cache.set("key3", b"data3", tag="ff3")
        cache.set("key4", b"data4", tag="ff4")
    
    # key=None should trigger cache.clear_all()
    assert clear_cache() == 2
