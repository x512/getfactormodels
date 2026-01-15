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
import os
import shutil
import time
from unittest.mock import MagicMock, patch
import httpx
import pytest
from getfactormodels.utils.http_client import ClientNotOpenError
from getfactormodels.utils.http_client import \
    _HttpClient as HttpClient  # RENAMED


@pytest.fixture
def temp_cache_dir(tmp_path):
    cache_dir = tmp_path / "test_cache"
    cache_dir.mkdir()
    return str(cache_dir)

@pytest.fixture
def http_client_factory(temp_cache_dir):
    """Returns an unopened instance for testing the context manager itself."""
    return HttpClient(cache_dir=temp_cache_dir)

@pytest.fixture
def open_http_client(http_client_factory):
    """Return an already open instance for download/stream tests."""
    with http_client_factory as client:
        yield client


def test_cache_dir_default():
    with patch('getfactormodels.utils.http_client.user_cache_path') as mock_path:
        mock_path.return_value.resolve.return_value = "~/some/dir"
        
        client = HttpClient()
        assert client.cache_dir == "~/some/dir"
    client.close()


def test_custom_cache_dir():
    client = HttpClient(cache_dir="~/custom/path")
    assert client.cache_dir == "~/custom/path"
    client.close()


def test_default_timeout_and_cache_ttl():
    client = HttpClient()
    assert client.timeout == 15.0
    assert client.default_cache_ttl == 86400
    client.close()


def test_custom_cache_ttl_param():
    client = HttpClient(default_cache_ttl=50)
    assert client.default_cache_ttl == 50
    client.close()


def test_custom_http_timeout_param():
    client = HttpClient(timeout=8.5)
    assert client.timeout == 8.5
    client.close()


# test factory here, test the 'with' block behavior
def test_context_manager_enter_exit(http_client_factory):
    client = http_client_factory
    assert client._client is None 
    
    with client:
        assert client._client is not None
        assert isinstance(client._client, httpx.Client)
    assert client._client is None


# test unopened here, because it better error out!
def test_http_error_when_used_outside_context(http_client_factory):
    client = http_client_factory
    
    # error before enter
    with pytest.raises(ClientNotOpenError):
        client.check_connection("http://example.com")

    with pytest.raises(ClientNotOpenError):
        client.download("http://example.com")

    with client:
        with patch.object(client._client, 'head', return_value=MagicMock(status_code=200)):
            assert client.check_connection("http://example.com") is True
    
    # verify raises error after exiting context
    with pytest.raises(ClientNotOpenError):
        client.check_connection("http://example.com")


def test_creating_cache_dir_when_doesnt_exist(temp_cache_dir):
    # remove it first
    shutil.rmtree(temp_cache_dir, ignore_errors=True)
    
    # Create client - should create the directory when entering context
    client = HttpClient(cache_dir=temp_cache_dir)
    with client:
        assert os.path.exists(temp_cache_dir)
        assert os.path.isdir(temp_cache_dir)
    client.close()  #fix (leaving db open, tox)


def test_download_with_http_client(temp_cache_dir):
    # Uses a fresh client for this specific test
    client = HttpClient(cache_dir=temp_cache_dir)
    
    mock_response = MagicMock()
    mock_response.content = b"test data"
    mock_response.status_code = 200
    mock_response.headers = {"ETag": "123"}
    
    with client, patch.object(client.cache, 'get') as mock_cache_get, \
             patch.object(client._client, 'get') as mock_get:
        
        mock_cache_get.return_value = (None, None) 
        mock_get.return_value = mock_response
        
        result = client.download("https://example.com")
        assert result == b"test data"


def test_download_returns_cached_data(open_http_client):
    client = open_http_client
    with patch.object(client.cache, 'get') as mock_cache_get:
        future_time = time.time() + 3600
        mock_cache_get.return_value = (b"cached data", {"expires_at": future_time})
        
        result = client.download("https://example.com")
        
        assert result == b"cached data"


def test_get_metadata_exception_safety(open_http_client):
    """Test _get_metadata returns empty dict on exception (don't crash)."""
    client = open_http_client
    
    with patch.object(client._client, 'head', side_effect=httpx.RequestError("big-error")):
        meta = client._get_metadata("https://example.com")
        assert meta == {} # Should not raise exception!


def test_check_connection_fallback_to_get(open_http_client):
    """Test that check_connection tries GET if HEAD fails."""
    client = open_http_client
    url = "https://example.com/api"

    # HEAD fail (405), GET succeed (200)
    mock_head = MagicMock()
    mock_head.is_success = False
    mock_head.status_code = 405

    mock_get = MagicMock()
    mock_get.is_success = True
    mock_get.status_code = 200

    with patch.object(client._client, 'head', return_value=mock_head) as m_head, \
         patch.object(client._client, 'get', return_value=mock_get) as m_get:
        
        result = client.check_connection(url)
        
        assert result is True
        m_head.assert_called_once_with(url, timeout=4.0)
        m_get.assert_called_once_with(url, timeout=4.0)


def test_stream_downloads_and_caches(open_http_client):
    client = open_http_client
    url = "https://example.com/large-file"
    
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"Content-Length": "10"}
    # mock_resp.iter_bytes is called by _progress_bar
    mock_resp.iter_bytes.return_value = [b"data-chunk"]
    
    # enter context manager mock
    client._client.stream = MagicMock()
    client._client.stream.return_value.__enter__.return_value = mock_resp

    with patch.object(client, '_check_for_update', return_value=("key", None, True)), \
         patch.object(client.cache, 'set') as mock_cache_set:
        
        result = client.stream(url, cache_ttl=3600, model_name="AQR")
        
        assert result == b"data-chunk"
        mock_cache_set.assert_called_once()
        assert "expires_at" in mock_cache_set.call_args[1]['metadata']


def test_refresh_ttl_updates_expiry(open_http_client):
    client = open_http_client
    key = "test_key"
    data = b"some data"
    meta = {"etag": "abc", "expires_at": 100} # Old expiry
    
    with patch.object(client.cache, 'set') as mock_set:
        client._refresh_ttl(key, data, meta)

        # check if metadata in the call has a new timestamp
        called_args = mock_set.call_args[1]
        assert called_args['metadata']['expires_at'] > time.time()
        assert called_args['metadata']['etag'] == "abc"

