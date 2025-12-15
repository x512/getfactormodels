import os
import shutil
from unittest.mock import MagicMock, patch
import httpx
import pytest
from getfactormodels.utils.http_client import ClientNotOpenError, HttpClient


@pytest.fixture
def temp_cache_dir(tmp_path):
    cache_dir = tmp_path / "test_cache"
    cache_dir.mkdir()
    return str(cache_dir)


@pytest.fixture
def http_client(temp_cache_dir):
    client = HttpClient(cache_dir=temp_cache_dir)
    yield client
    # Ensure client is closed
    try:
        client.close()
    except Exception:
        pass

@pytest.fixture
def open_http_client(http_client):
    http_client.__enter__()
    yield http_client
    http_client.__exit__(None, None, None)    

def test_cache_dir_default():
    with patch(f'getfactormodels.utils.http_client.user_cache_path') as mock_path:
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

def test_context_manager_enter_exit(http_client):
    #test http client properly enters/exists
    assert http_client._client is None
    
    with http_client as client:
        assert client._client is not None
        assert isinstance(client._client, httpx.Client)
        assert client is http_client
    
    assert http_client._client is None # sets to None on close now

def test_http_error_when_used_outside_context(http_client):
    # Before enter
    with pytest.raises(ClientNotOpenError):
        http_client.check_connection("http://example.com")
    
    with pytest.raises(ClientNotOpenError):
        http_client.download("http://example.com")
    
    # After exit it exists but is closed
    with http_client:
        pass

    http_client.close()
    
    # httpx should error because client is closed, when trying to use it:
    with pytest.raises(ClientNotOpenError, match="not open"):
        http_client.check_connection("http://example.com")


def test_creating_cache_dir_when_doesnt_exist(temp_cache_dir):
    # remove it first
    shutil.rmtree(temp_cache_dir, ignore_errors=True)
    
    # Create client - should create the directory when entering context
    client = HttpClient(cache_dir=temp_cache_dir)
    with client:
        assert os.path.exists(temp_cache_dir)
        assert os.path.isdir(temp_cache_dir)
    client.close()  #fix (leaving db open, tox)

def test_download_with_http_client(open_http_client):
    client = open_http_client
    
    # Mock response
    mock_response = MagicMock()
    mock_response.content = b"test data"
    mock_response.status_code = 200
    
    with patch.object(client._client, 'get') as mock_get, \
         patch.object(client.cache, 'get') as mock_cache_get, \
         patch.object(client.cache, 'set') as mock_cache_set:
        
        # no cache hit
        mock_cache_get.return_value = None
        mock_get.return_value = mock_response
        
        result = client.download("https://example.com")
        
        assert result == b"test data"
        mock_get.assert_called_once_with("https://example.com")
        mock_cache_set.assert_called_once()
    client.close()


def test_download_returns_cached_data(open_http_client):
    client = open_http_client
    
    with patch.object(client.cache, 'get') as mock_cache_get:
        # Cache hit
        mock_cache_get.return_value = b"cached data"
        
        result = client.download("https://example.com")
        
        assert result == b"cached data"
    client.close()
# clear cache
# dl error
# check connection 
# head, get 


