import os
import shutil
from unittest.mock import MagicMock, patch
import pytest
from getfactormodels.http_client import HttpClient


@pytest.fixture
def test_client():
    """Creates a test HttpClient and cleans up after."""
    client = HttpClient(cache_dir="~/.test_cache")
    yield client  # This gives the client to the test
    client.close()
    if os.path.exists(client.cache.directory):
        shutil.rmtree(client.cache.directory)


def test_client_creates_cache_dir(test_client):
    """Test that HttpClient creates its cache folder."""
    assert os.path.exists(test_client.cache.directory)


def test_client_can_be_used_in_with_statement():
    """Test that we can use 'with HttpClient() as client:' syntax."""
    cache_dir = "~/.test_with_cache"
    client = HttpClient(cache_dir=cache_dir)

    with client:
        # inside, connection should be open
        assert client._client.is_closed is False

    # outside the 'with', connection should be closed
    assert client._client.is_closed is True

    # cleanup
    client.close()
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)


@patch('httpx.Client.head')
def test_check_connection_good_website(mock_head, test_client):
    """Test check_connection returns True for a working website."""
    # fake successful response
    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.is_success = True
    mock_head.return_value = fake_response
    result = test_client.check_connection("https://google.com")

    assert result is True


@patch('httpx.Client.head')
@patch('httpx.Client.get')  # Mock both head() and get() methods
def test_check_connection_bad_website(mock_get, mock_head, test_client):
    """Test check_connection returns False for a broken website."""
    # fake failed responses
    fake_head_response = MagicMock()
    fake_head_response.status_code = 404
    fake_head_response.is_success = False

    fake_get_response = MagicMock()
    fake_get_response.status_code = 500
    fake_get_response.is_success = False

    mock_head.return_value = fake_head_response
    mock_get.return_value = fake_get_response

    result = test_client.check_connection("https://broken-website.com")

    assert result is False


@patch('httpx.Client.head')
def test_check_connection_no_internet(mock_head, test_client):
    """Test check_connection handles network errors gracefully."""
    # mock raise an exception
    mock_head.side_effect = Exception("No internet connection")

    result = test_client.check_connection("https://zzzzzasxasdasdadsasdasd.com")

    # Should return False, not crash
    assert result is False
