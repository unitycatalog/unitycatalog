import time
from unittest.mock import MagicMock, patch

import pytest

from unitycatalog.ai.bedrock.ratelimiter import _rate_limiter, retry


def test_rate_limiter_wait_if_needed():
    """Test the rate limiter's wait_if_needed method."""
    model_id = "test_model"
    rpm_limit = 2  # Allow 2 requests per minute

    # Simulate two calls within the rate limit
    _rate_limiter.wait_if_needed(model_id, rpm_limit)
    _rate_limiter.wait_if_needed(model_id, rpm_limit)

    # Simulate a third call exceeding the rate limit
    start_time = time.time()
    _rate_limiter.wait_if_needed(model_id, rpm_limit)
    elapsed_time = time.time() - start_time

    # Ensure the rate limiter enforced a wait of approximately 30 seconds
    assert elapsed_time >= 30


def test_rate_limiter_reduce_limit():
    """Test the rate limiter's reduce_limit method."""
    model_id = "test_model"
    initial_limit = 10
    _rate_limiter._model_limits[model_id] = initial_limit

    # Reduce the limit by 50%
    _rate_limiter.reduce_limit(model_id, factor=0.5)
    assert _rate_limiter._model_limits[model_id] == 5

    # Ensure the limit does not go below 1
    _rate_limiter.reduce_limit(model_id, factor=0.1)
    assert _rate_limiter._model_limits[model_id] == 1


@patch("time.sleep", return_value=None)
def test_retry_decorator_success(mock_sleep):
    """Test the retry decorator for a successful function call."""
    mock_function = MagicMock(return_value="success")

    @retry(max_retries=3, base_delay=1, backoff_factor=2)
    def test_function():
        return mock_function()

    result = test_function()
    assert result == "success"
    mock_function.assert_called_once()
    mock_sleep.assert_not_called()


@patch("time.sleep", return_value=None)
def test_retry_decorator_with_retries(mock_sleep):
    """Test the retry decorator with retries for retryable errors."""
    mock_function = MagicMock(
        side_effect=[Exception("timeout"), Exception("connection"), "success"]
    )

    @retry(max_retries=3, base_delay=1, backoff_factor=2)
    def test_function():
        return mock_function()

    result = test_function()
    assert result == "success"
    assert mock_function.call_count == 3
    assert mock_sleep.call_count == 2  # Retries occurred


@patch("time.sleep", return_value=None)
def test_retry_decorator_exceeds_retries(mock_sleep):
    """Test the retry decorator when retries are exhausted."""
    mock_function = MagicMock(side_effect=Exception("timeout"))

    @retry(max_retries=3, base_delay=1, backoff_factor=2)
    def test_function():
        return mock_function()

    with pytest.raises(Exception, match="timeout"):
        test_function()

    # The function should be called 3 times (initial call + 2 retries)
    assert mock_function.call_count == 3

    # `time.sleep` should be called 2 times (only for the first 2 retries)
    assert mock_sleep.call_count == 2


@patch("time.sleep", return_value=None)
def test_retry_decorator_non_retryable_error(mock_sleep):
    """Test the retry decorator with a non-retryable error."""
    mock_function = MagicMock(side_effect=Exception("non-retryable error"))

    @retry(max_retries=3, base_delay=1, backoff_factor=2)
    def test_function():
        return mock_function()

    with pytest.raises(Exception, match="non-retryable error"):
        test_function()

    mock_function.assert_called_once()
    mock_sleep.assert_not_called()


@patch("time.sleep", return_value=None)
def test_retry_decorator_with_throttling(mock_sleep):
    """Test the retry decorator with throttling errors."""
    model_id = "test_model"
    rpm_limit = 2
    _rate_limiter._model_limits[model_id] = rpm_limit

    mock_function = MagicMock(side_effect=[Exception("throttling"), "success"])

    @retry(max_retries=3, base_delay=1, backoff_factor=2, model_id=model_id, rpm_limit=rpm_limit)
    def test_function():
        return mock_function()

    result = test_function()
    assert result == "success"
    assert mock_function.call_count == 2
    assert mock_sleep.call_count == 1  # Retry occurred
