import functools
import logging
import random
import time
from datetime import datetime, timedelta
from threading import Lock

logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self):
        self._lock = Lock()
        self._last_calls = {}
        self._model_limits = {}

    def wait_if_needed(self, model_id, rpm_limit=None):
        """Wait if we're exceeding the rate limit for the specified model."""
        if not model_id:
            return

        with self._lock:
            rate_limit = rpm_limit or self._model_limits.get(model_id, 1)
            window_seconds = 60
            now = datetime.now()
            window_ago = now - timedelta(seconds=window_seconds)

            recent_calls = self._last_calls.get(model_id, [])
            recent_calls = [t for t in recent_calls if t > window_ago]

            if len(recent_calls) >= rate_limit:
                next_slot_time = recent_calls[0] + timedelta(seconds=window_seconds)
                wait_seconds = (next_slot_time - now).total_seconds()

                if wait_seconds > 0:
                    logger.info(f"Rate limiting for {model_id}: waiting {wait_seconds:.1f}s")
                    time.sleep(wait_seconds + 0.1)

                    now = datetime.now()

            recent_calls.append(now)
            self._last_calls[model_id] = recent_calls
            logger.debug(f"Rate limit status: {len(recent_calls)}/{rate_limit} calls in window")

    def reduce_limit(self, model_id, factor=0.5):
        """Reduce the rate limit for a model after throttling."""
        if not model_id or model_id not in self._model_limits:
            return

        with self._lock:
            old_limit = self._model_limits.get(model_id, 0)
            if old_limit > 0:
                new_limit = max(1, int(old_limit * factor))
                self._model_limits[model_id] = new_limit
                logger.debug(f"Reducing rate limit for {model_id} from {old_limit} to {new_limit}")


_rate_limiter = RateLimiter()


def retry(
    max_retries=5,
    base_delay=1,
    backoff_factor=2,
    retryable_errors=None,
    model_id=None,
    rpm_limit=None,
):
    """
    Decorator for retrying a function with exponential backoff and rate limiting.

    Args:
        max_retries: Maximum retry attempts
        base_delay: Initial retry delay in seconds
        backoff_factor: Multiplier for delay between retries
        retryable_errors: List of error strings that should trigger retries
        model_id: Optional model ID for rate limiting
        rpm_limit: Optional requests per minute limit

    Returns:
        Decorated function with retry logic
    """
    if retryable_errors is None:
        retryable_errors = ["timeout", "connection", "throttling", "rate exceeded", "server error"]

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if model_id:
                _rate_limiter.wait_if_needed(model_id, rpm_limit)

            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)

                except Exception as e:
                    error_message = str(e).lower()
                    is_retryable = any(err in error_message for err in retryable_errors)

                    if not is_retryable:
                        logger.error(f"Non-retryable error: {e}")
                        raise

                    if "throttling" in error_message or "rate" in error_message:
                        _rate_limiter.reduce_limit(model_id)

                    if attempt >= max_retries:
                        logger.error(f"Max retries ({max_retries}) reached. Last error: {e}")
                        raise

                    delay = base_delay * (backoff_factor ** (attempt - 1))
                    jitter = random.uniform(0.05, 0.15) * delay
                    sleep_time = delay + jitter

                    logger.warning(
                        f"Retryable error: '{error_message}'. "
                        f"Retry {attempt}/{max_retries} in {sleep_time:.1f}s"
                    )
                    time.sleep(sleep_time)

        return wrapper

    return decorator
