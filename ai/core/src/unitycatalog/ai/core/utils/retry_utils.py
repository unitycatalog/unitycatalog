import functools
import logging
import time

from unitycatalog.ai.core.envs.databricks_env_vars import UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS

_logger = logging.getLogger(__name__)

SESSION_EXPIRED_MESSAGE = "session_id is no longer usable"
SESSION_CHANGED_MESSAGE = "existing Spark server driver"
SESSION_HANDLE_INVALID_MESSAGE = "INVALID_HANDLE"
SESSION_RETRY_BASE_DELAY = 1
SESSION_RETRY_MAX_DELAY = 4

_SESSION_EXPIRATION_PATTERNS = [
    SESSION_CHANGED_MESSAGE,
    SESSION_EXPIRED_MESSAGE,
    SESSION_HANDLE_INVALID_MESSAGE,
]


def _is_session_expired(error_message: str) -> bool:
    """
    Check if the error message indicates that the session is expired.

    Internal utility function used by the retry decorator.

    Args:
        error_message (str): The error message to check.

    Returns:
        bool: True if the session is expired, False otherwise.
    """
    if not error_message:
        return False
    return any(pattern in error_message for pattern in _SESSION_EXPIRATION_PATTERNS)


class SessionExpirationException(Exception):
    """Exception raised when a session expiration error is detected."""

    pass


def retry_on_session_expiration(func):
    """
    Decorator to retry a method upon session expiration errors with exponential backoff.
    """
    max_attempts = UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS.get()

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        for attempt in range(1, max_attempts + 1):
            try:
                result = func(self, *args, **kwargs)
                if hasattr(result, "error") and result.error and _is_session_expired(result.error):
                    raise SessionExpirationException(result.error)
                _logger.info("Successfully re-acquired connection to a serverless instance.")
                return result
            except SessionExpirationException as e:
                if not hasattr(self, "_is_default_client") or not self._is_default_client:
                    refresh_message = (
                        f"Failed to execute {func.__name__} due to session expiration. "
                        "Unable to automatically refresh session when using a custom client. "
                        "Recreate the DatabricksFunctionClient with a new client to recreate "
                        "the custom client session."
                    )
                    raise RuntimeError(refresh_message) from e

                if attempt < max_attempts:
                    delay = min(
                        SESSION_RETRY_BASE_DELAY * (2 ** (attempt - 1)), SESSION_RETRY_MAX_DELAY
                    )
                    _logger.warning(
                        f"Session expired. Retrying attempt {attempt} of {max_attempts}. "
                        f"Refreshing session and retrying after {delay} seconds..."
                    )
                    if hasattr(self, "refresh_client_and_session"):
                        self.refresh_client_and_session()
                    time.sleep(delay)
                    continue
                else:
                    refresh_failure_message = (
                        f"Failed to execute {func.__name__} after {max_attempts} attempts due to session expiration. "
                        "Generate a new session_id by detaching and reattaching your compute."
                    )
                    raise RuntimeError(refresh_failure_message) from e

    return wrapper
