# Temp folder for unitycatalog python package
# This should be excluded for ucai-core package release

# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from . import types
from ._base_client import DefaultAsyncHttpxClient, DefaultHttpxClient
from ._client import (
    AsyncClient,
    AsyncStream,
    AsyncUnitycatalog,
    Client,
    RequestOptions,
    Stream,
    Timeout,
    Transport,
    Unitycatalog,
)
from ._constants import DEFAULT_CONNECTION_LIMITS, DEFAULT_MAX_RETRIES, DEFAULT_TIMEOUT
from ._exceptions import (
    APIConnectionError,
    APIError,
    APIResponseValidationError,
    APIStatusError,
    APITimeoutError,
    AuthenticationError,
    BadRequestError,
    ConflictError,
    InternalServerError,
    NotFoundError,
    PermissionDeniedError,
    RateLimitError,
    UnitycatalogError,
    UnprocessableEntityError,
)
from ._models import BaseModel
from ._response import APIResponse as APIResponse
from ._response import AsyncAPIResponse as AsyncAPIResponse
from ._types import NOT_GIVEN, NoneType, NotGiven, ProxiesTypes, Transport
from ._utils import file_from_path
from ._utils._logs import setup_logging as _setup_logging
from ._version import __title__, __version__

__all__ = [
    "types",
    "__version__",
    "__title__",
    "NoneType",
    "Transport",
    "ProxiesTypes",
    "NotGiven",
    "NOT_GIVEN",
    "UnitycatalogError",
    "APIError",
    "APIStatusError",
    "APITimeoutError",
    "APIConnectionError",
    "APIResponseValidationError",
    "BadRequestError",
    "AuthenticationError",
    "PermissionDeniedError",
    "NotFoundError",
    "ConflictError",
    "UnprocessableEntityError",
    "RateLimitError",
    "InternalServerError",
    "Timeout",
    "RequestOptions",
    "Client",
    "AsyncClient",
    "Stream",
    "AsyncStream",
    "Unitycatalog",
    "AsyncUnitycatalog",
    "file_from_path",
    "BaseModel",
    "DEFAULT_TIMEOUT",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_CONNECTION_LIMITS",
    "DefaultHttpxClient",
    "DefaultAsyncHttpxClient",
]

_setup_logging()

# Update the __module__ attribute for exported symbols so that
# error messages point to this module instead of the module
# it was originally defined in, e.g.
# unitycatalog._exceptions.NotFoundError -> unitycatalog.NotFoundError
__locals = locals()
for __name in __all__:
    if not __name.startswith("__"):
        try:
            __locals[__name].__module__ = "unitycatalog"
        except (TypeError, AttributeError):
            # Some of our exported symbols are builtins which we can't set attributes for.
            pass
