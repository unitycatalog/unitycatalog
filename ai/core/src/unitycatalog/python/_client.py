# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

import os
from typing import Any, Mapping, Union

import httpx
from typing_extensions import Self, override

from . import _exceptions, resources
from ._base_client import (
    DEFAULT_MAX_RETRIES,
    AsyncAPIClient,
    SyncAPIClient,
)
from ._exceptions import APIStatusError
from ._qs import Querystring
from ._streaming import AsyncStream as AsyncStream
from ._streaming import Stream as Stream
from ._types import (
    NOT_GIVEN,
    NotGiven,
    Omit,
    ProxiesTypes,
    RequestOptions,
    Timeout,
    Transport,
)
from ._utils import (
    get_async_library,
    is_given,
)
from ._version import __version__

__all__ = [
    "Timeout",
    "Transport",
    "ProxiesTypes",
    "RequestOptions",
    "resources",
    "Unitycatalog",
    "AsyncUnitycatalog",
    "Client",
    "AsyncClient",
]


class Unitycatalog(SyncAPIClient):
    catalogs: resources.CatalogsResource
    schemas: resources.SchemasResource
    tables: resources.TablesResource
    volumes: resources.VolumesResource
    temporary_table_credentials: resources.TemporaryTableCredentialsResource
    temporary_volume_credentials: resources.TemporaryVolumeCredentialsResource
    functions: resources.FunctionsResource
    with_raw_response: UnitycatalogWithRawResponse
    with_streaming_response: UnitycatalogWithStreamedResponse

    # client options

    def __init__(
        self,
        *,
        base_url: str | httpx.URL | None = None,
        timeout: Union[float, Timeout, None, NotGiven] = NOT_GIVEN,
        max_retries: int = DEFAULT_MAX_RETRIES,
        default_headers: Mapping[str, str] | None = None,
        default_query: Mapping[str, object] | None = None,
        # Configure a custom httpx client.
        # We provide a `DefaultHttpxClient` class that you can pass to retain the default values we use for `limits`, `timeout` & `follow_redirects`.
        # See the [httpx documentation](https://www.python-httpx.org/api/#client) for more details.
        http_client: httpx.Client | None = None,
        # Enable or disable schema validation for data returned by the API.
        # When enabled an error APIResponseValidationError is raised
        # if the API responds with invalid data for the expected schema.
        #
        # This parameter may be removed or changed in the future.
        # If you rely on this feature, please open a GitHub issue
        # outlining your use-case to help us decide if it should be
        # part of our public interface in the future.
        _strict_response_validation: bool = False,
    ) -> None:
        """Construct a new synchronous unitycatalog client instance."""
        if base_url is None:
            base_url = os.environ.get("UNITYCATALOG_BASE_URL")
        if base_url is None:
            base_url = f"http://localhost:8080/api/2.1/unity-catalog"

        super().__init__(
            version=__version__,
            base_url=base_url,
            max_retries=max_retries,
            timeout=timeout,
            http_client=http_client,
            custom_headers=default_headers,
            custom_query=default_query,
            _strict_response_validation=_strict_response_validation,
        )

        self.catalogs = resources.CatalogsResource(self)
        self.schemas = resources.SchemasResource(self)
        self.tables = resources.TablesResource(self)
        self.volumes = resources.VolumesResource(self)
        self.temporary_table_credentials = resources.TemporaryTableCredentialsResource(self)
        self.temporary_volume_credentials = resources.TemporaryVolumeCredentialsResource(self)
        self.functions = resources.FunctionsResource(self)
        self.with_raw_response = UnitycatalogWithRawResponse(self)
        self.with_streaming_response = UnitycatalogWithStreamedResponse(self)

    @property
    @override
    def qs(self) -> Querystring:
        return Querystring(array_format="comma")

    @property
    @override
    def default_headers(self) -> dict[str, str | Omit]:
        return {
            **super().default_headers,
            "X-Stainless-Async": "false",
            **self._custom_headers,
        }

    def copy(
        self,
        *,
        base_url: str | httpx.URL | None = None,
        timeout: float | Timeout | None | NotGiven = NOT_GIVEN,
        http_client: httpx.Client | None = None,
        max_retries: int | NotGiven = NOT_GIVEN,
        default_headers: Mapping[str, str] | None = None,
        set_default_headers: Mapping[str, str] | None = None,
        default_query: Mapping[str, object] | None = None,
        set_default_query: Mapping[str, object] | None = None,
        _extra_kwargs: Mapping[str, Any] = {},
    ) -> Self:
        """
        Create a new client instance re-using the same options given to the current client with optional overriding.
        """
        if default_headers is not None and set_default_headers is not None:
            raise ValueError(
                "The `default_headers` and `set_default_headers` arguments are mutually exclusive"
            )

        if default_query is not None and set_default_query is not None:
            raise ValueError(
                "The `default_query` and `set_default_query` arguments are mutually exclusive"
            )

        headers = self._custom_headers
        if default_headers is not None:
            headers = {**headers, **default_headers}
        elif set_default_headers is not None:
            headers = set_default_headers

        params = self._custom_query
        if default_query is not None:
            params = {**params, **default_query}
        elif set_default_query is not None:
            params = set_default_query

        http_client = http_client or self._client
        return self.__class__(
            base_url=base_url or self.base_url,
            timeout=self.timeout if isinstance(timeout, NotGiven) else timeout,
            http_client=http_client,
            max_retries=max_retries if is_given(max_retries) else self.max_retries,
            default_headers=headers,
            default_query=params,
            **_extra_kwargs,
        )

    # Alias for `copy` for nicer inline usage, e.g.
    # client.with_options(timeout=10).foo.create(...)
    with_options = copy

    @override
    def _make_status_error(
        self,
        err_msg: str,
        *,
        body: object,
        response: httpx.Response,
    ) -> APIStatusError:
        if response.status_code == 400:
            return _exceptions.BadRequestError(err_msg, response=response, body=body)

        if response.status_code == 401:
            return _exceptions.AuthenticationError(err_msg, response=response, body=body)

        if response.status_code == 403:
            return _exceptions.PermissionDeniedError(err_msg, response=response, body=body)

        if response.status_code == 404:
            return _exceptions.NotFoundError(err_msg, response=response, body=body)

        if response.status_code == 409:
            return _exceptions.ConflictError(err_msg, response=response, body=body)

        if response.status_code == 422:
            return _exceptions.UnprocessableEntityError(err_msg, response=response, body=body)

        if response.status_code == 429:
            return _exceptions.RateLimitError(err_msg, response=response, body=body)

        if response.status_code >= 500:
            return _exceptions.InternalServerError(err_msg, response=response, body=body)
        return APIStatusError(err_msg, response=response, body=body)


class AsyncUnitycatalog(AsyncAPIClient):
    catalogs: resources.AsyncCatalogsResource
    schemas: resources.AsyncSchemasResource
    tables: resources.AsyncTablesResource
    volumes: resources.AsyncVolumesResource
    temporary_table_credentials: resources.AsyncTemporaryTableCredentialsResource
    temporary_volume_credentials: resources.AsyncTemporaryVolumeCredentialsResource
    functions: resources.AsyncFunctionsResource
    with_raw_response: AsyncUnitycatalogWithRawResponse
    with_streaming_response: AsyncUnitycatalogWithStreamedResponse

    # client options

    def __init__(
        self,
        *,
        base_url: str | httpx.URL | None = None,
        timeout: Union[float, Timeout, None, NotGiven] = NOT_GIVEN,
        max_retries: int = DEFAULT_MAX_RETRIES,
        default_headers: Mapping[str, str] | None = None,
        default_query: Mapping[str, object] | None = None,
        # Configure a custom httpx client.
        # We provide a `DefaultAsyncHttpxClient` class that you can pass to retain the default values we use for `limits`, `timeout` & `follow_redirects`.
        # See the [httpx documentation](https://www.python-httpx.org/api/#asyncclient) for more details.
        http_client: httpx.AsyncClient | None = None,
        # Enable or disable schema validation for data returned by the API.
        # When enabled an error APIResponseValidationError is raised
        # if the API responds with invalid data for the expected schema.
        #
        # This parameter may be removed or changed in the future.
        # If you rely on this feature, please open a GitHub issue
        # outlining your use-case to help us decide if it should be
        # part of our public interface in the future.
        _strict_response_validation: bool = False,
    ) -> None:
        """Construct a new async unitycatalog client instance."""
        if base_url is None:
            base_url = os.environ.get("UNITYCATALOG_BASE_URL")
        if base_url is None:
            base_url = f"http://localhost:8080/api/2.1/unity-catalog"

        super().__init__(
            version=__version__,
            base_url=base_url,
            max_retries=max_retries,
            timeout=timeout,
            http_client=http_client,
            custom_headers=default_headers,
            custom_query=default_query,
            _strict_response_validation=_strict_response_validation,
        )

        self.catalogs = resources.AsyncCatalogsResource(self)
        self.schemas = resources.AsyncSchemasResource(self)
        self.tables = resources.AsyncTablesResource(self)
        self.volumes = resources.AsyncVolumesResource(self)
        self.temporary_table_credentials = resources.AsyncTemporaryTableCredentialsResource(self)
        self.temporary_volume_credentials = resources.AsyncTemporaryVolumeCredentialsResource(self)
        self.functions = resources.AsyncFunctionsResource(self)
        self.with_raw_response = AsyncUnitycatalogWithRawResponse(self)
        self.with_streaming_response = AsyncUnitycatalogWithStreamedResponse(self)

    @property
    @override
    def qs(self) -> Querystring:
        return Querystring(array_format="comma")

    @property
    @override
    def default_headers(self) -> dict[str, str | Omit]:
        return {
            **super().default_headers,
            "X-Stainless-Async": f"async:{get_async_library()}",
            **self._custom_headers,
        }

    def copy(
        self,
        *,
        base_url: str | httpx.URL | None = None,
        timeout: float | Timeout | None | NotGiven = NOT_GIVEN,
        http_client: httpx.AsyncClient | None = None,
        max_retries: int | NotGiven = NOT_GIVEN,
        default_headers: Mapping[str, str] | None = None,
        set_default_headers: Mapping[str, str] | None = None,
        default_query: Mapping[str, object] | None = None,
        set_default_query: Mapping[str, object] | None = None,
        _extra_kwargs: Mapping[str, Any] = {},
    ) -> Self:
        """
        Create a new client instance re-using the same options given to the current client with optional overriding.
        """
        if default_headers is not None and set_default_headers is not None:
            raise ValueError(
                "The `default_headers` and `set_default_headers` arguments are mutually exclusive"
            )

        if default_query is not None and set_default_query is not None:
            raise ValueError(
                "The `default_query` and `set_default_query` arguments are mutually exclusive"
            )

        headers = self._custom_headers
        if default_headers is not None:
            headers = {**headers, **default_headers}
        elif set_default_headers is not None:
            headers = set_default_headers

        params = self._custom_query
        if default_query is not None:
            params = {**params, **default_query}
        elif set_default_query is not None:
            params = set_default_query

        http_client = http_client or self._client
        return self.__class__(
            base_url=base_url or self.base_url,
            timeout=self.timeout if isinstance(timeout, NotGiven) else timeout,
            http_client=http_client,
            max_retries=max_retries if is_given(max_retries) else self.max_retries,
            default_headers=headers,
            default_query=params,
            **_extra_kwargs,
        )

    # Alias for `copy` for nicer inline usage, e.g.
    # client.with_options(timeout=10).foo.create(...)
    with_options = copy

    @override
    def _make_status_error(
        self,
        err_msg: str,
        *,
        body: object,
        response: httpx.Response,
    ) -> APIStatusError:
        if response.status_code == 400:
            return _exceptions.BadRequestError(err_msg, response=response, body=body)

        if response.status_code == 401:
            return _exceptions.AuthenticationError(err_msg, response=response, body=body)

        if response.status_code == 403:
            return _exceptions.PermissionDeniedError(err_msg, response=response, body=body)

        if response.status_code == 404:
            return _exceptions.NotFoundError(err_msg, response=response, body=body)

        if response.status_code == 409:
            return _exceptions.ConflictError(err_msg, response=response, body=body)

        if response.status_code == 422:
            return _exceptions.UnprocessableEntityError(err_msg, response=response, body=body)

        if response.status_code == 429:
            return _exceptions.RateLimitError(err_msg, response=response, body=body)

        if response.status_code >= 500:
            return _exceptions.InternalServerError(err_msg, response=response, body=body)
        return APIStatusError(err_msg, response=response, body=body)


class UnitycatalogWithRawResponse:
    def __init__(self, client: Unitycatalog) -> None:
        self.catalogs = resources.CatalogsResourceWithRawResponse(client.catalogs)
        self.schemas = resources.SchemasResourceWithRawResponse(client.schemas)
        self.tables = resources.TablesResourceWithRawResponse(client.tables)
        self.volumes = resources.VolumesResourceWithRawResponse(client.volumes)
        self.temporary_table_credentials = (
            resources.TemporaryTableCredentialsResourceWithRawResponse(
                client.temporary_table_credentials
            )
        )
        self.temporary_volume_credentials = (
            resources.TemporaryVolumeCredentialsResourceWithRawResponse(
                client.temporary_volume_credentials
            )
        )
        self.functions = resources.FunctionsResourceWithRawResponse(client.functions)


class AsyncUnitycatalogWithRawResponse:
    def __init__(self, client: AsyncUnitycatalog) -> None:
        self.catalogs = resources.AsyncCatalogsResourceWithRawResponse(client.catalogs)
        self.schemas = resources.AsyncSchemasResourceWithRawResponse(client.schemas)
        self.tables = resources.AsyncTablesResourceWithRawResponse(client.tables)
        self.volumes = resources.AsyncVolumesResourceWithRawResponse(client.volumes)
        self.temporary_table_credentials = (
            resources.AsyncTemporaryTableCredentialsResourceWithRawResponse(
                client.temporary_table_credentials
            )
        )
        self.temporary_volume_credentials = (
            resources.AsyncTemporaryVolumeCredentialsResourceWithRawResponse(
                client.temporary_volume_credentials
            )
        )
        self.functions = resources.AsyncFunctionsResourceWithRawResponse(client.functions)


class UnitycatalogWithStreamedResponse:
    def __init__(self, client: Unitycatalog) -> None:
        self.catalogs = resources.CatalogsResourceWithStreamingResponse(client.catalogs)
        self.schemas = resources.SchemasResourceWithStreamingResponse(client.schemas)
        self.tables = resources.TablesResourceWithStreamingResponse(client.tables)
        self.volumes = resources.VolumesResourceWithStreamingResponse(client.volumes)
        self.temporary_table_credentials = (
            resources.TemporaryTableCredentialsResourceWithStreamingResponse(
                client.temporary_table_credentials
            )
        )
        self.temporary_volume_credentials = (
            resources.TemporaryVolumeCredentialsResourceWithStreamingResponse(
                client.temporary_volume_credentials
            )
        )
        self.functions = resources.FunctionsResourceWithStreamingResponse(client.functions)


class AsyncUnitycatalogWithStreamedResponse:
    def __init__(self, client: AsyncUnitycatalog) -> None:
        self.catalogs = resources.AsyncCatalogsResourceWithStreamingResponse(client.catalogs)
        self.schemas = resources.AsyncSchemasResourceWithStreamingResponse(client.schemas)
        self.tables = resources.AsyncTablesResourceWithStreamingResponse(client.tables)
        self.volumes = resources.AsyncVolumesResourceWithStreamingResponse(client.volumes)
        self.temporary_table_credentials = (
            resources.AsyncTemporaryTableCredentialsResourceWithStreamingResponse(
                client.temporary_table_credentials
            )
        )
        self.temporary_volume_credentials = (
            resources.AsyncTemporaryVolumeCredentialsResourceWithStreamingResponse(
                client.temporary_volume_credentials
            )
        )
        self.functions = resources.AsyncFunctionsResourceWithStreamingResponse(client.functions)


Client = Unitycatalog

AsyncClient = AsyncUnitycatalog
