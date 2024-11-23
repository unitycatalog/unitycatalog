# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

import httpx
from typing_extensions import Literal

from .._base_client import (
    make_request_options,
)
from .._compat import cached_property
from .._resource import AsyncAPIResource, SyncAPIResource
from .._response import (
    async_to_raw_response_wrapper,
    async_to_streamed_response_wrapper,
    to_raw_response_wrapper,
    to_streamed_response_wrapper,
)
from .._types import NOT_GIVEN, Body, Headers, NotGiven, Query
from .._utils import (
    async_maybe_transform,
    maybe_transform,
)
from ..types import temporary_table_credential_create_params
from ..types.generate_temporary_table_credential_response import (
    GenerateTemporaryTableCredentialResponse,
)

__all__ = ["TemporaryTableCredentialsResource", "AsyncTemporaryTableCredentialsResource"]


class TemporaryTableCredentialsResource(SyncAPIResource):
    @cached_property
    def with_raw_response(self) -> TemporaryTableCredentialsResourceWithRawResponse:
        return TemporaryTableCredentialsResourceWithRawResponse(self)

    @cached_property
    def with_streaming_response(self) -> TemporaryTableCredentialsResourceWithStreamingResponse:
        return TemporaryTableCredentialsResourceWithStreamingResponse(self)

    def create(
        self,
        *,
        operation: Literal["UNKNOWN_TABLE_OPERATION", "READ", "READ_WRITE"],
        table_id: str,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> GenerateTemporaryTableCredentialResponse:
        """
        Generate temporary table credentials.

        Args:
          table_id: Table id for which temporary credentials are generated. Can be obtained from
              tables/{full_name} (get table info) API.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return self._post(
            "/temporary-table-credentials",
            body=maybe_transform(
                {
                    "operation": operation,
                    "table_id": table_id,
                },
                temporary_table_credential_create_params.TemporaryTableCredentialCreateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=GenerateTemporaryTableCredentialResponse,
        )


class AsyncTemporaryTableCredentialsResource(AsyncAPIResource):
    @cached_property
    def with_raw_response(self) -> AsyncTemporaryTableCredentialsResourceWithRawResponse:
        return AsyncTemporaryTableCredentialsResourceWithRawResponse(self)

    @cached_property
    def with_streaming_response(
        self,
    ) -> AsyncTemporaryTableCredentialsResourceWithStreamingResponse:
        return AsyncTemporaryTableCredentialsResourceWithStreamingResponse(self)

    async def create(
        self,
        *,
        operation: Literal["UNKNOWN_TABLE_OPERATION", "READ", "READ_WRITE"],
        table_id: str,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> GenerateTemporaryTableCredentialResponse:
        """
        Generate temporary table credentials.

        Args:
          table_id: Table id for which temporary credentials are generated. Can be obtained from
              tables/{full_name} (get table info) API.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return await self._post(
            "/temporary-table-credentials",
            body=await async_maybe_transform(
                {
                    "operation": operation,
                    "table_id": table_id,
                },
                temporary_table_credential_create_params.TemporaryTableCredentialCreateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=GenerateTemporaryTableCredentialResponse,
        )


class TemporaryTableCredentialsResourceWithRawResponse:
    def __init__(self, temporary_table_credentials: TemporaryTableCredentialsResource) -> None:
        self._temporary_table_credentials = temporary_table_credentials

        self.create = to_raw_response_wrapper(
            temporary_table_credentials.create,
        )


class AsyncTemporaryTableCredentialsResourceWithRawResponse:
    def __init__(self, temporary_table_credentials: AsyncTemporaryTableCredentialsResource) -> None:
        self._temporary_table_credentials = temporary_table_credentials

        self.create = async_to_raw_response_wrapper(
            temporary_table_credentials.create,
        )


class TemporaryTableCredentialsResourceWithStreamingResponse:
    def __init__(self, temporary_table_credentials: TemporaryTableCredentialsResource) -> None:
        self._temporary_table_credentials = temporary_table_credentials

        self.create = to_streamed_response_wrapper(
            temporary_table_credentials.create,
        )


class AsyncTemporaryTableCredentialsResourceWithStreamingResponse:
    def __init__(self, temporary_table_credentials: AsyncTemporaryTableCredentialsResource) -> None:
        self._temporary_table_credentials = temporary_table_credentials

        self.create = async_to_streamed_response_wrapper(
            temporary_table_credentials.create,
        )
