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
from ..types import temporary_volume_credential_create_params
from ..types.generate_temporary_volume_credential_response import (
    GenerateTemporaryVolumeCredentialResponse,
)

__all__ = ["TemporaryVolumeCredentialsResource", "AsyncTemporaryVolumeCredentialsResource"]


class TemporaryVolumeCredentialsResource(SyncAPIResource):
    @cached_property
    def with_raw_response(self) -> TemporaryVolumeCredentialsResourceWithRawResponse:
        return TemporaryVolumeCredentialsResourceWithRawResponse(self)

    @cached_property
    def with_streaming_response(self) -> TemporaryVolumeCredentialsResourceWithStreamingResponse:
        return TemporaryVolumeCredentialsResourceWithStreamingResponse(self)

    def create(
        self,
        *,
        operation: Literal["UNKNOWN_VOLUME_OPERATION", "READ_VOLUME", "WRITE_VOLUME"],
        volume_id: str,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> GenerateTemporaryVolumeCredentialResponse:
        """
        Generate temporary volume credentials.

        Args:
          volume_id: Volume id for which temporary credentials are generated. Can be obtained from
              volumes/{full_name} (get volume info) API.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return self._post(
            "/temporary-volume-credentials",
            body=maybe_transform(
                {
                    "operation": operation,
                    "volume_id": volume_id,
                },
                temporary_volume_credential_create_params.TemporaryVolumeCredentialCreateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=GenerateTemporaryVolumeCredentialResponse,
        )


class AsyncTemporaryVolumeCredentialsResource(AsyncAPIResource):
    @cached_property
    def with_raw_response(self) -> AsyncTemporaryVolumeCredentialsResourceWithRawResponse:
        return AsyncTemporaryVolumeCredentialsResourceWithRawResponse(self)

    @cached_property
    def with_streaming_response(
        self,
    ) -> AsyncTemporaryVolumeCredentialsResourceWithStreamingResponse:
        return AsyncTemporaryVolumeCredentialsResourceWithStreamingResponse(self)

    async def create(
        self,
        *,
        operation: Literal["UNKNOWN_VOLUME_OPERATION", "READ_VOLUME", "WRITE_VOLUME"],
        volume_id: str,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> GenerateTemporaryVolumeCredentialResponse:
        """
        Generate temporary volume credentials.

        Args:
          volume_id: Volume id for which temporary credentials are generated. Can be obtained from
              volumes/{full_name} (get volume info) API.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return await self._post(
            "/temporary-volume-credentials",
            body=await async_maybe_transform(
                {
                    "operation": operation,
                    "volume_id": volume_id,
                },
                temporary_volume_credential_create_params.TemporaryVolumeCredentialCreateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=GenerateTemporaryVolumeCredentialResponse,
        )


class TemporaryVolumeCredentialsResourceWithRawResponse:
    def __init__(self, temporary_volume_credentials: TemporaryVolumeCredentialsResource) -> None:
        self._temporary_volume_credentials = temporary_volume_credentials

        self.create = to_raw_response_wrapper(
            temporary_volume_credentials.create,
        )


class AsyncTemporaryVolumeCredentialsResourceWithRawResponse:
    def __init__(
        self, temporary_volume_credentials: AsyncTemporaryVolumeCredentialsResource
    ) -> None:
        self._temporary_volume_credentials = temporary_volume_credentials

        self.create = async_to_raw_response_wrapper(
            temporary_volume_credentials.create,
        )


class TemporaryVolumeCredentialsResourceWithStreamingResponse:
    def __init__(self, temporary_volume_credentials: TemporaryVolumeCredentialsResource) -> None:
        self._temporary_volume_credentials = temporary_volume_credentials

        self.create = to_streamed_response_wrapper(
            temporary_volume_credentials.create,
        )


class AsyncTemporaryVolumeCredentialsResourceWithStreamingResponse:
    def __init__(
        self, temporary_volume_credentials: AsyncTemporaryVolumeCredentialsResource
    ) -> None:
        self._temporary_volume_credentials = temporary_volume_credentials

        self.create = async_to_streamed_response_wrapper(
            temporary_volume_credentials.create,
        )
