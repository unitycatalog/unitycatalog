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
from ..types import volume_create_params, volume_list_params, volume_update_params
from ..types.volume_info import VolumeInfo
from ..types.volume_list_response import VolumeListResponse

__all__ = ["VolumesResource", "AsyncVolumesResource"]


class VolumesResource(SyncAPIResource):
    @cached_property
    def with_raw_response(self) -> VolumesResourceWithRawResponse:
        return VolumesResourceWithRawResponse(self)

    @cached_property
    def with_streaming_response(self) -> VolumesResourceWithStreamingResponse:
        return VolumesResourceWithStreamingResponse(self)

    def create(
        self,
        *,
        catalog_name: str,
        name: str,
        schema_name: str,
        storage_location: str,
        volume_type: Literal["MANAGED", "EXTERNAL"],
        comment: str | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> VolumeInfo:
        """
        Creates a new volume.

        Args:
          catalog_name: The name of the catalog where the schema and the volume are

          name: The name of the volume

          schema_name: The name of the schema where the volume is

          storage_location: The storage location of the volume

          volume_type: The type of the volume

          comment: The comment attached to the volume

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return self._post(
            "/volumes",
            body=maybe_transform(
                {
                    "catalog_name": catalog_name,
                    "name": name,
                    "schema_name": schema_name,
                    "storage_location": storage_location,
                    "volume_type": volume_type,
                    "comment": comment,
                },
                volume_create_params.VolumeCreateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=VolumeInfo,
        )

    def retrieve(
        self,
        name: str,
        *,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> VolumeInfo:
        """
        Gets a volume for a specific catalog and schema.

        Args:
          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not name:
            raise ValueError(f"Expected a non-empty value for `name` but received {name!r}")
        return self._get(
            f"/volumes/{name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=VolumeInfo,
        )

    def update(
        self,
        name: str,
        *,
        comment: str | NotGiven = NOT_GIVEN,
        new_name: str | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> VolumeInfo:
        """
        Updates the specified volume under the specified parent catalog and schema.

        Currently only the name or the comment of the volume could be updated.

        Args:
          comment: The comment attached to the volume

          new_name: New name for the volume.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not name:
            raise ValueError(f"Expected a non-empty value for `name` but received {name!r}")
        return self._patch(
            f"/volumes/{name}",
            body=maybe_transform(
                {
                    "comment": comment,
                    "new_name": new_name,
                },
                volume_update_params.VolumeUpdateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=VolumeInfo,
        )

    def list(
        self,
        *,
        catalog_name: str,
        schema_name: str,
        max_results: int | NotGiven = NOT_GIVEN,
        page_token: str | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> VolumeListResponse:
        """Gets an array of available volumes under the parent catalog and schema.

        There is
        no guarantee of a specific ordering of the elements in the array.

        Args:
          catalog_name: The identifier of the catalog

          schema_name: The identifier of the schema

          max_results: Maximum number of volumes to return (page length).

              If not set, the page length is set to a server configured value.

              - when set to a value greater than 0, the page length is the minimum of this
                value and a server configured value;
              - when set to 0, the page length is set to a server configured value;
              - when set to a value less than 0, an invalid parameter error is returned;

              Note: this parameter controls only the maximum number of volumes to return. The
              actual number of volumes returned in a page may be smaller than this value,
              including 0, even if there are more pages.

          page_token: Opaque token returned by a previous request. It must be included in the request
              to retrieve the next page of results (pagination).

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return self._get(
            "/volumes",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
                query=maybe_transform(
                    {
                        "catalog_name": catalog_name,
                        "schema_name": schema_name,
                        "max_results": max_results,
                        "page_token": page_token,
                    },
                    volume_list_params.VolumeListParams,
                ),
            ),
            cast_to=VolumeListResponse,
        )

    def delete(
        self,
        name: str,
        *,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> object:
        """
        Deletes a volume from the specified parent catalog and schema.

        Args:
          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not name:
            raise ValueError(f"Expected a non-empty value for `name` but received {name!r}")
        return self._delete(
            f"/volumes/{name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=object,
        )


class AsyncVolumesResource(AsyncAPIResource):
    @cached_property
    def with_raw_response(self) -> AsyncVolumesResourceWithRawResponse:
        return AsyncVolumesResourceWithRawResponse(self)

    @cached_property
    def with_streaming_response(self) -> AsyncVolumesResourceWithStreamingResponse:
        return AsyncVolumesResourceWithStreamingResponse(self)

    async def create(
        self,
        *,
        catalog_name: str,
        name: str,
        schema_name: str,
        storage_location: str,
        volume_type: Literal["MANAGED", "EXTERNAL"],
        comment: str | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> VolumeInfo:
        """
        Creates a new volume.

        Args:
          catalog_name: The name of the catalog where the schema and the volume are

          name: The name of the volume

          schema_name: The name of the schema where the volume is

          storage_location: The storage location of the volume

          volume_type: The type of the volume

          comment: The comment attached to the volume

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return await self._post(
            "/volumes",
            body=await async_maybe_transform(
                {
                    "catalog_name": catalog_name,
                    "name": name,
                    "schema_name": schema_name,
                    "storage_location": storage_location,
                    "volume_type": volume_type,
                    "comment": comment,
                },
                volume_create_params.VolumeCreateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=VolumeInfo,
        )

    async def retrieve(
        self,
        name: str,
        *,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> VolumeInfo:
        """
        Gets a volume for a specific catalog and schema.

        Args:
          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not name:
            raise ValueError(f"Expected a non-empty value for `name` but received {name!r}")
        return await self._get(
            f"/volumes/{name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=VolumeInfo,
        )

    async def update(
        self,
        name: str,
        *,
        comment: str | NotGiven = NOT_GIVEN,
        new_name: str | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> VolumeInfo:
        """
        Updates the specified volume under the specified parent catalog and schema.

        Currently only the name or the comment of the volume could be updated.

        Args:
          comment: The comment attached to the volume

          new_name: New name for the volume.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not name:
            raise ValueError(f"Expected a non-empty value for `name` but received {name!r}")
        return await self._patch(
            f"/volumes/{name}",
            body=await async_maybe_transform(
                {
                    "comment": comment,
                    "new_name": new_name,
                },
                volume_update_params.VolumeUpdateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=VolumeInfo,
        )

    async def list(
        self,
        *,
        catalog_name: str,
        schema_name: str,
        max_results: int | NotGiven = NOT_GIVEN,
        page_token: str | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> VolumeListResponse:
        """Gets an array of available volumes under the parent catalog and schema.

        There is
        no guarantee of a specific ordering of the elements in the array.

        Args:
          catalog_name: The identifier of the catalog

          schema_name: The identifier of the schema

          max_results: Maximum number of volumes to return (page length).

              If not set, the page length is set to a server configured value.

              - when set to a value greater than 0, the page length is the minimum of this
                value and a server configured value;
              - when set to 0, the page length is set to a server configured value;
              - when set to a value less than 0, an invalid parameter error is returned;

              Note: this parameter controls only the maximum number of volumes to return. The
              actual number of volumes returned in a page may be smaller than this value,
              including 0, even if there are more pages.

          page_token: Opaque token returned by a previous request. It must be included in the request
              to retrieve the next page of results (pagination).

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return await self._get(
            "/volumes",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
                query=await async_maybe_transform(
                    {
                        "catalog_name": catalog_name,
                        "schema_name": schema_name,
                        "max_results": max_results,
                        "page_token": page_token,
                    },
                    volume_list_params.VolumeListParams,
                ),
            ),
            cast_to=VolumeListResponse,
        )

    async def delete(
        self,
        name: str,
        *,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> object:
        """
        Deletes a volume from the specified parent catalog and schema.

        Args:
          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not name:
            raise ValueError(f"Expected a non-empty value for `name` but received {name!r}")
        return await self._delete(
            f"/volumes/{name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=object,
        )


class VolumesResourceWithRawResponse:
    def __init__(self, volumes: VolumesResource) -> None:
        self._volumes = volumes

        self.create = to_raw_response_wrapper(
            volumes.create,
        )
        self.retrieve = to_raw_response_wrapper(
            volumes.retrieve,
        )
        self.update = to_raw_response_wrapper(
            volumes.update,
        )
        self.list = to_raw_response_wrapper(
            volumes.list,
        )
        self.delete = to_raw_response_wrapper(
            volumes.delete,
        )


class AsyncVolumesResourceWithRawResponse:
    def __init__(self, volumes: AsyncVolumesResource) -> None:
        self._volumes = volumes

        self.create = async_to_raw_response_wrapper(
            volumes.create,
        )
        self.retrieve = async_to_raw_response_wrapper(
            volumes.retrieve,
        )
        self.update = async_to_raw_response_wrapper(
            volumes.update,
        )
        self.list = async_to_raw_response_wrapper(
            volumes.list,
        )
        self.delete = async_to_raw_response_wrapper(
            volumes.delete,
        )


class VolumesResourceWithStreamingResponse:
    def __init__(self, volumes: VolumesResource) -> None:
        self._volumes = volumes

        self.create = to_streamed_response_wrapper(
            volumes.create,
        )
        self.retrieve = to_streamed_response_wrapper(
            volumes.retrieve,
        )
        self.update = to_streamed_response_wrapper(
            volumes.update,
        )
        self.list = to_streamed_response_wrapper(
            volumes.list,
        )
        self.delete = to_streamed_response_wrapper(
            volumes.delete,
        )


class AsyncVolumesResourceWithStreamingResponse:
    def __init__(self, volumes: AsyncVolumesResource) -> None:
        self._volumes = volumes

        self.create = async_to_streamed_response_wrapper(
            volumes.create,
        )
        self.retrieve = async_to_streamed_response_wrapper(
            volumes.retrieve,
        )
        self.update = async_to_streamed_response_wrapper(
            volumes.update,
        )
        self.list = async_to_streamed_response_wrapper(
            volumes.list,
        )
        self.delete = async_to_streamed_response_wrapper(
            volumes.delete,
        )
