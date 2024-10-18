# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing import Dict

import httpx

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
from ..types import schema_create_params, schema_list_params, schema_update_params
from ..types.schema_info import SchemaInfo
from ..types.schema_list_response import SchemaListResponse

__all__ = ["SchemasResource", "AsyncSchemasResource"]


class SchemasResource(SyncAPIResource):
    @cached_property
    def with_raw_response(self) -> SchemasResourceWithRawResponse:
        return SchemasResourceWithRawResponse(self)

    @cached_property
    def with_streaming_response(self) -> SchemasResourceWithStreamingResponse:
        return SchemasResourceWithStreamingResponse(self)

    def create(
        self,
        *,
        catalog_name: str,
        name: str,
        comment: str | NotGiven = NOT_GIVEN,
        properties: Dict[str, str] | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> SchemaInfo:
        """
        Creates a new schema in the specified catalog.

        Args:
          catalog_name: Name of parent catalog.

          name: Name of schema, relative to parent catalog.

          comment: User-provided free-form text description.

          properties: A map of key-value properties attached to the securable.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return self._post(
            "/schemas",
            body=maybe_transform(
                {
                    "catalog_name": catalog_name,
                    "name": name,
                    "comment": comment,
                    "properties": properties,
                },
                schema_create_params.SchemaCreateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=SchemaInfo,
        )

    def retrieve(
        self,
        full_name: str,
        *,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> SchemaInfo:
        """
        Gets the specified schema for a catalog.

        Args:
          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not full_name:
            raise ValueError(
                f"Expected a non-empty value for `full_name` but received {full_name!r}"
            )
        return self._get(
            f"/schemas/{full_name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=SchemaInfo,
        )

    def update(
        self,
        full_name: str,
        *,
        comment: str | NotGiven = NOT_GIVEN,
        new_name: str | NotGiven = NOT_GIVEN,
        properties: Dict[str, str] | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> SchemaInfo:
        """
        Updates the specified schema.

        Args:
          comment: User-provided free-form text description.

          new_name: New name for the schema.

          properties: A map of key-value properties attached to the securable.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not full_name:
            raise ValueError(
                f"Expected a non-empty value for `full_name` but received {full_name!r}"
            )
        return self._patch(
            f"/schemas/{full_name}",
            body=maybe_transform(
                {
                    "comment": comment,
                    "new_name": new_name,
                    "properties": properties,
                },
                schema_update_params.SchemaUpdateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=SchemaInfo,
        )

    def list(
        self,
        *,
        catalog_name: str,
        max_results: int | NotGiven = NOT_GIVEN,
        page_token: str | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> SchemaListResponse:
        """Gets an array of schemas for a catalog.

        There is no guarantee of a specific
        ordering of the elements in the array.

        Args:
          catalog_name: Parent catalog for schemas of interest.

          max_results: Maximum number of schemas to return.

              - when set to a value greater than 0, the page length is the minimum of this
                value and a server configured value;
              - when set to 0, the page length is set to a server configured value;
              - when set to a value less than 0, an invalid parameter error is returned;

          page_token: Opaque pagination token to go to next page based on previous query.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return self._get(
            "/schemas",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
                query=maybe_transform(
                    {
                        "catalog_name": catalog_name,
                        "max_results": max_results,
                        "page_token": page_token,
                    },
                    schema_list_params.SchemaListParams,
                ),
            ),
            cast_to=SchemaListResponse,
        )

    def delete(
        self,
        full_name: str,
        *,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> object:
        """
        Deletes the specified schema from the parent catalog.

        Args:
          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not full_name:
            raise ValueError(
                f"Expected a non-empty value for `full_name` but received {full_name!r}"
            )
        return self._delete(
            f"/schemas/{full_name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=object,
        )


class AsyncSchemasResource(AsyncAPIResource):
    @cached_property
    def with_raw_response(self) -> AsyncSchemasResourceWithRawResponse:
        return AsyncSchemasResourceWithRawResponse(self)

    @cached_property
    def with_streaming_response(self) -> AsyncSchemasResourceWithStreamingResponse:
        return AsyncSchemasResourceWithStreamingResponse(self)

    async def create(
        self,
        *,
        catalog_name: str,
        name: str,
        comment: str | NotGiven = NOT_GIVEN,
        properties: Dict[str, str] | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> SchemaInfo:
        """
        Creates a new schema in the specified catalog.

        Args:
          catalog_name: Name of parent catalog.

          name: Name of schema, relative to parent catalog.

          comment: User-provided free-form text description.

          properties: A map of key-value properties attached to the securable.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return await self._post(
            "/schemas",
            body=await async_maybe_transform(
                {
                    "catalog_name": catalog_name,
                    "name": name,
                    "comment": comment,
                    "properties": properties,
                },
                schema_create_params.SchemaCreateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=SchemaInfo,
        )

    async def retrieve(
        self,
        full_name: str,
        *,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> SchemaInfo:
        """
        Gets the specified schema for a catalog.

        Args:
          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not full_name:
            raise ValueError(
                f"Expected a non-empty value for `full_name` but received {full_name!r}"
            )
        return await self._get(
            f"/schemas/{full_name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=SchemaInfo,
        )

    async def update(
        self,
        full_name: str,
        *,
        comment: str | NotGiven = NOT_GIVEN,
        new_name: str | NotGiven = NOT_GIVEN,
        properties: Dict[str, str] | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> SchemaInfo:
        """
        Updates the specified schema.

        Args:
          comment: User-provided free-form text description.

          new_name: New name for the schema.

          properties: A map of key-value properties attached to the securable.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not full_name:
            raise ValueError(
                f"Expected a non-empty value for `full_name` but received {full_name!r}"
            )
        return await self._patch(
            f"/schemas/{full_name}",
            body=await async_maybe_transform(
                {
                    "comment": comment,
                    "new_name": new_name,
                    "properties": properties,
                },
                schema_update_params.SchemaUpdateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=SchemaInfo,
        )

    async def list(
        self,
        *,
        catalog_name: str,
        max_results: int | NotGiven = NOT_GIVEN,
        page_token: str | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> SchemaListResponse:
        """Gets an array of schemas for a catalog.

        There is no guarantee of a specific
        ordering of the elements in the array.

        Args:
          catalog_name: Parent catalog for schemas of interest.

          max_results: Maximum number of schemas to return.

              - when set to a value greater than 0, the page length is the minimum of this
                value and a server configured value;
              - when set to 0, the page length is set to a server configured value;
              - when set to a value less than 0, an invalid parameter error is returned;

          page_token: Opaque pagination token to go to next page based on previous query.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return await self._get(
            "/schemas",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
                query=await async_maybe_transform(
                    {
                        "catalog_name": catalog_name,
                        "max_results": max_results,
                        "page_token": page_token,
                    },
                    schema_list_params.SchemaListParams,
                ),
            ),
            cast_to=SchemaListResponse,
        )

    async def delete(
        self,
        full_name: str,
        *,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> object:
        """
        Deletes the specified schema from the parent catalog.

        Args:
          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not full_name:
            raise ValueError(
                f"Expected a non-empty value for `full_name` but received {full_name!r}"
            )
        return await self._delete(
            f"/schemas/{full_name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=object,
        )


class SchemasResourceWithRawResponse:
    def __init__(self, schemas: SchemasResource) -> None:
        self._schemas = schemas

        self.create = to_raw_response_wrapper(
            schemas.create,
        )
        self.retrieve = to_raw_response_wrapper(
            schemas.retrieve,
        )
        self.update = to_raw_response_wrapper(
            schemas.update,
        )
        self.list = to_raw_response_wrapper(
            schemas.list,
        )
        self.delete = to_raw_response_wrapper(
            schemas.delete,
        )


class AsyncSchemasResourceWithRawResponse:
    def __init__(self, schemas: AsyncSchemasResource) -> None:
        self._schemas = schemas

        self.create = async_to_raw_response_wrapper(
            schemas.create,
        )
        self.retrieve = async_to_raw_response_wrapper(
            schemas.retrieve,
        )
        self.update = async_to_raw_response_wrapper(
            schemas.update,
        )
        self.list = async_to_raw_response_wrapper(
            schemas.list,
        )
        self.delete = async_to_raw_response_wrapper(
            schemas.delete,
        )


class SchemasResourceWithStreamingResponse:
    def __init__(self, schemas: SchemasResource) -> None:
        self._schemas = schemas

        self.create = to_streamed_response_wrapper(
            schemas.create,
        )
        self.retrieve = to_streamed_response_wrapper(
            schemas.retrieve,
        )
        self.update = to_streamed_response_wrapper(
            schemas.update,
        )
        self.list = to_streamed_response_wrapper(
            schemas.list,
        )
        self.delete = to_streamed_response_wrapper(
            schemas.delete,
        )


class AsyncSchemasResourceWithStreamingResponse:
    def __init__(self, schemas: AsyncSchemasResource) -> None:
        self._schemas = schemas

        self.create = async_to_streamed_response_wrapper(
            schemas.create,
        )
        self.retrieve = async_to_streamed_response_wrapper(
            schemas.retrieve,
        )
        self.update = async_to_streamed_response_wrapper(
            schemas.update,
        )
        self.list = async_to_streamed_response_wrapper(
            schemas.list,
        )
        self.delete = async_to_streamed_response_wrapper(
            schemas.delete,
        )
