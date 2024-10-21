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
from ..types import (
    catalog_create_params,
    catalog_delete_params,
    catalog_list_params,
    catalog_update_params,
)
from ..types.catalog_info import CatalogInfo
from ..types.catalog_list_response import CatalogListResponse

__all__ = ["CatalogsResource", "AsyncCatalogsResource"]


class CatalogsResource(SyncAPIResource):
    @cached_property
    def with_raw_response(self) -> CatalogsResourceWithRawResponse:
        return CatalogsResourceWithRawResponse(self)

    @cached_property
    def with_streaming_response(self) -> CatalogsResourceWithStreamingResponse:
        return CatalogsResourceWithStreamingResponse(self)

    def create(
        self,
        *,
        name: str,
        comment: str | NotGiven = NOT_GIVEN,
        properties: Dict[str, str] | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> CatalogInfo:
        """
        Creates a new catalog instance.

        Args:
          name: Name of catalog.

          comment: User-provided free-form text description.

          properties: A map of key-value properties attached to the securable.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return self._post(
            "/catalogs",
            body=maybe_transform(
                {
                    "name": name,
                    "comment": comment,
                    "properties": properties,
                },
                catalog_create_params.CatalogCreateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=CatalogInfo,
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
    ) -> CatalogInfo:
        """
        Gets the specified catalog.

        Args:
          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not name:
            raise ValueError(f"Expected a non-empty value for `name` but received {name!r}")
        return self._get(
            f"/catalogs/{name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=CatalogInfo,
        )

    def update(
        self,
        name: str,
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
    ) -> CatalogInfo:
        """
        Updates the catalog that matches the supplied name.

        Args:
          comment: User-provided free-form text description.

          new_name: New name for the catalog.

          properties: A map of key-value properties attached to the securable.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not name:
            raise ValueError(f"Expected a non-empty value for `name` but received {name!r}")
        return self._patch(
            f"/catalogs/{name}",
            body=maybe_transform(
                {
                    "comment": comment,
                    "new_name": new_name,
                    "properties": properties,
                },
                catalog_update_params.CatalogUpdateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=CatalogInfo,
        )

    def list(
        self,
        *,
        max_results: int | NotGiven = NOT_GIVEN,
        page_token: str | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> CatalogListResponse:
        """Lists the available catalogs.

        There is no guarantee of a specific ordering of
        the elements in the list.

        Args:
          max_results: Maximum number of catalogs to return.

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
            "/catalogs",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
                query=maybe_transform(
                    {
                        "max_results": max_results,
                        "page_token": page_token,
                    },
                    catalog_list_params.CatalogListParams,
                ),
            ),
            cast_to=CatalogListResponse,
        )

    def delete(
        self,
        name: str,
        *,
        force: bool | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> object:
        """
        Deletes the catalog that matches the supplied name.

        Args:
          force: Force deletion even if the catalog is not empty.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not name:
            raise ValueError(f"Expected a non-empty value for `name` but received {name!r}")
        return self._delete(
            f"/catalogs/{name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
                query=maybe_transform({"force": force}, catalog_delete_params.CatalogDeleteParams),
            ),
            cast_to=object,
        )


class AsyncCatalogsResource(AsyncAPIResource):
    @cached_property
    def with_raw_response(self) -> AsyncCatalogsResourceWithRawResponse:
        return AsyncCatalogsResourceWithRawResponse(self)

    @cached_property
    def with_streaming_response(self) -> AsyncCatalogsResourceWithStreamingResponse:
        return AsyncCatalogsResourceWithStreamingResponse(self)

    async def create(
        self,
        *,
        name: str,
        comment: str | NotGiven = NOT_GIVEN,
        properties: Dict[str, str] | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> CatalogInfo:
        """
        Creates a new catalog instance.

        Args:
          name: Name of catalog.

          comment: User-provided free-form text description.

          properties: A map of key-value properties attached to the securable.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return await self._post(
            "/catalogs",
            body=await async_maybe_transform(
                {
                    "name": name,
                    "comment": comment,
                    "properties": properties,
                },
                catalog_create_params.CatalogCreateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=CatalogInfo,
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
    ) -> CatalogInfo:
        """
        Gets the specified catalog.

        Args:
          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not name:
            raise ValueError(f"Expected a non-empty value for `name` but received {name!r}")
        return await self._get(
            f"/catalogs/{name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=CatalogInfo,
        )

    async def update(
        self,
        name: str,
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
    ) -> CatalogInfo:
        """
        Updates the catalog that matches the supplied name.

        Args:
          comment: User-provided free-form text description.

          new_name: New name for the catalog.

          properties: A map of key-value properties attached to the securable.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not name:
            raise ValueError(f"Expected a non-empty value for `name` but received {name!r}")
        return await self._patch(
            f"/catalogs/{name}",
            body=await async_maybe_transform(
                {
                    "comment": comment,
                    "new_name": new_name,
                    "properties": properties,
                },
                catalog_update_params.CatalogUpdateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=CatalogInfo,
        )

    async def list(
        self,
        *,
        max_results: int | NotGiven = NOT_GIVEN,
        page_token: str | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> CatalogListResponse:
        """Lists the available catalogs.

        There is no guarantee of a specific ordering of
        the elements in the list.

        Args:
          max_results: Maximum number of catalogs to return.

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
            "/catalogs",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
                query=await async_maybe_transform(
                    {
                        "max_results": max_results,
                        "page_token": page_token,
                    },
                    catalog_list_params.CatalogListParams,
                ),
            ),
            cast_to=CatalogListResponse,
        )

    async def delete(
        self,
        name: str,
        *,
        force: bool | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> object:
        """
        Deletes the catalog that matches the supplied name.

        Args:
          force: Force deletion even if the catalog is not empty.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if not name:
            raise ValueError(f"Expected a non-empty value for `name` but received {name!r}")
        return await self._delete(
            f"/catalogs/{name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
                query=await async_maybe_transform(
                    {"force": force}, catalog_delete_params.CatalogDeleteParams
                ),
            ),
            cast_to=object,
        )


class CatalogsResourceWithRawResponse:
    def __init__(self, catalogs: CatalogsResource) -> None:
        self._catalogs = catalogs

        self.create = to_raw_response_wrapper(
            catalogs.create,
        )
        self.retrieve = to_raw_response_wrapper(
            catalogs.retrieve,
        )
        self.update = to_raw_response_wrapper(
            catalogs.update,
        )
        self.list = to_raw_response_wrapper(
            catalogs.list,
        )
        self.delete = to_raw_response_wrapper(
            catalogs.delete,
        )


class AsyncCatalogsResourceWithRawResponse:
    def __init__(self, catalogs: AsyncCatalogsResource) -> None:
        self._catalogs = catalogs

        self.create = async_to_raw_response_wrapper(
            catalogs.create,
        )
        self.retrieve = async_to_raw_response_wrapper(
            catalogs.retrieve,
        )
        self.update = async_to_raw_response_wrapper(
            catalogs.update,
        )
        self.list = async_to_raw_response_wrapper(
            catalogs.list,
        )
        self.delete = async_to_raw_response_wrapper(
            catalogs.delete,
        )


class CatalogsResourceWithStreamingResponse:
    def __init__(self, catalogs: CatalogsResource) -> None:
        self._catalogs = catalogs

        self.create = to_streamed_response_wrapper(
            catalogs.create,
        )
        self.retrieve = to_streamed_response_wrapper(
            catalogs.retrieve,
        )
        self.update = to_streamed_response_wrapper(
            catalogs.update,
        )
        self.list = to_streamed_response_wrapper(
            catalogs.list,
        )
        self.delete = to_streamed_response_wrapper(
            catalogs.delete,
        )


class AsyncCatalogsResourceWithStreamingResponse:
    def __init__(self, catalogs: AsyncCatalogsResource) -> None:
        self._catalogs = catalogs

        self.create = async_to_streamed_response_wrapper(
            catalogs.create,
        )
        self.retrieve = async_to_streamed_response_wrapper(
            catalogs.retrieve,
        )
        self.update = async_to_streamed_response_wrapper(
            catalogs.update,
        )
        self.list = async_to_streamed_response_wrapper(
            catalogs.list,
        )
        self.delete = async_to_streamed_response_wrapper(
            catalogs.delete,
        )
