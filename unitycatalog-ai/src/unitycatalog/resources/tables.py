# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing import Dict, Iterable

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
from ..types import table_create_params, table_list_params
from ..types.table_info import TableInfo
from ..types.table_list_response import TableListResponse

__all__ = ["TablesResource", "AsyncTablesResource"]


class TablesResource(SyncAPIResource):
    @cached_property
    def with_raw_response(self) -> TablesResourceWithRawResponse:
        return TablesResourceWithRawResponse(self)

    @cached_property
    def with_streaming_response(self) -> TablesResourceWithStreamingResponse:
        return TablesResourceWithStreamingResponse(self)

    def create(
        self,
        *,
        catalog_name: str,
        columns: Iterable[table_create_params.Column],
        data_source_format: Literal["DELTA", "CSV", "JSON", "AVRO", "PARQUET", "ORC", "TEXT"],
        name: str,
        schema_name: str,
        table_type: Literal["MANAGED", "EXTERNAL"],
        comment: str | NotGiven = NOT_GIVEN,
        properties: Dict[str, str] | NotGiven = NOT_GIVEN,
        storage_location: str | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> TableInfo:
        """Creates a new table instance.

        WARNING: This API is experimental and will change
        in future versions.

        Args:
          catalog_name: Name of parent catalog.

          columns: The array of **ColumnInfo** definitions of the table's columns.

          data_source_format: Data source format

          name: Name of table, relative to parent schema.

          schema_name: Name of parent schema relative to its parent catalog.

          comment: User-provided free-form text description.

          properties: A map of key-value properties attached to the securable.

          storage_location: Storage root URL for table (for **MANAGED**, **EXTERNAL** tables)

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return self._post(
            "/tables",
            body=maybe_transform(
                {
                    "catalog_name": catalog_name,
                    "columns": columns,
                    "data_source_format": data_source_format,
                    "name": name,
                    "schema_name": schema_name,
                    "table_type": table_type,
                    "comment": comment,
                    "properties": properties,
                    "storage_location": storage_location,
                },
                table_create_params.TableCreateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=TableInfo,
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
    ) -> TableInfo:
        """
        Gets a table for a specific catalog and schema.

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
            f"/tables/{full_name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=TableInfo,
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
    ) -> TableListResponse:
        """Gets the list of all available tables under the parent catalog and schema.

        There
        is no guarantee of a specific ordering of the elements in the array.

        Args:
          catalog_name: Name of parent catalog for tables of interest.

          schema_name: Parent schema of tables.

          max_results: Maximum number of tables to return.

              - when set to a value greater than 0, the page length is the minimum of this
                value and a server configured value;
              - when set to 0, the page length is set to a server configured value;
              - when set to a value less than 0, an invalid parameter error is returned;

          page_token: Opaque token to send for the next page of results (pagination).

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return self._get(
            "/tables",
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
                    table_list_params.TableListParams,
                ),
            ),
            cast_to=TableListResponse,
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
        Deletes a table from the specified parent catalog and schema.

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
            f"/tables/{full_name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=object,
        )


class AsyncTablesResource(AsyncAPIResource):
    @cached_property
    def with_raw_response(self) -> AsyncTablesResourceWithRawResponse:
        return AsyncTablesResourceWithRawResponse(self)

    @cached_property
    def with_streaming_response(self) -> AsyncTablesResourceWithStreamingResponse:
        return AsyncTablesResourceWithStreamingResponse(self)

    async def create(
        self,
        *,
        catalog_name: str,
        columns: Iterable[table_create_params.Column],
        data_source_format: Literal["DELTA", "CSV", "JSON", "AVRO", "PARQUET", "ORC", "TEXT"],
        name: str,
        schema_name: str,
        table_type: Literal["MANAGED", "EXTERNAL"],
        comment: str | NotGiven = NOT_GIVEN,
        properties: Dict[str, str] | NotGiven = NOT_GIVEN,
        storage_location: str | NotGiven = NOT_GIVEN,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = NOT_GIVEN,
    ) -> TableInfo:
        """Creates a new table instance.

        WARNING: This API is experimental and will change
        in future versions.

        Args:
          catalog_name: Name of parent catalog.

          columns: The array of **ColumnInfo** definitions of the table's columns.

          data_source_format: Data source format

          name: Name of table, relative to parent schema.

          schema_name: Name of parent schema relative to its parent catalog.

          comment: User-provided free-form text description.

          properties: A map of key-value properties attached to the securable.

          storage_location: Storage root URL for table (for **MANAGED**, **EXTERNAL** tables)

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return await self._post(
            "/tables",
            body=await async_maybe_transform(
                {
                    "catalog_name": catalog_name,
                    "columns": columns,
                    "data_source_format": data_source_format,
                    "name": name,
                    "schema_name": schema_name,
                    "table_type": table_type,
                    "comment": comment,
                    "properties": properties,
                    "storage_location": storage_location,
                },
                table_create_params.TableCreateParams,
            ),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=TableInfo,
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
    ) -> TableInfo:
        """
        Gets a table for a specific catalog and schema.

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
            f"/tables/{full_name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=TableInfo,
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
    ) -> TableListResponse:
        """Gets the list of all available tables under the parent catalog and schema.

        There
        is no guarantee of a specific ordering of the elements in the array.

        Args:
          catalog_name: Name of parent catalog for tables of interest.

          schema_name: Parent schema of tables.

          max_results: Maximum number of tables to return.

              - when set to a value greater than 0, the page length is the minimum of this
                value and a server configured value;
              - when set to 0, the page length is set to a server configured value;
              - when set to a value less than 0, an invalid parameter error is returned;

          page_token: Opaque token to send for the next page of results (pagination).

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        return await self._get(
            "/tables",
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
                    table_list_params.TableListParams,
                ),
            ),
            cast_to=TableListResponse,
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
        Deletes a table from the specified parent catalog and schema.

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
            f"/tables/{full_name}",
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
            ),
            cast_to=object,
        )


class TablesResourceWithRawResponse:
    def __init__(self, tables: TablesResource) -> None:
        self._tables = tables

        self.create = to_raw_response_wrapper(
            tables.create,
        )
        self.retrieve = to_raw_response_wrapper(
            tables.retrieve,
        )
        self.list = to_raw_response_wrapper(
            tables.list,
        )
        self.delete = to_raw_response_wrapper(
            tables.delete,
        )


class AsyncTablesResourceWithRawResponse:
    def __init__(self, tables: AsyncTablesResource) -> None:
        self._tables = tables

        self.create = async_to_raw_response_wrapper(
            tables.create,
        )
        self.retrieve = async_to_raw_response_wrapper(
            tables.retrieve,
        )
        self.list = async_to_raw_response_wrapper(
            tables.list,
        )
        self.delete = async_to_raw_response_wrapper(
            tables.delete,
        )


class TablesResourceWithStreamingResponse:
    def __init__(self, tables: TablesResource) -> None:
        self._tables = tables

        self.create = to_streamed_response_wrapper(
            tables.create,
        )
        self.retrieve = to_streamed_response_wrapper(
            tables.retrieve,
        )
        self.list = to_streamed_response_wrapper(
            tables.list,
        )
        self.delete = to_streamed_response_wrapper(
            tables.delete,
        )


class AsyncTablesResourceWithStreamingResponse:
    def __init__(self, tables: AsyncTablesResource) -> None:
        self._tables = tables

        self.create = async_to_streamed_response_wrapper(
            tables.create,
        )
        self.retrieve = async_to_streamed_response_wrapper(
            tables.retrieve,
        )
        self.list = async_to_streamed_response_wrapper(
            tables.list,
        )
        self.delete = async_to_streamed_response_wrapper(
            tables.delete,
        )
