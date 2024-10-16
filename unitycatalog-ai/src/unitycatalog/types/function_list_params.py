# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing_extensions import Required, TypedDict

__all__ = ["FunctionListParams"]


class FunctionListParams(TypedDict, total=False):
    catalog_name: Required[str]
    """Name of parent catalog for functions of interest."""

    schema_name: Required[str]
    """Parent schema of functions."""

    max_results: int
    """Maximum number of functions to return.

    - when set to a value greater than 0, the page length is the minimum of this
      value and a server configured value;
    - when set to 0, the page length is set to a server configured value;
    - when set to a value less than 0, an invalid parameter error is returned;
    """

    page_token: str
    """Opaque pagination token to go to next page based on previous query."""
