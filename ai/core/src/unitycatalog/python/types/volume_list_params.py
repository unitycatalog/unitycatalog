# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing_extensions import Required, TypedDict

__all__ = ["VolumeListParams"]


class VolumeListParams(TypedDict, total=False):
    catalog_name: Required[str]
    """The identifier of the catalog"""

    schema_name: Required[str]
    """The identifier of the schema"""

    max_results: int
    """Maximum number of volumes to return (page length).

    If not set, the page length is set to a server configured value.

    - when set to a value greater than 0, the page length is the minimum of this
      value and a server configured value;
    - when set to 0, the page length is set to a server configured value;
    - when set to a value less than 0, an invalid parameter error is returned;

    Note: this parameter controls only the maximum number of volumes to return. The
    actual number of volumes returned in a page may be smaller than this value,
    including 0, even if there are more pages.
    """

    page_token: str
    """Opaque token returned by a previous request.

    It must be included in the request to retrieve the next page of results
    (pagination).
    """
