# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing_extensions import TypedDict

__all__ = ["CatalogDeleteParams"]


class CatalogDeleteParams(TypedDict, total=False):
    force: bool
    """Force deletion even if the catalog is not empty."""
