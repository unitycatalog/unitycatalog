# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing import Dict

from typing_extensions import TypedDict

__all__ = ["CatalogUpdateParams"]


class CatalogUpdateParams(TypedDict, total=False):
    comment: str
    """User-provided free-form text description."""

    new_name: str
    """New name for the catalog."""

    properties: Dict[str, str]
    """A map of key-value properties attached to the securable."""
