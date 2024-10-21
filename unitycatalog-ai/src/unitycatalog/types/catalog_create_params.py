# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing import Dict

from typing_extensions import Required, TypedDict

__all__ = ["CatalogCreateParams"]


class CatalogCreateParams(TypedDict, total=False):
    name: Required[str]
    """Name of catalog."""

    comment: str
    """User-provided free-form text description."""

    properties: Dict[str, str]
    """A map of key-value properties attached to the securable."""
