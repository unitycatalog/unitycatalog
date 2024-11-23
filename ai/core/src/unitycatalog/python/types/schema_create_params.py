# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing import Dict

from typing_extensions import Required, TypedDict

__all__ = ["SchemaCreateParams"]


class SchemaCreateParams(TypedDict, total=False):
    catalog_name: Required[str]
    """Name of parent catalog."""

    name: Required[str]
    """Name of schema, relative to parent catalog."""

    comment: str
    """User-provided free-form text description."""

    properties: Dict[str, str]
    """A map of key-value properties attached to the securable."""
