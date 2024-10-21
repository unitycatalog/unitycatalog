# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from typing import Dict, Optional

from .._models import BaseModel

__all__ = ["CatalogInfo"]


class CatalogInfo(BaseModel):
    id: Optional[str] = None
    """Unique identifier for the catalog."""

    comment: Optional[str] = None
    """User-provided free-form text description."""

    created_at: Optional[int] = None
    """Time at which this catalog was created, in epoch milliseconds."""

    name: Optional[str] = None
    """Name of catalog."""

    properties: Optional[Dict[str, str]] = None
    """A map of key-value properties attached to the securable."""

    updated_at: Optional[int] = None
    """Time at which this catalog was last modified, in epoch milliseconds."""
