# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from typing import Optional

from typing_extensions import Literal

from .._models import BaseModel

__all__ = ["VolumeInfo"]


class VolumeInfo(BaseModel):
    catalog_name: Optional[str] = None
    """The name of the catalog where the schema and the volume are"""

    comment: Optional[str] = None
    """The comment attached to the volume"""

    created_at: Optional[int] = None
    """Time at which this volume was created, in epoch milliseconds."""

    full_name: Optional[str] = None
    """
    Full name of volume, in form of
    **catalog_name**.**schema_name**.**volume_name**.
    """

    name: Optional[str] = None
    """The name of the volume"""

    schema_name: Optional[str] = None
    """The name of the schema where the volume is"""

    storage_location: Optional[str] = None
    """The storage location of the volume"""

    updated_at: Optional[int] = None
    """Time at which this volume was last modified, in epoch milliseconds."""

    volume_id: Optional[str] = None
    """Unique identifier for the volume"""

    volume_type: Optional[Literal["MANAGED", "EXTERNAL"]] = None
    """The type of the volume"""
