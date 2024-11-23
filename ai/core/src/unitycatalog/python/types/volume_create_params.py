# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing_extensions import Literal, Required, TypedDict

__all__ = ["VolumeCreateParams"]


class VolumeCreateParams(TypedDict, total=False):
    catalog_name: Required[str]
    """The name of the catalog where the schema and the volume are"""

    name: Required[str]
    """The name of the volume"""

    schema_name: Required[str]
    """The name of the schema where the volume is"""

    storage_location: Required[str]
    """The storage location of the volume"""

    volume_type: Required[Literal["MANAGED", "EXTERNAL"]]
    """The type of the volume"""

    comment: str
    """The comment attached to the volume"""
