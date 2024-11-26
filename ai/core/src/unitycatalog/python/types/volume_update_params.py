# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing_extensions import TypedDict

__all__ = ["VolumeUpdateParams"]


class VolumeUpdateParams(TypedDict, total=False):
    comment: str
    """The comment attached to the volume"""

    new_name: str
    """New name for the volume."""
