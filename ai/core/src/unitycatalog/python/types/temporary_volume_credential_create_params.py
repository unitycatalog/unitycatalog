# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing_extensions import Literal, Required, TypedDict

__all__ = ["TemporaryVolumeCredentialCreateParams"]


class TemporaryVolumeCredentialCreateParams(TypedDict, total=False):
    operation: Required[Literal["UNKNOWN_VOLUME_OPERATION", "READ_VOLUME", "WRITE_VOLUME"]]

    volume_id: Required[str]
    """Volume id for which temporary credentials are generated.

    Can be obtained from volumes/{full_name} (get volume info) API.
    """
