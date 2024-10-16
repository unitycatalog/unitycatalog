# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing_extensions import Literal, Required, TypedDict

__all__ = ["TemporaryTableCredentialCreateParams"]


class TemporaryTableCredentialCreateParams(TypedDict, total=False):
    operation: Required[Literal["UNKNOWN_TABLE_OPERATION", "READ", "READ_WRITE"]]

    table_id: Required[str]
    """Table id for which temporary credentials are generated.

    Can be obtained from tables/{full_name} (get table info) API.
    """
