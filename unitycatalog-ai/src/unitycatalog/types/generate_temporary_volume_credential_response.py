# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from typing import Optional

from .._models import BaseModel

__all__ = ["GenerateTemporaryVolumeCredentialResponse", "AwsTempCredentials"]


class AwsTempCredentials(BaseModel):
    access_key_id: Optional[str] = None
    """The access key ID that identifies the temporary credentials."""

    secret_access_key: Optional[str] = None
    """The secret access key that can be used to sign AWS API requests."""

    session_token: Optional[str] = None
    """The token that users must pass to AWS API to use the temporary credentials."""


class GenerateTemporaryVolumeCredentialResponse(BaseModel):
    aws_temp_credentials: Optional[AwsTempCredentials] = None

    expiration_time: Optional[int] = None
    """
    Server time when the credential will expire, in epoch milliseconds. The API
    client is advised to cache the credential given this expiration time.
    """
