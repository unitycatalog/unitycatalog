# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from typing import List, Optional

from .._models import BaseModel
from .volume_info import VolumeInfo

__all__ = ["VolumeListResponse"]


class VolumeListResponse(BaseModel):
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results.

    Absent if there are no more pages. **page_token** should be set to this value
    for the next request to retrieve the next page of results.
    """

    volumes: Optional[List[VolumeInfo]] = None
