# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from typing import List, Optional

from .._models import BaseModel
from .catalog_info import CatalogInfo

__all__ = ["CatalogListResponse"]


class CatalogListResponse(BaseModel):
    catalogs: Optional[List[CatalogInfo]] = None
    """An array of catalog information objects."""

    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results.

    Absent if there are no more pages. **page_token** should be set to this value
    for the next request (for the next page of results).
    """
