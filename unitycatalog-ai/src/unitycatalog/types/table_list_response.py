# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from typing import List, Optional

from .._models import BaseModel
from .table_info import TableInfo

__all__ = ["TableListResponse"]


class TableListResponse(BaseModel):
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results.

    Absent if there are no more pages. **page_token** should be set to this value
    for the next request (for the next page of results).
    """

    tables: Optional[List[TableInfo]] = None
    """An array of table information objects."""
