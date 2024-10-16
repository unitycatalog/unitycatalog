# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from typing import List, Optional

from .._models import BaseModel
from .schema_info import SchemaInfo

__all__ = ["SchemaListResponse"]


class SchemaListResponse(BaseModel):
    next_page_token: Optional[str] = None
    """Opaque token to retrieve the next page of results.

    Absent if there are no more pages. **page_token** should be set to this value
    for the next request (for the next page of results).
    """

    schemas: Optional[List[SchemaInfo]] = None
    """An array of schema information objects."""
