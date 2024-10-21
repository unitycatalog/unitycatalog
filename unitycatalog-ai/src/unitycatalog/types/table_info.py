# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from typing import Dict, List, Optional

from typing_extensions import Literal

from .._models import BaseModel

__all__ = ["TableInfo", "Column"]


class Column(BaseModel):
    comment: Optional[str] = None
    """User-provided free-form text description."""

    name: Optional[str] = None
    """Name of Column."""

    nullable: Optional[bool] = None
    """Whether field may be Null."""

    partition_index: Optional[int] = None
    """Partition index for column."""

    position: Optional[int] = None
    """Ordinal position of column (starting at position 0)."""

    type_interval_type: Optional[str] = None
    """Format of IntervalType."""

    type_json: Optional[str] = None
    """Full data type specification, JSON-serialized."""

    type_name: Optional[
        Literal[
            "BOOLEAN",
            "BYTE",
            "SHORT",
            "INT",
            "LONG",
            "FLOAT",
            "DOUBLE",
            "DATE",
            "TIMESTAMP",
            "TIMESTAMP_NTZ",
            "STRING",
            "BINARY",
            "DECIMAL",
            "INTERVAL",
            "ARRAY",
            "STRUCT",
            "MAP",
            "CHAR",
            "NULL",
            "USER_DEFINED_TYPE",
            "TABLE_TYPE",
        ]
    ] = None
    """Name of type (INT, STRUCT, MAP, etc.)."""

    type_precision: Optional[int] = None
    """Digits of precision; required for DecimalTypes."""

    type_scale: Optional[int] = None
    """Digits to right of decimal; Required for DecimalTypes."""

    type_text: Optional[str] = None
    """Full data type specification as SQL/catalogString text."""


class TableInfo(BaseModel):
    catalog_name: Optional[str] = None
    """Name of parent catalog."""

    columns: Optional[List[Column]] = None
    """The array of **ColumnInfo** definitions of the table's columns."""

    comment: Optional[str] = None
    """User-provided free-form text description."""

    created_at: Optional[int] = None
    """Time at which this table was created, in epoch milliseconds."""

    data_source_format: Optional[
        Literal["DELTA", "CSV", "JSON", "AVRO", "PARQUET", "ORC", "TEXT"]
    ] = None
    """Data source format"""

    name: Optional[str] = None
    """Name of table, relative to parent schema."""

    properties: Optional[Dict[str, str]] = None
    """A map of key-value properties attached to the securable."""

    schema_name: Optional[str] = None
    """Name of parent schema relative to its parent catalog."""

    storage_location: Optional[str] = None
    """Storage root URL for table (for **MANAGED**, **EXTERNAL** tables)"""

    table_id: Optional[str] = None
    """Unique identifier for the table."""

    table_type: Optional[Literal["MANAGED", "EXTERNAL"]] = None

    updated_at: Optional[int] = None
    """Time at which this table was last modified, in epoch milliseconds."""
