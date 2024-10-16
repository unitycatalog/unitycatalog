# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing import Dict, Iterable

from typing_extensions import Literal, Required, TypedDict

__all__ = ["TableCreateParams", "Column"]


class TableCreateParams(TypedDict, total=False):
    catalog_name: Required[str]
    """Name of parent catalog."""

    columns: Required[Iterable[Column]]
    """The array of **ColumnInfo** definitions of the table's columns."""

    data_source_format: Required[Literal["DELTA", "CSV", "JSON", "AVRO", "PARQUET", "ORC", "TEXT"]]
    """Data source format"""

    name: Required[str]
    """Name of table, relative to parent schema."""

    schema_name: Required[str]
    """Name of parent schema relative to its parent catalog."""

    table_type: Required[Literal["MANAGED", "EXTERNAL"]]

    comment: str
    """User-provided free-form text description."""

    properties: Dict[str, str]
    """A map of key-value properties attached to the securable."""

    storage_location: str
    """Storage root URL for table (for **MANAGED**, **EXTERNAL** tables)"""


class Column(TypedDict, total=False):
    comment: str
    """User-provided free-form text description."""

    name: str
    """Name of Column."""

    nullable: bool
    """Whether field may be Null."""

    partition_index: int
    """Partition index for column."""

    position: int
    """Ordinal position of column (starting at position 0)."""

    type_interval_type: str
    """Format of IntervalType."""

    type_json: str
    """Full data type specification, JSON-serialized."""

    type_name: Literal[
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
    """Name of type (INT, STRUCT, MAP, etc.)."""

    type_precision: int
    """Digits of precision; required for DecimalTypes."""

    type_scale: int
    """Digits to right of decimal; Required for DecimalTypes."""

    type_text: str
    """Full data type specification as SQL/catalogString text."""
