# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing import Iterable

from typing_extensions import Literal, Required, TypedDict

__all__ = [
    "FunctionCreateParams",
    "FunctionInfo",
    "FunctionInfoInputParams",
    "FunctionInfoInputParamsParameter",
    "FunctionInfoReturnParams",
    "FunctionInfoReturnParamsParameter",
    "FunctionInfoRoutineDependencies",
    "FunctionInfoRoutineDependenciesDependency",
    "FunctionInfoRoutineDependenciesDependencyFunction",
    "FunctionInfoRoutineDependenciesDependencyTable",
]


class FunctionCreateParams(TypedDict, total=False):
    function_info: Required[FunctionInfo]


class FunctionInfoInputParamsParameter(TypedDict, total=False):
    name: Required[str]
    """Name of parameter."""

    position: Required[int]
    """Ordinal position of column (starting at position 0)."""

    type_json: Required[str]
    """Full data type spec, JSON-serialized."""

    type_name: Required[
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
    ]
    """Name of type (INT, STRUCT, MAP, etc.)."""

    type_text: Required[str]
    """Full data type spec, SQL/catalogString text."""

    comment: str
    """User-provided free-form text description."""

    parameter_default: str
    """Default value of the parameter."""

    parameter_mode: Literal["IN"]
    """The mode of the function parameter."""

    parameter_type: Literal["PARAM", "COLUMN"]
    """The type of function parameter."""

    type_interval_type: str
    """Format of IntervalType."""

    type_precision: int
    """Digits of precision; required on Create for DecimalTypes."""

    type_scale: int
    """Digits to right of decimal; Required on Create for DecimalTypes."""


class FunctionInfoInputParams(TypedDict, total=False):
    parameters: Iterable[FunctionInfoInputParamsParameter]
    """
    The array of **FunctionParameterInfo** definitions of the function's parameters.
    """


class FunctionInfoReturnParamsParameter(TypedDict, total=False):
    name: Required[str]
    """Name of parameter."""

    position: Required[int]
    """Ordinal position of column (starting at position 0)."""

    type_json: Required[str]
    """Full data type spec, JSON-serialized."""

    type_name: Required[
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
    ]
    """Name of type (INT, STRUCT, MAP, etc.)."""

    type_text: Required[str]
    """Full data type spec, SQL/catalogString text."""

    comment: str
    """User-provided free-form text description."""

    parameter_default: str
    """Default value of the parameter."""

    parameter_mode: Literal["IN"]
    """The mode of the function parameter."""

    parameter_type: Literal["PARAM", "COLUMN"]
    """The type of function parameter."""

    type_interval_type: str
    """Format of IntervalType."""

    type_precision: int
    """Digits of precision; required on Create for DecimalTypes."""

    type_scale: int
    """Digits to right of decimal; Required on Create for DecimalTypes."""


class FunctionInfoReturnParams(TypedDict, total=False):
    parameters: Iterable[FunctionInfoReturnParamsParameter]
    """
    The array of **FunctionParameterInfo** definitions of the function's parameters.
    """


class FunctionInfoRoutineDependenciesDependencyFunction(TypedDict, total=False):
    function_full_name: Required[str]
    """
    Full name of the dependent function, in the form of
    **catalog_name**.**schema_name**.**function_name**.
    """


class FunctionInfoRoutineDependenciesDependencyTable(TypedDict, total=False):
    table_full_name: Required[str]
    """
    Full name of the dependent table, in the form of
    **catalog_name**.**schema_name**.**table_name**.
    """


class FunctionInfoRoutineDependenciesDependency(TypedDict, total=False):
    function: FunctionInfoRoutineDependenciesDependencyFunction
    """A function that is dependent on a SQL object."""

    table: FunctionInfoRoutineDependenciesDependencyTable
    """A table that is dependent on a SQL object."""


class FunctionInfoRoutineDependencies(TypedDict, total=False):
    dependencies: Iterable[FunctionInfoRoutineDependenciesDependency]
    """Array of dependencies."""


class FunctionInfo(TypedDict, total=False):
    catalog_name: Required[str]
    """Name of parent catalog."""

    data_type: Required[
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
    ]
    """Name of type (INT, STRUCT, MAP, etc.)."""

    full_data_type: Required[str]
    """Pretty printed function data type."""

    input_params: Required[FunctionInfoInputParams]

    is_deterministic: Required[bool]
    """Whether the function is deterministic."""

    is_null_call: Required[bool]
    """Function null call."""

    name: Required[str]
    """Name of function, relative to parent schema."""

    parameter_style: Required[Literal["S"]]
    """Function parameter style. **S** is the value for SQL."""

    properties: Required[str]
    """JSON-serialized key-value pair map, encoded (escaped) as a string."""

    routine_body: Required[Literal["SQL", "EXTERNAL"]]
    """Function language.

    When **EXTERNAL** is used, the language of the routine function should be
    specified in the **external_language** field, and the **return_params** of the
    function cannot be used (as **TABLE** return type is not supported), and the
    **sql_data_access** field must be **NO_SQL**.
    """

    routine_definition: Required[str]
    """Function body."""

    schema_name: Required[str]
    """Name of parent schema relative to its parent catalog."""

    security_type: Required[Literal["DEFINER"]]
    """Function security type."""

    specific_name: Required[str]
    """Specific name of the function; Reserved for future use."""

    sql_data_access: Required[Literal["CONTAINS_SQL", "READS_SQL_DATA", "NO_SQL"]]
    """Function SQL data access."""

    comment: str
    """User-provided free-form text description."""

    external_language: str
    """External language of the function."""

    return_params: FunctionInfoReturnParams

    routine_dependencies: FunctionInfoRoutineDependencies
    """A list of dependencies."""
