# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from typing import List, Optional

from typing_extensions import Literal

from .._models import BaseModel

__all__ = [
    "FunctionInfo",
    "InputParams",
    "InputParamsParameter",
    "ReturnParams",
    "ReturnParamsParameter",
    "RoutineDependencies",
    "RoutineDependenciesDependency",
    "RoutineDependenciesDependencyFunction",
    "RoutineDependenciesDependencyTable",
]


class InputParamsParameter(BaseModel):
    name: str
    """Name of parameter."""

    position: int
    """Ordinal position of column (starting at position 0)."""

    type_json: str
    """Full data type spec, JSON-serialized."""

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

    type_text: str
    """Full data type spec, SQL/catalogString text."""

    comment: Optional[str] = None
    """User-provided free-form text description."""

    parameter_default: Optional[str] = None
    """Default value of the parameter."""

    parameter_mode: Optional[Literal["IN"]] = None
    """The mode of the function parameter."""

    parameter_type: Optional[Literal["PARAM", "COLUMN"]] = None
    """The type of function parameter."""

    type_interval_type: Optional[str] = None
    """Format of IntervalType."""

    type_precision: Optional[int] = None
    """Digits of precision; required on Create for DecimalTypes."""

    type_scale: Optional[int] = None
    """Digits to right of decimal; Required on Create for DecimalTypes."""


class InputParams(BaseModel):
    parameters: Optional[List[InputParamsParameter]] = None
    """
    The array of **FunctionParameterInfo** definitions of the function's parameters.
    """


class ReturnParamsParameter(BaseModel):
    name: str
    """Name of parameter."""

    position: int
    """Ordinal position of column (starting at position 0)."""

    type_json: str
    """Full data type spec, JSON-serialized."""

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

    type_text: str
    """Full data type spec, SQL/catalogString text."""

    comment: Optional[str] = None
    """User-provided free-form text description."""

    parameter_default: Optional[str] = None
    """Default value of the parameter."""

    parameter_mode: Optional[Literal["IN"]] = None
    """The mode of the function parameter."""

    parameter_type: Optional[Literal["PARAM", "COLUMN"]] = None
    """The type of function parameter."""

    type_interval_type: Optional[str] = None
    """Format of IntervalType."""

    type_precision: Optional[int] = None
    """Digits of precision; required on Create for DecimalTypes."""

    type_scale: Optional[int] = None
    """Digits to right of decimal; Required on Create for DecimalTypes."""


class ReturnParams(BaseModel):
    parameters: Optional[List[ReturnParamsParameter]] = None
    """
    The array of **FunctionParameterInfo** definitions of the function's parameters.
    """


class RoutineDependenciesDependencyFunction(BaseModel):
    function_full_name: str
    """
    Full name of the dependent function, in the form of
    **catalog_name**.**schema_name**.**function_name**.
    """


class RoutineDependenciesDependencyTable(BaseModel):
    table_full_name: str
    """
    Full name of the dependent table, in the form of
    **catalog_name**.**schema_name**.**table_name**.
    """


class RoutineDependenciesDependency(BaseModel):
    function: Optional[RoutineDependenciesDependencyFunction] = None
    """A function that is dependent on a SQL object."""

    table: Optional[RoutineDependenciesDependencyTable] = None
    """A table that is dependent on a SQL object."""


class RoutineDependencies(BaseModel):
    dependencies: Optional[List[RoutineDependenciesDependency]] = None
    """Array of dependencies."""


class FunctionInfo(BaseModel):
    catalog_name: Optional[str] = None
    """Name of parent catalog."""

    comment: Optional[str] = None
    """User-provided free-form text description."""

    created_at: Optional[int] = None
    """Time at which this function was created, in epoch milliseconds."""

    data_type: Optional[
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

    external_language: Optional[str] = None
    """External language of the function."""

    full_data_type: Optional[str] = None
    """Pretty printed function data type."""

    full_name: Optional[str] = None
    """
    Full name of function, in form of
    **catalog_name**.**schema_name**.**function**name\\__\\__
    """

    function_id: Optional[str] = None
    """Id of Function, relative to parent schema."""

    input_params: Optional[InputParams] = None

    is_deterministic: Optional[bool] = None
    """Whether the function is deterministic."""

    is_null_call: Optional[bool] = None
    """Function null call."""

    name: Optional[str] = None
    """Name of function, relative to parent schema."""

    parameter_style: Optional[Literal["S"]] = None
    """Function parameter style. **S** is the value for SQL."""

    properties: Optional[str] = None
    """JSON-serialized key-value pair map, encoded (escaped) as a string."""

    return_params: Optional[ReturnParams] = None

    routine_body: Optional[Literal["SQL", "EXTERNAL"]] = None
    """Function language.

    When **EXTERNAL** is used, the language of the routine function should be
    specified in the **external_language** field, and the **return_params** of the
    function cannot be used (as **TABLE** return type is not supported), and the
    **sql_data_access** field must be **NO_SQL**.
    """

    routine_definition: Optional[str] = None
    """Function body."""

    routine_dependencies: Optional[RoutineDependencies] = None
    """A list of dependencies."""

    schema_name: Optional[str] = None
    """Name of parent schema relative to its parent catalog."""

    security_type: Optional[Literal["DEFINER"]] = None
    """Function security type."""

    specific_name: Optional[str] = None
    """Specific name of the function; Reserved for future use."""

    sql_data_access: Optional[Literal["CONTAINS_SQL", "READS_SQL_DATA", "NO_SQL"]] = None
    """Function SQL data access."""

    updated_at: Optional[int] = None
    """Time at which this function was last updated, in epoch milliseconds."""
