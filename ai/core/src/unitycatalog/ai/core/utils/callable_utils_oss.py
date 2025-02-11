import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, List

from unitycatalog.ai.core.utils.callable_utils import (
    FunctionMetadata,
    extract_function_metadata,
    extract_wrapped_functions,
)

if TYPE_CHECKING:
    from unitycatalog.client.models import FunctionParameterInfo


@dataclass
class FunctionInfoDefinition:
    """
    Dataclass to define information about a function for Unity Catalog.

    Attributes:
        callable_name: The name of the callable function.
        routine_definition: The body of the function.
        data_type: The base SQL data type of the function's return value.
        full_data_type: The full SQL data type of the function's return value.
        parameters: List of function parameter information.
        comment: Description of the function.
    """

    callable_name: str
    routine_definition: str
    data_type: str
    full_data_type: str
    parameters: List["FunctionParameterInfo"]
    comment: str


def generate_function_info(func: Callable[..., Any]) -> FunctionInfoDefinition:
    """
    Generates a FunctionInfoDefinition object for a given Python function.

    This object encapsulates all necessary information about the function,
    including its name, definition, data types, parameters, and comments,
    which can be used for integration with Unity Catalog.

    Args:
        func: The Python function to generate information for.

    Returns:
        An object containing detailed information about the function.
    """

    metadata = extract_function_metadata(func)

    parameters = create_parameter_info(metadata)

    return FunctionInfoDefinition(
        callable_name=metadata.func_name,
        routine_definition=metadata.function_body,
        data_type=metadata.base_return_type_name,
        full_data_type=metadata.sql_return_type,
        parameters=parameters,
        comment=metadata.docstring_info.description,
    )


def generate_wrapped_function_info(
    primary_func: Callable[..., Any], functions: list[Callable[..., Any]]
) -> FunctionInfoDefinition:
    """
    Generates a FunctionInfoDefinition object for a primary function and its wrapped functions.

    This object encapsulates all necessary information about the primary function and in-lines the wrapped functions into the primary function's definition.

    Args:
        primary_func: The primary Python function to generate information for.
        functions: List of wrapped functions to be in-lined into the primary function.

    Returns:
        An object containing detailed information about the primary function and its wrapped functions.
    """

    wrapped_function_content = extract_wrapped_functions(functions)

    primary_metadata = extract_function_metadata(primary_func)

    primary_metadata.function_body = (
        wrapped_function_content + "\n" + primary_metadata.function_body
    )

    parameters = create_parameter_info(primary_metadata)

    return FunctionInfoDefinition(
        callable_name=primary_metadata.func_name,
        routine_definition=primary_metadata.function_body,
        data_type=primary_metadata.base_return_type_name,
        full_data_type=primary_metadata.sql_return_type,
        parameters=parameters,
        comment=primary_metadata.docstring_info.description,
    )


def create_parameter_info(metadata: FunctionMetadata) -> list["FunctionParameterInfo"]:
    """
    Creates a list of FunctionParameterInfo objects from the metadata.

    Args:
        metadata: The metadata object containing parameter information.

    Returns:
        A list of FunctionParameterInfo objects.
    """

    from unitycatalog.client.models import FunctionParameterInfo

    parameters = []
    for param_info in metadata.parameters:
        # Remove when Unity Catalog OSS supports VARIANT TYPE
        if param_info["sql_type"] == "VARIANT":
            raise ValueError(
                "Variant type is not supported for function parameters. "
                "Please use a concrete type instead in your function signature. "
                f"Argument {param_info['name']} uses an unsupported type."
            )
        type_json_dict = {
            "name": param_info["name"],
            "type": param_info["base_type_name"].lower(),
            "nullable": param_info["parameter_default"] is not None,
            "metadata": {"comment": param_info["comment"] or ""},
        }
        type_json_str = json.dumps(type_json_dict)

        function_param_info = FunctionParameterInfo(
            name=param_info["name"],
            type_name=param_info["base_type_name"],
            type_text=param_info["sql_type"],
            type_json=type_json_str,
            position=param_info["position"],
            parameter_default=param_info["parameter_default"],
            comment=param_info["comment"],
        )
        parameters.append(function_param_info)
    return parameters
