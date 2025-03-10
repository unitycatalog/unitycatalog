import ast
import decimal
import inspect
import json
import logging
import os
from hashlib import md5
from io import StringIO
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
import pydantic
from packaging.version import Version
from pydantic import Field, create_model

from unitycatalog.ai.core.utils.config import JSON_SCHEMA_TYPE, UC_LIST_FUNCTIONS_MAX_RESULTS
from unitycatalog.ai.core.utils.pydantic_utils import (
    PydanticField,
    PydanticFunctionInputParams,
    PydanticType,
)
from unitycatalog.ai.core.utils.type_utils import (
    UC_DEFAULT_VALUE_TO_PYTHON_EQUIVALENT_MAPPING,
    UC_TYPE_JSON_MAPPING,
)
from unitycatalog.ai.core.utils.validation_utils import FullFunctionName

_logger = logging.getLogger(__name__)

IS_PYDANTIC_V2_OR_NEWER = Version(pydantic.VERSION).major >= 2


def uc_type_json_to_pydantic_type(
    uc_type_json: Union[str, Dict[str, Any]], strict: bool = False
) -> PydanticType:
    """
    Convert Unity Catalog type json to Pydantic type.

    For simple types, the type json is a string representing the type name. For example:
        "STRING" -> str
        "INTEGER" -> int
    For complex types, the type json is a dictionary representing the type. For example:
        {"type": "array", "elementType": "STRING", "containsNull": true} -> List[Optional[str]]

    Args:
        uc_type_json: The Unity Catalog function input parameter type json.
        strict: Whether the type strictly follows the JSON schema type. This is used for OpenAI only.

    Returns:
        PydanticType:
            pydantic_type: The python type or Pydantic type.
            strict: Whether the type strictly follows the JSON schema type. This is used for OpenAI only.
    """
    if isinstance(uc_type_json, str):
        type_name = uc_type_json.upper()
        if type_name in UC_TYPE_JSON_MAPPING:
            pydantic_type = Union[UC_TYPE_JSON_MAPPING[type_name]]
        # the type text contains the precision and scale
        elif type_name.startswith("DECIMAL"):
            pydantic_type = Union[decimal.Decimal, float]
        else:
            raise TypeError(
                f"Type {uc_type_json} is not supported. Supported "
                f"types are: {UC_TYPE_JSON_MAPPING.keys()}"
            )
        if type_name not in JSON_SCHEMA_TYPE:
            strict = False
    elif isinstance(uc_type_json, dict):
        type_ = uc_type_json["type"]
        if type_ == "array":
            element_pydantic_type = uc_type_json_to_pydantic_type(
                uc_type_json["elementType"], strict=strict
            )
            strict = strict and element_pydantic_type.strict
            element_type = element_pydantic_type.pydantic_type
            if uc_type_json["containsNull"]:
                element_type = Optional[element_type]
            pydantic_type = Union[List[element_type], Tuple[element_type, ...]]
        elif type_ == "map":
            key_type = uc_type_json["keyType"]
            if key_type != "string":
                raise TypeError(f"Only support STRING key type for MAP but got {key_type}.")
            value_pydantic_type = uc_type_json_to_pydantic_type(
                uc_type_json["valueType"], strict=strict
            )
            strict = strict and value_pydantic_type.strict
            value_type = value_pydantic_type.pydantic_type
            if uc_type_json["valueContainsNull"]:
                value_type = Optional[value_type]
            pydantic_type = Dict[str, value_type]
        elif type_ == "struct":
            fields = {}
            for field in uc_type_json["fields"]:
                field_pydantic_type = uc_type_json_to_pydantic_type(field["type"])
                strict = strict and field_pydantic_type.strict
                field_type = field_pydantic_type.pydantic_type
                comment = field.get("metadata", {}).get("comment")
                if field.get("nullable"):
                    field_type = Optional[field_type]
                    fields[field["name"]] = (field_type, Field(default=None, description=comment))
                else:
                    fields[field["name"]] = (field_type, Field(..., description=comment))
            uc_type_json_str = json.dumps(uc_type_json, sort_keys=True)
            type_hash = md5(uc_type_json_str.encode(), usedforsecurity=False).hexdigest()[:8]
            pydantic_type = create_model(f"Struct_{type_hash}", **fields)
    else:
        raise TypeError(f"Unknown type {uc_type_json}.")
    return PydanticType(pydantic_type=pydantic_type, strict=strict)


def get_tool_name(func_name: str) -> str:
    # OpenAI has constriant on the function name:
    # Must be a-z, A-Z, 0-9, or contain underscores and dashes, with a maximum length of 64.
    full_func_name = FullFunctionName.validate_full_function_name(func_name)
    tool_name = full_func_name.to_tool_name()
    if len(tool_name) > 64:
        _logger.warning(
            f"Function name {tool_name} is too long, truncating to 64 characters {tool_name[-64:]}."
        )
        return tool_name[-64:]
    return tool_name


def construct_original_function_name(tool_name: str) -> str:
    """
    Args:
        tool_name: The tool name in the form of `catalog__schema__function`.
            It's the tool name converted by `get_tool_name` function.

    Note:
        This only works if catalog, schema and function names in the
        original function name don't include `__` and the total length
        of the original function name is less than 64 characters.

    Returns:
        Original function name in the form of `catalog.schema.function`.
    """
    parts = tool_name.split("__")
    if len(parts) != 3:
        raise ValueError(f"Invalid tool name: {tool_name}")
    return ".".join(parts)


def process_function_names(
    function_names: List[str],
    tools_dict: Dict[str, Any],
    client,
    uc_function_to_tool_func: Callable,
    **kwargs,
) -> Dict[str, Any]:
    """
    Process function names and update the tools dictionary.
    Iterates over the provided function names, converts them into tool instances
    using the provided conversion function, and updates the `tools_dict`.
    Handles wildcard function names (e.g., function name ending with '*') by
    listing all functions in the specified catalog and schema.
    Args:
        function_names (List[str]): A list of function names to process.
        tools_dict (Dict[str, Any]): A dictionary to store the tool instances.
        client: The client used to list functions.
        uc_function_to_tool_func (Callable): A function that converts a UC function
            into a tool instance. This function should accept kwargs only to make
            sure the parameters are passed correctly.
        **kwargs: Additional keyword arguments to pass to the conversion function for
            tool framework-specific configuration.
    Returns:
        Dict[str, Any]: The updated tools dictionary.
    """
    max_results = int(
        os.environ.get("UC_LIST_FUNCTIONS_MAX_RESULTS", UC_LIST_FUNCTIONS_MAX_RESULTS)
    )
    for name in function_names:
        if name not in tools_dict:
            full_func_name = FullFunctionName.validate_full_function_name(name)
            if full_func_name.function == "*":
                token = None
                # functions with BROWSE permission should not be included since this
                # function should include only the functions that can be executed
                list_kwarg = (
                    {"include_browse": False}
                    if "include_browse" in inspect.signature(client.list_functions).parameters
                    else {}
                )
                while True:
                    functions = client.list_functions(
                        catalog=full_func_name.catalog,
                        schema=full_func_name.schema,
                        max_results=max_results,
                        page_token=token,
                        **list_kwarg,
                    )
                    # TODO: get functions in parallel
                    for f in functions:
                        if f.full_name not in tools_dict:
                            tools_dict[f.full_name] = uc_function_to_tool_func(
                                function_name=f.full_name, client=client, **kwargs
                            )
                    token = functions.token
                    if token is None:
                        break
            else:
                tool = uc_function_to_tool_func(function_name=name, client=client, **kwargs)
                # Skip adding this tool if this function returns None
                if tool:
                    tools_dict[name] = tool
    return tools_dict


def param_info_to_pydantic_type(param_info: Any, strict: bool = False) -> PydanticField:
    """
    Convert Unity Catalog function parameter information to Pydantic type.

    Args:
        param_info: The Unity Catalog function parameter information.
            It must be either databricks.sdk.service.catalog.FunctionParameterInfo or
            unitycatalog.types.function_info.InputParamsParameter object.
        strict: Whether the type strictly follows the JSON schema type. This is used for OpenAI only.
    """
    if not isinstance(param_info, supported_param_info_types()):
        raise TypeError(f"Unsupported parameter info type: {type(param_info)}")
    if param_info.type_json is None:
        raise ValueError(f"Parameter type json is None for parameter {param_info.name}.")
    type_json = json.loads(param_info.type_json)
    nullable = type_json.get("nullable")
    pydantic_type = uc_type_json_to_pydantic_type(type_json["type"], strict=strict)
    pydantic_field_type = pydantic_type.pydantic_type
    default = None
    description = param_info.comment or ""
    if param_info.parameter_default:
        # Note: DEFAULT is supported for LANGUAGE SQL only.
        # TODO: verify this for all types
        default = json.loads(param_info.parameter_default)
        description = f"{description} (Default: {param_info.parameter_default})"
    elif nullable:
        pydantic_field_type = Optional[pydantic_field_type]
    return PydanticField(
        pydantic_type=pydantic_field_type,
        description=description,
        default=default,
        strict=pydantic_type.strict,
    )


def generate_function_input_params_schema(
    function_info: Any, strict: bool = False
) -> PydanticFunctionInputParams:
    """
    Generate a Pydantic model based on a Unity Catalog function information.

    Args:
        function_info: The Unity Catalog function information.
            It must either be databricks.sdk.service.catalog.FunctionInfo or
            unitycatalog.types.function_info.FunctionInfo object.
        strict: Whether the type strictly follows the JSON schema type. This is used for OpenAI only.

    Returns:
        PydanticFunctionInputParams:
            pydantic_model: The Pydantic model representing the function input parameters.
            strict: Whether the type strictly follows the JSON schema type. This is used for OpenAI only.
    """
    if not isinstance(function_info, supported_function_info_types()):
        raise TypeError(f"Unsupported function info type: {type(function_info)}")
    params_name = (
        f"{function_info.catalog_name}__{function_info.schema_name}__{function_info.name}__params"
    )
    if function_info.input_params is None:
        return PydanticFunctionInputParams(pydantic_model=create_model(params_name), strict=strict)
    param_infos = function_info.input_params.parameters
    if param_infos is None:
        raise ValueError("Function input parameters are None.")
    fields = {}
    for param_info in param_infos:
        pydantic_field = param_info_to_pydantic_type(param_info, strict=strict)
        fields[param_info.name] = (
            pydantic_field.pydantic_type,
            Field(default=pydantic_field.default, description=pydantic_field.description),
        )
    if IS_PYDANTIC_V2_OR_NEWER:
        model = create_model(params_name, **fields, __config__={"extra": "forbid"})
    else:
        model = create_model(params_name, **fields, config=pydantic.ConfigDict(extra="forbid"))

    return PydanticFunctionInputParams(pydantic_model=model, strict=pydantic_field.strict)


# TODO: add UC OSS support
def supported_param_info_types():
    types = ()
    try:
        from databricks.sdk.service.catalog import FunctionParameterInfo

        types += (FunctionParameterInfo,)
    except ImportError:
        pass

    try:
        from unitycatalog.client.models import FunctionParameterInfo as UCFunctionParameterInfo

        types += (UCFunctionParameterInfo,)
    except ImportError:
        pass

    return types


# TODO: add UC OSS support
def supported_function_info_types():
    types = ()
    try:
        from databricks.sdk.service.catalog import FunctionInfo

        types += (FunctionInfo,)
    except ImportError:
        pass
    try:
        from unitycatalog.client.models import FunctionInfo as UCFunctionInfo

        types += (UCFunctionInfo,)
    except ImportError:
        pass

    return types


def process_retriever_output(result: "FunctionExecutionResult") -> List[Dict[str, Any]]:
    """
    Process retriever output from result into mlflow.entities.Document format for tracing.

    Args:
        result: The result of the function execution to be processed.

    Returns:
        Retriever output formatted into a list of Documents.
    """
    if result.format == "CSV":
        df = pd.read_csv(StringIO(result.value))
        if "metadata" in df.columns:
            df["metadata"] = df["metadata"].apply(ast.literal_eval)
        output = df.to_dict(orient="records")
    else:
        value = result.value
        output = ast.literal_eval(value) if isinstance(value, str) else value

    return output


def _execute_uc_function_with_retriever_tracing(
    _execute_uc_function: Callable,
    function_info: "FunctionInfo",
    parameters: Dict[str, Any],
    **kwargs: Any,
) -> "FunctionExecutionResult":
    """
    Executes a UC function with MLflow tracing with span type RETRIEVER enabled. If MLflow cannot
    be imported, the function executes without tracing and logs a warning.

    Args:
        _execute_uc_function (Callable): A function that executes the given UC function.
        function_info (FunctionInfo): Metadata about the UC function to be executed.
        parameters (Dict[str, Any]): Parameters to be passed to the function during execution.
        **kwargs (Any): Additional keyword arguments to be passed to the function.

    Returns:
        Any: The output of the function execution.
    """
    try:
        import mlflow
        from mlflow.entities import SpanType

        result = None

        @mlflow.trace(name=function_info.full_name, span_type=SpanType.RETRIEVER)
        def execute_retriever(parameters):
            # Set inputs manually so we log {"query": "..."} instead of {"parameters": {"query": "..."}}
            if span := mlflow.get_current_active_span():
                span.set_inputs(parameters)

            nonlocal result
            result = _execute_uc_function(function_info, parameters, **kwargs)

            # Re-raise errors so they can get traced
            if result.error:
                raise Exception(result.error)

            return process_retriever_output(result)

        try:
            execute_retriever(parameters)
        except Exception:  # Catch all errors that are re-raised
            pass

        return result
    except ImportError as e:
        _logger.warn(
            f"Skipping tracing {function_info.full_name} as a retriever because of the following error:\n {e}"
        )
        return _execute_uc_function(function_info, parameters, **kwargs)


def process_function_parameter_defaults(
    function_info: "FunctionInfo", parameters: Optional[dict[str, Any]] = None
) -> dict[str, Any]:
    """
    Handle default values for input parameters.

    Args:
        function_info: The FunctionInfo object containing the function metadata.
        parameters: The parameters to handle.

    Returns:
        The updated parameters with default values filled in.
    """
    defaults = {}
    if function_info.input_params and function_info.input_params.parameters:
        for param in function_info.input_params.parameters:
            if param.parameter_default is not None:
                default_str = param.parameter_default.strip()
                upper_str = default_str.upper()
                if upper_str in UC_DEFAULT_VALUE_TO_PYTHON_EQUIVALENT_MAPPING:
                    defaults[param.name] = UC_DEFAULT_VALUE_TO_PYTHON_EQUIVALENT_MAPPING[upper_str]
                else:
                    try:
                        defaults[param.name] = ast.literal_eval(default_str)
                    except ValueError:
                        defaults[param.name] = default_str
    if parameters is None:
        parameters = {}
    return defaults | parameters
