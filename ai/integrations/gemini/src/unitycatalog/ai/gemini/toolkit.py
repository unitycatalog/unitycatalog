import hashlib
import json
import logging
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any, Callable, Dict, List, Optional, Union

from google.generativeai.types import CallableFunctionDeclaration, content_types
from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_core import to_jsonable_python

from unitycatalog.ai.core.client import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import (
    get_tool_name,
    param_info_to_pydantic_type,
    process_function_names,
)
from unitycatalog.ai.core.utils.pydantic_utils import (
    PydanticFunctionInputParams,
)
from unitycatalog.ai.core.utils.type_utils import UC_TYPE_JSON_MAPPING
from unitycatalog.ai.core.utils.validation_utils import mlflow_tracing_enabled

_logger = logging.getLogger(__name__)

# The Gemini FunctionDeclaration name allows a-z, A-Z, 0-9, underscores and dashes, with a maximum
# length of 63.
_GEMINI_NAME_MAX_LENGTH = 63
_GEMINI_NAME_DISALLOWED_CHARACTERS = re.compile(r"[^a-zA-Z0-9_-]")
_DISAMBIGUATION_HASH_LENGTH = 8


def _gemini_function_name(function_name: str) -> str:
    """
    Returns the name Gemini declares a UC function by: its tool name, with every character that
    Gemini does not allow replaced by an underscore, cut to its last 63 characters.
    """
    name = _GEMINI_NAME_DISALLOWED_CHARACTERS.sub("_", get_tool_name(function_name))
    return name[-_GEMINI_NAME_MAX_LENGTH:]


def _disambiguated_gemini_function_name(name: str, function_name: str) -> str:
    """
    Returns `name` with a short hash of the full function name appended, still within 63
    characters, for a function whose declared name another function in the toolkit also has.
    """
    digest = hashlib.sha256(function_name.encode("utf-8")).hexdigest()
    suffix = "_" + digest[:_DISAMBIGUATION_HASH_LENGTH]
    return name[-(_GEMINI_NAME_MAX_LENGTH - len(suffix)) :] + suffix


def _function_call_value_to_python(value: Any) -> Any:
    """
    Converts a value that `to_jsonable_python` does not know into a JSON-compatible one.

    Gemini passes the STRUCT, ARRAY and MAP arguments of a function call as protobuf
    `MapComposite` and `RepeatedComposite` objects, which are converted to a dict and a list.
    Any other value is converted to its string form.
    """
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, Sequence):
        return list(value)
    return str(value)


# Built from the mapping the function clients validate arguments against, so both agree on which
# types need an int.
_INTEGER_TYPE_NAMES = frozenset(
    name for name, python_type in UC_TYPE_JSON_MAPPING.items() if python_type is int
)

# A double holds every integer below 2**53 exactly. A float of 2**53 or more may be a rounded
# integer, such as 2**53 + 1 arriving as 2**53.
_EXACT_FLOAT_INTEGER_LIMIT = 2**53


def _restore_integers(value: Any, uc_type_json: Union[str, Dict[str, Any]]) -> Any:
    """
    Converts the integral floats in a function call argument back to int where the UC type is an
    integer type, including in STRUCT fields, ARRAY elements and MAP values.

    Gemini passes the arguments of a function call as a protobuf Struct, which returns every number
    as a float. A float of 2**53 or more is not converted, since it may be a rounded integer. Any
    other value is returned as it is. The function client checks only the type of a top-level
    value, so it rejects such a float for an integer parameter, but a value nested in a STRUCT,
    ARRAY or MAP is not validated.
    """
    if isinstance(uc_type_json, str):
        if (
            uc_type_json.upper() in _INTEGER_TYPE_NAMES
            and isinstance(value, float)
            and value.is_integer()
            and abs(value) < _EXACT_FLOAT_INTEGER_LIMIT
        ):
            return int(value)
        return value
    type_ = uc_type_json["type"]
    if type_ == "array" and isinstance(value, list):
        return [_restore_integers(element, uc_type_json["elementType"]) for element in value]
    if type_ == "map" and isinstance(value, dict):
        return {
            key: _restore_integers(item, uc_type_json["valueType"]) for key, item in value.items()
        }
    if type_ == "struct" and isinstance(value, dict):
        field_types = {field["name"]: field["type"] for field in uc_type_json["fields"]}
        return {
            key: _restore_integers(item, field_types[key]) if key in field_types else item
            for key, item in value.items()
        }
    return value


class GeminiTool(BaseModel):
    """
    Model representing an Autogen tool.
    """

    fn: Callable = Field(
        description="Callable that will be used to execute the UC Function, registered to the Gemini Agent definition"
    )

    name: str = Field(
        description="The name of the function.",
    )
    description: str = Field(
        description="A brief description of the function's purpose.",
    )
    schema: Dict = Field(description="Gemini compatible Tool Definition")

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the Autogen Tool instance into a dictionary for the Autogen API.
        """
        return {"name": self.name, "description": self.description, "schema": self.schema}


def _with_unique_names(tools_dict: Dict[str, GeminiTool]) -> Dict[str, GeminiTool]:
    """
    Gives every tool whose declared name another tool in the toolkit shares a name of its own,
    since Gemini rejects two declarations with the same name.
    """
    counts = Counter(tool.name for tool in tools_dict.values())
    unique_tools = {}
    for function_name, tool in tools_dict.items():
        if counts[tool.name] > 1:
            name = _disambiguated_gemini_function_name(tool.name, function_name)
            tool = tool.model_copy(update={"name": name, "schema": {**tool.schema, "name": name}})
        unique_tools[function_name] = tool
    return unique_tools


class UCFunctionToolkit(BaseModel):
    """
    A toolkit for managing Unity Catalog functions and converting them into Autogen tools.
    """

    function_names: List[str] = Field(
        description="List of function names in 'catalog.schema.function' format.",
    )
    tools_dict: Dict[str, GeminiTool] = Field(
        default_factory=dict,
        description="Dictionary mapping function names to their corresponding tools.",
    )
    client: Optional[BaseFunctionClient] = Field(
        default=None, description="The client for managing functions."
    )
    filter_accessible_functions: bool = Field(
        default=False,
        description="When set to true, UCFunctionToolkit is initialized with functions that only the client has access to",
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def validate_toolkit(self) -> "UCFunctionToolkit":
        """
        Validates the toolkit, ensuring the client is properly set and function names are processed.
        """
        self.client = validate_or_set_default_client(self.client)

        if not self.function_names:
            raise ValueError("Cannot create tool instances without function_names being provided.")

        self.tools_dict = process_function_names(
            function_names=self.function_names,
            tools_dict=self.tools_dict,
            client=self.client,
            filter_accessible_functions=self.filter_accessible_functions,
            uc_function_to_tool_func=self.uc_function_to_gemini_tool,
        )
        self.tools_dict = _with_unique_names(self.tools_dict)
        return self

    @staticmethod
    def convert_to_gemini_schema(
        function_info: PydanticFunctionInputParams,
        strict: str = True,
        name: Optional[str] = None,
    ):
        """
        Converts Unity Catalog function metadata into a Gemini-compatible schema.

        Args:
            function_info (PydanticFunctionInputParams):
                The input parameter metadata of the UC function.
            strict (bool):
                Indicates whether to enforce strict typing rules in parameter conversion.
            name (Optional[str]):
                The name to declare the function by. Defaults to the Gemini name built from the
                function's `full_name`.

        Returns:
            Dict[str, Any]: A dictionary containing the Gemini-compatible function schema.
        """

        fields_dict = {}
        param_infos = function_info.input_params.parameters if function_info.input_params else []
        if param_infos is None:
            raise ValueError("Function input parameters are None.")

        for param_info in param_infos:
            pydantic_field = param_info_to_pydantic_type(param_info, strict=strict)
            fields_dict[param_info.name] = (
                pydantic_field.pydantic_type,
                Field(description=pydantic_field.description),
            )

        parameters = content_types._build_schema(function_info.name, fields_dict)

        parameters["required"] = [
            k.name
            for k in param_infos
            if (
                k.parameter_default is None
                and (
                    k.parameter_type is None or getattr(k.parameter_type, "value", None) == "PARAM"
                )
            )
        ]
        schema = dict(
            name=name or _gemini_function_name(function_info.full_name),
            description=function_info.comment,
        )
        if parameters["properties"]:
            schema["parameters"] = parameters
        return schema

    @staticmethod
    def uc_function_to_gemini_tool(
        *,
        client: Optional[BaseFunctionClient] = None,
        function_name: Optional[str] = None,
        function_info: Optional[Any] = None,
        filter_accessible_functions: bool = False,
    ) -> Optional[GeminiTool]:
        """
        Converts a Unity Catalog function to an Autogen tool.

        Args:
            client (Optional[BaseFunctionClient]): The client for managing functions.
            function_name (Optional[str]): The full name of the function in 'catalog.schema.function' format.
            function_info (Optional[Any]): The function info object returned by the client.

        Returns:
            GeminiTool: The corresponding Autogen tool.
        """
        if function_name and function_info:
            raise ValueError("Only one of function_name or function_info should be provided.")
        client = validate_or_set_default_client(client)

        if function_name:
            try:
                function_info = client.get_function(function_name)
            except PermissionError as e:
                _logger.info(f"Skipping {function_name} due to permission errors.")
                if filter_accessible_functions:
                    return None
                raise e
        elif function_info:
            function_name = function_info.full_name
        else:
            raise ValueError("Either function_name or function_info should be provided.")

        # Gemini dispatches by the declared name, and the client's full_name can differ in case.
        tool_name = _gemini_function_name(function_name)
        schema = UCFunctionToolkit.convert_to_gemini_schema(
            function_info, strict=True, name=tool_name
        )
        param_infos = function_info.input_params.parameters if function_info.input_params else []
        params_type_json = {
            "type": "struct",
            "fields": [
                {"name": param_info.name, "type": json.loads(param_info.type_json)["type"]}
                for param_info in param_infos
            ],
        }

        def func(**kwargs: Any) -> str:
            arguments = to_jsonable_python(kwargs, fallback=_function_call_value_to_python)
            result = client.execute_function(
                function_name=function_name,
                parameters=_restore_integers(arguments, params_type_json),
                enable_retriever_tracing=mlflow_tracing_enabled("gemini"),
            )

            return result.to_json()

        return GeminiTool(
            fn=func,
            name=tool_name,
            description=function_info.comment or "",
            schema=schema,
        )

    @property
    def tools(self) -> List[GeminiTool]:
        """
        Retrieves the list of Gemini tools managed by the toolkit.
        """
        return list(self.tools_dict.values())

    def generate_callable_tool_list(self) -> None:
        """
        Converts all managed tools into a list of CallableFunctionDeclaration objects
        compatible with the Gemini agent.

        Returns:
            List[CallableFunctionDeclaration]:
                A list of CallableFunctionDeclaration instances representing the registered tools.
        """

        return [CallableFunctionDeclaration(**tool.schema, function=tool.fn) for tool in self.tools]
