import json
import logging
from typing import Any, Callable, Dict, List, Optional

from google.generativeai.types import CallableFunctionDeclaration, content_types
from pydantic import BaseModel, ConfigDict, Field, model_validator

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
from unitycatalog.ai.core.utils.validation_utils import mlflow_tracing_enabled

_logger = logging.getLogger(__name__)


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
        return self

    @staticmethod
    def convert_to_gemini_schema(function_info: PydanticFunctionInputParams, strict: str = True):
        """
        Converts Unity Catalog function metadata into a Gemini-compatible schema.

        Args:
            function_info (PydanticFunctionInputParams):
                The input parameter metadata of the UC function.
            strict (bool):
                Indicates whether to enforce strict typing rules in parameter conversion.

        Returns:
            Dict[str, Any]: A dictionary containing the Gemini-compatible function schema.
        """

        fields_dict = {}
        param_infos = function_info.input_params.parameters
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
        schema = dict(name=function_info.name, description=function_info.comment)
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

        schema = UCFunctionToolkit.convert_to_gemini_schema(function_info, strict=True)

        def func(**kwargs: Any) -> str:
            args_json = json.loads(json.dumps(kwargs, default=str))
            result = client.execute_function(
                function_name=function_name,
                parameters=args_json,
                enable_retriever_tracing=mlflow_tracing_enabled("gemini"),
            )

            return result.to_json()

        return GeminiTool(
            fn=func,
            name=get_tool_name(function_name),
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
