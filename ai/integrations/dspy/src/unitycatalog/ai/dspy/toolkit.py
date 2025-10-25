import json
import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

import dspy
from unitycatalog.ai.core.base import BaseFunctionClient
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


class UnityCatalogDSPyToolWrapper(BaseModel):
    """Pydantic wrapper for Unity Catalog DSPy Tool that holds the real dspy.Tool instance."""

    tool: dspy.Tool = Field(description="The underlying dspy.Tool instance")
    uc_function_name: str = Field(description="The full UC function name")
    client_config: Dict[str, Any] = Field(description="Client configuration dictionary")

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def to_dict(self) -> Dict[str, Any]:
        """Converts the wrapper into a dictionary."""
        return {
            "uc_function_name": self.uc_function_name,
            "client_config": self.client_config,
            "tool_info": {
                "name": self.tool.name,
                "description": self.tool.desc,
                "args": self.tool.args,
                "arg_types": self.tool.arg_types,
                "arg_desc": self.tool.arg_desc,
            },
        }


class UCFunctionToolkit(BaseModel):
    """Toolkit for managing Unity Catalog functions as DSPy tools."""

    function_names: List[str] = Field(
        description="List of function names in 'catalog.schema.function' format.",
    )

    tools_dict: Dict[str, UnityCatalogDSPyToolWrapper] = Field(default_factory=dict)

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
            uc_function_to_tool_func=self.uc_function_to_dspy_tool,
        )
        return self

    @staticmethod
    def convert_to_dspy_schema(function_info: PydanticFunctionInputParams, strict: bool = True):
        """
        Converts Unity Catalog function metadata into a DSPy-compatible schema.

        Args:
            function_info (PydanticFunctionInputParams):
                The input parameter metadata of the UC function.
            strict (bool):
                Indicates whether to enforce strict typing rules in parameter conversion.

        Returns:
            Dict[str, Any]: A dictionary containing the DSPy-compatible function schema with args_dict, args_desc, and args_type.
        """
        param_infos = function_info.input_params.parameters
        if param_infos is None:
            raise ValueError("Function input parameters are None.")

        args_dict = {}
        args_desc = {}
        args_type = {}

        for param_info in param_infos:
            pydantic_field = param_info_to_pydantic_type(param_info, strict=strict)
            args = TypeAdapter(pydantic_field.pydantic_type).json_schema()

            if pydantic_field.default:
                args["default"] = pydantic_field.default

            if pydantic_field.description:
                args_desc[param_info.name] = pydantic_field.description

            args_dict[param_info.name] = args
            args_type[param_info.name] = pydantic_field.pydantic_type

        return {"args_dict": args_dict, "args_desc": args_desc, "args_type": args_type}

    @staticmethod
    def uc_function_to_dspy_tool(
        *,
        client: Optional[BaseFunctionClient] = None,
        function_name: Optional[str] = None,
        function_info: Optional[Any] = None,
        filter_accessible_functions: bool = False,
    ) -> Optional[UnityCatalogDSPyToolWrapper]:
        """
        Converts a Unity Catalog function to a DSPy tool wrapper.

        Args:
            client (Optional[BaseFunctionClient]): The client for managing functions.
            function_name (Optional[str]): The full name of the function in 'catalog.schema.function' format.
            function_info (Optional[Any]): The function info object returned by the client.
            filter_accessible_functions (bool): Whether to filter out inaccessible functions.

        Returns:
            UnityCatalogDSPyToolWrapper: The corresponding DSPy tool wrapper.
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

        if function_info is None:
            raise ValueError(
                "Could not find function info for the given function name or function info."
            )

        schema_data = UCFunctionToolkit.convert_to_dspy_schema(function_info, strict=True)

        def func(**kwargs: Any) -> str:
            """Execute the Unity Catalog function with the given parameters."""
            args_json = json.loads(json.dumps(kwargs, default=str))
            result = client.execute_function(
                function_name=function_name,
                parameters=args_json,
                enable_retriever_tracing=mlflow_tracing_enabled("dspy"),
            )
            return result.to_json()

        # Create the real dspy.Tool instance
        dspy_tool = dspy.Tool(
            func=func,
            name=get_tool_name(function_name),
            desc=function_info.comment or "",
            args=schema_data["args_dict"],
            arg_types=schema_data["args_type"],
            arg_desc=schema_data["args_desc"],
        )

        # Wrap it in our Pydantic wrapper
        return UnityCatalogDSPyToolWrapper(
            tool=dspy_tool,
            uc_function_name=function_name,
            client_config=client.to_dict(),
        )

    @property
    def tools(self) -> List[dspy.Tool]:
        """Get all underlying dspy.Tool instances from the toolkit."""
        return [wrapper.tool for wrapper in self.tools_dict.values()]

    def get_tool(self, function_name: str) -> Optional[dspy.Tool]:
        """Get a specific underlying tool by function name."""
        wrapper = self.tools_dict.get(function_name)
        return wrapper.tool if wrapper else None
