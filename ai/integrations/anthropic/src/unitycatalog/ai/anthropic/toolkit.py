import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from unitycatalog.ai.core.base import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import (
    generate_function_input_params_schema,
    get_tool_name,
    process_function_names,
)

_logger = logging.getLogger(__name__)


class AnthropicTool(BaseModel):
    """
    Model representing an Anthropic tool.
    """

    name: str = Field(
        description="The name of the function.",
    )
    description: str = Field(
        description="A brief description of the function's purpose.",
    )
    input_schema: Dict[str, Any] = Field(
        description="The input schema representing the parameters required by the tool."
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the AnthropicTool instance into a dictionary for the Anthropic API.
        """
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
        }


class UCFunctionToolkit(BaseModel):
    """
    A toolkit for managing Unity Catalog functions and converting them into Anthropic tools.
    """

    function_names: List[str] = Field(
        default_factory=list,
        description="List of function names in 'catalog.schema.function' format.",
    )
    tools_dict: Dict[str, AnthropicTool] = Field(
        default_factory=dict,
        description="Dictionary mapping function names to their corresponding Anthropic tools.",
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

        self.tools_dict = process_function_names(
            function_names=self.function_names,
            tools_dict=self.tools_dict,
            client=self.client,
            filter_accessible_functions=self.filter_accessible_functions,
            uc_function_to_tool_func=self.uc_function_to_anthropic_tool,
        )
        return self

    @staticmethod
    def uc_function_to_anthropic_tool(
        *,
        function_name: str,
        client: Optional[BaseFunctionClient] = None,
        filter_accessible_functions: bool = False,
    ) -> AnthropicTool:
        """
        Converts a Unity Catalog function to an Anthropic tool.

        Args:
            function_name (str): The full name of the function in 'catalog.schema.function' format.
            client (Optional[BaseFunctionClient]): The client for managing functions.

        Returns:
            AnthropicTool: The corresponding Anthropic tool.
        """
        client = validate_or_set_default_client(client)

        if function_name is None:
            raise ValueError("function_name must be provided.")
        try:
            function_info = client.get_function(function_name)
        except PermissionError as e:
            _logger.info(f"Skipping {function_name} due to permission errors.")
            if filter_accessible_functions:
                return None
            raise e

        fn_schema = generate_function_input_params_schema(function_info)

        input_schema = {
            "type": "object",
            "properties": fn_schema.pydantic_model.model_json_schema().get("properties", {}),
            "required": fn_schema.pydantic_model.model_json_schema().get("required", []),
        }

        return AnthropicTool(
            name=get_tool_name(function_name),
            description=function_info.comment or "",
            input_schema=input_schema,
        )

    @property
    def tools(self) -> List[AnthropicTool]:
        """
        Retrieves the list of Anthropic tools managed by the toolkit.
        """
        return list(self.tools_dict.values())
