import logging
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from openai import pydantic_function_tool
from openai.types.chat import ChatCompletionToolParam
from unitycatalog.ai.core.base import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import (
    generate_function_input_params_schema,
    get_tool_name,
    process_function_names,
)

_logger = logging.getLogger(__name__)


class UCFunctionToolkit(BaseModel):
    function_names: List[str] = Field(
        default_factory=list,
        description="The list of function names in the form of 'catalog.schema.function'",
    )

    client: Optional[BaseFunctionClient] = Field(
        default=None,
        description="The client for managing functions, must be an instance of BaseFunctionClient",
    )

    tools_dict: Dict[str, ChatCompletionToolParam] = Field(
        default_factory=dict,
        description="The tools dictionary storing the function name and tool definition mapping, no need to provide this field",
    )

    filter_accessible_functions: bool = Field(
        default=False,
        description="When set to true, UCFunctionToolkit is initialized with functions that only the client has access to",
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def validate_toolkit(self) -> "UCFunctionToolkit":
        self.client = validate_or_set_default_client(self.client)

        self.tools_dict = process_function_names(
            function_names=self.function_names,
            tools_dict={},
            client=self.client,
            filter_accessible_functions=self.filter_accessible_functions,
            uc_function_to_tool_func=self.uc_function_to_openai_function_definition,
        )
        return self

    @staticmethod
    def uc_function_to_openai_function_definition(
        *,
        function_name: str,
        client: Optional[BaseFunctionClient] = None,
        filter_accessible_functions: bool = False,
    ) -> Optional[ChatCompletionToolParam]:
        """
        Convert a UC function to OpenAI function definition.

        Args:
            function_name: The full name of the function in the form of 'catalog.schema.function'
            client: The client for managing functions, must be an instance of BaseFunctionClient
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

        function_input_params_schema = generate_function_input_params_schema(
            function_info, strict=True
        )
        tool = pydantic_function_tool(
            function_input_params_schema.pydantic_model,
            name=get_tool_name(function_name),
            description=function_info.comment or "",
        )
        # strict is set to true only if all params are supported JSON schema types
        tool["function"]["strict"] = function_input_params_schema.strict
        return tool

    @property
    def tools(self) -> List[ChatCompletionToolParam]:
        return list(self.tools_dict.values())
