from typing import Any, Dict, List, Optional

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

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def validate_toolkit(self) -> "UCFunctionToolkit":
        self.client = validate_or_set_default_client(self.client)

        self.tools_dict = process_function_names(
            function_names=self.function_names,
            tools_dict={},
            client=self.client,
            uc_function_to_tool_func=self.uc_function_to_openai_function_definition,
        )
        return self

    @staticmethod
    def uc_function_to_openai_function_definition(
        *,
        client: Optional[BaseFunctionClient] = None,
        function_name: Optional[str] = None,
        function_info: Optional[Any] = None,
    ) -> ChatCompletionToolParam:
        """
        Convert a UC function to OpenAI function definition.

        Args:
            client: The client for managing functions, must be an instance of BaseFunctionClient
            function_name: The full name of the function in the form of 'catalog.schema.function'
            function_info: The function info object returned by the client.get_function() method

            .. note::
                Only one of function_name or function_info should be provided.
        """
        if function_name and function_info:
            raise ValueError("Only one of function_name or function_info should be provided.")
        client = validate_or_set_default_client(client)

        if function_name:
            function_info = client.get_function(function_name)
        elif function_info:
            function_name = function_info.full_name
        else:
            raise ValueError("Either function_name or function_info should be provided.")

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
