import json
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dspy.predict.react import Tool
from unitycatalog.ai.core.client import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import (
    generate_function_input_params_schema,
    get_tool_name,
    process_function_names,
)


class UCFunctionToolkit(BaseModel):
    """
    A toolkit for managing Unity Catalog functions and converting them into DSPy tools.
    """

    function_names: list[str] = Field(
        default_factory=list,
        description="List of function names in 'catalog.schema.function' format.",
    )
    tools_dict: dict[str, Tool] = Field(
        default_factory=dict,
        description="Dictionary mapping function names to their corresponding DSPy tools.",
    )
    client: Optional[BaseFunctionClient] = Field(
        default=None, description="The client for managing functions."
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
            uc_function_to_tool_func=self.uc_function_to_dspy_tool,
        )

    @staticmethod
    def uc_function_to_dspy_tool(
        *,
        client: Optional[BaseFunctionClient] = None,
        function_name: Optional[str] = None,
        function_info: Optional[Any] = None,
    ) -> Tool:
        """
        Converts a Unity Catalog function to a DSPy Tool.

        Args:
            client (Optional[BaseFunctionClient]): The client for managing functions.
            function_name (Optional[str]): The full name of the function in 'catalog.schema.function' format.
            function_info (Optional[Any]): The function info object returned by the client.

        Returns:
            Tool: The corresponding DSPy tool.
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

        def func(*args: Any, **kwargs: Any) -> str:
            args_json = json.loads(json.dumps(kwargs, default=str))
            result = client.execute_function(
                function_name=function_name,
                parameters=args_json,
            )
            return result.to_json()

        args = generate_function_input_params_schema(function_info).pydantic_model().model_dump()
        dspy_tool = Tool(
            func=func,
            name=get_tool_name(function_name),
            desc=function_info.comment or "",
            args=args,
        )

        if not args:
            # NB: Override DSPy tool default of *args and **kwargs if the UC function doesn't
            # take arguments. These are inherited from Tool.__call__
            dspy_tool.args = {}

        return dspy_tool

    @property
    def tools(self) -> list[Tool]:
        """
        Retrieves the list of DSPy Tools managed by the toolkit.
        """
        return list(self.tools_dict.values())
