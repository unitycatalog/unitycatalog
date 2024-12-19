import json
from typing import Any, Dict, List, Optional

from langchain_core.pydantic_v1 import BaseModel, Field, root_validator
from langchain_core.tools import StructuredTool

from unitycatalog.ai.core.base import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import (
    generate_function_input_params_schema,
    get_tool_name,
    process_function_names,
)


class UnityCatalogTool(StructuredTool):
    uc_function_name: str = Field(
        description="The full name of the function in the form of 'catalog.schema.function'",
    )
    client_config: Dict[str, Any] = Field(
        description="Configuration of the client for managing the tool",
    )


class UCFunctionToolkit(BaseModel):
    function_names: List[str] = Field(
        default_factory=list,
        description="The list of function names in the form of 'catalog.schema.function'",
    )

    tools_dict: Dict[str, UnityCatalogTool] = Field(
        default_factory=dict,
        description="The tools dictionary storing the function name and langchain tool mapping, no need to provide this field",
    )

    client: Optional[BaseFunctionClient] = Field(
        default=None,
        description="The client for managing functions, must be an instance of BaseFunctionClient",
    )

    class Config:
        arbitrary_types_allowed = True

    @root_validator(skip_on_failure=True)
    def validate_toolkit(cls, values) -> Dict[str, Any]:
        client = validate_or_set_default_client(values.get("client"))
        values["client"] = client

        function_names = values["function_names"]
        tools_dict = values.get("tools_dict", {})

        values["tools_dict"] = process_function_names(
            function_names=function_names,
            tools_dict=tools_dict,
            client=client,
            uc_function_to_tool_func=cls.uc_function_to_langchain_tool,
        )
        return values

    @staticmethod
    def uc_function_to_langchain_tool(
        *,
        client: Optional[BaseFunctionClient] = None,
        function_name: Optional[str] = None,
        function_info: Optional[Any] = None,
    ) -> UnityCatalogTool:
        """
        Convert a UC function to Langchain StructuredTool

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

        def func(*args: Any, **kwargs: Any) -> str:
            args_json = json.loads(json.dumps(kwargs, default=str))
            result = client.execute_function(
                function_name=function_name,
                parameters=args_json,
            )
            return result.to_json()

        return UnityCatalogTool(
            name=get_tool_name(function_name),
            description=function_info.comment or "",
            func=func,
            args_schema=generate_function_input_params_schema(function_info).pydantic_model,
            uc_function_name=function_name,
            client_config=client.to_dict(),
        )

    @property
    def tools(self) -> List[UnityCatalogTool]:
        return list(self.tools_dict.values())
