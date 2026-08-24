import json
import logging
from typing import Any, Dict, List, Optional

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field, model_validator

from unitycatalog.ai.core.base import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import (
    generate_function_input_params_schema,
    get_tool_name,
    process_function_names,
)
from unitycatalog.ai.core.utils.validation_utils import mlflow_tracing_enabled

_logger = logging.getLogger(__name__)


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

    filter_accessible_functions: bool = Field(
        default=False,
        description="When set to true, UCFunctionToolkit is initialized with functions that only the client has access to",
    )

    class Config:
        arbitrary_types_allowed = True

    @model_validator(mode="after")
    def validate_toolkit(self) -> "UCFunctionToolkit":
        client = validate_or_set_default_client(self.client)
        self.client = client

        function_names = self.function_names
        tools_dict = self.tools_dict

        self.tools_dict = process_function_names(
            function_names=function_names,
            tools_dict=tools_dict,
            client=client,
            filter_accessible_functions=self.filter_accessible_functions,
            uc_function_to_tool_func=self.uc_function_to_langchain_tool,
        )
        return self

    @staticmethod
    def uc_function_to_langchain_tool(
        *,
        function_name: str,
        client: Optional[BaseFunctionClient] = None,
        filter_accessible_functions: bool = False,
    ) -> Optional[UnityCatalogTool]:
        """
        Convert a UC function to Langchain StructuredTool

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

        def func(*args: Any, **kwargs: Any) -> str:
            args_json = json.loads(json.dumps(kwargs, default=str))
            result = client.execute_function(
                function_name=function_name,
                parameters=args_json,
                enable_retriever_tracing=mlflow_tracing_enabled("langchain"),
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
