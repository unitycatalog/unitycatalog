import logging
from typing import Dict, List, Optional

from autogen_core import CancellationToken
from autogen_core.tools import BaseTool
from pydantic import BaseModel, ConfigDict, Field, model_validator

from unitycatalog.ai.core.base import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import (
    generate_function_input_params_schema,
    get_tool_name,
    process_function_names,
)

_logger = logging.getLogger(__name__)


class UCFunctionTool(BaseTool):
    """
    A custom tool for calling a Unity Catalog function via a UC client.
    `args_type` is dynamically generated from the UC function schema,
    and `return_type` can be str or a custom Pydantic model, if desired.
    """

    def __init__(
        self,
        name: str,
        description: str,
        function_full_name: str,
        client: BaseFunctionClient,
        args_type: BaseModel,
    ):
        """
        Args:
            name: The short tool name (what the LLM sees for `function_call`).
            description: A brief description.
            function_full_name: e.g. "catalog.schema.function".
            client: A Unity Catalog client with `.execute_function(...)`.
            args_type: A Pydantic model class representing the function's input parameters.
        """
        super().__init__(
            args_type=args_type,
            return_type=str,
            name=name,
            description=description,
        )
        self._function_full_name = function_full_name
        self._client = client

    async def run(self, args: BaseModel, cancellation_token: CancellationToken) -> str:
        """
        Actually call the UC function with the provided parameters.
        """
        param_dict = args.model_dump()

        result = self._client.execute_function(
            function_name=self._function_full_name,
            parameters=param_dict,
        )
        return result.to_json()


class UCFunctionToolkit(BaseModel):
    """
    A toolkit for managing Unity Catalog functions and converting them into
    Autogen (v0.4+) tools (UCFunctionTool).
    """

    function_names: List[str] = Field(
        description="List of function names in 'catalog.schema.function' format."
    )
    tools_dict: Dict[str, UCFunctionTool] = Field(
        default_factory=dict,
        description="Dictionary mapping function names to their corresponding UCFunctionTool.",
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
            uc_function_to_tool_func=self.uc_function_to_autogen_tool,
        )
        return self

    @staticmethod
    def uc_function_to_autogen_tool(
        *,
        function_name: str,
        client: Optional[BaseFunctionClient] = None,
        filter_accessible_functions: bool = False,
    ) -> Optional[UCFunctionTool]:
        """
        Converts a Unity Catalog function to a UCFunctionTool (Autogen v0.4+).

        Args:
            function_name: e.g. "catalog.schema.function".
            client: The client for managing functions.
        """
        client = validate_or_set_default_client(client)

        # Retrieve the FunctionInfo if only a name is provided
        if function_name is None:
            raise ValueError("function_name must be provided.")
        try:
            function_info = client.get_function(function_name)
        except PermissionError as e:
            _logger.info(f"Skipping {function_name} due to permission errors.")
            if filter_accessible_functions:
                return None
            raise e

        # Use your existing logic to build a dynamic Pydantic model for input parameters
        function_input_params_schema = generate_function_input_params_schema(
            function_info, strict=True
        )

        args_model = function_input_params_schema.pydantic_model

        # Build the UCFunctionTool
        tool_name = get_tool_name(function_name)  # short name for the LLM
        description = function_info.comment or ""

        return UCFunctionTool(
            name=tool_name,
            description=description,
            function_full_name=function_name,
            client=client,
            args_type=args_model,
        )

    @property
    def tools(self) -> List[UCFunctionTool]:
        """
        Retrieves the list of UCFunctionTool objects managed by this toolkit.
        """
        return list(self.tools_dict.values())
