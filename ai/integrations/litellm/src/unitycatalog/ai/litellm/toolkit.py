import logging
from typing import Any, Optional

from openai import pydantic_function_tool
from pydantic import BaseModel, ConfigDict, Field, model_validator

from unitycatalog.ai.core.client import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import (
    generate_function_input_params_schema,
    get_tool_name,
    process_function_names,
)

_logger = logging.getLogger(__name__)


class UCFunctionToolkit(BaseModel):
    """
    A toolkit for managing Unity Catalog functions and converting them into LiteLLM tools.
    """

    function_names: list[str] = Field(
        default_factory=list,
        description="List of function names in 'catalog.schema.function' format.",
    )
    tools_dict: dict[str, dict] = Field(
        default_factory=dict,
        description="Dictionary mapping function names to their corresponding LiteLLM tools.",
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
            uc_function_to_tool_func=self.uc_function_to_litellm_tool,
        )

    @staticmethod
    def uc_function_to_litellm_tool(
        *,
        client: Optional[BaseFunctionClient] = None,
        function_name: Optional[str] = None,
        function_info: Optional[Any] = None,
        filter_accessible_functions: bool = False,
    ) -> Optional[dict]:
        """
        Converts a Unity Catalog function to an Lite LLM tool definition.

        Args:
            client (Optional[BaseFunctionClient]): The client for managing functions.
            function_name (Optional[str]): The full name of the function in 'catalog.schema.function' format.
            function_info (Optional[Any]): The function info object returned by the client.

        Returns:
            Dict: The corresponding Lite LLM tool definition.
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

        generated_model = generate_function_input_params_schema(function_info).pydantic_model

        if generated_model == BaseModel:
            # NB: Pydantic BaseModel.schema(), which is used by LiteLLM, requires a subclass
            # of BaseClass. When function_info.input_params is None, we return a BaseModel as the
            # pydantic_model
            _BaseModelWrapper = type("BaseModelWrapper", (generated_model,), {})

            generated_model = _BaseModelWrapper

        tool = pydantic_function_tool(
            model=generated_model,
            name=get_tool_name(function_name),
            description=function_info.comment or "",
        )

        return dict(
            name=get_tool_name(function_name),
            description=function_info.comment or "",
            tool=tool,
        )

    @property
    def tools(self) -> list[dict]:
        """
        Retrieves the list of Lite LLM tools managed by the toolkit.
        """
        return list(self.tools_dict.values())
