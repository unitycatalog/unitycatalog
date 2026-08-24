import json
import logging
from dataclasses import asdict
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from llama_index.core.tools import FunctionTool
from llama_index.core.tools.types import ToolMetadata
from unitycatalog.ai.core.base import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import (
    generate_function_input_params_schema,
    get_tool_name,
    process_function_names,
)
from unitycatalog.ai.core.utils.validation_utils import mlflow_tracing_enabled

_logger = logging.getLogger(__name__)


class UnityCatalogTool(FunctionTool):
    """
    A tool class that integrates Unity Catalog functions into a tool structure.

    Attributes:
        uc_function_name (str): The full name of the function in the form of 'catalog.schema.function'.
        client_config (Dict[str, Any]): Configuration of the client for managing the tool.
    """

    uc_function_name: str = Field(
        description="The full name of the function in the form of 'catalog.schema.function'",
    )

    client_config: Dict[str, Any] = Field(
        description="Configuration of the client for managing the tool",
    )

    def __init__(
        self,
        fn: Callable,
        metadata: ToolMetadata,
        uc_function_name: str,
        client_config: Dict[str, Any],
        *args,
        **kwargs,
    ):
        """
        Initializes the UnityCatalogTool.

        Args:
            fn (Callable): The function that represents the tool's functionality.
            metadata (ToolMetadata): Metadata about the tool, including name, description, and schema.
            uc_function_name (str): The full name of the function in the form of 'catalog.schema.function'.
            client_config (Dict[str, Any]): Configuration dictionary for the client used to manage the tool.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        super().__init__(*args, fn=fn, metadata=metadata, **kwargs)
        self.uc_function_name = uc_function_name
        self.client_config = client_config

    def __repr__(self) -> str:
        return (
            "UnityCatalogTool("
            + ", ".join(f"{k}={v!r}" for k, v in asdict(self.metadata).items() if v is not None)
            + ")"
        )


class UCFunctionToolkit(BaseModel):
    """
    A toolkit for managing Unity Catalog functions and converting them into tools.

    Attributes:
        function_names (List[str]): List of function names in 'catalog.schema.function' format.
        tools_dict (Dict[str, FunctionTool]): A dictionary mapping function names to their corresponding tools.
        client (Optional[BaseFunctionClient]): The client used to manage functions.
        return_direct (bool): Whether the tool should return the output directly. If this is set to True, the
            response from an agent is returned directly, without being interpreted and rewritten by the agent.
    """

    function_names: List[str] = Field(
        default_factory=list,
        description="List of function names in 'catalog.schema.function' format",
    )
    tools_dict: Dict[str, FunctionTool] = Field(default_factory=dict)
    client: Optional[BaseFunctionClient] = Field(
        default=None,
        description="Client for managing functions",
    )
    return_direct: bool = Field(
        default=False,
        description="Whether the tool should return the output directly",
    )
    filter_accessible_functions: bool = Field(
        default=False,
        description="When set to true, UCFunctionToolkit is initialized with functions that only the client has access to",
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def validate_toolkit(self) -> "UCFunctionToolkit":
        """
        Validates the toolkit configuration and processes function names.

        Returns:
            UCFunctionToolkit: The validated and updated toolkit instance for LlamaIndex integration.
        """
        client = validate_or_set_default_client(self.client)
        self.client = client

        if not self.function_names:
            raise ValueError("Cannot create tool instances without function_names being provided.")

        self.tools_dict = process_function_names(
            function_names=self.function_names,
            tools_dict=self.tools_dict,
            client=client,
            filter_accessible_functions=self.filter_accessible_functions,
            uc_function_to_tool_func=self.uc_function_to_llama_tool,
            return_direct=self.return_direct,
        )

        # Since the 'properties' key is a reserved arg in LlamaIndex, disallow creating a tool with a
        # function that has a 'properties' key in its input schema for LlamaIndex tool usage.
        for tool_name, tool in self.tools_dict.items():
            if "properties" in tool.metadata.fn_schema.model_fields:
                raise ValueError(
                    f"Function '{tool_name}' has a 'properties' key in its input schema. "
                    "Cannot create a tool with this function due to LlamaIndex reserving this argument name."
                )
        return self

    @staticmethod
    def uc_function_to_llama_tool(
        *,
        function_name: str,
        client: Optional[BaseFunctionClient] = None,
        return_direct: bool = False,
        filter_accessible_functions: bool = False,
    ) -> Optional[FunctionTool]:
        """
        Converts a Unity Catalog function into a Llama tool.

        Args:
            function_name (str): The name of the function in 'catalog.schema.function' format.
            client (Optional[BaseFunctionClient]): The client used to manage functions. Defaults to None.
            return_direct (Optional[bool]): Whether the tool should return the output directly. Defaults to False.

        Returns:
            FunctionTool: The constructed tool from the Unity Catalog function.
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

        def func(**kwargs: Any) -> str:
            """
            Executes the Unity Catalog function with the provided parameters.

            Args:
                **kwargs (Any): Keyword arguments representing function parameters.

            Returns:
                str: The JSON result of the function execution.
            """
            kwargs = extract_properties(kwargs)
            args_json = json.loads(json.dumps(kwargs, default=str))
            result = client.execute_function(
                function_name=function_name,
                parameters=args_json,
                enable_retriever_tracing=mlflow_tracing_enabled("llama_index"),
            )
            return result.to_json()

        metadata = ToolMetadata(
            name=get_tool_name(function_name),
            description=function_info.comment or "",
            fn_schema=fn_schema.pydantic_model,
            return_direct=return_direct,
        )

        return UnityCatalogTool(
            fn=func,
            metadata=metadata,
            uc_function_name=function_name,
            client_config=client.to_dict(),
        )

    @property
    def tools(self) -> List[FunctionTool]:
        """
        Retrieves the list of tools managed by the toolkit.

        Returns:
            List[FunctionTool]: A list of tools available in the toolkit.
        """
        return list(self.tools_dict.values())


def extract_properties(data: dict[str, Any]) -> dict[str, Any]:
    """
    Extracts the 'properties' dictionary from the input dictionary,
    merges its key-value pairs into the top-level dictionary, and returns a new dictionary.

    Args:
        data (dict[str, Any]): The original dictionary possibly containing a 'properties' key.

    Returns:
        dict[str, Any]: A new dictionary with 'properties' merged into the top-level.

    Raises:
        TypeError: If 'properties' exists but is not a dictionary.
        KeyError: If there are key collisions between 'properties' and the top-level keys.
    """
    if not isinstance(data, dict):
        raise TypeError(f"Input must be a dictionary. Received: {type(data).__name__}")

    properties = data.get("properties")
    if properties is None:
        return data

    if not isinstance(properties, dict):
        raise TypeError("'properties' must be a dictionary.")

    if overlapping_keys := (set(data) - {"properties"}) & set(properties):
        raise KeyError(
            f"Key collision detected for keys: {', '.join(overlapping_keys)}. Cannot merge 'properties'."
        )

    merged_data = {**{k: v for k, v in data.items() if k != "properties"}, **properties}

    return merged_data
