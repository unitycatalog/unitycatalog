import json
from typing import Any, Callable, Dict, List, Optional, Union

from openai import pydantic_function_tool
from packaging import version
from pydantic import BaseModel, ConfigDict, Field, model_validator

from autogen import ConversableAgent
from autogen import __version__ as autogen_version
from unitycatalog.ai.core.base import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import (
    generate_function_input_params_schema,
    get_tool_name,
    process_function_names,
)

# Ensure the version of autogen is compatible
if version.parse(autogen_version) >= version.parse("0.4.0"):
    raise Exception(
        "Autogen version should be less than 0.4.0 as the newer version has major API changes"
    )


class AutogenTool(BaseModel):
    """
    Model representing an Autogen tool.
    """

    fn: Callable = Field(
        description="Callable that will be used to execute the UC Function, registered to the AutoGen Agent definition"
    )

    name: str = Field(
        description="The name of the function.",
    )
    description: str = Field(
        description="A brief description of the function's purpose.",
    )
    tool: Dict = Field(description="OpenAI compatible Tool Definition")

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the Autogen Tool instance into a dictionary for the Autogen API.
        """
        return {"name": self.name, "description": self.description, "tool": self.tool}

    def register_function(
        self,
        *,
        callers: Union[ConversableAgent, List[ConversableAgent]],
        executors: Union[ConversableAgent, List[ConversableAgent]],
    ) -> None:
        """
        Registers the function associated with the Autogen tool for all combinations of callers and executors.

        This method registers the callable function (`fn`) with each provided executor.
        It also updates the tool signature with each caller to ensure that the function's definition
        is correctly reflected in the overall toolset.

        Args:
            callers (ConversableAgent or List[ConversableAgent]): The agent(s) that will call the tool.
            executors (ConversableAgent or List[ConversableAgent]): The agent(s) that will execute the tool's function.
        """
        # Ensure callers is a list
        if isinstance(callers, ConversableAgent):
            callers = [callers]
        elif not isinstance(callers, list):
            raise TypeError(
                "callers must be a ConversableAgent or a list of ConversableAgent instances."
            )

        # Ensure executors is a list
        if isinstance(executors, ConversableAgent):
            executors = [executors]
        elif not isinstance(executors, list):
            raise TypeError(
                "executors must be a ConversableAgent or a list of ConversableAgent instances."
            )

        # Update tool signature for each caller
        for caller in callers:
            caller.update_tool_signature(self.tool, is_remove=False)

        # Register function with each executor
        for executor in executors:
            executor.register_function({self.name: executor._wrap_function(self.fn)})


class UCFunctionToolkit(BaseModel):
    """
    A toolkit for managing Unity Catalog functions and converting them into Autogen tools.
    """

    function_names: List[str] = Field(
        description="List of function names in 'catalog.schema.function' format.",
    )
    tools_dict: Dict[str, AutogenTool] = Field(
        default_factory=dict,
        description="Dictionary mapping function names to their corresponding tools.",
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
            uc_function_to_tool_func=self.uc_function_to_autogen_tool,
        )
        return self

    @staticmethod
    def uc_function_to_autogen_tool(
        *,
        client: Optional[BaseFunctionClient] = None,
        function_name: Optional[str] = None,
        function_info: Optional[Any] = None,
    ) -> AutogenTool:
        """
        Converts a Unity Catalog function to an Autogen tool.

        Args:
            client (Optional[BaseFunctionClient]): The client for managing functions.
            function_name (Optional[str]): The full name of the function in 'catalog.schema.function' format.
            function_info (Optional[Any]): The function info object returned by the client.

        Returns:
            AutogenTool: The corresponding Autogen tool.
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

        def func(**kwargs: Any) -> str:
            args_json = json.loads(json.dumps(kwargs, default=str))
            result = client.execute_function(
                function_name=function_name,
                parameters=args_json,
            )

            return result.to_json()

        return AutogenTool(
            fn=func,
            name=get_tool_name(function_name),
            description=function_info.comment or "",
            tool=tool,
        )

    @property
    def tools(self) -> List[AutogenTool]:
        """
        Retrieves the list of Autogen tools managed by the toolkit.
        """
        return list(self.tools_dict.values())

    def register_with_agents(
        self, *, callers: ConversableAgent, executors: ConversableAgent
    ) -> None:
        """
        Registers all tools in the toolkit with the specified caller and executor agents.

        Args:
            caller (ConversableAgent): The agent that will call the tools.
            executor (ConversableAgent): The agent that will execute the tools' functions.
        """
        for tool in self.tools:
            tool.register_function(callers=callers, executors=executors)
