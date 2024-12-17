import json
import logging
from typing import Any, Callable, Dict, List, Optional

from crewai_tools import BaseTool as CrewAIBaseTool
from pydantic import BaseModel, ConfigDict, Field, model_validator

from unitycatalog.ai.core.base import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import (
    generate_function_input_params_schema,
    get_tool_name,
    process_function_names,
)

_logger = logging.getLogger(__name__)


class UnityCatalogTool(CrewAIBaseTool):
    """
    A tool class that integrates Unity Catalog functions into a tool structure.

    Attributes:
        fn (Callable): A function to override the _run method of CrewAI.
        client_config (Dict[str, Any]): Configuration settings for managing the client.
    """

    fn: Callable = Field(description="Callable that will override the CrewAI _run() method.")
    client_config: Dict[str, Any] = Field(
        description="Configuration of the client for managing the tool"
    )

    def __init__(self, fn: Callable, client_config: Dict[str, Any], **kwargs):
        """
        A tool class that integrates Unity Catalog functions into a tool structure.

        Args:
            fn (Callable): The function that represents the tool's functionality.
            client_config (Dict[str, Any]): Configuration dictionary for the client used to manage the tool.
            **kwargs: Additional keyword arguments.
        """
        super().__init__(fn=fn, client_config=client_config, **kwargs)

    def _run(self, *args: Any, **kwargs: Any) -> Any:
        """Override of the CrewAI BaseTool run method."""
        return self.fn(*args, **kwargs)


class UCFunctionToolkit(BaseModel):
    """
    A toolkit for managing Unity Catalog functions and converting them into tools.

    Attributes:
        function_names (List[str]): List of function names in 'catalog.schema.function' format.
        tools_dict (Dict[str, FunctionTool]): A dictionary mapping function names to their
            corresponding tools.
        client (Optional[BaseFunctionClient]): The client used to manage functions.
        model_config (ConfigDict): The pydantic BaseModel configuration. Note that arbitrary types
            are allowed.
        description_updated (Optional[Bool]): Flag to check if the description has been updated.
        cache_function (Optional[Callable]): Function that will be used to determine if the tool
            should be cached, should return a boolean. If None, the tool will be cached.
        result_as_answer (Optional[Bool]): Flag to check if the tool should be the final
            agent answer.

    """

    function_names: List[str] = Field(
        default_factory=list,
        description="List of function names in 'catalog.schema.function' format",
    )
    tools_dict: Dict[str, CrewAIBaseTool] = Field(default_factory=dict)
    client: Optional[BaseFunctionClient] = Field(
        default=None,
        description="Client for managing functions",
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # CrewAI parameters, which can be found in the link below
    # https://github.com/crewAIInc/crewAI-tools/blob/main/crewai_tools/tools/base_tool.py#L21
    description_updated: bool = Field(
        default=False, description="Flag to check if the description has been updated."
    )
    cache_function: Callable = Field(
        default=lambda _args, _result: True,
        description=(
            "Function that will be used to determine if the tool should be cached, should return "
            "a boolean. If None, the tool will be cached."
        ),
    )
    result_as_answer: bool = Field(
        default=False, description="Flag to check if the tool should be the final agent answer."
    )

    @model_validator(mode="after")
    def validate_toolkit(self) -> "UCFunctionToolkit":
        self.client = validate_or_set_default_client(self.client)

        if not self.function_names:
            raise ValueError("Cannot create tool instances without function_names being provided.")

        self.tools_dict = process_function_names(
            function_names=self.function_names,
            tools_dict=self.tools_dict,
            client=self.client,
            uc_function_to_tool_func=self.uc_function_to_crewai_tool,
            description_updated=self.description_updated,
            cache_function=self.cache_function,
            result_as_answer=self.result_as_answer,
        )
        return self

    @staticmethod
    def uc_function_to_crewai_tool(
        *,
        client: Optional[BaseFunctionClient] = None,
        function_name: Optional[str] = None,
        function_info: Optional[Any] = None,
        description_updated: Optional[bool] = False,
        cache_function: Callable = lambda _args, _result: True,
        result_as_answer: bool = False,
        **kwargs,
    ) -> CrewAIBaseTool:
        """
        Converts a Unity Catalog function into a CrewAI tool.

        Args:
            client (Optional[BaseFunctionClient]): Client for executing the function.
            function_name (Optional[str]): Name of the function to convert.
            function_info (Optional[Any]): Detailed information of the function.
            description_updated (Optional[Bool]): Flag to check if the description has been updated.
            cache_function (Optional[Callable]): Function that will be used to determine if the tool
                should be cached, should return a boolean. If None, the tool will be cached.
            result_as_answer (Optional[Bool]): Flag to check if the tool should be the final
                agent answer.

        Returns:
            CrewAIBaseTool: A tool representation of the Unity Catalog function.
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

        def func(**kwargs: Any) -> str:
            args_json = json.loads(json.dumps(kwargs, default=str))
            result = client.execute_function(
                function_name=function_name,
                parameters=args_json,
            )

            return result.to_json()

        generated_model = generate_function_input_params_schema(function_info).pydantic_model

        if generated_model == BaseModel:
            # NB: Pydantic BaseModel.schema(), which is used by CrewAI, requires a subclass
            # of BaseClass. When function_info.input_params is None, we return a BaseModel as the
            # pydantic_model
            class _BaseModelWrapper(generated_model): ...

            generated_model = _BaseModelWrapper

        return UnityCatalogTool(
            # UnityCatalogTool params
            fn=func,
            client_config=client.to_dict(),
            # CrewAI params from UC
            name=get_tool_name(function_name),
            description=function_info.comment or "",
            args_schema=generated_model,
            # CrewAI params from user
            description_updated=description_updated,
            cache_function=cache_function,
            result_as_answer=result_as_answer,
        )

    @property
    def tools(self) -> List[CrewAIBaseTool]:
        """
        Retrieves the list of tools managed by the toolkit.

        Returns:
            List[BaseTool]: A list of tools available in the toolkit.
        """
        return list(self.tools_dict.values())
