import json
import logging
from typing import Any, Dict, List, Optional, Callable

from pydantic import BaseModel, ConfigDict, Field, model_validator

from semantic_kernel import Kernel
from semantic_kernel.functions import kernel_function
from semantic_kernel.functions.kernel_function_decorator import kernel_function
from semantic_kernel.functions.kernel_function_from_method import KernelFunctionFromMethod
from semantic_kernel.functions.kernel_parameter_metadata import KernelParameterMetadata


from unitycatalog.ai.core.client import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import (
    get_tool_name,
    process_function_names,
)
from unitycatalog.ai.core.utils.validation_utils import mlflow_tracing_enabled

_logger = logging.getLogger(__name__)


class SemanticKernelTool(BaseModel):
    """
    Model representing a Semantic Kernel tool.
    """

    fn: Callable = Field(
        description="Kernel Function that will be used to execute the UC Function"
    )
    name: str = Field(
        description="The name of the function.",
    )
    description: str = Field(
        description="A brief description of the function's purpose.",
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)


class UCFunctionToolkit(BaseModel):
    """
    A toolkit for managing Unity Catalog functions and converting them into Semantic Kernel tools.
    """

    function_names: List[str] = Field(
        description="List of function names in 'catalog.schema.function' format.",
    )
    tools_dict: Dict[str, SemanticKernelTool] = Field(
        default_factory=dict,
        description="Dictionary mapping function names to their corresponding tools.",
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
            raise ValueError(
                "Cannot create tool instances without function_names being provided.")

        self.tools_dict = process_function_names(
            function_names=self.function_names,
            tools_dict=self.tools_dict,
            client=self.client,
            filter_accessible_functions=self.filter_accessible_functions,
            uc_function_to_tool_func=self.uc_function_to_semantic_kernel_tool,
        )
        return self

    @staticmethod
    def convert_function_params_to_kernel_metadata(function_info) -> List[KernelParameterMetadata]:
        """
        Convert Unity Catalog function info's input parameters to a list of KernelParameterMetadata.
        
        Args:
            function_info_dict: The dictionary representation of function_info from function_info.as_dict()
            
        Returns:
            list[KernelParameterMetadata]: List of kernel parameter metadata objects
        """
        kernel_params = []
        input_params = getattr(getattr(function_info, "input_params",'{}'), 'parameters',[])
        
        for param in input_params:
            # Parse the type_json to get parameter details
            type_json = json.loads(getattr(param, "type_json",  '{}'))
            param_type = type_json.get('type', 'string')  # Default to string if type not specified
            
            # Map SQL types to Python types
            type_mapping = {
                    'string': str,
                    'varchar': str,
                    'char': str,
                    'text': str,
                    'double': float,
                    'float': float,
                    'integer': int,
                    'bigint': int,
                    'smallint': int,
                    'long': int,          
                    'boolean': bool,
                    'array': list,
                    'object': dict,
                }
            
            # Get the Python type object
            type_object = type_mapping.get(param_type, str)
            
            # Create the KernelParameterMetadata
            kernel_param = KernelParameterMetadata(
                name=getattr(param, "name",  ''),
                description=getattr(param, "comment",  ''),
                default_value = getattr(param, "parameter_default",  None),
                type=param_type,
                is_required=True,  # Unity Catalog parameters are required by default
                type_object=type_object
            )
            kernel_params.append(kernel_param)
        
        return kernel_params

    @staticmethod
    def uc_function_to_semantic_kernel_tool(
        *,
        client: Optional[BaseFunctionClient] = None,
        function_name: Optional[str] = None,
        function_info: Optional[Any] = None,
        filter_accessible_functions: bool = False,
    ) -> Optional[SemanticKernelTool]:
        """
        Converts a Unity Catalog function to a Semantic Kernel tool.
        """
        if function_name and function_info:
            raise ValueError(
                "Only one of function_name or function_info should be provided.")

        client = validate_or_set_default_client(client)

        if function_name:
            try:
                function_info = client.get_function(function_name)
            except PermissionError as e:
                _logger.info(
                    "Skipping %s due to permission errors.", function_name)
                if filter_accessible_functions:
                    return None
                raise e
        elif function_info:
            function_name = function_info.full_name
        else:
            raise ValueError(
                "Either function_name or function_info should be provided.")

        input_params = UCFunctionToolkit.convert_function_params_to_kernel_metadata(
            function_info)

        output_params = KernelParameterMetadata(
            name="results",
            description="The Result of the function.",
            type="string",
            type_object=str,
            is_required=True,
        )

        @kernel_function(name=function_info.name, description=function_info.comment)
        def kernel_func(**kwargs: Any) -> str:
            args_json = json.loads(json.dumps(kwargs, default=str))
            result = client.execute_function(
                function_name=function_name,
                parameters=args_json,
                enable_retriever_tracing=mlflow_tracing_enabled(
                    "semantic-kernel"),
            )
            return result.to_json()

        kernel_func = KernelFunctionFromMethod(
            method=kernel_func,
            parameters=input_params,
            return_parameter=output_params,
        )

        return SemanticKernelTool(
            fn=kernel_func,
            name=get_tool_name(function_name),
            description=function_info.comment or "")

    def register_with_kernel(self, kernel: Kernel, plugin_name: str = "unity_catalog") -> Kernel:
        """
        Register all tools with a Semantic Kernel instance.
        
        Args:
            kernel: A Semantic Kernel instance
            plugin_name: The name to give to the Unity Catalog plugin
            
        Returns:
            The Kernel instance with the tools registered
        """
        for tool in self.tools_dict.values():
            k_function = tool.fn
            kernel.add_function(plugin_name, k_function)
        
        return kernel
    
    
    
    @property
    def tools(self) -> List[SemanticKernelTool]:
        """
        Retrieves the list of Semantic Kernel tools managed by the toolkit.
        """
        return list(self.tools_dict.values())
