import json
import os
import asyncio
from unittest import mock


import pytest
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from semantic_kernel import Kernel
from semantic_kernel.functions import kernel_function,KernelArguments
from semantic_kernel.functions.kernel_function_from_method import KernelFunctionFromMethod
from semantic_kernel.functions.kernel_parameter_metadata import KernelParameterMetadata
from semantic_kernel.exceptions import KernelInvokeException
from pydantic import ValidationError

from unitycatalog.ai.core.client import FunctionExecutionResult
from unitycatalog.ai.core.databricks import ExecutionMode
from unitycatalog.ai.semantic_kernel.toolkit import SemanticKernelTool, UCFunctionToolkit
from unitycatalog.ai.test_utils.client_utils import (
    TEST_IN_DATABRICKS,
    client,  # noqa: F401
    get_client,
    requires_databricks,
    set_default_client,
)
from unitycatalog.ai.test_utils.function_utils import (
    CATALOG,
    create_function_and_cleanup,
)
from unitycatalog.client.models.function_parameter_type import FunctionParameterType

SCHEMA = os.environ.get("SCHEMA", "ucai_semantic_kernel_test")


@pytest.fixture
def sample_semantic_kernel_tool():
    @kernel_function(name="sample_function", description="A sample function for testing.")
    def dummy_function(**kwargs) -> str:
        """A sample function for testing.

        Args:
            location (float): A test parameter.

        Returns:
            str: A dummy result.
        """
        return "dummy_result"

    return SemanticKernelTool(
        fn=dummy_function,
        name="sample_function",
        description="A sample function for testing."
    )


def test_semantic_kernel_tool_to_dict(sample_semantic_kernel_tool):
    """Test the `to_dict` method of SemanticKernelTool."""
    expected_output = {
        "name": sample_semantic_kernel_tool.name,
        "description": sample_semantic_kernel_tool.description,
        "fn": sample_semantic_kernel_tool.fn,
    }
    assert sample_semantic_kernel_tool.model_dump() == expected_output


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@pytest.mark.asyncio
@requires_databricks
def test_toolkit_e2e(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionMode(execution_mode)
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name])
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert func_obj.comment in tool.description

        input_args = {"number": 4}
        k_ = Kernel()
        output =   asyncio.run(k_.invoke(
                            tool.fn,
                            KernelArguments(**input_args)))
        result = json.loads(output.value)["value"]
        assert result == "14"

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in client.list_functions(CATALOG, SCHEMA)]
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@pytest.mark.asyncio
@requires_databricks
def test_toolkit_e2e_manually_passing_client(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionMode(execution_mode)
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert func_obj.comment in tool.description
        input_args = {"number": 5}
        k_ = Kernel()
        output =   asyncio.run(k_.invoke(
                            tool.fn,
                            KernelArguments(**input_args)))
        result = json.loads(output.value)["value"]
        assert result == "15"

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in client.list_functions(CATALOG, SCHEMA)],
            client=client,
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


def test_toolkit_creation_errors():
    with pytest.raises(ValidationError, match=r"Input should be an instance of BaseFunctionClient"):
        UCFunctionToolkit(function_names=[], client="client")


def test_toolkit_creation_errors_missing_function_names(client):
    with pytest.raises(
        ValueError, match=r"Cannot create tool instances without function_names being provided."
    ):
        UCFunctionToolkit(function_names=[], client=client)


def test_toolkit_function_argument_errors(client):
    with pytest.raises(
        ValidationError,
        match=r"1 validation error for UCFunctionToolkit\nfunction_names\n  Field required",
    ):
        UCFunctionToolkit(client=client)


def generate_function_info(
    catalog="catalog",
    schema="schema",
    name="test",
    data_type=None,
    full_data_type=None,
    return_params=None,
):
    parameters = [
        {
            "comment": "test comment",
            "name": "x",
            "type_text": "string",
            "type_json": '{"name":"x","type":"string","nullable":true,"metadata":{"EXISTS_DEFAULT":"\\"123\\"","default":"\\"123\\"","CURRENT_DEFAULT":"\\"123\\""}}',
            "type_name": "STRING",
            "type_precision": 0,
            "type_scale": 0,
            "position": 17,
            "parameter_type": FunctionParameterType.PARAM,
            "parameter_default" : "123"
        }
    ]
    return FunctionInfo(
        catalog_name=catalog,
        schema_name=schema,
        name=name,
        full_name=f"{catalog}.{schema}.{name}",
        input_params=FunctionParameterInfos(
            parameters=[FunctionParameterInfo(**param) for param in parameters]
        ),
        comment="Executes Python code and returns its stdout.",
        data_type=data_type,
        full_data_type=full_data_type,
        return_params=return_params,
    )


def test_convert_function_params_to_kernel_metadata():
    """Test conversion of function parameters to kernel metadata."""
    function_info = generate_function_info()
    kernel_params = UCFunctionToolkit.convert_function_params_to_kernel_metadata(function_info)
    
    assert len(kernel_params) == 1
    param = kernel_params[0]
    assert isinstance(param, KernelParameterMetadata)
    assert param.name == "x"
    assert param.description == "test comment"
    assert param.default_value == '123'
    assert param.type_ == "string"
    assert param.is_required == True

@pytest.mark.asyncio
def test_uc_function_to_semantic_kernel_tool(client):
    mock_function_info = generate_function_info()
    with (
        mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
            return_value=mock_function_info,
        ),
        mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.execute_function",
            return_value=FunctionExecutionResult(format="SCALAR", value="some_string"),
        ),
    ):
        tool = UCFunctionToolkit.uc_function_to_semantic_kernel_tool(
            function_name=f"{CATALOG}.{SCHEMA}.test", client=client
        )
        k_ = Kernel()
        output = asyncio.run( k_.invoke(tool.fn,
                            KernelArguments(**{"x":"some_string"})))
        
        result = json.loads(output.value)["value"]
        assert result == "some_string"


def test_toolkit_with_invalid_function_input(client):
    """Test toolkit with invalid input parameters for function conversion."""
    mock_function_info = generate_function_info()

    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=client,
        ),
        mock.patch.object(client, "get_function", return_value=mock_function_info),
    ):
        invalid_inputs = {"unexpected_key": "value"}
        tool = UCFunctionToolkit.uc_function_to_semantic_kernel_tool(
            function_name="catalog.schema.test", client=client
        )

        with pytest.raises(KernelInvokeException, match="Error occurred while invoking function: 'test'"):
            k_ = Kernel()
            output = asyncio.run( k_.invoke(
                            tool.fn,
                            KernelArguments(**invalid_inputs)))


def test_register_with_kernel(client):
    """Test registering tools with a Semantic Kernel instance."""
    from semantic_kernel import Kernel
    
    mock_function_info = generate_function_info()
    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=client,
        ),
        mock.patch.object(client, "get_function", return_value=mock_function_info),
    ):
        toolkit = UCFunctionToolkit(
            function_names=["catalog.schema.test"], 
            client=client
        )
        
        kernel = Kernel()
        toolkit.register_with_kernel(kernel, plugin_name="test_plugin")
        
        # Verify the function was registered
        assert "test_plugin" in kernel.plugins
        assert "test" in [func for func in kernel.plugins["test_plugin"].functions]