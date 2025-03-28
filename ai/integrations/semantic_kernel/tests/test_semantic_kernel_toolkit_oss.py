import json
import os
from unittest import mock

import pytest
import pytest_asyncio
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from semantic_kernel import Kernel
from semantic_kernel.functions import kernel_function, KernelArguments
from semantic_kernel.functions.kernel_function_from_method import KernelFunctionFromMethod
from semantic_kernel.functions.kernel_parameter_metadata import KernelParameterMetadata
from semantic_kernel.exceptions import KernelInvokeException
from pydantic import ValidationError

from unitycatalog.ai.core.base import FunctionExecutionResult
from unitycatalog.ai.core.client import (
    ExecutionMode,
    FunctionExecutionResult,
    UnitycatalogFunctionClient,
)
from unitycatalog.ai.semantic_kernel.toolkit import SemanticKernelTool, UCFunctionToolkit
from unitycatalog.ai.test_utils.function_utils_oss import (
    CATALOG,
    create_function_and_cleanup_oss,
)
from unitycatalog.client import (
    ApiClient,
    Configuration,
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from unitycatalog.client.models.function_parameter_type import FunctionParameterType

try:
    # v2
    from pydantic_core._pydantic_core import ValidationError
except ImportError:
    # v1
    from pydantic.error_wrappers import ValidationError

SCHEMA = os.environ.get("SCHEMA", "ucai_semantic_kernel_test")


@pytest_asyncio.fixture
async def uc_client():
    config = Configuration()
    config.host = "http://localhost:8080/api/2.1/unity-catalog"
    uc_api_client = ApiClient(configuration=config)

    uc_client = UnitycatalogFunctionClient(api_client=uc_api_client)
    uc_client.uc.create_catalog(name=CATALOG)
    uc_client.uc.create_schema(name=SCHEMA, catalog_name=CATALOG)

    yield uc_client

    uc_client.close()
    await uc_api_client.close()


@pytest.fixture
def sample_semantic_kernel_tool():
    @kernel_function(name="sample_function", description="A sample function for testing.")
    def dummy_function(**kwargs) -> str:
        return "dummy_result"

    return SemanticKernelTool(
        fn=dummy_function, name="sample_function", description="A sample function for testing."
    )


def test_semantic_kernel_tool_to_dict(sample_semantic_kernel_tool):
    """Test the `to_dict` method of SemanticKernelTool."""
    expected_output = {
        "name": sample_semantic_kernel_tool.name,
        "description": sample_semantic_kernel_tool.description,
        "fn": sample_semantic_kernel_tool.fn,
    }
    assert sample_semantic_kernel_tool.model_dump() == expected_output


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_toolkit_e2e(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert func_obj.comment in tool.description
        print("this is the tool name", tool.name)

        input_args = {"a": 5, "b": 6}
        k_ = Kernel()
        output = await k_.invoke(tool.fn, KernelArguments(**input_args))

        result = json.loads(output.value)["value"]
        assert result == "11"

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in uc_client.list_functions(CATALOG, SCHEMA)],
            client=uc_client,
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_toolkit_e2e_manually_passing_client(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert func_obj.comment in tool.description
        input_args = {"a": 3, "b": 4}
        k_ = Kernel()
        output = await k_.invoke(tool.fn, KernelArguments(**input_args))

        result = json.loads(output.value)["value"]
        assert result == "7"

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in uc_client.list_functions(CATALOG, SCHEMA)],
            client=uc_client,
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


def test_toolkit_creation_errors_no_client(monkeypatch):
    monkeypatch.setattr("unitycatalog.ai.core.base._is_databricks_client_available", lambda: False)

    with pytest.raises(
        ValidationError,
        match=r"No client provided, either set the client when creating a toolkit or set the default client",
    ):
        UCFunctionToolkit(function_names=["test.test.test"])


def test_toolkit_creation_errors_invalid_client(uc_client):
    with pytest.raises(ValidationError, match=r"Input should be an instance of BaseFunctionClient"):
        UCFunctionToolkit(function_names=[], client="client")


def test_toolkit_creation_errors_missing_function_names(uc_client):
    with pytest.raises(
        ValidationError,
        match=r"Cannot create tool instances without function_names being provided.",
    ):
        UCFunctionToolkit(function_names=[], client=uc_client)


def test_toolkit_function_argument_errors(uc_client):
    with pytest.raises(
        ValidationError,
        match=r"1 validation error for UCFunctionToolkit\nfunction_names\n  Field required",
    ):
        UCFunctionToolkit(client=uc_client)


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
            "parameter_default": "123",
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
    print(function_info.model_dump())

    assert len(kernel_params) == 1
    param = kernel_params[0]
    assert isinstance(param, KernelParameterMetadata)
    assert param.name == "x"
    assert param.description == "test comment"
    assert param.default_value == "123"
    assert param.type_ == "string"
    assert param.is_required == True


@pytest.mark.asyncio
async def test_uc_function_to_semantic_kernel_tool(uc_client):
    mock_function_info = generate_function_info()
    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=uc_client,
        ),
        mock.patch.object(uc_client, "get_function", return_value=mock_function_info),
        mock.patch.object(
            uc_client,
            "execute_function",
            return_value=FunctionExecutionResult(format="SCALAR", value="some_string"),
        ),
    ):
        tool = UCFunctionToolkit.uc_function_to_semantic_kernel_tool(
            function_name="catalog.schema.test", client=uc_client
        )
        k_ = Kernel()
        output = await k_.invoke(tool.fn, KernelArguments(**{"x": "some_string"}))

        result = json.loads(output.value)["value"]

        assert result == "some_string"


@pytest.mark.asyncio
async def test_toolkit_with_invalid_function_input(uc_client):
    """Test toolkit with invalid input parameters for function conversion."""
    mock_function_info = generate_function_info()

    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=uc_client,
        ),
        mock.patch.object(uc_client, "get_function", return_value=mock_function_info),
    ):
        invalid_inputs = {"unexpected_key": "value"}
        tool = UCFunctionToolkit.uc_function_to_semantic_kernel_tool(
            function_name="catalog.schema.test", client=uc_client
        )

        with pytest.raises(
            KernelInvokeException, match="Error occurred while invoking function: 'test'"
        ):
            k_ = Kernel()
            output = await k_.invoke(tool.fn, KernelArguments(**invalid_inputs))


def test_register_with_kernel(uc_client):
    """Test registering tools with a Semantic Kernel instance."""
    from semantic_kernel import Kernel

    mock_function_info = generate_function_info()
    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=uc_client,
        ),
        mock.patch.object(uc_client, "get_function", return_value=mock_function_info),
    ):
        toolkit = UCFunctionToolkit(function_names=["catalog.schema.test"], client=uc_client)

        kernel = Kernel()
        toolkit.register_with_kernel(kernel, plugin_name="test_plugin")

        # Verify the function was registered
        assert "test_plugin" in kernel.plugins
        assert "test" in [func for func in kernel.plugins["test_plugin"].functions]
