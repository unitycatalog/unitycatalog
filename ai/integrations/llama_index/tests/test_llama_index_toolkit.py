import json
import os
import re
from typing import Any, Dict
from unittest import mock

import pytest
from databricks.sdk.service.catalog import (
    ColumnTypeName,
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from pydantic import BaseModel, ValidationError

from unitycatalog.ai.core.base import (
    BaseFunctionClient,
    FunctionExecutionResult,
)
from unitycatalog.ai.llama_index.toolkit import UCFunctionToolkit, extract_properties
from unitycatalog.ai.test_utils.client_utils import (
    USE_SERVERLESS,
    client,  # noqa: F401
    get_client,
    requires_databricks,
    set_default_client,
)
from unitycatalog.ai.test_utils.function_utils import (
    CATALOG,
    create_function_and_cleanup,
    create_python_function_and_cleanup,
)

SCHEMA = os.environ.get("SCHEMA", "ucai_llama_index_test")


class MockFnSchemaWithProperties(BaseModel):
    x: str
    properties: Dict[str, Any]


class MockFnSchemaWithoutProperties(BaseModel):
    x: str


class MockFunctionExecutionResult:
    def __init__(self, return_value: str):
        self.return_value = return_value

    def to_json(self) -> str:
        return self.return_value


def generate_mock_function_info(has_properties: bool = False) -> FunctionInfo:
    parameters = [
        FunctionParameterInfo(
            name="x",
            type_text="string",
            type_json='{"name":"x","type":"STRING","nullable":true}',
            type_name=ColumnTypeName.STRING,
            type_precision=0,
            type_scale=0,
            position=1,
            parameter_type="PARAM",
            parameter_default='"default_x"',
        )
    ]

    if has_properties:
        parameters.append(
            FunctionParameterInfo(
                name="properties",
                type_text="map<string, string>",
                type_json='{"name":"properties","type":"MAP","nullable":true}',
                type_name=ColumnTypeName.MAP,
                type_precision=0,
                type_scale=0,
                position=2,
                parameter_type="PARAM",
                parameter_default=None,
            )
        )

    return FunctionInfo(
        catalog_name="catalog",
        schema_name="schema",
        name="test_function",
        input_params=FunctionParameterInfos(parameters=parameters),
        comment="A test function with properties argument"
        if has_properties
        else "A test function without properties argument",
    )


def generate_mock_execution_result(return_value: str = "result") -> FunctionExecutionResult:
    return FunctionExecutionResult(format="SCALAR", value=return_value)


def test_toolkit_creation_with_properties_argument_mocked():
    """
    Test that UCFunctionToolkit raises a ValueError when the function has a 'properties' argument using mocks.
    """
    mock_function_info = generate_mock_function_info(has_properties=True)

    mock_client = mock.create_autospec(BaseFunctionClient, instance=True)
    mock_client.get_function.return_value = mock_function_info
    mock_client.to_dict.return_value = {"mock": "config"}

    mock_fn_schema = mock.Mock()
    mock_fn_schema.pydantic_model = {"properties": {}}

    with (
        mock.patch(
            "unitycatalog.ai.core.utils.function_processing_utils.generate_function_input_params_schema",
            return_value=mock_fn_schema,
        ),
        mock.patch(
            "unitycatalog.ai.llama_index.toolkit.validate_or_set_default_client",
            return_value=mock_client,
        ),
    ):
        with pytest.raises(ValueError, match="has a 'properties' key in its input schema"):
            UCFunctionToolkit(function_names=["catalog.schema.test_function"], client=mock_client)


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_toolkit_e2e(use_serverless, monkeypatch):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(
            function_names=[func_obj.full_function_name], return_direct=True
        )
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.metadata.name == func_obj.tool_name
        assert tool.metadata.return_direct
        assert tool.metadata.description == func_obj.comment
        assert tool.uc_function_name == func_obj.full_function_name
        assert tool.client_config == client.to_dict()

        input_args = {"code": "print(1)"}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "1\n"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.metadata.name for t in toolkit.tools]


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_toolkit_e2e_manually_passing_client(use_serverless, monkeypatch):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(
            function_names=[func_obj.full_function_name], client=client, return_direct=True
        )
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.metadata.name == func_obj.tool_name
        assert tool.metadata.return_direct
        assert tool.metadata.description == func_obj.comment
        assert tool.uc_function_name == func_obj.full_function_name
        assert tool.client_config == client.to_dict()
        input_args = {"code": "print(1)"}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "1\n"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=client)
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.metadata.name for t in toolkit.tools]


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_multiple_toolkits(use_serverless, monkeypatch):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name])
        toolkit2 = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.metadata.name == func_obj.tool_name][0]
        input_args = {"code": "print(1)"}
        result1 = json.loads(tool1.fn(**input_args))["value"]
        result2 = json.loads(tool2.fn(**input_args))["value"]
        assert result1 == result2


def test_toolkit_creation_errors():
    with pytest.raises(ValidationError, match=r"No client provided"):
        UCFunctionToolkit(function_names=[])

    with pytest.raises(ValidationError, match=r"Input should be an instance of BaseFunctionClient"):
        UCFunctionToolkit(function_names=[], client="client")


def test_toolkit_function_argument_errors(client):
    with pytest.raises(
        ValidationError,
        match=r".*Cannot create tool instances without function_names being provided.*",
    ):
        UCFunctionToolkit(client=client)


def generate_function_info():
    parameters = [
        {
            "name": "x",
            "type_text": "string",
            "type_json": '{"name":"x","type":"string","nullable":true,"metadata":{"EXISTS_DEFAULT":"\\"123\\"","default":"\\"123\\"","CURRENT_DEFAULT":"\\"123\\""}}',
            "type_name": "STRING",
            "type_precision": 0,
            "type_scale": 0,
            "position": 17,
            "parameter_type": "PARAM",
            "parameter_default": '"123"',
        }
    ]
    return FunctionInfo(
        catalog_name="catalog",
        schema_name="schema",
        name="test",
        input_params=FunctionParameterInfos(
            parameters=[FunctionParameterInfo(**param) for param in parameters]
        ),
    )


def test_uc_function_to_llama_tool(client):
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
        tool = UCFunctionToolkit.uc_function_to_llama_tool(
            function_name=f"{CATALOG}.{SCHEMA}.test", client=client, return_direct=True
        )
        # Validate passthrough of LlamaIndex argument
        assert tool.metadata.return_direct

        # Validate tool repr
        assert str(tool).startswith(
            f"UnityCatalogTool(description='', name='{CATALOG}__{SCHEMA}__test',"
        )

        result = json.loads(tool.fn(x="some_string"))["value"]
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
        # Test with invalid input params that are not matching expected schema
        invalid_inputs = {"unexpected_key": "value"}
        tool = UCFunctionToolkit.uc_function_to_llama_tool(
            function_name="catalog.schema.test", client=client, return_direct=True
        )

        with pytest.raises(ValueError, match="Extra parameters provided that are not defined"):
            tool.fn(**invalid_inputs)


def test_toolkit_with_tracing_as_retriever(client):
    """Test toolkit with invalid input parameters for function conversion."""
    mock_function_info = generate_function_info()

    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=client,
        ),
        mock.patch.object(client, "get_function", return_value=mock_function_info),
        mock.patch.object(
            client,
            "_execute_uc_function",
            return_value=generate_mock_execution_result(
                "[{'page_content': 'This is the page content.'}]"
            ),
        ),
    ):
        import mlflow

        mlflow.llama_index.autolog()

        tool = UCFunctionToolkit.uc_function_to_llama_tool(
            function_name="catalog.schema.test", client=client, return_direct=True
        )
        tool.fn({"query": "some input"})

        import mlflow

        trace = mlflow.get_last_active_trace()
        assert trace is not None
        assert trace.info.execution_time_ms is not None
        assert trace.data.request == {"query": "some input"}
        assert trace.data.response == "[{'page_content': 'This is the page content.'}]"
        assert trace.data.spans[0].name == mock_function_info.full_function_name


def test_extract_properties_success():
    data = {
        "properties": {"location": "abc", "temp": 1234},
        "metadata": "something",
        "name": "something else",
    }

    expected = {"metadata": "something", "name": "something else", "location": "abc", "temp": 1234}

    assert extract_properties(data) == expected


def test_extract_properties_no_properties():
    data = {"metadata": "something", "name": "something else"}

    expected = {"metadata": "something", "name": "something else"}

    assert extract_properties(data) == expected


def test_extract_properties_properties_not_dict():
    data = {"properties": "not a dict", "metadata": "something", "name": "something else"}

    with pytest.raises(TypeError, match="'properties' must be a dictionary."):
        extract_properties(data)


def test_extract_properties_empty_properties():
    data = {"properties": {}, "metadata": "something", "name": "something else"}

    expected = {"metadata": "something", "name": "something else"}

    assert extract_properties(data) == expected


@pytest.mark.parametrize(
    "properties, expected_keys",
    [
        ({"location": "abc", "metadata": "conflict"}, "metadata"),
        ({"location": "abc", "metadata": "conflict", "name": "conflict_name"}, "metadata, name"),
    ],
)
def test_extract_properties_key_collisions(properties, expected_keys):
    data = {
        "properties": properties,
        "metadata": "something",
        "name": "something else",
    }

    expected_keys_set = set(expected_keys.split(", "))
    pattern = (
        re.escape("Key collision detected for keys: ")
        + ".*".join(re.escape(key) for key in expected_keys_set)
        + r".*Cannot merge 'properties'."
    )

    with pytest.raises(KeyError, match=pattern):
        extract_properties(data)


def test_extract_properties_nested_properties():
    data = {
        "properties": {"location": "abc", "details": {"temp": 1234, "humidity": 80}},
        "metadata": "something",
        "name": "something else",
    }

    expected = {
        "metadata": "something",
        "name": "something else",
        "location": "abc",
        "details": {"temp": 1234, "humidity": 80},
    }

    assert extract_properties(data) == expected


def test_extract_properties_non_dict_input():
    with pytest.raises(TypeError, match="Input must be a dictionary."):
        extract_properties(0)


@requires_databricks
def test_toolkit_creation_with_properties_argument(client):
    def func_with_properties(properties: dict[str, str]) -> str:
        """
        A function that has 'properties' as an argument.

        Args:
            properties: A dictionary of properties.

        Returns:
            str: A message indicating that the function should fail.
        """
        return f"This should fail due to the 'properties' key. Please provide an alternative arg name for the values in {properties}"

    with create_python_function_and_cleanup(
        client, func=func_with_properties, schema=SCHEMA
    ) as func_obj:
        with pytest.raises(ValidationError, match="has a 'properties' key in its input schema"):
            UCFunctionToolkit(function_names=[func_obj.full_function_name], client=client)


def test_toolkit_creation_without_properties_argument_mocked():
    """
    Test that UCFunctionToolkit successfully creates a tool when the function does not have a 'properties' argument using mocks.
    """
    mock_function_info = generate_mock_function_info(has_properties=False)

    mock_client = mock.create_autospec(BaseFunctionClient, instance=True)
    mock_client.get_function.return_value = mock_function_info
    mock_client.to_dict.return_value = {"mock": "config"}
    mock_client.execute_function.return_value = MockFunctionExecutionResult(
        return_value='{"value": "some_string"}'
    )

    class MockPydanticModelWithoutProperties(BaseModel):
        x: str

    mock_fn_schema = mock.Mock()
    mock_fn_schema.pydantic_model = MockPydanticModelWithoutProperties

    with (
        mock.patch(
            "unitycatalog.ai.core.utils.function_processing_utils.generate_function_input_params_schema",
            return_value=mock_fn_schema,
        ),
        mock.patch(
            "unitycatalog.ai.llama_index.toolkit.validate_or_set_default_client",
            return_value=mock_client,
        ),
    ):
        toolkit = UCFunctionToolkit(
            function_names=["catalog.schema.test_function"], client=mock_client
        )
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.metadata.name == "catalog__schema__test_function"
        assert not tool.metadata.return_direct
        assert tool.metadata.description == "A test function without properties argument"
        assert tool.client_config == {"mock": "config"}

        input_args = {"x": "some_string"}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "some_string"


def test_uc_function_to_llama_tool_mocked():
    """
    Test the conversion of a Unity Catalog function to a Llama tool without 'properties' argument using mocks.
    """
    mock_function_info = generate_mock_function_info(has_properties=False)

    class MockPydanticModelWithoutProperties(BaseModel):
        x: str

    mock_fn_schema = mock.Mock()
    mock_fn_schema.pydantic_model = MockPydanticModelWithoutProperties

    mock_client = mock.create_autospec(BaseFunctionClient, instance=True)
    mock_client.get_function.return_value = mock_function_info
    mock_client.to_dict.return_value = {"mock": "config"}
    mock_client.execute_function.return_value = MockFunctionExecutionResult(
        return_value='{"value": "some_string"}'
    )

    with (
        mock.patch(
            "unitycatalog.ai.llama_index.toolkit.validate_or_set_default_client",
            return_value=mock_client,
        ) as mock_validate_client,
    ):
        tool = UCFunctionToolkit.uc_function_to_llama_tool(
            function_name="catalog.schema.test_function", client=mock_client, return_direct=True
        )
        assert tool.metadata.return_direct

        input_args = {"x": "some_string"}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "some_string"
        mock_validate_client.assert_called_once()
        mock_client.get_function.assert_called_once_with("catalog.schema.test_function")
        mock_client.to_dict.assert_called_once()
        mock_client.execute_function.assert_called_once_with(
            function_name="catalog.schema.test_function",
            parameters=input_args,
            enable_trace_as_retriever=False,
        )


def test_toolkit_with_invalid_function_input_mocked():
    """
    Test toolkit with invalid input parameters for function conversion using mocks.
    """
    mock_function_info = generate_mock_function_info(has_properties=False)
    mock_fn_schema = mock.Mock()
    mock_fn_schema.pydantic_model = {"x": {"type": "string"}}

    mock_client = mock.create_autospec(BaseFunctionClient, instance=True)
    mock_client.get_function.return_value = mock_function_info
    mock_client.to_dict.return_value = {"mock": "config"}

    with (
        mock.patch(
            "unitycatalog.ai.llama_index.toolkit.validate_or_set_default_client",
            return_value=mock_client,
        ) as mock_validate_client,
    ):
        tool = UCFunctionToolkit.uc_function_to_llama_tool(
            function_name="catalog.schema.test_function", client=mock_client, return_direct=True
        )

        mock_client.execute_function.side_effect = ValueError(
            "Extra parameters provided that are not defined"
        )

        invalid_inputs = {"unexpected_key": "value"}

        with pytest.raises(ValueError, match="Extra parameters provided that are not defined"):
            tool.fn(**invalid_inputs)
        mock_validate_client.assert_called_once()
        mock_client.get_function.assert_called_once_with("catalog.schema.test_function")
        mock_client.to_dict.assert_called_once()
        mock_client.execute_function.assert_called_once_with(
            function_name="catalog.schema.test_function",
            parameters=invalid_inputs,
            enable_trace_as_retriever=False,
        )
