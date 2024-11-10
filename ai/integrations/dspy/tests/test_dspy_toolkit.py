import os

os.environ["WAREHOUSE_ID"] = "475b94ddc7cd5211"
os.environ["CATALOG"] = "main"
os.environ["SCHEMA"] = "default"
os.environ["TEST_IN_DATABRICKS"] = "true"

# MAIN
import json
import os
from unittest.mock import patch

import pytest
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from pydantic import ValidationError

from unitycatalog.ai.core.client import FunctionExecutionResult
from unitycatalog.ai.dspy.toolkit import UCFunctionToolkit
from unitycatalog.ai.test_utils.client_utils import (
    USE_SERVERLESS,
    client,  # noqa: F401
    get_client,
    requires_databricks,
    set_default_client,
)
from unitycatalog.ai.test_utils.function_utils import create_function_and_cleanup

CATALOG = os.environ.get("CATALOG", "integration_testing")
SCHEMA = os.environ.get("SCHEMA", "ucai_core_test")
# TODO
CATALOG = "main"


@pytest.fixture
def function_info():
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


def test_uc_function_to_dspy_tool(function_info, client):
    with (
        patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
            return_value=function_info,
        ),
        patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.execute_function",
            return_value=FunctionExecutionResult(format="SCALAR", value="some_string"),
        ),
    ):
        tool = UCFunctionToolkit.uc_function_to_dspy_tool(
            function_name=f"{CATALOG}.{SCHEMA}.test", client=client
        )
        result = json.loads(tool.func(x="some_string"))["value"]
        assert result == "some_string"


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_toolkit_e2e(use_serverless, monkeypatch):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(
            function_names=[func_obj.full_function_name], return_direct=True
        )
        tool = toolkit.tools[0]
        assert tool.name == func_obj.tool_name
        assert tool.desc == func_obj.comment
        assert tool.args == {"code": "None"}

        result = json.loads(tool.func(code="print(1)"))["value"]
        assert result == "1\n"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_toolkit_e2e_with_client(use_serverless, monkeypatch):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(
            function_names=[func_obj.full_function_name], return_direct=True, client=client
        )
        tool = toolkit.tools[0]
        assert tool.name == func_obj.tool_name
        assert tool.desc == func_obj.comment
        assert tool.args == {"code": "None"}

        result = json.loads(tool.func(code="print(1)"))["value"]
        assert result == "1\n"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_multiple_toolkits(use_serverless, monkeypatch):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name])
        toolkit2 = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])

        input_args = {"code": "print(1)"}
        assert (
            json.loads(toolkit1.tools[0].func(**input_args))["value"]
            == json.loads(
                [t for t in toolkit2.tools if t.name == func_obj.tool_name][0].func(**input_args)
            )["value"]
        )


def test_toolkit_creation_errors():
    with pytest.raises(ValidationError, match="No client provided"):
        UCFunctionToolkit(function_names=[])

    with pytest.raises(ValidationError, match="Input should be an instance of BaseFunctionClient"):
        UCFunctionToolkit(function_names=[], client="client")


def test_toolkit_creation_errors_with_client(client):
    with pytest.raises(
        ValueError, match="Cannot create tool instances without function_names being provided."
    ):
        UCFunctionToolkit(function_names=[], client=client)
