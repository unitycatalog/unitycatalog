import unittest
from unittest.mock import MagicMock

from unitycatalog.ai.bedrock.utils import (
    execute_tool_calls,
    extract_response_details,
    extract_tool_calls,
    generate_tool_call_session_state,
)


class TestUtils(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.catalog_name = "test_catalog"
        self.schema_name = "test_schema"
        self.function_name = "test_function"

    def test_extract_response_details(self):
        response = {
            "completion": [
                {"chunk": {"bytes": b"chunk1"}},
                {
                    "returnControl": {
                        "invocationInputs": [
                            {
                                "functionInvocationInput": {
                                    "actionGroup": "group1",
                                    "function": "func1",
                                    "parameters": [{"name": "param1", "value": "value1"}],
                                }
                            },
                        ],
                        "invocationId": "inv1",
                    }
                },
            ]
        }
        expected = {
            "chunks": "chunk1",
            "tool_calls": [
                {
                    "action_group": "group1",
                    "function": "func1",
                    "function_name": "group1__func1",
                    "parameters": {"param1": "value1"},
                    "invocation_id": "inv1",
                }
            ],
        }
        result = extract_response_details(response)
        self.assertEqual(result, expected)

    def test_extract_tool_calls(self):
        response = {
            "completion": [
                {
                    "returnControl": {
                        "invocationInputs": [
                            {
                                "functionInvocationInput": {
                                    "actionGroup": "group1",
                                    "function": "func1",
                                    "parameters": [{"name": "param1", "value": "value1"}],
                                }
                            },
                        ],
                        "invocationId": "inv1",
                    }
                }
            ]
        }
        expected = [
            {
                "action_group": "group1",
                "function": "func1",
                "function_name": "group1__func1",
                "parameters": {"param1": "value1"},
                "invocation_id": "inv1",
            }
        ]
        result = extract_tool_calls(response)
        self.assertEqual(result, expected)

    def test_execute_tool_calls(self):
        tool_calls = [
            {
                "action_group": "group1",
                "function": "func1",
                "function_name": "group1__func1",
                "parameters": {"param1": "value1"},
                "invocation_id": "inv1",
            }
        ]
        self.mock_client.get_function.return_value = {"function_info": "info"}
        self.mock_client.execute_function.return_value = MagicMock(value="result_value")

        expected = [{"invocation_id": "inv1", "result": "result_value"}]
        result = execute_tool_calls(
            tool_calls, self.mock_client, self.catalog_name, self.schema_name, self.function_name
        )
        self.assertEqual(result, expected)

    def test_generate_tool_call_session_state(self):
        tool_result = {"invocation_id": "inv1", "result": "result_value"}
        tool_call = {"action_group": "group1", "function": "func1"}
        expected = {
            "invocationId": "inv1",
            "returnControlInvocationResults": [
                {
                    "functionResult": {
                        "actionGroup": "group1",
                        "function": "func1",
                        "confirmationState": "CONFIRM",
                        "responseBody": {"TEXT": {"body": "result_value"}},
                    }
                }
            ],
        }
        result = generate_tool_call_session_state(tool_result, tool_call)
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
