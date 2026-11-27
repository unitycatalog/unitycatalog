import json
from copy import deepcopy
from unittest import mock

import dspy
import pytest

from unitycatalog.ai.core.base import BaseFunctionClient
from unitycatalog.ai.dspy.toolkit import UCFunctionToolkit, _inline_schema_refs
from unitycatalog.ai.test_utils.function_utils import generate_function_info_with_params
from unitycatalog.ai.test_utils.function_utils_oss import generate_function_info_with_params_oss

INT_STRUCT = {
    "type": "struct",
    "fields": [{"name": "a", "type": "integer", "nullable": False, "metadata": {}}],
}
INT_OBJECT = {"type": "object", "properties": {"a": {"type": "integer"}}, "required": ["a"]}
STRUCT_ARRAY = {"type": "array", "elementType": INT_STRUCT, "containsNull": False}
STRUCT_MAP = {
    "type": "map",
    "keyType": "string",
    "valueType": INT_STRUCT,
    "valueContainsNull": False,
}


def _without_schema_titles(schema):
    if isinstance(schema, dict):
        return {
            key: _without_schema_titles(value)
            for key, value in schema.items()
            if key != "title" or not isinstance(value, str)
        }
    if isinstance(schema, list):
        return [_without_schema_titles(item) for item in schema]
    return schema


@pytest.mark.parametrize(
    "generate_function_info",
    [generate_function_info_with_params, generate_function_info_with_params_oss],
    ids=["databricks", "oss"],
)
@pytest.mark.parametrize(
    ("type_text", "type_json", "nullable", "expected"),
    [
        pytest.param("struct<a:int>", INT_STRUCT, False, INT_OBJECT, id="struct"),
        pytest.param(
            "struct<a:int>",
            INT_STRUCT,
            True,
            {"anyOf": [INT_OBJECT, {"type": "null"}]},
            id="nullable-struct",
        ),
        pytest.param(
            "array<struct<a:int>>",
            STRUCT_ARRAY,
            False,
            {"type": "array", "items": INT_OBJECT},
            id="array-of-structs",
        ),
        pytest.param(
            "array<struct<a:int>>",
            {**STRUCT_ARRAY, "containsNull": True},
            False,
            {"type": "array", "items": {"anyOf": [INT_OBJECT, {"type": "null"}]}},
            id="array-of-nullable-structs",
        ),
        pytest.param(
            "map<string,struct<a:int>>",
            STRUCT_MAP,
            False,
            {"type": "object", "additionalProperties": INT_OBJECT},
            id="map-of-structs",
        ),
        pytest.param(
            "struct<b:struct<a:int>>",
            {
                "type": "struct",
                "fields": [{"name": "b", "type": INT_STRUCT, "nullable": False, "metadata": {}}],
            },
            False,
            {"type": "object", "properties": {"b": INT_OBJECT}, "required": ["b"]},
            id="nested-struct",
        ),
        pytest.param(
            "array<map<string,array<struct<a:int>>>>",
            {
                **STRUCT_ARRAY,
                "elementType": {**STRUCT_MAP, "valueType": STRUCT_ARRAY},
            },
            False,
            {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": {"type": "array", "items": INT_OBJECT},
                },
            },
            id="nested-arrays-and-maps",
        ),
    ],
)
def test_tool_inlines_struct_definitions(
    generate_function_info, type_text, type_json, nullable, expected
):
    function_info = generate_function_info({"point": (type_text, type_json)})
    if nullable:
        param = function_info.input_params.parameters[0]
        parsed = json.loads(param.type_json)
        parsed["nullable"] = True
        param.type_json = json.dumps(parsed)

    client = mock.create_autospec(BaseFunctionClient, instance=True)
    client.get_function.return_value = function_info
    client.to_dict.return_value = {}

    schema = UCFunctionToolkit.convert_to_dspy_schema(function_info)
    toolkit = UCFunctionToolkit(function_names=[function_info.full_name], client=client)
    tool = toolkit.tools[0]

    assert isinstance(tool, dspy.Tool)
    assert tool.args == schema["args_dict"]
    assert _without_schema_titles(tool.args) == {"point": expected}

    litellm_params = tool.format_as_litellm_function_call()["function"]["parameters"]
    assert "$defs" not in litellm_params
    assert '"$ref"' not in json.dumps(litellm_params)
    assert _without_schema_titles(litellm_params["properties"]) == {"point": expected}


def test_inline_schema_refs_preserves_siblings_and_literal_data():
    ref = {"$ref": "#/$defs/Alias"}
    default = {"$ref": "#/$defs/Literal"}
    schema = {
        "prefixItems": [{**ref, "description": "argument", "default": default}, ref],
        "$defs": {
            "Alias": {"$ref": "#/$defs/Value", "description": "alias"},
            "Value": {"type": "object", "description": "target", "default": {}},
        },
    }
    original = deepcopy(schema)

    assert _inline_schema_refs(schema) == {
        "prefixItems": [
            {"type": "object", "description": "argument", "default": default},
            {"type": "object", "description": "alias", "default": {}},
        ],
    }
    assert schema == original


def test_inline_schema_refs_isolates_shared_mutable_leaves():
    shared_schema = {
        "properties": {
            "first": {"$ref": "#/$defs/Shared"},
            "second": {"$ref": "#/$defs/Shared"},
        },
        "$defs": {
            "Shared": {
                "type": "object",
                "properties": {"count": {"type": "integer"}},
                "required": ["count"],
            },
        },
    }
    inlined = _inline_schema_refs(shared_schema)
    first = inlined["properties"]["first"]
    second = inlined["properties"]["second"]
    assert first["required"] is not second["required"]
    assert first["required"] is not shared_schema["$defs"]["Shared"]["required"]
    assert first["properties"]["count"] == {"type": "integer"}
