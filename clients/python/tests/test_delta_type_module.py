"""
Unit tests for delta_type_module.py string-or-object serde patches.

Tests use JSON strings (via from_json/to_json) to exercise the full
deserialization chain: JSON string -> json.loads -> from_dict -> model,
matching what happens on the wire.

These tests do NOT require a running server. They require generated models
(run `sbt pythonClient/generate` first).
"""

import json

import pytest

from unitycatalog.delta.models.primitive_type import PrimitiveType
from unitycatalog.delta.models.decimal_type import DecimalType
from unitycatalog.delta.models.array_type import ArrayType
from unitycatalog.delta.models.map_type import MapType
from unitycatalog.delta.models.struct_type import StructType
from unitycatalog.delta.models.struct_field import StructField

# Importing delta_type_module applies the monkey-patches
import unitycatalog.delta.serde.delta_type_module  # noqa: F401
from unitycatalog.delta.serde.delta_type_module import (
    _parse_type,
    _type_to_dict,
)


class TestDeserFromJson:
    """Deserialize from JSON strings, matching the wire-format path."""

    def test_primitive_types(self):
        for t in ["long", "string", "boolean", "binary", "date", "timestamp",
                   "integer", "short", "byte", "float", "double"]:
            json_str = json.dumps({
                "name": "col", "type": t, "nullable": True, "metadata": {}
            })
            col = StructField.from_json(json_str)
            assert isinstance(col.type, PrimitiveType)
            assert col.type.type == t

    def test_decimal_type(self):
        json_str = json.dumps({
            "name": "price", "type": "decimal(10,2)", "nullable": True, "metadata": {}
        })
        col = StructField.from_json(json_str)
        assert isinstance(col.type, DecimalType)
        assert col.type.precision == 10
        assert col.type.scale == 2

    def test_bare_decimal_is_primitive(self):
        json_str = json.dumps({
            "name": "col", "type": "decimal", "nullable": True, "metadata": {}
        })
        col = StructField.from_json(json_str)
        assert isinstance(col.type, PrimitiveType)
        assert col.type.type == "decimal"

    def test_array_type(self):
        json_str = json.dumps({
            "name": "tags",
            "type": {"type": "array", "element-type": "string", "contains-null": True},
            "nullable": True,
            "metadata": {},
        })
        col = StructField.from_json(json_str)
        assert isinstance(col.type, ArrayType)
        assert isinstance(col.type.element_type, PrimitiveType)
        assert col.type.element_type.type == "string"
        assert col.type.contains_null is True

    def test_map_type(self):
        json_str = json.dumps({
            "name": "props",
            "type": {
                "type": "map",
                "key-type": "string",
                "value-type": "integer",
                "value-contains-null": False,
            },
            "nullable": True,
            "metadata": {},
        })
        col = StructField.from_json(json_str)
        assert isinstance(col.type, MapType)
        assert isinstance(col.type.key_type, PrimitiveType)
        assert isinstance(col.type.value_type, PrimitiveType)
        assert col.type.value_contains_null is False

    def test_struct_type(self):
        json_str = json.dumps({
            "name": "nested",
            "type": {
                "type": "struct",
                "fields": [
                    {"name": "x", "type": "long", "nullable": False, "metadata": {}},
                ],
            },
            "nullable": True,
            "metadata": {},
        })
        col = StructField.from_json(json_str)
        assert isinstance(col.type, StructType)
        assert len(col.type.fields) == 1
        assert col.type.fields[0].name == "x"
        assert isinstance(col.type.fields[0].type, PrimitiveType)

    def test_nested_map_array_struct(self):
        json_str = json.dumps({
            "name": "data",
            "type": {
                "type": "map",
                "key-type": "string",
                "value-type": {
                    "type": "array",
                    "element-type": {
                        "type": "struct",
                        "fields": [
                            {"name": "v", "type": "double", "nullable": False, "metadata": {}},
                        ],
                    },
                    "contains-null": True,
                },
                "value-contains-null": True,
            },
            "nullable": True,
            "metadata": {},
        })
        col = StructField.from_json(json_str)
        assert isinstance(col.type, MapType)
        arr = col.type.value_type
        assert isinstance(arr, ArrayType)
        st = arr.element_type
        assert isinstance(st, StructType)
        assert st.fields[0].type.type == "double"


class TestSerToJson:
    """Serialize to JSON and verify wire format."""

    def test_primitive_round_trip(self):
        for t in ["long", "string", "boolean"]:
            col = StructField(
                name="col", type=PrimitiveType(type=t), nullable=True, metadata={})
            wire = json.loads(col.to_json())
            assert wire["type"] == t  # bare string, not {"type": "long"}

    def test_decimal_round_trip(self):
        col = StructField(
            name="col",
            type=DecimalType(type="decimal", precision=10, scale=2),
            nullable=True,
            metadata={},
        )
        wire = json.loads(col.to_json())
        assert wire["type"] == "decimal(10,2)"

    def test_array_round_trip(self):
        col = StructField(
            name="col",
            type=ArrayType(
                type="array",
                element_type=PrimitiveType(type="string"),
                contains_null=True,
            ),
            nullable=True,
            metadata={},
        )
        wire = json.loads(col.to_json())
        assert wire["type"] == {
            "type": "array",
            "element-type": "string",
            "contains-null": True,
        }


class TestJsonRoundTrip:
    """from_json -> to_json round-trip with complex schema."""

    def test_complex_schema(self):
        original = {
            "name": "root",
            "type": {
                "type": "struct",
                "fields": [
                    {"name": "id", "type": "long", "nullable": False, "metadata": {}},
                    {"name": "price", "type": "decimal(10,2)", "nullable": True, "metadata": {}},
                    {
                        "name": "tags",
                        "type": {
                            "type": "array",
                            "element-type": "string",
                            "contains-null": True,
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                ],
            },
            "nullable": True,
            "metadata": {},
        }
        col = StructField.from_json(json.dumps(original))
        reserialized = json.loads(col.to_json())
        assert reserialized == original


class TestParseTypeEdgeCases:
    """Error handling for _parse_type."""

    def test_dict_missing_type_field_raises(self):
        with pytest.raises(ValueError, match="missing 'type' field"):
            _parse_type({})

    def test_non_str_non_dict_raises(self):
        with pytest.raises(ValueError, match="Expected str or dict"):
            _parse_type(42)


class TestParseTypeDirect:
    """Direct unit tests for _parse_type -- dispatch logic without going through the wire."""

    def test_primitive(self):
        dt = _parse_type("long")
        assert isinstance(dt, PrimitiveType)
        assert dt.type == "long"

    def test_decimal_from_string(self):
        dt = _parse_type("decimal(10,2)")
        assert isinstance(dt, DecimalType)
        assert dt.precision == 10
        assert dt.scale == 2

    def test_array_from_dict(self):
        dt = _parse_type({"type": "array", "element-type": "string", "contains-null": True})
        assert isinstance(dt, ArrayType)
        assert isinstance(dt.element_type, PrimitiveType)
        assert dt.element_type.type == "string"
        assert dt.contains_null is True

    def test_map_from_dict(self):
        dt = _parse_type({
            "type": "map",
            "key-type": "string",
            "value-type": "double",
            "value-contains-null": False,
        })
        assert isinstance(dt, MapType)
        assert dt.key_type.type == "string"
        assert dt.value_type.type == "double"
        assert dt.value_contains_null is False

    def test_struct_from_dict(self):
        dt = _parse_type({
            "type": "struct",
            "fields": [{"name": "zip", "type": "integer", "nullable": False, "metadata": {}}],
        })
        assert isinstance(dt, StructType)
        assert len(dt.fields) == 1
        assert dt.fields[0].name == "zip"


class TestTypeToDictDirect:
    """Direct unit tests for _type_to_dict -- serialization logic without going through the wire."""

    def test_primitive(self):
        assert _type_to_dict(PrimitiveType(type="long")) == "long"

    def test_decimal(self):
        assert _type_to_dict(DecimalType(type="decimal", precision=10, scale=2)) == "decimal(10,2)"

    def test_array(self):
        d = _type_to_dict(
            ArrayType(type="array", element_type=PrimitiveType(type="string"), contains_null=True)
        )
        assert d == {"type": "array", "element-type": "string", "contains-null": True}

    def test_roundtrip_nested(self):
        original = {
            "type": "map",
            "key-type": "string",
            "value-type": {
                "type": "array",
                "element-type": {
                    "type": "struct",
                    "fields": [
                        {"name": "v", "type": "double", "nullable": False, "metadata": {}},
                    ],
                },
                "contains-null": True,
            },
            "value-contains-null": True,
        }
        assert _type_to_dict(_parse_type(original)) == original
