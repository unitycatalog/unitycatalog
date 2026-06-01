"""
Patches DeltaDataType to handle the string-or-object polymorphism
in DeltaStructField.type.

This module is auto-imported by the generated models/__init__.py
via post-build injection. Users do not need to import it manually.
"""

import re

from unitycatalog.delta.models.delta_array_type import DeltaArrayType
from unitycatalog.delta.models.delta_data_type import DeltaDataType
from unitycatalog.delta.models.delta_decimal_type import DeltaDecimalType
from unitycatalog.delta.models.delta_map_type import DeltaMapType
from unitycatalog.delta.models.delta_primitive_type import DeltaPrimitiveType
from unitycatalog.delta.models.delta_struct_field import DeltaStructField
from unitycatalog.delta.models.delta_struct_type import DeltaStructType

_DECIMAL_PATTERN = re.compile(r"decimal\((\d+),\s*(\d+)\)")


def _parse_type(obj):
    """Parse a type value (string or dict) into a typed DeltaDataType."""
    if isinstance(obj, str):
        m = _DECIMAL_PATTERN.match(obj)
        if m:
            return DeltaDecimalType(
                type=obj,
                precision=int(m.group(1)),
                scale=int(m.group(2)))
        return DeltaPrimitiveType(type=obj)
    if isinstance(obj, dict):
        t = obj.get("type")
        if t == "array":
            return DeltaArrayType(
                type=t,
                element_type=_parse_type(obj.get("element-type")),
                contains_null=obj.get("contains-null", True))
        if t == "map":
            return DeltaMapType(
                type=t,
                key_type=_parse_type(obj.get("key-type")),
                value_type=_parse_type(obj.get("value-type")),
                value_contains_null=obj.get("value-contains-null", True))
        if t == "struct":
            fields = [_parse_field(f) for f in obj.get("fields", [])]
            return DeltaStructType(type=t, fields=fields)
        if t:
            return DeltaPrimitiveType(type=t)
        raise ValueError("dict type value missing 'type' field: {}".format(obj))
    raise ValueError("Expected str or dict for type, got: {}".format(type(obj).__name__))


def _parse_field(obj):
    """Parse a DeltaStructField dict."""
    if obj is None:
        return None
    return DeltaStructField(
        name=obj.get("name"),
        type=_parse_type(obj.get("type")),
        nullable=obj.get("nullable", True),
        metadata=obj.get("metadata", {}))


def _type_to_dict(dt):
    """Serialize a DeltaDataType to a string or dict."""
    if isinstance(dt, DeltaDecimalType):
        return "decimal({},{})".format(dt.precision, dt.scale)
    if isinstance(dt, DeltaPrimitiveType):
        return dt.type
    d = {"type": dt.type}
    if isinstance(dt, DeltaArrayType):
        d["element-type"] = _type_to_dict(dt.element_type)
        d["contains-null"] = dt.contains_null
    elif isinstance(dt, DeltaMapType):
        d["key-type"] = _type_to_dict(dt.key_type)
        d["value-type"] = _type_to_dict(dt.value_type)
        d["value-contains-null"] = dt.value_contains_null
    elif isinstance(dt, DeltaStructType):
        d["fields"] = [_field_to_dict(f) for f in dt.fields]
    return d


def _field_to_dict(sf):
    """Serialize a DeltaStructField to a dict."""
    return {
        "name": sf.name,
        "type": _type_to_dict(sf.type),
        "nullable": sf.nullable,
        "metadata": sf.metadata,
    }


# Patch class methods
@classmethod
def _patched_dt_from_dict(cls, obj):
    return _parse_type(obj)


@classmethod
def _patched_sf_from_dict(cls, obj):
    return _parse_field(obj)


DeltaDataType.from_dict = _patched_dt_from_dict
DeltaStructField.from_dict = _patched_sf_from_dict

# Patch instance methods
DeltaDataType.to_dict = _type_to_dict
DeltaPrimitiveType.to_dict = _type_to_dict
DeltaDecimalType.to_dict = _type_to_dict
DeltaArrayType.to_dict = _type_to_dict
DeltaMapType.to_dict = _type_to_dict
DeltaStructType.to_dict = _type_to_dict
DeltaStructField.to_dict = _field_to_dict
