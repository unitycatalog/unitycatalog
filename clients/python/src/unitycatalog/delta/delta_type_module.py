"""
Patches DeltaType to handle the string-or-object polymorphism
in StructField.type.

This module is auto-imported by the generated models/__init__.py
via post-build injection. Users do not need to import it manually.
"""

import re

from unitycatalog.delta.models.delta_type import DeltaType
from unitycatalog.delta.models.primitive_type import PrimitiveType
from unitycatalog.delta.models.decimal_type import DecimalType
from unitycatalog.delta.models.array_type import ArrayType
from unitycatalog.delta.models.map_type import MapType
from unitycatalog.delta.models.struct_type import StructType
from unitycatalog.delta.models.struct_field import StructField

_DECIMAL_PATTERN = re.compile(r"decimal\((\d+),\s*(\d+)\)")


def _parse_type(obj):
    """Parse a type value (string or dict) into a typed DeltaType."""
    if isinstance(obj, str):
        m = _DECIMAL_PATTERN.match(obj)
        if m:
            return DecimalType(
                type=obj,
                precision=int(m.group(1)),
                scale=int(m.group(2)))
        return PrimitiveType(type=obj)
    if isinstance(obj, dict):
        t = obj.get("type")
        if t == "array":
            return ArrayType(
                type=t,
                element_type=_parse_type(obj.get("element-type")),
                contains_null=obj.get("contains-null", True))
        if t == "map":
            return MapType(
                type=t,
                key_type=_parse_type(obj.get("key-type")),
                value_type=_parse_type(obj.get("value-type")),
                value_contains_null=obj.get("value-contains-null", True))
        if t == "struct":
            fields = [_parse_field(f) for f in obj.get("fields", [])]
            return StructType(type=t, fields=fields)
        return PrimitiveType(type=t) if t else DeltaType(type="unknown")
    return None


def _parse_field(obj):
    """Parse a StructField dict."""
    if obj is None:
        return None
    return StructField(
        name=obj.get("name"),
        type=_parse_type(obj.get("type")),
        nullable=obj.get("nullable", True),
        metadata=obj.get("metadata", {}))


def _type_to_dict(dt):
    """Serialize a DeltaType to a string or dict."""
    if isinstance(dt, DecimalType):
        return "decimal({},{})".format(dt.precision, dt.scale)
    if isinstance(dt, PrimitiveType):
        return dt.type
    d = {"type": dt.type}
    if isinstance(dt, ArrayType):
        d["element-type"] = _type_to_dict(dt.element_type)
        d["contains-null"] = dt.contains_null
    elif isinstance(dt, MapType):
        d["key-type"] = _type_to_dict(dt.key_type)
        d["value-type"] = _type_to_dict(dt.value_type)
        d["value-contains-null"] = dt.value_contains_null
    elif isinstance(dt, StructType):
        d["fields"] = [_field_to_dict(f) for f in dt.fields]
    return d


def _field_to_dict(sf):
    """Serialize a StructField to a dict."""
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


DeltaType.from_dict = _patched_dt_from_dict
StructField.from_dict = _patched_sf_from_dict

# Patch instance methods
DeltaType.to_dict = _type_to_dict
PrimitiveType.to_dict = _type_to_dict
DecimalType.to_dict = _type_to_dict
ArrayType.to_dict = _type_to_dict
MapType.to_dict = _type_to_dict
StructType.to_dict = _type_to_dict
StructField.to_dict = _field_to_dict
