# Delta Type Custom Serde

Patches `DeltaType.from_dict`/`to_dict` and `StructField.from_dict`/`to_dict` to handle the string-or-object polymorphism in `StructField.type`.

## The Problem

Delta's wire format uses bare JSON strings for primitive types (`"long"`, `"decimal(10,2)"`) and JSON objects for complex types. The generated Pydantic model's `from_dict` uses a discriminator lookup on `obj["type"]` which fails for bare strings (a `str` has no `"type"` key).

## How It Works

`delta_type_module.py` monkey-patches `from_dict` and `to_dict` on `DeltaType`, `StructField`, and all subtype classes:

- **`_parse_type(obj)`**: If `str`, creates `PrimitiveType` or `DecimalType` (via regex). If `dict`, dispatches on `obj["type"]` to `ArrayType`/`MapType`/`StructType`.
- **`_type_to_dict(dt)`**: `PrimitiveType` returns bare string. `DecimalType` derives `"decimal(p,s)"`. Complex types return dict with `"type"` discriminator.

## Registration

Auto-imported via `PythonClientPostBuild.scala` which copies the module into the generated package and patches `models/__init__.py` with an import. No manual setup needed.
