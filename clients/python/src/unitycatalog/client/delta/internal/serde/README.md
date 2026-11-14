# Delta Type Custom Serde (internal)

Python counterpart of the server/client Java `DeltaDataTypeModule`. Patches `to_dict` on `DeltaDataType`, its subtypes and `DeltaStructField`, and `from_dict` on `DeltaDataType`, `DeltaStructType` and `DeltaStructField`, to handle the string-or-object polymorphism in `DeltaStructField.type`.

This package lives under `internal/serde/` because it is implementation detail; consumers should never import from `unitycatalog.client.delta.internal.serde` directly.

## The Problem

Delta's wire format uses bare JSON strings for primitive types (`"long"`, `"decimal(10,2)"`) and JSON objects for complex types. The generated Pydantic model's `from_dict` uses a discriminator lookup on `obj["type"]` which fails for bare strings (a `str` has no `"type"` key).

## How It Works

`delta_data_type_module.py` monkey-patches `to_dict` on `DeltaDataType`, `DeltaStructField`, and all subtype classes, and `from_dict` on `DeltaDataType`, `DeltaStructField` and `DeltaStructType`, using two helpers:

- **`_parse_type(obj)`**: If `str`, creates `DeltaPrimitiveType` or `DeltaDecimalType` (via regex). If `dict`, dispatches on `obj["type"]` to `DeltaArrayType`/`DeltaMapType`/`DeltaStructType`.
- **`_type_to_dict(dt)`**: `DeltaPrimitiveType` returns bare string. `DeltaDecimalType` derives `"decimal(p,s)"`. Complex types return dict with `"type"` discriminator.

`DeltaStructType` is the only subtype the generated models parse directly (the `columns` of a table), so it is the only subtype whose `from_dict` is patched. It rejects anything that does not parse to a struct with `fields`. The other subtypes keep their generated `from_dict`.

## Registration

Auto-imported via `PythonClientPostBuild.scala` which copies the module into `unitycatalog/client/delta/internal/serde/` in the generated package and patches the Delta `models/__init__.py` with an import. No manual setup needed.
