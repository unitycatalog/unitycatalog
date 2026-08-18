# Delta Type Custom Serde

Custom Jackson serializer/deserializer for `DeltaDataType` that handles the string-or-object polymorphism in `DeltaStructField.type`.

## The Problem

Delta's wire format uses bare JSON strings for primitive types (`"long"`, `"decimal(10,2)"`) and JSON objects for complex types (`{"type": "array", "element-type": "string", "contains-null": true}`). OpenAPI's `discriminator` + `allOf` pattern generates `@JsonTypeInfo`/`@JsonSubTypes` which handles object types correctly, but cannot handle bare strings -- Jackson expects `START_OBJECT` to read the discriminator property.

## How It Works

`DeltaDataTypeModule` is the public entry point. It registers a `BeanDeserializerModifier` and a `BeanSerializerModifier` that swap in the custom (de)serializers from the `internal/` subpackage:

- **`internal/DeltaTypeDeserializer`**: Intercepts `DeltaDataType` deserialization. If the JSON token is `VALUE_STRING`, parses it as `DeltaPrimitiveType` or `DeltaDecimalType` (via regex for `decimal(p,s)`). If the token is an object, delegates to the default deserializer which uses Jackson's `@JsonTypeInfo` dispatch.

- **`internal/DeltaTypeSerializer`**: Intercepts `DeltaDataType`, `DeltaPrimitiveType`, and `DeltaDecimalType` serialization. `DeltaPrimitiveType` writes the type name as a bare string. `DeltaDecimalType` derives `"decimal(p,s)"` from precision and scale fields. Complex types delegate to the default serializer which auto-generates the `"type"` discriminator field.

Classes under `internal/` are implementation detail; consumers should depend on `DeltaDataTypeModule` only.

## Registration

Register `DeltaDataTypeModule` on any `ObjectMapper` that needs to handle `DeltaDataType`:

```java
mapper.registerModule(new DeltaDataTypeModule());
```

On the server, this is done in `DeltaApiMappers` on the UC Delta API `ObjectMapper`.
