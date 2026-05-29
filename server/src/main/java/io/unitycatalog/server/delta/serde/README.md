# Delta Type Custom Serde

Custom Jackson serializer/deserializer for `DataType` that handles the string-or-object polymorphism in `StructField.type`.

## The Problem

Delta's wire format uses bare JSON strings for primitive types (`"long"`, `"decimal(10,2)"`) and JSON objects for complex types (`{"type": "array", "element-type": "string", "contains-null": true}`). OpenAPI's `discriminator` + `allOf` pattern generates `@JsonTypeInfo`/`@JsonSubTypes` which handles object types correctly, but cannot handle bare strings -- Jackson expects `START_OBJECT` to read the discriminator property.

## How It Works

`DataTypeModule` registers both a `BeanDeserializerModifier` and a `BeanSerializerModifier`:

- **Deserializer**: Intercepts `DataType` deserialization. If the JSON token is `VALUE_STRING`, parses it as `PrimitiveType` or `DecimalType` (via regex for `decimal(p,s)`). If the token is an object, delegates to the default deserializer which uses Jackson's `@JsonTypeInfo` dispatch.

- **Serializer**: Intercepts `DataType`, `PrimitiveType`, and `DecimalType` serialization. `PrimitiveType` writes the type name as a bare string. `DecimalType` derives `"decimal(p,s)"` from precision and scale fields. Complex types delegate to the default serializer which auto-generates the `"type"` discriminator field.

## Registration

Register `DataTypeModule` on any `ObjectMapper` that needs to handle `DataType`:

```java
mapper.registerModule(new DataTypeModule());
```

On the server, this is done in `UnityCatalogServer.addDeltaApiServices()` on the delta-specific `ObjectMapper`.
