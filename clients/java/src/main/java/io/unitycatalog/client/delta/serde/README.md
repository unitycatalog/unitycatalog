# Delta Type Custom Serde

Custom Jackson serializer/deserializer for `DeltaDataType` that handles the string-or-object polymorphism in `DeltaStructField.type`.

## The Problem

Delta's wire format uses bare JSON strings for primitive types (`"long"`, `"decimal(10,2)"`) and JSON objects for complex types (`{"type": "array", "element-type": "string", "contains-null": true}`). OpenAPI's `discriminator` + `allOf` pattern generates `@JsonTypeInfo`/`@JsonSubTypes` which handles object types correctly, but cannot handle bare strings -- Jackson expects `START_OBJECT` to read the discriminator property.

## How It Works

`DeltaDataTypeModule` is the public entry point applications register on their `ObjectMapper`. It installs the `BeanSerializer`/`BeanDeserializer` modifiers that swap in the custom string-aware (de)serializers from the `internal/` subpackage. Classes under `internal/` are implementation detail; consumers should depend on `DeltaDataTypeModule` only. Same design as the server-side serde (see `server/delta/serde/README.md`); the only difference is the Java language level: the client targets Java 11, so `instanceof` pattern matching is not used.

## Registration

`ApiClientBuilder.build()` automatically registers `DeltaDataTypeModule` on the client's `ObjectMapper`. No manual setup needed when using `ApiClientBuilder`.
