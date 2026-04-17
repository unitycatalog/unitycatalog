# Delta Type Custom Serde

Custom Jackson serializer/deserializer for `DeltaType` that handles the string-or-object polymorphism in `StructField.type`.

## The Problem

Delta's wire format uses bare JSON strings for primitive types (`"long"`, `"decimal(10,2)"`) and JSON objects for complex types (`{"type": "array", "element-type": "string", "contains-null": true}`). OpenAPI's `discriminator` + `allOf` pattern generates `@JsonTypeInfo`/`@JsonSubTypes` which handles object types correctly, but cannot handle bare strings -- Jackson expects `START_OBJECT` to read the discriminator property.

## How It Works

Same design as the server-side serde (see `server/delta/serde/README.md`). The only difference is the Java language level: the client targets Java 11, so `instanceof` pattern matching is not used.

## Registration

`ApiClientBuilder.build()` automatically registers `DeltaTypeModule` on the client's `ObjectMapper`. No manual setup needed when using `ApiClientBuilder`.
