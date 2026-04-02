package io.unitycatalog.server.delta.type;

/**
 * Marker interface for all Delta column data types. Implemented by PrimitiveDataType,
 * DecimalDataType, ArrayDataType, MapDataType, StructDataType.
 *
 * <p>Use {@link DataTypes#resolve(Object)} to convert a DeltaColumn's raw {@code type} field
 * (String or Map from Jackson) into a typed DataType instance.
 */
public interface DataType {}
