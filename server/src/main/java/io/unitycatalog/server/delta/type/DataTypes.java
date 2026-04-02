package io.unitycatalog.server.delta.type;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.delta.model.DeltaColumn;
import java.util.Map;

/**
 * Resolves DeltaColumn's raw {@code type} field (String or Map from Jackson deserialization) into a
 * fully typed {@link DataType} tree. All nested types are recursively resolved.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * DataType dt = DataTypes.resolve(column);
 * if (dt instanceof PrimitiveDataType p) { p.getName(); }
 * else if (dt instanceof DecimalDataType d) { d.getPrecision(); }
 * else if (dt instanceof ArrayDataType a) { DataType elem = (DataType) a.getElementType(); }
 * else if (dt instanceof MapDataType m) { ... }
 * else if (dt instanceof StructDataType s) { ... }
 * }</pre>
 */
public final class DataTypes {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private DataTypes() {}

  /** Resolve a DeltaColumn's type field into a fully typed DataType tree. */
  public static DataType resolve(DeltaColumn column) {
    return resolve(column.getType());
  }

  /**
   * Resolve a raw type value into a typed DataType, recursively resolving nested types.
   *
   * @param type raw value from {@link DeltaColumn#getType()} (String or Map)
   * @return a DataType instance with all nested types also resolved
   */
  public static DataType resolve(Object type) {
    if (type == null) {
      return null;
    }
    if (type instanceof DataType) {
      return (DataType) type;
    }
    if (type instanceof String) {
      return resolveString((String) type);
    }
    if (type instanceof Map) {
      return resolveMap(type);
    }
    throw new IllegalArgumentException("Unknown type: " + type.getClass());
  }

  private static DataType resolveString(String s) {
    DecimalDataType decimal = DecimalDataType.parse(s);
    if (decimal != null) {
      return decimal;
    }
    return new PrimitiveDataType(s);
  }

  @SuppressWarnings("unchecked")
  private static DataType resolveMap(Object map) {
    String kind = (String) ((Map<String, Object>) map).get("type");
    if (kind == null) {
      throw new IllegalArgumentException(
          "Missing 'type' in complex type: " + map);
    }
    return switch (kind) {
      case "array" -> resolveArray(map);
      case "map" -> resolveMapType(map);
      case "struct" -> resolveStruct(map);
      default ->
          throw new IllegalArgumentException("Unknown complex type: " + kind);
    };
  }

  private static ArrayDataType resolveArray(Object map) {
    ArrayDataType arr = MAPPER.convertValue(map, ArrayDataType.class);
    arr.setElementType(resolve(arr.getElementType()));
    return arr;
  }

  private static MapDataType resolveMapType(Object map) {
    MapDataType m = MAPPER.convertValue(map, MapDataType.class);
    m.setKeyType(resolve(m.getKeyType()));
    m.setValueType(resolve(m.getValueType()));
    return m;
  }

  private static StructDataType resolveStruct(Object map) {
    StructDataType s = MAPPER.convertValue(map, StructDataType.class);
    if (s.getFields() != null) {
      s.getFields().forEach(col -> col.setType(resolve(col.getType())));
    }
    return s;
  }
}
