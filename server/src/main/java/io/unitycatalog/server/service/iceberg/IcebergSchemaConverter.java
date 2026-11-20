package io.unitycatalog.server.service.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.unitycatalog.server.delta.model.DeltaArrayType;
import io.unitycatalog.server.delta.model.DeltaDataType;
import io.unitycatalog.server.delta.model.DeltaDecimalType;
import io.unitycatalog.server.delta.model.DeltaMapType;
import io.unitycatalog.server.delta.model.DeltaStructField;
import io.unitycatalog.server.delta.model.DeltaStructType;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.ColumnTypeName;
import io.unitycatalog.server.utils.ColumnUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Converts between Iceberg schemas and Unity Catalog {@link ColumnInfo}s. Iceberg table metadata
 * remains authoritative for table clients; its conversion feeds UC's column listing. Native view
 * snapshots derive their schemas from the canonical UC columns.
 *
 * <p>{@code type_json} is rendered in the Spark StructField JSON shape that UC stores for all other
 * formats (see {@code ColumnUtils.validateTypeJson}).
 */
public final class IcebergSchemaConverter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private IcebergSchemaConverter() {}

  public static List<ColumnInfo> toColumnInfos(Schema schema) {
    List<ColumnInfo> columns = new ArrayList<>();
    List<Types.NestedField> fields = schema.columns();
    for (int i = 0; i < fields.size(); i++) {
      Types.NestedField field = fields.get(i);
      ColumnInfo column =
          new ColumnInfo()
              .name(field.name())
              .typeText(typeText(field.type()))
              .typeJson(structFieldJson(field).toString())
              .typeName(typeName(field.type()))
              .position(i)
              .nullable(field.isOptional())
              .comment(field.doc());
      if (field.type().typeId() == Type.TypeID.DECIMAL) {
        Types.DecimalType decimal = (Types.DecimalType) field.type();
        column.typePrecision(decimal.precision()).typeScale(decimal.scale());
      }
      columns.add(column);
    }
    return columns;
  }

  /** Native views have no stored Iceberg field IDs; assign stable IDs in column order. */
  static Schema toIcebergSchema(List<ColumnInfo> columns) {
    AtomicInteger ids = new AtomicInteger();
    List<Types.NestedField> fields = new ArrayList<>();
    var ordered = columns.stream().sorted(Comparator.comparing(ColumnInfo::getPosition)).toList();
    var names = new HashSet<String>();
    for (int position = 0; position < ordered.size(); position++) {
      ColumnInfo column = ordered.get(position);
      if (column.getPosition() != position || !names.add(column.getName())) {
        throw new BadRequestException(
            "Native view column order or name is invalid: %s", column.getName());
      }
      try {
        DeltaStructField field = ColumnUtils.toStructField(column);
        int id = ids.incrementAndGet();
        fields.add(
            Types.NestedField.of(
                id,
                !Boolean.FALSE.equals(field.getNullable()),
                field.getName(),
                toIcebergType(field.getType(), ids),
                column.getComment()));
      } catch (IllegalStateException | IllegalArgumentException e) {
        throw new BadRequestException(
            e, "Native view column cannot be represented in Iceberg: %s", column.getName());
      }
    }
    return new Schema(fields);
  }

  private static Type toIcebergType(DeltaDataType type, AtomicInteger ids) {
    if (type instanceof DeltaDecimalType decimal) {
      return Types.DecimalType.of(decimal.getPrecision(), decimal.getScale());
    }
    if (type instanceof DeltaStructType struct) {
      List<Types.NestedField> fields = new ArrayList<>();
      for (DeltaStructField field : struct.getFields()) {
        int id = ids.incrementAndGet();
        Object comment = field.getMetadata() == null ? null : field.getMetadata().get("comment");
        fields.add(
            Types.NestedField.of(
                id,
                !Boolean.FALSE.equals(field.getNullable()),
                field.getName(),
                toIcebergType(field.getType(), ids),
                comment instanceof String text ? text : null));
      }
      return Types.StructType.of(fields);
    }
    if (type instanceof DeltaArrayType array) {
      int id = ids.incrementAndGet();
      Type element = toIcebergType(array.getElementType(), ids);
      return Boolean.FALSE.equals(array.getContainsNull())
          ? Types.ListType.ofRequired(id, element)
          : Types.ListType.ofOptional(id, element);
    }
    if (type instanceof DeltaMapType map) {
      int keyId = ids.incrementAndGet();
      int valueId = ids.incrementAndGet();
      Type key = toIcebergType(map.getKeyType(), ids);
      Type value = toIcebergType(map.getValueType(), ids);
      return Boolean.FALSE.equals(map.getValueContainsNull())
          ? Types.MapType.ofRequired(keyId, valueId, key, value)
          : Types.MapType.ofOptional(keyId, valueId, key, value);
    }
    if (type == null) {
      throw new BadRequestException("Missing native view column type");
    }
    return switch (type.getType()) {
      case "boolean" -> Types.BooleanType.get();
      case "integer", "int" -> Types.IntegerType.get();
      case "long" -> Types.LongType.get();
      case "float" -> Types.FloatType.get();
      case "double" -> Types.DoubleType.get();
      case "date" -> Types.DateType.get();
      case "timestamp" -> Types.TimestampType.withZone();
      case "timestamp_ntz" -> Types.TimestampType.withoutZone();
      case "string" -> Types.StringType.get();
      case "binary" -> Types.BinaryType.get();
      case "variant" -> Types.VariantType.get();
      default -> throw new BadRequestException("Unsupported native view type: %s", type.getType());
    };
  }

  private static ObjectNode structFieldJson(Types.NestedField field) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("name", field.name());
    node.set("type", typeJson(field.type()));
    node.put("nullable", field.isOptional());
    ObjectNode metadata = MAPPER.createObjectNode();
    if (field.doc() != null) {
      metadata.put("comment", field.doc());
    }
    node.set("metadata", metadata);
    return node;
  }

  private static JsonNode typeJson(Type type) {
    return switch (type.typeId()) {
      case STRUCT -> {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("type", "struct");
        ArrayNode fields = node.putArray("fields");
        ((Types.StructType) type).fields().forEach(f -> fields.add(structFieldJson(f)));
        yield node;
      }
      case LIST -> {
        Types.ListType list = (Types.ListType) type;
        ObjectNode node = MAPPER.createObjectNode();
        node.put("type", "array");
        node.set("elementType", typeJson(list.elementType()));
        node.put("containsNull", list.isElementOptional());
        yield node;
      }
      case MAP -> {
        Types.MapType map = (Types.MapType) type;
        ObjectNode node = MAPPER.createObjectNode();
        node.put("type", "map");
        node.set("keyType", typeJson(map.keyType()));
        node.set("valueType", typeJson(map.valueType()));
        node.put("valueContainsNull", map.isValueOptional());
        yield node;
      }
      default -> MAPPER.getNodeFactory().textNode(primitiveTypeName(type));
    };
  }

  private static String typeText(Type type) {
    return switch (type.typeId()) {
      case STRUCT ->
          ((Types.StructType) type)
              .fields().stream()
                  .map(f -> f.name() + ":" + typeText(f.type()))
                  .collect(Collectors.joining(",", "struct<", ">"));
      case LIST -> "array<" + typeText(((Types.ListType) type).elementType()) + ">";
      case MAP -> {
        Types.MapType map = (Types.MapType) type;
        yield "map<" + typeText(map.keyType()) + "," + typeText(map.valueType()) + ">";
      }
      case INTEGER -> "int";
      case LONG -> "bigint";
      default -> primitiveTypeName(type);
    };
  }

  private static String primitiveTypeName(Type type) {
    return switch (type.typeId()) {
      case BOOLEAN -> "boolean";
      case INTEGER -> "integer";
      case LONG -> "long";
      case FLOAT -> "float";
      case DOUBLE -> "double";
      case DATE -> "date";
      case TIMESTAMP ->
          ((Types.TimestampType) type).shouldAdjustToUTC() ? "timestamp" : "timestamp_ntz";
      case STRING, UUID -> "string";
      case FIXED, BINARY -> "binary";
      case DECIMAL -> {
        Types.DecimalType decimal = (Types.DecimalType) type;
        yield String.format("decimal(%d,%d)", decimal.precision(), decimal.scale());
      }
      case VARIANT -> "variant";
      // Iceberg has no standard GEOMETRY or GEOGRAPHY primitive type today. Add explicit mappings
      // here if those types are standardized instead of silently coercing extension types.
      default ->
          throw new BadRequestException(
              "Iceberg type %s is not supported by Unity Catalog",
              type.toString().toUpperCase(Locale.ROOT));
    };
  }

  private static ColumnTypeName typeName(Type type) {
    return switch (type.typeId()) {
      case BOOLEAN -> ColumnTypeName.BOOLEAN;
      case INTEGER -> ColumnTypeName.INT;
      case LONG -> ColumnTypeName.LONG;
      case FLOAT -> ColumnTypeName.FLOAT;
      case DOUBLE -> ColumnTypeName.DOUBLE;
      case DATE -> ColumnTypeName.DATE;
      case TIMESTAMP ->
          ((Types.TimestampType) type).shouldAdjustToUTC()
              ? ColumnTypeName.TIMESTAMP
              : ColumnTypeName.TIMESTAMP_NTZ;
      case STRING, UUID -> ColumnTypeName.STRING;
      case FIXED, BINARY -> ColumnTypeName.BINARY;
      case DECIMAL -> ColumnTypeName.DECIMAL;
      case VARIANT -> ColumnTypeName.VARIANT;
      case STRUCT -> ColumnTypeName.STRUCT;
      case LIST -> ColumnTypeName.ARRAY;
      case MAP -> ColumnTypeName.MAP;
      // Iceberg has no standard GEOMETRY or GEOGRAPHY primitive type today. Keep this rejection in
      // sync with primitiveTypeName until Iceberg defines interoperable type IDs for them.
      default ->
          throw new BadRequestException(
              "Iceberg type %s is not supported by Unity Catalog",
              type.toString().toUpperCase(Locale.ROOT));
    };
  }
}
