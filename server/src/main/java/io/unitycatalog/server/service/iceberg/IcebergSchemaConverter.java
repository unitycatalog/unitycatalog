package io.unitycatalog.server.service.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.ColumnTypeName;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Converts an Iceberg schema into Unity Catalog {@link ColumnInfo}s so tables created through the
 * Iceberg REST catalog are browsable through the UC API and UI. The Iceberg metadata file remains
 * the source of truth for Iceberg clients; this conversion only feeds UC's own column listing.
 *
 * <p>{@code type_json} is rendered in the Spark StructField JSON shape that UC stores for all
 * other formats (see {@code ColumnUtils.validateTypeJson}).
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

  private static ObjectNode structFieldJson(Types.NestedField field) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("name", field.name());
    node.set("type", typeJson(field.type()));
    node.put("nullable", field.isOptional());
    node.set("metadata", MAPPER.createObjectNode());
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
      default ->
          throw new BadRequestException(
              "Iceberg type %s is not supported by Unity Catalog",
              type.toString().toUpperCase(Locale.ROOT));
    };
  }
}
