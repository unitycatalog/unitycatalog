package io.unitycatalog.server.utils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.delta.model.ArrayType;
import io.unitycatalog.server.delta.model.DeltaType;
import io.unitycatalog.server.delta.model.MapType;
import io.unitycatalog.server.delta.model.StructField;
import io.unitycatalog.server.delta.serde.DeltaTypeModule;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.ColumnTypeName;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class ColumnUtils {

  static Map<String, String> typeNameVsTypeText =
      new HashMap<String, String>() {
        {
          put("LONG", "bigint");
          put("SHORT", "smallint");
          put("BYTE", "tinyint");
        }
      };

  static Map<String, String> typeNameVsJsonText =
      new HashMap<String, String>() {
        {
          put("INT", "integer");
        }
      };

  static String getTypeText(ColumnTypeName typeName) {
    return typeNameVsTypeText.getOrDefault(
        typeName.getValue(), typeName.getValue().toLowerCase(Locale.ROOT));
  }

  static String getTypeJson(
      ColumnTypeName typeName, String columnName, boolean isNullable, String precisionAndScale) {
    return String.format(
        "{\"name\":\"%s\",\"type\":\"%s\",\"nullable\":%s,\"metadata\":{}}",
        columnName,
        typeNameVsJsonText.getOrDefault(
                typeName.getValue(), typeName.getValue().toLowerCase(Locale.ROOT))
            + (precisionAndScale != null ? precisionAndScale : ""),
        isNullable);
  }

  static String getPrecisionAndScale(ColumnInfoDAO columnInfoDAO) {
    if (columnInfoDAO.getTypePrecision() == null || columnInfoDAO.getTypeScale() == null) {
      columnInfoDAO.setTypePrecision(0);
      columnInfoDAO.setTypeScale(0);
      return "";
    }
    return getPrecisionAndScale(columnInfoDAO.getTypePrecision(), columnInfoDAO.getTypeScale());
  }

  public static String getPrecisionAndScale(int precision, int scale) {
    return "(" + precision + "," + scale + ")";
  }

  public static void addTypeTextAndJsonText(ColumnInfoDAO columnInfoDAO) {
    String typeName = columnInfoDAO.getTypeName();
    columnInfoDAO.setTypeText(
        typeNameVsTypeText.getOrDefault(typeName, typeName.toLowerCase(Locale.ROOT)));
    columnInfoDAO.setTypeJson(
        getTypeJson(
            ColumnTypeName.valueOf(columnInfoDAO.getTypeName()),
            columnInfoDAO.getName(),
            columnInfoDAO.isNullable(),
            getPrecisionAndScale(columnInfoDAO)));
  }

  // ObjectMapper for reading/writing Spark's camelCase typeJson.
  // Getter mixins override the generated kebab-case @JsonProperty
  // so both ser and deser use camelCase.
  private static final ObjectMapper TYPE_MAPPER = createTypeMapper();

  private static ObjectMapper createTypeMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new DeltaTypeModule());
    mapper.addMixIn(ArrayType.class, CamelCaseArrayMixin.class);
    mapper.addMixIn(MapType.class, CamelCaseMapMixin.class);
    return mapper;
  }

  abstract static class CamelCaseArrayMixin {
    @JsonProperty("elementType")
    abstract DeltaType getElementType();

    @JsonSetter("elementType")
    abstract void setElementType(DeltaType v);

    @JsonProperty("containsNull")
    abstract Boolean getContainsNull();

    @JsonSetter("containsNull")
    abstract void setContainsNull(Boolean v);
  }

  abstract static class CamelCaseMapMixin {
    @JsonProperty("keyType")
    abstract DeltaType getKeyType();

    @JsonSetter("keyType")
    abstract void setKeyType(DeltaType v);

    @JsonProperty("valueType")
    abstract DeltaType getValueType();

    @JsonSetter("valueType")
    abstract void setValueType(DeltaType v);

    @JsonProperty("valueContainsNull")
    abstract Boolean getValueContainsNull();

    @JsonSetter("valueContainsNull")
    abstract void setValueContainsNull(Boolean v);
  }

  /**
   * Convert a UC ColumnInfo to a Delta REST API StructField by parsing typeJson directly. The
   * typeJson is in Spark's StructField format and contains the complete field definition (name,
   * type, nullable, metadata). Only partitionIndex comes from ColumnInfo, not typeJson.
   */
  public static StructField toStructField(ColumnInfo column) {
    String typeJson = column.getTypeJson();
    if (typeJson == null || typeJson.isEmpty()) {
      throw new IllegalStateException("Column " + column.getName() + " has null/empty typeJson");
    }
    try {
      return TYPE_MAPPER.readValue(typeJson, StructField.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(
          "Failed to parse typeJson for column " + column.getName() + ": " + typeJson, e);
    }
  }

  /**
   * Validate that a UC column stores typeJson as Spark StructField JSON. The Spark connector and
   * Delta REST APIs depend on name, type, nullable, and metadata being present in this payload.
   */
  public static void validateTypeJson(ColumnInfo column) {
    if (column == null) {
      throw invalidTypeJson(null, "column cannot be null");
    }
    JsonNode typeJson = parseTypeJson(column);
    requireTypeJsonField(column, typeJson, StructField.JSON_PROPERTY_NAME);
    requireTypeJsonField(column, typeJson, StructField.JSON_PROPERTY_TYPE);
    requireTypeJsonField(column, typeJson, StructField.JSON_PROPERTY_NULLABLE);
    JsonNode metadata = requireTypeJsonField(column, typeJson, StructField.JSON_PROPERTY_METADATA);
    if (!metadata.isObject()) {
      throw invalidTypeJson(column, "type_json.metadata must be a JSON object");
    }

    StructField field;
    try {
      field = toStructField(column);
    } catch (IllegalStateException e) {
      throw invalidTypeJson(column, e.getMessage(), e);
    }
    if (!field.getName().equals(column.getName())) {
      throw invalidTypeJson(
          column,
          String.format(
              "field name %s does not match column name %s", field.getName(), column.getName()));
    }
    if (column.getNullable() != null && !field.getNullable().equals(column.getNullable())) {
      throw invalidTypeJson(
          column,
          String.format(
              "field nullable %s does not match column nullable %s",
              field.getNullable(), column.getNullable()));
    }
  }

  private static JsonNode parseTypeJson(ColumnInfo column) {
    if (column.getTypeJson() == null || column.getTypeJson().isEmpty()) {
      throw invalidTypeJson(column, "type_json cannot be null or empty");
    }
    try {
      JsonNode typeJson = TYPE_MAPPER.readTree(column.getTypeJson());
      if (typeJson == null || !typeJson.isObject()) {
        throw invalidTypeJson(column, "type_json must be a JSON object");
      }
      return typeJson;
    } catch (JsonProcessingException e) {
      throw invalidTypeJson(column, "Failed to parse typeJson: " + column.getTypeJson(), e);
    }
  }

  private static JsonNode requireTypeJsonField(ColumnInfo column, JsonNode typeJson, String name) {
    JsonNode field = typeJson.get(name);
    if (field == null || field.isNull()) {
      throw invalidTypeJson(column, "type_json." + name + " is required");
    }
    return field;
  }

  private static BaseException invalidTypeJson(ColumnInfo column, String message) {
    return invalidTypeJson(column, message, null);
  }

  private static BaseException invalidTypeJson(ColumnInfo column, String message, Throwable cause) {
    String columnName = column != null && column.getName() != null ? column.getName() : "<unknown>";
    return new BaseException(
        ErrorCode.INVALID_ARGUMENT,
        "Invalid type_json for column " + columnName + ": " + message,
        cause);
  }

  /** Serialize a Delta StructField to Spark's camelCase typeJson format for UC database storage. */
  public static String toTypeJson(StructField field) {
    try {
      return TYPE_MAPPER.writeValueAsString(field);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(
          "Failed to serialize typeJson for field " + field.getName(), e);
    }
  }
}
