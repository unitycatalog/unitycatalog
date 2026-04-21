package io.unitycatalog.server.utils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.delta.model.ArrayType;
import io.unitycatalog.server.delta.model.DeltaType;
import io.unitycatalog.server.delta.model.MapType;
import io.unitycatalog.server.delta.model.StructField;
import io.unitycatalog.server.delta.serde.DeltaTypeModule;
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
