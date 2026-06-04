package io.unitycatalog.server.utils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
<<<<<<< HEAD
import io.unitycatalog.server.delta.model.DeltaArrayType;
import io.unitycatalog.server.delta.model.DeltaDecimalType;
import io.unitycatalog.server.delta.model.DeltaDataType;
import io.unitycatalog.server.delta.model.DeltaMapType;
import io.unitycatalog.server.delta.model.DeltaStructField;
import io.unitycatalog.server.delta.model.DeltaStructType;
=======
import io.unitycatalog.server.delta.model.ArrayType;
import io.unitycatalog.server.delta.model.DecimalType;
import io.unitycatalog.server.delta.model.DeltaType;
import io.unitycatalog.server.delta.model.MapType;
import io.unitycatalog.server.delta.model.PrimitiveType;
import io.unitycatalog.server.delta.model.StructField;
import io.unitycatalog.server.delta.model.StructFieldMetadata;
import io.unitycatalog.server.delta.model.StructType;
>>>>>>> main
import io.unitycatalog.server.delta.serde.DeltaTypeModule;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.ColumnTypeName;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    mapper.addMixIn(DeltaArrayType.class, CamelCaseArrayMixin.class);
    mapper.addMixIn(DeltaMapType.class, CamelCaseMapMixin.class);
    return mapper;
  }

  abstract static class CamelCaseArrayMixin {
    @JsonProperty("elementType")
    abstract DeltaDataType getElementType();

    @JsonSetter("elementType")
    abstract void setElementType(DeltaDataType v);

    @JsonProperty("containsNull")
    abstract Boolean getContainsNull();

    @JsonSetter("containsNull")
    abstract void setContainsNull(Boolean v);
  }

  abstract static class CamelCaseMapMixin {
    @JsonProperty("keyType")
    abstract DeltaDataType getKeyType();

    @JsonSetter("keyType")
    abstract void setKeyType(DeltaDataType v);

    @JsonProperty("valueType")
    abstract DeltaDataType getValueType();

    @JsonSetter("valueType")
    abstract void setValueType(DeltaDataType v);

    @JsonProperty("valueContainsNull")
    abstract Boolean getValueContainsNull();

    @JsonSetter("valueContainsNull")
    abstract void setValueContainsNull(Boolean v);
  }

  /**
   * Convert a UC ColumnInfo to a Delta REST API DeltaStructField by parsing typeJson directly. The
   * typeJson is in Spark's StructField format and contains the complete field definition (name,
   * type, nullable, metadata). Only partitionIndex comes from ColumnInfo, not typeJson.
   */
  public static DeltaStructField toStructField(ColumnInfo column) {
    String typeJson = column.getTypeJson();
    if (typeJson == null || typeJson.isEmpty()) {
      throw new IllegalStateException("Column " + column.getName() + " has null/empty typeJson");
    }
    try {
      return TYPE_MAPPER.readValue(typeJson, DeltaStructField.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(
          "Failed to parse typeJson for column " + column.getName() + ": " + typeJson, e);
    }
  }

  /**
   * Validate that a UC column stores {@code typeJson} as Spark {@link StructField} JSON. Shape
   * (deserializability) and name/nullable cross-match run for all formats. The
   * Delta-protocol primitive-type closed-set check runs only for {@link DataSourceFormat#DELTA} --
   * non-Delta tables (Parquet, CSV, ...) may legitimately carry Spark types that the closed set
   * rejects (e.g. {@code char(N)} / {@code varchar(N)}).
   */
  public static void validateTypeJson(ColumnInfo column, DataSourceFormat format) {
    if (column == null) {
      throw invalidTypeJson(null, "column cannot be null");
    }
<<<<<<< HEAD
    JsonNode typeJson = parseTypeJson(column);
    requireTypeJsonField(column, typeJson, DeltaStructField.JSON_PROPERTY_NAME);
    requireTypeJsonField(column, typeJson, DeltaStructField.JSON_PROPERTY_TYPE);
    requireTypeJsonField(column, typeJson, DeltaStructField.JSON_PROPERTY_NULLABLE);
    JsonNode metadata = requireTypeJsonField(column, typeJson,
        DeltaStructField.JSON_PROPERTY_METADATA);
    if (!metadata.isObject()) {
      throw invalidTypeJson(column, "type_json.metadata must be a JSON object");
    }

    DeltaStructField field;
=======
    StructField field;
>>>>>>> main
    try {
      field = toStructField(column);
    } catch (IllegalStateException e) {
      throw invalidTypeJson(column, e.getMessage(), e);
    }
    if (format == DataSourceFormat.DELTA) {
      try {
        validateStructField(field, "type_json");
      } catch (BaseException e) {
        throw invalidTypeJson(column, e.getErrorMessage(), e);
      }
    } else {
      // A shallow check for non-Delta
      requireNonNull(field.getName(), "type_json.name");
      requireNonNull(field.getType(), "type_json.type");
      requireNonNull(field.getNullable(), "type_json.nullable");
      requireNonNull(field.getMetadata(), "type_json.metadata");
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

  /**
   * Serialize a DeltaStructField to Spark's camelCase typeJson format for UC database storage.
   */
  public static String toTypeJson(DeltaStructField field) {
    try {
      return TYPE_MAPPER.writeValueAsString(field);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(
          "Failed to serialize typeJson for field " + field.getName(), e);
    }
  }

  /**
<<<<<<< HEAD
   * Convert a UC Delta API {@link DeltaStructField} into a UC {@link ColumnInfo}, mirroring
=======
   * Validate that a {@link StructType} is well-formed against the Delta spec's wire shape.
   * Centralizes structural validation so callers can pass {@code columns} directly without
   * pre-checking nullness/emptiness.
   *
   * <p>Field-name uniqueness is enforced case-insensitively (per Delta's schema rule "All column
   * names must be unique regardless of casing"). Names are stored case-preserving on
   * {@link StructField#getName()}; only the duplicate check ignores case.
   */
  public static void validateStructType(StructType structType, String path) {
    requireNonNull(structType, path);
    requireNonNull(structType.getFields(), path + ".fields");
    List<StructField> fields = structType.getFields();
    if (fields.isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, path + ".fields must contain at least one field.");
    }
    Set<String> seenLower = new HashSet<>();
    for (int i = 0; i < fields.size(); i++) {
      String fieldPath = path + ".fields[" + i + "]";
      requireNonNull(fields.get(i), fieldPath);
      validateStructField(fields.get(i), fieldPath);
      String name = fields.get(i).getName();
      if (!seenLower.add(name.toLowerCase(Locale.ROOT))) {
        throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Duplicate field name (case-insensitive) in " + fieldPath + ": " + name);
      }
    }
  }

  private static void validateStructField(StructField field, String path) {
    String name = field.getName();
    if (name == null || name.isBlank()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, path + ".name is required.");
    }
    requireNonNull(field.getType(), path + ".type");
    requireNonNull(field.getNullable(), path + ".nullable");
    requireNonNull(field.getMetadata(), path + ".metadata");
    validateType(field.getType(), path + ".type");
  }

  private static void validateType(DeltaType type, String path) {
    if (type instanceof StructType s) {
      validateStructType(s, path);
    } else if (type instanceof ArrayType a) {
      validateArrayType(a, path);
    } else if (type instanceof MapType m) {
      validateMapType(m, path);
    } else if (type instanceof DecimalType d) {
      validateDecimalType(d, path);
    } else if (type instanceof PrimitiveType p) {
      validatePrimitiveType(p, path);
    }
  }

  private static void validateArrayType(ArrayType array, String path) {
    requireNonNull(array.getElementType(), path + ".element-type");
    requireNonNull(array.getContainsNull(), path + ".contains-null");
    validateType(array.getElementType(), path + ".element-type");
  }

  private static void validateMapType(MapType map, String path) {
    requireNonNull(map.getKeyType(), path + ".key-type");
    requireNonNull(map.getValueType(), path + ".value-type");
    requireNonNull(map.getValueContainsNull(), path + ".value-contains-null");
    validateType(map.getKeyType(), path + ".key-type");
    validateType(map.getValueType(), path + ".value-type");
  }

  private static void validateDecimalType(DecimalType decimal, String path) {
    requireNonNull(decimal.getPrecision(), path + ".precision");
    requireNonNull(decimal.getScale(), path + ".scale");
    int precision = decimal.getPrecision();
    int scale = decimal.getScale();
    // Matches io.delta.kernel.types.DecimalType bounds (precision=0 is allowed there, even
    // though it's degenerate).
    if (precision < 0 || precision > 38) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          String.format("%s.precision must be in [0, 38], got %d.", path, precision));
    }
    if (scale < 0 || scale > precision) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          String.format("%s.scale must be in [0, precision=%d], got %d.", path, precision, scale));
    }
  }

  private static void validatePrimitiveType(PrimitiveType primitive, String path) {
    // The PrimitiveType discriminator (the "type" string) IS the primitive name; the caller's
    // path already ends in `.type`, so no further suffix here. Resolve to confirm the name is in
    // the closed set we support; rethrow with the field path attached so the error mirrors the
    // shape of every other validation message in this file (downstream toColumnInfos would
    // otherwise throw without path context).
    requireNonNull(primitive.getType(), path);
    try {
      resolveColumnTypeName(primitive);
    } catch (BaseException e) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, path + ": " + e.getMessage(), e);
    }
  }

  private static void requireNonNull(Object value, String path) {
    if (value == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, path + " is required.");
    }
  }

  /**
   * Convert a UC Delta API {@link StructField} into a UC {@link ColumnInfo}, mirroring
>>>>>>> main
   * {@code UCSingleCatalog.createTable}'s per-column projection so Delta-created tables render
   * identically to Spark-created ones.
   *
   * <p>Caller contract: {@code field} must have already been validated by
   * {@link #validateStructType}. This method does not re-check shape -- malformed input will
   * surface as a downstream {@link IllegalStateException} from {@link #toTypeJson} or a
   * {@link BaseException} from {@link #resolveColumnTypeName}, without the path-context that
   * {@code validateStructType} provides.
   *
   * <p>Fields populated (matching UCSingleCatalog exactly):
   * <ul>
   *   <li>{@code name}, {@code nullable}, {@code position}
   *   <li>{@code comment} -- only when present in {@code metadata.comment} (UCSingleCatalog
   *       skips the setter when {@code field.getComment().isEmpty}); when absent the field is
   *       left at its default {@code null}, which produces the same wire shape
   *   <li>{@code typeJson} -- {@code field.dataType.json}, the Spark-format column spec
   *   <li>{@code typeName} -- {@code convertDataTypeToTypeName(field.dataType)}
   *   <li>{@code typeText} -- {@code field.dataType.catalogString}
   * </ul>
   *
   * <p>Fields intentionally not populated (matching UCSingleCatalog):
   * {@code typePrecision}, {@code typeScale}, {@code typeIntervalType}, {@code partitionIndex}
   * ({@code partitionIndex} is stamped separately by {@link #applyPartitionColumns}).
   */
  public static ColumnInfo toColumnInfo(DeltaStructField field, int position) {
    DeltaDataType type = field.getType();
    ColumnInfo column =
        new ColumnInfo()
            .name(field.getName())
            .nullable(Boolean.TRUE.equals(field.getNullable()))
            .position(position)
            .typeJson(toTypeJson(field))
            .typeName(resolveColumnTypeName(type))
            .typeText(toCatalogString(type));
    String comment = extractComment(field);
    if (comment != null) {
      column.comment(comment);
    }
    return column;
  }

  /**
   * Project a list of {@link DeltaStructField}s into UC {@link ColumnInfo}s, stamping each
   * column's {@code position} from its index in the list. Used by both the create and update paths
   * of the UC Delta API so the wire-order-to-position mapping stays in one place.
   */
  public static List<ColumnInfo> toColumnInfos(List<DeltaStructField> fields) {
    List<ColumnInfo> columns = new ArrayList<>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      columns.add(toColumnInfo(fields.get(i), i));
    }
    return columns;
  }

  /**
   * Render a {@link DeltaDataType} as a Spark-style catalog string -- the same format produced by
   * {@code org.apache.spark.sql.types.DataType#catalogString}. Primitives go through the shared
   * {@link #typeNameVsTypeText} override map (so {@code long}→{@code bigint}, {@code
   * integer}→{@code int}, etc.); decimals include precision/scale; array/map/struct recurse.
   *
   * @throws BaseException with {@code INVALID_ARGUMENT} if {@code type} (or any nested element /
   *     key / value / field type) is an unsupported Delta primitive -- this method delegates the
   *     primitive lookup to {@link #resolveColumnTypeName}, which throws on unknown names.
   */
  public static String toCatalogString(DeltaDataType type) {
    if (type instanceof DeltaDecimalType d) {
      return "decimal" + getPrecisionAndScale(d.getPrecision(), d.getScale());
    }
    if (type instanceof DeltaArrayType a) {
      return "array<" + toCatalogString(a.getElementType()) + ">";
    }
    if (type instanceof DeltaMapType m) {
      return "map<" + toCatalogString(m.getKeyType()) + "," + toCatalogString(m.getValueType())
          + ">";
    }
    if (type instanceof DeltaStructType s) {
      List<DeltaStructField> fields = s.getFields() != null ? s.getFields() : List.of();
      StringBuilder sb = new StringBuilder("struct<");
      for (int i = 0; i < fields.size(); i++) {
        if (i > 0) sb.append(",");
        DeltaStructField f = fields.get(i);
        sb.append(f.getName()).append(":").append(toCatalogString(f.getType()));
      }
      return sb.append(">").toString();
    }
    return getTypeText(resolveColumnTypeName(type));
  }

<<<<<<< HEAD
  private static String extractComment(DeltaStructField field) {
    Map<String, Object> metadata = field.getMetadata();
=======
  private static String extractComment(StructField field) {
    StructFieldMetadata metadata = field.getMetadata();
>>>>>>> main
    if (metadata == null) return null;
    // StructFieldMetadata extends HashMap via additionalProperties; the schema deliberately does
    // not promote "comment" to a typed property (see api/delta.yaml). Read via Map access.
    Object fromMap = metadata.get("comment");
    return fromMap instanceof String s ? s : null;
  }

  private static ColumnTypeName resolveColumnTypeName(DeltaDataType type) {
    if (type instanceof DeltaArrayType) return ColumnTypeName.ARRAY;
    if (type instanceof DeltaDecimalType) return ColumnTypeName.DECIMAL;
    if (type instanceof DeltaMapType) return ColumnTypeName.MAP;
    if (type instanceof DeltaStructType) return ColumnTypeName.STRUCT;
    // DeltaPrimitiveType: the discriminator string IS the primitive name. The accepted set is the
    // closed list of Delta-protocol primitives -- Kernel's BasePrimitiveType registry. Reject
    // with INVALID_ARGUMENT for anything else; the server-side ColumnTypeName enum has no
    // UNKNOWN_DEFAULT_OPEN_API sentinel (OpenAPI generator adds it only for clients).
    //
    // Not handled, and why:
    //   - INTERVAL: not a Delta primitive.
    //   - NULL ("void" in Spark JSON): UCSingleCatalog has a NullType branch because it runs
    //     upstream of Delta's NullType-drop -- a user's CREATE TABLE can carry NullType and UC
    //     stores it before Delta strips it on write (SchemaUtils.dropNullTypeColumns). Delta runs
    //     downstream: Spark-Delta clients have already dropped NullType before sending, and
    //     Kernel rejects "void" on read (DataTypeJsonSerDe.voidTypeEncountered). No conforming
    //     Delta wire payload contains it.
    //   - CHAR / UDT: Spark's Delta writer normalizes CHAR/VARCHAR to STRING (via
    //     CharVarcharUtils) and unwraps UDTs to their underlying sqlType before persisting, so
    //     Delta clients never send "char(n)" or {"type":"udt",...}.
    //   - TABLE_TYPE: UC-internal (foreign tables); doesn't flow over the Delta wire.
    String primitive = type.getType();
    return switch (primitive) {
      case "boolean" -> ColumnTypeName.BOOLEAN;
      case "byte" -> ColumnTypeName.BYTE;
      case "short" -> ColumnTypeName.SHORT;
      case "integer" -> ColumnTypeName.INT;
      case "long" -> ColumnTypeName.LONG;
      case "float" -> ColumnTypeName.FLOAT;
      case "double" -> ColumnTypeName.DOUBLE;
      case "date" -> ColumnTypeName.DATE;
      case "timestamp" -> ColumnTypeName.TIMESTAMP;
      case "timestamp_ntz" -> ColumnTypeName.TIMESTAMP_NTZ;
      case "string" -> ColumnTypeName.STRING;
      case "binary" -> ColumnTypeName.BINARY;
      case "variant" -> ColumnTypeName.VARIANT;
      default ->
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT, "Unsupported Delta primitive type: " + primitive);
    };
  }

  /**
   * Stamp each {@link ColumnInfo} whose name appears in {@code partitionColumns} with its
   * partition index (the position within {@code partitionColumns}). Throws {@code INVALID_ARGUMENT}
   * at the first partition-column name that does not match any column in {@code columns}.
   *
   * <p>The input {@code columns} list is mutated in place. Designed to be shared between the
   * UC Delta API create and update/commit paths -- both take a kebab-case {@code
   * partition-columns} list of names from the wire and project it onto UC's
   * partition-index-per-column representation (only create is wired up today).
   *
   * <p>Runs in {@code O(|columns| + |partitionColumns|)}: builds a name → ColumnInfo lookup once,
   * then walks {@code partitionColumns} stamping indices and rejecting unknown names inline.
   */
  public static void applyPartitionColumns(
      List<ColumnInfo> columns, List<String> partitionColumns) {
    if (partitionColumns == null || partitionColumns.isEmpty()) {
      return;
    }
    Map<String, ColumnInfo> columnsByLowerName =
        columns.stream()
            .collect(
                Collectors.toMap(c -> c.getName().toLowerCase(Locale.ROOT), Function.identity()));
    Set<String> seenLower = new HashSet<>();
    for (int i = 0; i < partitionColumns.size(); i++) {
      String partName = partitionColumns.get(i);
      String lowerPartName = partName.toLowerCase(Locale.ROOT);
      // Duplicate detection within partition-columns, comparing case-insensitively.
      if (!seenLower.add(lowerPartName)) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "partition-columns contains duplicate entry (case-insensitive): " + partName);
      }
      // Column-name lookup against the schema, comparing case-insensitively.
      ColumnInfo match = columnsByLowerName.get(lowerPartName);
      if (match == null) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "partition-columns references unknown column: " + partName);
      }
      match.setPartitionIndex(i);
    }
  }
}
