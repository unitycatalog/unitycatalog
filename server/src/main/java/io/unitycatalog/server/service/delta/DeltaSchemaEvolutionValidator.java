package io.unitycatalog.server.service.delta;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.SchemaUtils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.unitycatalog.server.delta.model.DeltaArrayType;
import io.unitycatalog.server.delta.model.DeltaDataType;
import io.unitycatalog.server.delta.model.DeltaMapType;
import io.unitycatalog.server.delta.model.DeltaStructField;
import io.unitycatalog.server.delta.model.DeltaStructFieldMetadata;
import io.unitycatalog.server.delta.model.DeltaStructType;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.utils.ColumnUtils;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Checks a set-columns rename against Delta's schema evolution rules with Delta Kernel. Kernel
 * pairs fields by column-mapping id and rejects what Delta rejects: a new physical name under the
 * same id, a new field reusing an id at or below {@code delta.columnMapping.maxColumnId}, a field
 * moved to another struct, a type change that type widening does not allow, tightened nullability,
 * a new non-nullable field, and a field without an id.
 *
 * <p>A dropped clustering column is checked here, by its full physical path. Kernel compares only
 * the last physical name of a path, so it would take an unclustered field for a clustered one when
 * both end in the same physical name.
 *
 * <p>This is the only class that uses Kernel. The Kernel methods it calls are internal APIs with no
 * compatibility guarantee; {@code DeltaSchemaEvolutionValidatorTest} pins each behavior used.
 */
final class DeltaSchemaEvolutionValidator {

  private static final ObjectMapper JSON = new ObjectMapper();

  private DeltaSchemaEvolutionValidator() {}

  /** A field's name and physical name, as Kernel compares them for one column-mapping id. */
  private record Identity(String name, Object physicalName) {}

  /**
   * Runs the check when the request renames a top-level column, or keeps some field's id while
   * changing its name or physical name at any depth. Other requests skip it: Kernel rejects a
   * {@code delta.typeChanges} entry that differs from the stored one, which Delta writes on ALTER
   * COLUMN TYPE, while a rename commit carries the stored entries unchanged.
   *
   * @param currentProperties the table properties before this request is applied
   * @throws BaseException {@link ErrorCode#INVALID_ARGUMENT} when Delta would reject the change, or
   *     when Kernel cannot read a schema the check has to run on
   */
  static void validate(
      List<ColumnInfo> currentColumns,
      List<ColumnInfo> incomingColumns,
      Map<String, String> currentProperties,
      boolean renamesTopLevelColumn) {
    if (!renamesTopLevelColumn && !keepsAnIdUnderANewIdentity(currentColumns, incomingColumns)) {
      return;
    }
    StructType currentSchema;
    StructType incomingSchema;
    try {
      currentSchema = toKernelSchema(currentColumns);
      incomingSchema = toKernelSchema(incomingColumns);
    } catch (RuntimeException e) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "set-columns: the table schema cannot be read: " + e.getMessage(),
          e);
    }
    // Clustering columns are checked below by full path. Kernel compares only the last physical
    // name of a path, so it takes an unclustered field for a clustered one when both end in the
    // same physical name.
    // TODO: pass the paths to Kernel once it compares full paths (delta-io/delta#7976).
    List<List<String>> clusteringPaths = clusteringPaths(currentProperties);
    // The returned schema is deliberately unused: the catalog stores the schema the client commits.
    try {
      SchemaUtils.validateSchemaEvolutionById(
          currentSchema,
          incomingSchema,
          /* clusteringColumnPhysicalNames= */ Set.of(),
          maxColumnId(currentProperties),
          /* allowNewRequiredFields= */ false,
          TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.fromMetadata(currentProperties),
          TableConfig.TYPE_WIDENING_ENABLED.fromMetadata(currentProperties));
      rejectDroppedClusteringColumns(currentSchema, incomingSchema, clusteringPaths);
    } catch (KernelException | IllegalArgumentException e) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "set-columns: " + e.getMessage(), e);
    }
  }

  /** The stored {@code maxColumnId}; a value outside the int range is rejected, not truncated. */
  private static int maxColumnId(Map<String, String> properties) {
    long max;
    try {
      max = TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.fromMetadata(properties);
    } catch (IllegalArgumentException e) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "set-columns: the stored property "
              + DeltaConsts.TableProperties.COLUMN_MAPPING_MAX_COLUMN_ID
              + " is not a valid number: "
              + e.getMessage(),
          e);
    }
    if (max < 0 || max > Integer.MAX_VALUE) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "set-columns: the stored property "
              + DeltaConsts.TableProperties.COLUMN_MAPPING_MAX_COLUMN_ID
              + " is out of range: "
              + max);
    }
    return (int) max;
  }

  /** Each column's {@code type_json} is a full Delta struct field, so the list is a struct. */
  private static StructType toKernelSchema(List<ColumnInfo> columns) {
    String fields = columns.stream().map(ColumnInfo::getTypeJson).collect(Collectors.joining(","));
    return DataTypeJsonSerDe.deserializeStructType(
        "{\"type\":\"struct\",\"fields\":[" + fields + "]}");
  }

  /**
   * Paths of the clustering columns, from the stored {@code clusteringColumns} property only. A
   * path lists the physical name of each field from the top level down. Null or empty paths and
   * null segments are skipped.
   */
  private static List<List<String>> clusteringPaths(Map<String, String> properties) {
    String json = properties.get(DeltaConsts.TableProperties.CLUSTERING_COLUMNS);
    if (json == null) {
      return List.of();
    }
    List<List<String>> paths;
    try {
      paths = JSON.readValue(json, new TypeReference<List<List<String>>>() {});
    } catch (JsonProcessingException e) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "set-columns: the stored property "
              + DeltaConsts.TableProperties.CLUSTERING_COLUMNS
              + " cannot be read: "
              + e.getOriginalMessage(),
          e);
    }
    if (paths == null) {
      return List.of();
    }
    return paths.stream()
        .filter(Objects::nonNull)
        .map(path -> path.stream().filter(Objects::nonNull).collect(Collectors.toList()))
        .filter(path -> !path.isEmpty())
        .collect(Collectors.toList());
  }

  /**
   * Rejects the change when the field of a clustering column is gone from the incoming schema.
   * Fields are paired by column-mapping id, as Kernel pairs them. A path is resolved by physical
   * name from the top level, so a field that only shares the last physical name of a clustering
   * path is not taken for it. A path that names no field is ignored.
   */
  private static void rejectDroppedClusteringColumns(
      StructType currentSchema, StructType incomingSchema, List<List<String>> clusteringPaths) {
    Set<Integer> incomingIds = new HashSet<>();
    collectIds(incomingSchema, incomingIds);
    for (List<String> path : clusteringPaths) {
      StructField field = fieldAt(currentSchema, path);
      if (field != null && !incomingIds.contains(ColumnMapping.getColumnId(field))) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "set-columns: Cannot drop clustering column " + field.getName());
      }
    }
  }

  /** The field at a path of physical names from the top level, or null when there is none. */
  private static StructField fieldAt(StructType schema, List<String> path) {
    StructType level = schema;
    StructField field = null;
    for (String physicalName : path) {
      if (level == null) {
        return null;
      }
      field =
          level.fields().stream()
              .filter(f -> ColumnMapping.getPhysicalName(f).equals(physicalName))
              .findFirst()
              .orElse(null);
      if (field == null) {
        return null;
      }
      level = field.getDataType() instanceof StructType struct ? struct : null;
    }
    return field;
  }

  /** The column-mapping id of every struct field under {@code type}, at any depth. */
  private static void collectIds(DataType type, Set<Integer> ids) {
    if (type instanceof StructType struct) {
      for (StructField field : struct.fields()) {
        ids.add(ColumnMapping.getColumnId(field));
        collectIds(field.getDataType(), ids);
      }
    } else if (type instanceof ArrayType array) {
      collectIds(array.getElementType(), ids);
    } else if (type instanceof MapType map) {
      collectIds(map.getKeyType(), ids);
      collectIds(map.getValueType(), ids);
    }
  }

  private static boolean keepsAnIdUnderANewIdentity(
      List<ColumnInfo> currentColumns, List<ColumnInfo> incomingColumns) {
    Map<Long, Identity> current = identitiesById(currentColumns);
    return identitiesById(incomingColumns).entrySet().stream()
        .anyMatch(
            e -> current.containsKey(e.getKey()) && !current.get(e.getKey()).equals(e.getValue()));
  }

  /** The identity of every field with an integer column-mapping id, at any depth. */
  private static Map<Long, Identity> identitiesById(List<ColumnInfo> columns) {
    Map<Long, Identity> byId = new HashMap<>();
    for (ColumnInfo column : columns) {
      try {
        collect(ColumnUtils.toStructField(column), byId);
      } catch (RuntimeException e) {
        // An unreadable stored column has no identity to compare. If the request still needs the
        // check, reading the whole schema fails closed in validate().
      }
    }
    return byId;
  }

  private static void collect(DeltaStructField field, Map<Long, Identity> byId) {
    DeltaStructFieldMetadata meta = field.getMetadata();
    if (meta != null
        && meta.get(DeltaConsts.TableProperties.COLUMN_MAPPING_ID) instanceof Number id) {
      byId.put(
          id.longValue(),
          new Identity(
              field.getName(), meta.get(DeltaConsts.TableProperties.COLUMN_MAPPING_PHYSICAL_NAME)));
    }
    collectIn(field.getType(), byId);
  }

  private static void collectIn(DeltaDataType type, Map<Long, Identity> byId) {
    if (type instanceof DeltaStructType struct && struct.getFields() != null) {
      struct.getFields().forEach(f -> collect(f, byId));
    } else if (type instanceof DeltaArrayType array) {
      collectIn(array.getElementType(), byId);
    } else if (type instanceof DeltaMapType map) {
      collectIn(map.getKeyType(), byId);
      collectIn(map.getValueType(), byId);
    }
  }
}
