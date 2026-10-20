package io.unitycatalog.server.service.delta;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.internal.util.SchemaUtils;
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
 * a new non-nullable field, a dropped clustering column, and a field without an id.
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
    Set<String> clusteringNames = clusteringPhysicalNames(currentProperties);
    // The returned schema is deliberately unused: the catalog stores the schema the client commits.
    try {
      SchemaUtils.validateSchemaEvolutionById(
          currentSchema,
          incomingSchema,
          clusteringNames,
          maxColumnId(currentProperties),
          /* allowNewRequiredFields= */ false,
          TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.fromMetadata(currentProperties),
          TableConfig.TYPE_WIDENING_ENABLED.fromMetadata(currentProperties));
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
   * Physical names of the clustering columns, from the stored {@code clusteringColumns} property
   * only. Kernel takes the leaf physical name of each path; the stored paths already use physical
   * names. Null or empty paths and null segments are skipped.
   */
  private static Set<String> clusteringPhysicalNames(Map<String, String> properties) {
    String json = properties.get(DeltaConsts.TableProperties.CLUSTERING_COLUMNS);
    if (json == null) {
      return Set.of();
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
      return Set.of();
    }
    return paths.stream()
        .filter(Objects::nonNull)
        .map(
            path ->
                path.stream().filter(Objects::nonNull).reduce((first, last) -> last).orElse(null))
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
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
