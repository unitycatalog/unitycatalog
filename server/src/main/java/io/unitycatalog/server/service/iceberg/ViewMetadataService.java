package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.delta.model.DeltaArrayType;
import io.unitycatalog.server.delta.model.DeltaDataType;
import io.unitycatalog.server.delta.model.DeltaDecimalType;
import io.unitycatalog.server.delta.model.DeltaMapType;
import io.unitycatalog.server.delta.model.DeltaPrimitiveType;
import io.unitycatalog.server.delta.model.DeltaStructField;
import io.unitycatalog.server.delta.model.DeltaStructFieldMetadata;
import io.unitycatalog.server.delta.model.DeltaStructType;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.utils.ColumnUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.rest.responses.ImmutableLoadViewResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewVersion;

public class ViewMetadataService {

  private static final String COMMENT_PROPERTY = "comment";
  private static final String SQL_DIALECT = "spark";
  private static final int INITIAL_VERSION_ID = 1;

  public LoadViewResponse loadView(
      TableInfo tableInfo, String viewStorageRoot, MetadataService metadataService) {
    validateViewInfo(tableInfo);

    long timestampMillis = updatedAtMillis(tableInfo);
    String viewLocation = viewLocation(viewStorageRoot, tableInfo);
    String metadataLocation = metadataLocation(viewLocation, timestampMillis);
    Schema schema = toIcebergSchema(tableInfo.getColumns());
    ViewVersion version =
        ImmutableViewVersion.builder()
            .versionId(INITIAL_VERSION_ID)
            .timestampMillis(timestampMillis)
            .schemaId(schema.schemaId())
            .summary(Map.of())
            .addRepresentations(
                ImmutableSQLViewRepresentation.builder()
                    .sql(tableInfo.getViewDefinition())
                    .dialect(SQL_DIALECT)
                    .build())
            .defaultCatalog(tableInfo.getCatalogName())
            .defaultNamespace(Namespace.of(tableInfo.getSchemaName()))
            .build();
    ViewMetadata metadata =
        ViewMetadata.builder()
            .assignUUID(tableInfo.getTableId())
            .setLocation(viewLocation)
            .setProperties(viewProperties(tableInfo))
            .setCurrentVersion(version, schema)
            .build();

    metadataService.writeViewMetadata(metadata, metadataLocation);

    return ImmutableLoadViewResponse.builder()
        .metadataLocation(metadataLocation)
        .metadata(metadataService.readViewMetadata(metadataLocation))
        .config(Map.of())
        .build();
  }

  private static void validateViewInfo(TableInfo tableInfo) {
    if (tableInfo.getTableId() == null || tableInfo.getTableId().isEmpty()) {
      throw new BadRequestException("View table_id is required");
    }
    if (tableInfo.getViewDefinition() == null || tableInfo.getViewDefinition().isEmpty()) {
      throw new BadRequestException("View view_definition is required");
    }
    if (tableInfo.getColumns() == null || tableInfo.getColumns().isEmpty()) {
      throw new BadRequestException("View columns are required");
    }
    if (tableInfo.getCatalogName() == null || tableInfo.getCatalogName().isEmpty()) {
      throw new BadRequestException("View catalog_name is required");
    }
    if (tableInfo.getSchemaName() == null || tableInfo.getSchemaName().isEmpty()) {
      throw new BadRequestException("View schema_name is required");
    }
  }

  private static String viewLocation(String viewStorageRoot, TableInfo tableInfo) {
    return String.join("/", viewStorageRoot, "views", tableInfo.getTableId());
  }

  private static String metadataLocation(String viewLocation, long timestampMillis) {
    return viewLocation + "/metadata/" + timestampMillis + ".metadata.json";
  }

  private static long updatedAtMillis(TableInfo tableInfo) {
    if (tableInfo.getUpdatedAt() != null) {
      return tableInfo.getUpdatedAt();
    }
    if (tableInfo.getCreatedAt() != null) {
      return tableInfo.getCreatedAt();
    }
    return System.currentTimeMillis();
  }

  private static Map<String, String> viewProperties(TableInfo tableInfo) {
    Map<String, String> properties = new LinkedHashMap<>();
    if (tableInfo.getProperties() != null) {
      properties.putAll(tableInfo.getProperties());
    }
    if (tableInfo.getComment() != null && !tableInfo.getComment().isEmpty()) {
      properties.put(COMMENT_PROPERTY, tableInfo.getComment());
    }
    return properties;
  }

  private static Schema toIcebergSchema(List<ColumnInfo> columns) {
    FieldIdAllocator fieldIds = new FieldIdAllocator();
    List<Types.NestedField> fields =
        columns.stream()
            .sorted(
                Comparator.comparing(
                    column ->
                        column.getPosition() == null ? Integer.MAX_VALUE : column.getPosition()))
            .map(column -> toIcebergField(column, fieldIds))
            .toList();
    return new Schema(fields);
  }

  private static Types.NestedField toIcebergField(
      ColumnInfo column, FieldIdAllocator fieldIds) {
    DeltaStructField field;
    try {
      field = ColumnUtils.toStructField(column);
    } catch (IllegalStateException e) {
      throw new BadRequestException(e, "Invalid type_json for view column %s", column.getName());
    }

    String name = field.getName() != null ? field.getName() : column.getName();
    int fieldId = fieldIds.next();
    Type type = toIcebergType(field.getType(), fieldIds);
    String comment = column.getComment() != null ? column.getComment() : commentFrom(field);
    boolean isOptional = field.getNullable() == null || field.getNullable();
    return nestedField(fieldId, isOptional, name, type, comment);
  }

  private static Type toIcebergType(DeltaDataType deltaType, FieldIdAllocator fieldIds) {
    if (deltaType == null) {
      throw new BadRequestException("Unsupported view column type: null");
    }
    if (deltaType instanceof DeltaPrimitiveType primitive) {
      return toIcebergPrimitiveType(primitive);
    } else if (deltaType instanceof DeltaDecimalType decimal) {
      if (decimal.getPrecision() == null || decimal.getScale() == null) {
        throw new BadRequestException("Invalid decimal view column type: %s", decimal);
      }
      return Types.DecimalType.of(decimal.getPrecision(), decimal.getScale());
    } else if (deltaType instanceof DeltaStructType struct) {
      return toIcebergStructType(struct, fieldIds);
    } else if (deltaType instanceof DeltaArrayType array) {
      return toIcebergListType(array, fieldIds);
    } else if (deltaType instanceof DeltaMapType map) {
      return toIcebergMapType(map, fieldIds);
    }
    throw new BadRequestException("Unsupported view column type: %s", deltaType.getType());
  }

  private static Type toIcebergPrimitiveType(DeltaPrimitiveType primitive) {
    String primitiveType = primitive.getType();
    if (primitiveType == null) {
      throw new BadRequestException("Unsupported view column type: null");
    }
    return switch (primitiveType) {
      case "boolean" -> Types.BooleanType.get();
      case "byte", "short", "integer" -> Types.IntegerType.get();
      case "long" -> Types.LongType.get();
      case "float" -> Types.FloatType.get();
      case "double" -> Types.DoubleType.get();
      case "date" -> Types.DateType.get();
      case "timestamp" -> Types.TimestampType.withZone();
      case "timestamp_ntz" -> Types.TimestampType.withoutZone();
      case "string" -> Types.StringType.get();
      case "binary" -> Types.BinaryType.get();
      default -> throw new BadRequestException("Unsupported view column type: %s", primitiveType);
    };
  }

  private static Type toIcebergStructType(DeltaStructType struct, FieldIdAllocator fieldIds) {
    List<Types.NestedField> fields = new ArrayList<>();
    if (struct.getFields() != null) {
      for (DeltaStructField field : struct.getFields()) {
        fields.add(toIcebergNestedField(field, fieldIds));
      }
    }
    return Types.StructType.of(fields);
  }

  private static Types.NestedField toIcebergNestedField(
      DeltaStructField field, FieldIdAllocator fieldIds) {
    int fieldId = fieldIds.next();
    Type type = toIcebergType(field.getType(), fieldIds);
    boolean isOptional = field.getNullable() == null || field.getNullable();
    return nestedField(fieldId, isOptional, field.getName(), type, commentFrom(field));
  }

  private static Types.NestedField nestedField(
      int id, boolean isOptional, String name, Type type, String comment) {
    Types.NestedField.Builder builder =
        Types.NestedField.builder().withId(id).isOptional(isOptional).withName(name).ofType(type);
    if (comment != null) {
      builder.withDoc(comment);
    }
    return builder.build();
  }

  private static Type toIcebergListType(DeltaArrayType array, FieldIdAllocator fieldIds) {
    int elementId = fieldIds.next();
    Type elementType = toIcebergType(array.getElementType(), fieldIds);
    if (Boolean.FALSE.equals(array.getContainsNull())) {
      return Types.ListType.ofRequired(elementId, elementType);
    }
    return Types.ListType.ofOptional(elementId, elementType);
  }

  private static Type toIcebergMapType(DeltaMapType map, FieldIdAllocator fieldIds) {
    int keyId = fieldIds.next();
    int valueId = fieldIds.next();
    Type keyType = toIcebergType(map.getKeyType(), fieldIds);
    Type valueType = toIcebergType(map.getValueType(), fieldIds);
    if (Boolean.FALSE.equals(map.getValueContainsNull())) {
      return Types.MapType.ofRequired(keyId, valueId, keyType, valueType);
    }
    return Types.MapType.ofOptional(keyId, valueId, keyType, valueType);
  }

  private static String commentFrom(DeltaStructField field) {
    DeltaStructFieldMetadata metadata = field.getMetadata();
    if (metadata == null) {
      return null;
    }
    Object comment = metadata.get(COMMENT_PROPERTY);
    return comment instanceof String ? (String) comment : null;
  }

  private static class FieldIdAllocator {
    private int nextId = 1;

    int next() {
      return nextId++;
    }
  }
}
