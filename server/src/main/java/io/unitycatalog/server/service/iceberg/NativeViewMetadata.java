package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.model.TableInfo;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;

/** Projects the current native Spark view definition as an Iceberg snapshot. */
final class NativeViewMetadata {
  private static final String CONTEXT_COUNT = "view.catalogAndNamespace.numParts";
  private static final String CONTEXT_PART = "view.catalogAndNamespace.part.";
  private static final String OUTPUT_COUNT = "view.query.out.numCols";
  private static final String OUTPUT_COLUMN = "view.query.out.col.";

  private NativeViewMetadata() {}

  static ViewMetadata from(TableInfo info, String location) {
    if (info.getViewDefinition() == null
        || info.getViewDefinition().isBlank()
        || info.getColumns().isEmpty()) {
      throw new BadRequestException("Native views require SQL and at least one output column");
    }
    Map<String, String> properties = new TreeMap<>(info.getProperties());
    Set<String> supported = new HashSet<>();
    String catalog = info.getCatalogName();
    Namespace namespace = Namespace.of(info.getSchemaName());
    if (properties.containsKey(CONTEXT_COUNT)) {
      int count = count(properties, CONTEXT_COUNT);
      if (count < 1 || count > 2) {
        throw new BadRequestException("Native views support a catalog and one namespace");
      }
      supported.add(CONTEXT_COUNT);
      catalog = required(properties, CONTEXT_PART + "0", supported);
      namespace =
          count == 2
              ? Namespace.of(required(properties, CONTEXT_PART + "1", supported))
              : Namespace.empty();
    }
    if (properties.containsKey(OUTPUT_COUNT)) {
      if (count(properties, OUTPUT_COUNT) != info.getColumns().size()) {
        throw new BadRequestException("Native view output count does not match its columns");
      }
      supported.add(OUTPUT_COUNT);
      for (int i = 0; i < info.getColumns().size(); i++) {
        required(properties, OUTPUT_COLUMN + i, supported);
      }
    }
    for (String key : properties.keySet()) {
      if (key.startsWith("view.") && !supported.contains(key)) {
        throw new BadRequestException("Unsupported native view resolution property: %s", key);
      }
    }
    properties.keySet().removeAll(supported);
    if (info.getComment() != null) {
      properties.put("comment", info.getComment());
    }
    var version =
        ImmutableViewVersion.builder()
            .versionId(1)
            .schemaId(0)
            .timestampMillis(
                info.getUpdatedAt() != null ? info.getUpdatedAt() : info.getCreatedAt())
            .defaultCatalog(catalog)
            .defaultNamespace(namespace)
            .addRepresentations(
                ImmutableSQLViewRepresentation.builder()
                    .dialect("spark")
                    .sql(info.getViewDefinition())
                    .build())
            .build();
    return ViewMetadata.builder()
        .assignUUID(info.getTableId())
        .setLocation(location)
        .setCurrentVersion(version, IcebergSchemaConverter.toIcebergSchema(info.getColumns()))
        .setProperties(properties)
        .build();
  }

  private static String required(
      Map<String, String> properties, String key, Set<String> supported) {
    String value = properties.get(key);
    if (value == null || value.isBlank()) {
      throw new BadRequestException("Missing native view resolution property: %s", key);
    }
    supported.add(key);
    return value;
  }

  private static int count(Map<String, String> properties, String key) {
    try {
      return Integer.parseInt(properties.get(key));
    } catch (NumberFormatException e) {
      throw new BadRequestException("Invalid native view resolution property: %s", key);
    }
  }
}
