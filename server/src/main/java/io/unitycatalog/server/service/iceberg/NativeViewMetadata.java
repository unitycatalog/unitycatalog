package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.model.TableInfo;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;

/** Projects the current native Spark view definition as an Iceberg snapshot. */
final class NativeViewMetadata {
  private static final String CONTEXT_COUNT = "view.catalogAndNamespace.numParts";
  private static final String CONTEXT_PART = "view.catalogAndNamespace.part.";
  private static final String SCHEMA_MODE = "view.schemaMode";
  private static final String SQL_CONFIG = "view.sqlConfig.";
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
    // With no saved catalog, Iceberg resolves names in the catalog that loaded the view.
    String catalog = null;
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
    if ("COMPENSATION".equals(properties.get(SCHEMA_MODE))) {
      supported.add(SCHEMA_MODE);
    }
    Schema schema = IcebergSchemaConverter.toIcebergSchema(info.getColumns());
    String query = bindOutputs(info.getViewDefinition(), schema, properties, supported);
    for (String key : properties.keySet()) {
      if (key.startsWith(SQL_CONFIG)) {
        throw new BadRequestException(
            "Iceberg readers cannot apply the saved Spark SQL setting %s. Read this view through"
                + " Unity Catalog.",
            key.substring(SQL_CONFIG.length()));
      }
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
                ImmutableSQLViewRepresentation.builder().dialect("spark").sql(query).build())
            .build();
    return ViewMetadata.builder()
        .assignUUID(info.getTableId())
        .setLocation(location)
        .setCurrentVersion(version, schema)
        .setProperties(properties)
        .build();
  }

  // Spark's native UC reader binds by query-output name. Iceberg binds by position, so first
  // select those names in their saved order, even when a SELECT * expands in a different order.
  private static String bindOutputs(
      String sql, Schema schema, Map<String, String> properties, Set<String> supported) {
    boolean hasSavedNames = properties.containsKey(OUTPUT_COUNT);
    if (hasSavedNames) {
      if (count(properties, OUTPUT_COUNT) != schema.columns().size()) {
        throw new BadRequestException("Native view output count does not match its columns");
      }
      supported.add(OUTPUT_COUNT);
    }
    var names = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
    var projection = new StringJoiner(", ");
    for (int i = 0; i < schema.columns().size(); i++) {
      String name =
          hasSavedNames
              ? required(properties, OUTPUT_COLUMN + i, supported)
              : schema.columns().get(i).name();
      if (!names.add(name)) {
        throw new BadRequestException("Ambiguous native view query output: %s", name);
      }
      // Spark can interpret punctuation inside backticks as a regex column selector.
      if (name.isEmpty() || !name.chars().allMatch(c -> Character.isLetterOrDigit(c) || c == '_')) {
        throw new BadRequestException(
            "Native view query output names must contain only letters, digits, or underscores: %s",
            name);
      }
      String column = "`" + name + "`";
      // A missing top-level field must fail even if Spark could resolve the name in the session.
      String field = "struct(*)." + column;
      String type =
          switch (schema.columns().get(i).type().typeId()) {
            case INTEGER -> "INT";
            case LONG -> "BIGINT";
            default -> null;
          };
      // Spark's native view raises on overflow even when the reader disables ANSI mode.
      if (type != null) {
        projection.add(
            ("COALESCE(TRY_CAST(%1$s AS %2$s), IF(%1$s IS NULL, NULL, "
                    + "RAISE_ERROR('Cannot cast native view output to %2$s'))) AS %3$s")
                .formatted(field, type, column));
      } else {
        projection.add(field + " AS " + column);
      }
    }
    String query = sql.stripTrailing();
    if (query.endsWith(";")) {
      query = query.substring(0, query.length() - 1);
    }
    // A stored query is embedded as one subquery. Conservatively exclude further semicolons
    // (including inside literals/comments) and session-dependent Spark variable substitutions.
    if (query.isBlank() || query.indexOf(';') >= 0 || query.contains("${")) {
      throw new BadRequestException(
          "Native view SQL must be one query without Spark variable substitutions; only a final"
              + " semicolon is supported");
    }
    // End a trailing line comment even when its final backslash escapes the first newline.
    return "SELECT " + projection + " FROM (\n" + query + "\n\n) AS view_source";
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
