package io.unitycatalog.server.migrate;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.sql.DataSource;

/**
 * A database's schema read through {@link DatabaseMetaData}, as sorted records that compare with
 * {@code equals}: tables, columns (type, size, nullability, default), primary keys, unique and
 * plain indexes, and foreign keys. Names are lower-cased. Casbin's rule table and Flyway's history
 * table are left out.
 */
record SchemaSnapshot(List<String> entries) {

  private static final Set<String> IGNORED_TABLES = Set.of("casbin_rule", "uc_schema_history");

  static SchemaSnapshot of(DataSource dataSource) {
    try (Connection connection = dataSource.getConnection()) {
      return read(connection);
    } catch (SQLException e) {
      throw new IllegalStateException("Could not read the schema", e);
    }
  }

  private static SchemaSnapshot read(Connection connection) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String catalog = connection.getCatalog();
    String schema = connection.getSchema();
    String schemaPattern = SchemaMigrator.exactPattern(metaData, schema);
    List<String> entries = new ArrayList<>();
    for (String tableName : tables(metaData, catalog, schemaPattern)) {
      entries.add("table " + lower(tableName));
      entries.addAll(columns(metaData, catalog, schemaPattern, tableName));
      entries.addAll(primaryKeys(metaData, catalog, schema, tableName));
      entries.addAll(indexes(metaData, catalog, schema, tableName));
      entries.addAll(foreignKeys(metaData, catalog, schema, tableName));
    }
    return new SchemaSnapshot(entries.stream().sorted().toList());
  }

  private static List<String> columns(
      DatabaseMetaData metaData, String catalog, String schemaPattern, String tableName)
      throws SQLException {
    String table = lower(tableName);
    List<String> entries = new ArrayList<>();
    try (ResultSet rs =
        metaData.getColumns(
            catalog, schemaPattern, SchemaMigrator.exactPattern(metaData, tableName), null)) {
      while (rs.next()) {
        entries.add(
            "column %s.%s type=%s size=%d nullable=%s default=%s"
                .formatted(
                    table,
                    lower(rs.getString("COLUMN_NAME")),
                    rs.getString("TYPE_NAME").toLowerCase(Locale.ROOT),
                    rs.getInt("COLUMN_SIZE"),
                    rs.getString("IS_NULLABLE"),
                    rs.getString("COLUMN_DEF")));
      }
    }
    return entries;
  }

  private static List<String> primaryKeys(
      DatabaseMetaData metaData, String catalog, String schema, String tableName)
      throws SQLException {
    String table = lower(tableName);
    List<String> entries = new ArrayList<>();
    try (ResultSet rs = metaData.getPrimaryKeys(catalog, schema, tableName)) {
      while (rs.next()) {
        entries.add(
            "pk %s.%s#%d"
                .formatted(table, lower(rs.getString("COLUMN_NAME")), rs.getInt("KEY_SEQ")));
      }
    }
    return entries;
  }

  private static List<String> indexes(
      DatabaseMetaData metaData, String catalog, String schema, String tableName)
      throws SQLException {
    String table = lower(tableName);
    List<String> entries = new ArrayList<>();
    try (ResultSet rs = metaData.getIndexInfo(catalog, schema, tableName, false, false)) {
      while (rs.next()) {
        if (rs.getString("INDEX_NAME") == null) {
          continue;
        }
        entries.add(
            "index %s.%s unique=%s column=%s#%d"
                .formatted(
                    table,
                    indexName(rs.getString("INDEX_NAME")),
                    !rs.getBoolean("NON_UNIQUE"),
                    lower(rs.getString("COLUMN_NAME")),
                    rs.getInt("ORDINAL_POSITION")));
      }
    }
    return entries;
  }

  private static List<String> foreignKeys(
      DatabaseMetaData metaData, String catalog, String schema, String tableName)
      throws SQLException {
    String table = lower(tableName);
    List<String> entries = new ArrayList<>();
    try (ResultSet rs = metaData.getImportedKeys(catalog, schema, tableName)) {
      while (rs.next()) {
        entries.add(
            "fk %s.%s %s.%s -> %s.%s#%d"
                .formatted(
                    table,
                    lower(rs.getString("FK_NAME")),
                    table,
                    lower(rs.getString("FKCOLUMN_NAME")),
                    lower(rs.getString("PKTABLE_NAME")),
                    lower(rs.getString("PKCOLUMN_NAME")),
                    rs.getInt("KEY_SEQ")));
      }
    }
    return entries;
  }

  private static List<String> tables(
      DatabaseMetaData metaData, String catalog, String schemaPattern) throws SQLException {
    List<String> tables = new ArrayList<>();
    try (ResultSet rs = metaData.getTables(catalog, schemaPattern, null, new String[] {"TABLE"})) {
      while (rs.next()) {
        String name = rs.getString("TABLE_NAME");
        if (!IGNORED_TABLES.contains(lower(name))) {
          tables.add(name);
        }
      }
    }
    return tables;
  }

  /** H2 names the index behind a key or constraint with a counter that differs between runs. */
  private static String indexName(String name) {
    return lower(name).replaceAll("(primary_key|constraint_index|_index)_[0-9a-f]+$", "$1_*");
  }

  private static String lower(String name) {
    return name == null ? null : name.toLowerCase(Locale.ROOT);
  }
}
