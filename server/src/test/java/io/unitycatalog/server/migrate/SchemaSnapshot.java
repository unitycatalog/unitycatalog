package io.unitycatalog.server.migrate;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.sql.DataSource;

/**
 * A database's schema read through {@link DatabaseMetaData}, as sorted records that compare with
 * {@code equals}: tables, columns (type, size, nullability, default), primary keys, unique and
 * plain indexes, foreign keys, and CHECK constraints (read from the database's catalog tables, by
 * clause: their generated names differ between a schema the steps built and one Hibernate built).
 * Names are lower-cased; CHECK clauses keep their case, since enum literals in them are
 * case-sensitive. Casbin's rule table and Unity Catalog's schema history table are left out.
 */
record SchemaSnapshot(List<String> entries) {

  private static final Set<String> IGNORED_TABLES = Set.of("casbin_rule", "uc_schema_history");

  /** H2 and MySQL list CHECK constraints in the standard information schema. */
  private static final String STANDARD_CHECKS =
      """
      select tc.table_name, cc.check_clause
      from information_schema.table_constraints tc
      join information_schema.check_constraints cc
        on cc.constraint_schema = tc.constraint_schema and cc.constraint_name = tc.constraint_name
      where tc.constraint_type = 'CHECK' and tc.table_schema = %s
      """;

  private static final String POSTGRES_CHECKS =
      """
      select c.relname, pg_get_constraintdef(con.oid)
      from pg_constraint con
      join pg_class c on c.oid = con.conrelid
      join pg_namespace n on n.oid = c.relnamespace
      where con.contype = 'c' and n.nspname = current_schema()
      """;

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
    List<String> entries = new ArrayList<>();
    for (String tableName : tables(metaData, catalog, schema)) {
      String table = lower(tableName);
      entries.add("table " + table);
      try (ResultSet rs = metaData.getColumns(catalog, schema, tableName, null)) {
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
      try (ResultSet rs = metaData.getPrimaryKeys(catalog, schema, tableName)) {
        while (rs.next()) {
          entries.add(
              "pk %s.%s#%d"
                  .formatted(table, lower(rs.getString("COLUMN_NAME")), rs.getInt("KEY_SEQ")));
        }
      }
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
    }
    entries.addAll(checks(connection));
    return new SchemaSnapshot(entries.stream().sorted().toList());
  }

  private static List<String> checks(Connection connection) throws SQLException {
    String query =
        switch (SchemaMigrator.DatabaseKind.fromJdbcUrl(connection.getMetaData().getURL())) {
          case H2 -> STANDARD_CHECKS.formatted("current_schema");
          case MYSQL -> STANDARD_CHECKS.formatted("database()");
          case POSTGRESQL -> POSTGRES_CHECKS;
        };
    List<String> checks = new ArrayList<>();
    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query)) {
      while (rs.next()) {
        String table = lower(rs.getString(1));
        if (!IGNORED_TABLES.contains(table)) {
          // PostgreSQL's definition starts with "CHECK"; the others give the bare clause.
          String clause = rs.getString(2).replaceAll("\\s+", " ").trim();
          checks.add("check %s %s".formatted(table, clause.replaceFirst("^(?i)check ", "")));
        }
      }
    }
    return checks;
  }

  private static List<String> tables(DatabaseMetaData metaData, String catalog, String schema)
      throws SQLException {
    List<String> tables = new ArrayList<>();
    try (ResultSet rs = metaData.getTables(catalog, schema, null, new String[] {"TABLE"})) {
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
