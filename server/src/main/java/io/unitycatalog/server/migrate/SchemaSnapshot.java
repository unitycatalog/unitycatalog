package io.unitycatalog.server.migrate;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.sql.DataSource;

/**
 * A database's schema read through {@link DatabaseMetaData}, as sorted records that compare with
 * {@code equals}: tables, columns (type with its enum values, size, numeric scale, nullability,
 * default, and a collation set on the column), primary keys, unique and plain indexes (with
 * descending columns and PostgreSQL's partial-index condition), foreign keys (with their update and
 * delete rules), and CHECK constraints (read from the database's catalog tables, by clause: their
 * generated names differ between a schema the steps built and one Hibernate built). Names are
 * lower-cased; CHECK clauses and enum values keep their case, since they are case-sensitive. Only
 * Unity Catalog's tables (names starting with {@code uc_}) are read, without its schema history
 * table: Casbin's rule table and other applications' tables are left out.
 */
record SchemaSnapshot(List<String> entries) {

  /** Sorted and without repeats, so that equal schemas compare equal. */
  SchemaSnapshot {
    entries = List.copyOf(new TreeSet<>(entries));
  }

  /** The schema before the first step: nothing. */
  static final SchemaSnapshot NONE = new SchemaSnapshot(List.of());

  /** Whether a table of this lower-case name is part of Unity Catalog's schema. */
  private static boolean isSchemaTable(String lowerName) {
    return lowerName.startsWith("uc_") && !lowerName.equals("uc_schema_history");
  }

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

  /** MySQL's enum and set values, and each column's collation where it is not the database's. */
  private static final String MYSQL_COLUMN_DETAILS =
      """
      select c.table_name, c.column_name,
        case when c.data_type in ('enum', 'set') then c.column_type end,
        case when c.collation_name <> s.default_collation_name then c.collation_name end
      from information_schema.columns c
      join information_schema.schemata s on s.schema_name = c.table_schema
      where c.table_schema = database()
      """;

  /** PostgreSQL columns with a collation set, other than the default one. */
  private static final String POSTGRES_COLUMN_DETAILS =
      """
      select c.relname, a.attname, null, co.collname
      from pg_attribute a
      join pg_class c on c.oid = a.attrelid
      join pg_namespace n on n.oid = c.relnamespace
      join pg_collation co on co.oid = a.attcollation
      where n.nspname = current_schema() and a.attnum > 0 and co.collname <> 'default'
      """;

  /**
   * The lines of a step's effect file that turn this schema into {@code after}: {@code - entry} for
   * each entry it removes and {@code + entry} for each it adds, by object, {@code -} first.
   */
  List<String> changesTo(SchemaSnapshot after) {
    List<String> lines = new ArrayList<>();
    entries.stream().filter(e -> !after.entries.contains(e)).forEach(e -> lines.add("- " + e));
    after.entries.stream().filter(e -> !entries.contains(e)).forEach(e -> lines.add("+ " + e));
    // By object (the entry's first two words), so that a changed object's two lines sit together.
    lines.sort(
        Comparator.comparing((String line) -> String.join(" ", firstWords(line.substring(2))))
            .thenComparing(line -> line.startsWith("+"))
            .thenComparing(line -> line));
    return lines;
  }

  /**
   * This schema with an effect file's lines applied.
   *
   * @throws IllegalArgumentException for a line that does not fit this schema: a {@code -} entry it
   *     lacks, a {@code +} entry it has, or a line without either. Blank lines are skipped.
   */
  SchemaSnapshot with(List<String> effect) {
    Set<String> result = new TreeSet<>(entries);
    for (String line : effect) {
      if (line.isBlank()) {
        continue;
      }
      String entry = line.length() > 2 ? line.substring(2) : "";
      boolean fits =
          line.startsWith("- ")
              ? result.remove(entry)
              : line.startsWith("+ ") && !entry.isEmpty() && result.add(entry);
      if (!fits) {
        throw new IllegalArgumentException("does not fit the schema before it: " + line);
      }
    }
    return new SchemaSnapshot(new ArrayList<>(result));
  }

  /**
   * The entries {@code expected} has and this lacks ({@code missing ...}) and those this has and it
   * lacks ({@code unexpected ...}), by object.
   */
  String differencesFrom(SchemaSnapshot expected) {
    return expected.changesTo(this).stream()
        .map(line -> (line.startsWith("-") ? "missing " : "unexpected ") + line.substring(2))
        .collect(Collectors.joining("\n"));
  }

  static SchemaSnapshot of(DataSource dataSource) {
    DatabaseKind kind = DatabaseKind.of(dataSource);
    try (Connection connection = dataSource.getConnection()) {
      return read(connection, kind);
    } catch (SQLException e) {
      throw new IllegalStateException("Could not read the schema: " + e.getMessage(), e);
    }
  }

  private static SchemaSnapshot read(Connection connection, DatabaseKind kind) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String catalog = connection.getCatalog();
    String schema = connection.getSchema();
    Map<String, String> details = columnDetails(connection, kind);
    List<String> entries = new ArrayList<>();
    for (String tableName : tables(metaData, catalog, schema)) {
      entries.add("table " + lower(tableName));
      entries.addAll(columns(metaData, catalog, schema, tableName, details));
      entries.addAll(primaryKeys(metaData, catalog, schema, tableName));
      entries.addAll(indexes(metaData, catalog, schema, tableName));
      entries.addAll(foreignKeys(metaData, catalog, schema, tableName));
    }
    entries.addAll(checks(connection, kind));
    return new SchemaSnapshot(entries);
  }

  private static List<String> checks(Connection connection, DatabaseKind kind) throws SQLException {
    String query =
        switch (kind) {
          case H2 -> STANDARD_CHECKS.formatted("current_schema");
          case MYSQL -> STANDARD_CHECKS.formatted("database()");
          case POSTGRESQL -> POSTGRES_CHECKS;
        };
    List<String> checks = new ArrayList<>();
    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query)) {
      while (rs.next()) {
        String table = lower(rs.getString(1));
        if (isSchemaTable(table)) {
          // PostgreSQL's definition starts with "CHECK"; the others give the bare clause.
          String clause = rs.getString(2).replaceAll("\\s+", " ").trim();
          checks.add("check %s %s".formatted(table, clause.replaceFirst("^(?i)check ", "")));
        }
      }
    }
    return checks;
  }

  /**
   * What {@link DatabaseMetaData#getColumns} leaves out, by {@code table.column}: MySQL's enum
   * values, and a column's collation (MySQL: where it is not the database's; PostgreSQL: where one
   * is set).
   */
  private static Map<String, String> columnDetails(Connection connection, DatabaseKind kind)
      throws SQLException {
    String query =
        switch (kind) {
          case H2 -> null; // H2's type name holds the enum values; its collation is per database
          case MYSQL -> MYSQL_COLUMN_DETAILS;
          case POSTGRESQL -> POSTGRES_COLUMN_DETAILS;
        };
    Map<String, String> details = new HashMap<>();
    if (query == null) {
      return details;
    }
    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query)) {
      while (rs.next()) {
        String detail =
            (rs.getString(3) == null ? "" : " values=" + rs.getString(3))
                + (rs.getString(4) == null ? "" : " collation=" + rs.getString(4));
        if (!detail.isEmpty()) {
          details.put(lower(rs.getString(1)) + "." + lower(rs.getString(2)), detail);
        }
      }
    }
    return details;
  }

  private static List<String> columns(
      DatabaseMetaData metaData,
      String catalog,
      String schema,
      String tableName,
      Map<String, String> details)
      throws SQLException {
    String table = lower(tableName);
    List<String> entries = new ArrayList<>();
    try (ResultSet rs =
        metaData.getColumns(
            catalog,
            SchemaMigrator.exactPattern(metaData, schema),
            SchemaMigrator.exactPattern(metaData, tableName),
            null)) {
      while (rs.next()) {
        String column = table + "." + lower(rs.getString("COLUMN_NAME"));
        String type = lowerOutsideQuotes(rs.getString("TYPE_NAME"));
        String scale =
            type.contains("decimal") || type.contains("numeric")
                ? " scale=" + rs.getInt("DECIMAL_DIGITS")
                : "";
        entries.add(
            "column %s type=%s size=%d%s nullable=%s default=%s%s"
                .formatted(
                    column,
                    type,
                    rs.getInt("COLUMN_SIZE"),
                    scale,
                    rs.getString("IS_NULLABLE"),
                    rs.getString("COLUMN_DEF"),
                    details.getOrDefault(column, "")));
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
        String condition = rs.getString("FILTER_CONDITION");
        entries.add(
            "index %s.%s unique=%s column=%s#%d%s%s"
                .formatted(
                    table,
                    indexName(rs.getString("INDEX_NAME")),
                    !rs.getBoolean("NON_UNIQUE"),
                    lower(rs.getString("COLUMN_NAME")),
                    rs.getInt("ORDINAL_POSITION"),
                    "D".equals(rs.getString("ASC_OR_DESC")) ? " desc" : "",
                    condition == null ? "" : " where " + condition));
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
            "fk %s.%s %s.%s -> %s.%s#%d on update %s on delete %s"
                .formatted(
                    table,
                    lower(rs.getString("FK_NAME")),
                    table,
                    lower(rs.getString("FKCOLUMN_NAME")),
                    lower(rs.getString("PKTABLE_NAME")),
                    lower(rs.getString("PKCOLUMN_NAME")),
                    rs.getInt("KEY_SEQ"),
                    rule(rs.getShort("UPDATE_RULE")),
                    rule(rs.getShort("DELETE_RULE"))));
      }
    }
    return entries;
  }

  private static List<String> tables(DatabaseMetaData metaData, String catalog, String schema)
      throws SQLException {
    List<String> tables = new ArrayList<>();
    try (ResultSet rs =
        metaData.getTables(
            catalog, SchemaMigrator.exactPattern(metaData, schema), null, new String[] {"TABLE"})) {
      while (rs.next()) {
        String name = rs.getString("TABLE_NAME");
        if (isSchemaTable(lower(name))) {
          tables.add(name);
        }
      }
    }
    return tables;
  }

  private static String rule(short rule) {
    return switch (rule) {
      case DatabaseMetaData.importedKeyCascade -> "cascade";
      case DatabaseMetaData.importedKeySetNull -> "set null";
      case DatabaseMetaData.importedKeySetDefault -> "set default";
      case DatabaseMetaData.importedKeyRestrict -> "restrict";
      case DatabaseMetaData.importedKeyNoAction -> "no action";
      default -> "rule " + rule;
    };
  }

  /** Lower-cases a type name but not the enum values in it, which are case-sensitive. */
  private static String lowerOutsideQuotes(String type) {
    StringBuilder result = new StringBuilder();
    boolean quoted = false;
    for (char c : type.toCharArray()) {
      quoted ^= c == '\'';
      result.append(quoted ? c : Character.toLowerCase(c));
    }
    return result.toString();
  }

  private static List<String> firstWords(String entry) {
    List<String> words = List.of(entry.split(" "));
    return words.subList(0, Math.min(2, words.size()));
  }

  /** H2 names the index behind a key or constraint with a counter that differs between runs. */
  private static String indexName(String name) {
    return lower(name).replaceAll("(primary_key|constraint_index|_index)_[0-9a-f]+$", "$1_*");
  }

  private static String lower(String name) {
    return name == null ? null : name.toLowerCase(Locale.ROOT);
  }
}
