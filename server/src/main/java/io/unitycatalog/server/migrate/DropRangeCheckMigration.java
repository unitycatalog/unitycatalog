package io.unitycatalog.server.migrate;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import javax.sql.DataSource;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drops an unnamed ordinal-range {@code CHECK} constraint (such as {@code BETWEEN 0 AND 21}) after
 * a prior step has added its wider replacement. Looks up the database-generated constraint name at
 * runtime so the step works on fresh databases and on databases whose checks were repaired or
 * renamed.
 */
abstract class DropRangeCheckMigration extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropRangeCheckMigration.class);

  private static final String STANDARD_LOOKUP =
      """
      SELECT tc.constraint_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.check_constraints cc
        ON cc.constraint_schema = tc.constraint_schema
       AND cc.constraint_name = tc.constraint_name
      WHERE tc.constraint_type = 'CHECK'
        AND tc.table_schema = %s
        AND LOWER(tc.table_name) = ?
        AND cc.check_clause = ?
      """;

  private static final String POSTGRES_LOOKUP =
      """
      SELECT con.conname
      FROM pg_constraint con
      JOIN pg_class c ON c.oid = con.conrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE con.contype = 'c'
        AND n.nspname = current_schema()
        AND c.relname = ?
        AND pg_get_constraintdef(con.oid) = ?
      """;

  private final String table;
  private final String column;
  private final int min;
  private final int max;

  DropRangeCheckMigration(String table, String column, int min, int max) {
    this.table = table.toLowerCase(Locale.ROOT);
    this.column = column.toLowerCase(Locale.ROOT);
    this.min = min;
    this.max = max;
  }

  String table() {
    return table;
  }

  @Override
  public Integer getChecksum() {
    return Objects.hash(table, column, min, max);
  }

  /**
   * Preflight check called before running pending steps so duplicate or missing CHECK constraints
   * fail before an earlier step (such as adding the wider CHECK) commits.
   */
  void verifySingleMatch(DataSource dataSource, DatabaseKind kind) {
    try (Connection connection = dataSource.getConnection()) {
      requireSingleConstraintName(connection, kind);
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Could not check existing CHECK constraints on %s: %s".formatted(table, e.getMessage()),
          e);
    }
  }

  @Override
  public void migrate(Context context) throws SQLException {
    Connection connection = context.getConnection();
    DatabaseKind kind = DatabaseKind.of(connection);
    String name = requireSingleConstraintName(connection, kind);
    String dropSql = dropStatement(kind, name);
    try (Statement statement = connection.createStatement()) {
      statement.execute(dropSql);
    }
    LOGGER.info("Dropped old range check {} on {}.{} ({}..{})", name, table, column, min, max);
  }

  private String requireSingleConstraintName(Connection connection, DatabaseKind kind)
      throws SQLException {
    String expectedClause = expectedClause(kind);
    List<String> names = findConstraintNames(connection, kind, expectedClause);
    if (names.size() != 1) {
      throw new IllegalStateException(
          "Expected exactly one CHECK constraint matching %s on %s, found %s"
              .formatted(expectedClause, table, names));
    }
    return names.get(0);
  }

  private String expectedClause(DatabaseKind kind) {
    return switch (kind) {
      case H2 -> "\"%s\" BETWEEN %d AND %d".formatted(column.toUpperCase(Locale.ROOT), min, max);
      case MYSQL -> "(`%s` between %d and %d)".formatted(column, min, max);
      case POSTGRESQL -> "CHECK (((%1$s >= %2$d) AND (%1$s <= %3$d)))".formatted(column, min, max);
    };
  }

  private List<String> findConstraintNames(
      Connection connection, DatabaseKind kind, String expectedClause) throws SQLException {
    String lookupSql =
        switch (kind) {
          case H2 -> STANDARD_LOOKUP.formatted("CURRENT_SCHEMA");
          case MYSQL -> STANDARD_LOOKUP.formatted("DATABASE()");
          case POSTGRESQL -> POSTGRES_LOOKUP;
        };
    List<String> names = new ArrayList<>();
    try (PreparedStatement ps = connection.prepareStatement(lookupSql)) {
      ps.setString(1, table);
      ps.setString(2, expectedClause);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          names.add(rs.getString(1));
        }
      }
    }
    return names;
  }

  private String dropStatement(DatabaseKind kind, String name) {
    return switch (kind) {
      case H2, POSTGRESQL ->
          "ALTER TABLE %s DROP CONSTRAINT \"%s\"".formatted(table, name.replace("\"", "\"\""));
      case MYSQL -> "ALTER TABLE %s DROP CONSTRAINT `%s`".formatted(table, name.replace("`", "``"));
    };
  }
}
