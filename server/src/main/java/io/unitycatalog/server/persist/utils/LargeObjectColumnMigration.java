package io.unitycatalog.server.persist.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts columns that earlier versions mapped as {@code @Lob} from PostgreSQL large objects
 * ({@code oid}) to {@code text}. It has to run before Hibernate's schema update: that update would
 * alter the column type with PostgreSQL's default cast, turning each value into the decimal string
 * of its oid.
 *
 * <p>Runs on every start and does nothing on other databases or once the columns are text. Servers
 * that start together take an advisory lock in turn, so only the first converts. The large objects
 * stay behind as orphans. Removing one takes a lock on it, and a whole catalog's worth would
 * overflow the lock table in a single transaction, so leave that to {@code vacuumlo}.
 */
final class LargeObjectColumnMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(LargeObjectColumnMigration.class);

  /**
   * Key for {@code pg_advisory_xact_lock}; any constant works if every server uses the same one.
   */
  static final long ADVISORY_LOCK_KEY = 0x75635f6c6f62L;

  /** Converting waits this long for the table lock, then fails the start and is retried. */
  static final String LOCK_TIMEOUT = "30s";

  static final List<TableColumn> COLUMNS =
      List.of(
          new TableColumn("uc_tables", "view_definition"),
          new TableColumn("uc_columns", "type_text"),
          new TableColumn("uc_functions", "routine_definition"),
          new TableColumn("uc_credentials", "credential"));

  record TableColumn(String table, String column) {}

  private LargeObjectColumnMigration() {}

  /**
   * @param schema the schema Hibernate qualifies tables with ({@code hibernate.default_schema}), or
   *     null to resolve them through the search path as Hibernate does
   */
  static void migrate(DataSource dataSource, String schema) {
    try (Connection connection = dataSource.getConnection()) {
      if (!"PostgreSQL".equals(connection.getMetaData().getDatabaseProductName())) {
        return;
      }
      boolean autoCommit = connection.getAutoCommit();
      connection.setAutoCommit(false);
      try {
        if (!largeObjectColumns(connection, schema).isEmpty()) {
          convert(connection, schema);
        }
        connection.commit();
      } catch (SQLException | RuntimeException e) {
        connection.rollback();
        throw e;
      } finally {
        connection.setAutoCommit(autoCommit);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to convert large object columns to text", e);
    }
  }

  private static void convert(Connection connection, String schema) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("select pg_advisory_xact_lock(" + ADVISORY_LOCK_KEY + ")");
      // Set after the advisory lock, which may wait for another server's whole conversion.
      statement.execute("set local lock_timeout = '" + LOCK_TIMEOUT + "'");
      // Look again: another server may have converted them while this one waited.
      for (TableColumn column : largeObjectColumns(connection, schema)) {
        String table = qualified(schema, column.table());
        LOGGER.info("Converting large object column {}.{} to text", table, column.column());
        long start = System.nanoTime();
        statement.execute(
            "alter table "
                + table
                + " alter column "
                + column.column()
                + " type text using convert_from(lo_get("
                + column.column()
                + "), 'UTF8')");
        LOGGER.info(
            "Converted {}.{} to text in {} ms",
            table,
            column.column(),
            (System.nanoTime() - start) / 1_000_000);
      }
    }
  }

  static List<TableColumn> largeObjectColumns(Connection connection, String schema)
      throws SQLException {
    List<TableColumn> result = new ArrayList<>();
    try (PreparedStatement statement =
        connection.prepareStatement(
            "select 1 from pg_attribute where attrelid = to_regclass(?) and attname = ?"
                + " and atttypid = 'oid'::regtype and not attisdropped")) {
      for (TableColumn column : COLUMNS) {
        statement.setString(1, qualified(schema, column.table()));
        statement.setString(2, column.column());
        try (ResultSet resultSet = statement.executeQuery()) {
          if (resultSet.next()) {
            result.add(column);
          }
        }
      }
    }
    return result;
  }

  private static String qualified(String schema, String table) {
    return schema == null || schema.isBlank() ? table : schema + "." + table;
  }
}
