package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.FunctionInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.boot.model.relational.internal.SqlStringGenerationContextImpl;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
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

  private static final List<Attribute> ATTRIBUTES =
      List.of(
          new Attribute(TableInfoDAO.class, "viewDefinition"),
          new Attribute(ColumnInfoDAO.class, "typeText"),
          new Attribute(FunctionInfoDAO.class, "routineDefinition"),
          new Attribute(CredentialDAO.class, "credential"));

  private record Attribute(Class<?> entity, String name) {}

  /** A column as SQL names it: the table qualified, and both quoted where Hibernate quotes them. */
  private record TableColumn(String table, String column) {}

  private LargeObjectColumnMigration() {}

  /**
   * @param settings the settings the schema update runs with, which decide the default schema and
   *     identifier quoting
   */
  static void migrate(DataSource dataSource, Metadata metadata, Map<String, Object> settings) {
    try (Connection connection = dataSource.getConnection()) {
      if (!"PostgreSQL".equals(connection.getMetaData().getDatabaseProductName())) {
        return;
      }
      List<TableColumn> columns = columns(metadata, settings);
      boolean autoCommit = connection.getAutoCommit();
      connection.setAutoCommit(false);
      try {
        if (!largeObjectColumns(connection, columns).isEmpty()) {
          convert(connection, columns);
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

  /** Names the columns the way Hibernate's schema update will. */
  private static List<TableColumn> columns(Metadata metadata, Map<String, Object> settings) {
    JdbcEnvironment jdbcEnvironment = metadata.getDatabase().getJdbcEnvironment();
    SqlStringGenerationContext context =
        SqlStringGenerationContextImpl.fromConfigurationMapForMigration(
            jdbcEnvironment, metadata.getDatabase(), settings);
    List<TableColumn> columns = new ArrayList<>();
    for (Attribute attribute : ATTRIBUTES) {
      PersistentClass entity = metadata.getEntityBinding(attribute.entity().getName());
      Column column = entity.getProperty(attribute.name()).getColumns().get(0);
      columns.add(
          new TableColumn(
              context.format(entity.getTable().getQualifiedTableName()),
              column.getQuotedName(jdbcEnvironment.getDialect())));
    }
    return columns;
  }

  private static void convert(Connection connection, List<TableColumn> columns)
      throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("select pg_advisory_xact_lock(" + ADVISORY_LOCK_KEY + ")");
      // Set after the advisory lock, which may wait for another server's whole conversion.
      statement.execute("set local lock_timeout = '" + LOCK_TIMEOUT + "'");
      // Look again: another server may have converted them while this one waited.
      for (TableColumn column : largeObjectColumns(connection, columns)) {
        LOGGER.info(
            "Converting large object column {}.{} to text", column.table(), column.column());
        long start = System.nanoTime();
        statement.execute(
            "alter table "
                + column.table()
                + " alter column "
                + column.column()
                + " type text using convert_from(lo_get("
                + column.column()
                + "), 'UTF8')");
        LOGGER.info(
            "Converted {}.{} to text in {} ms",
            column.table(),
            column.column(),
            (System.nanoTime() - start) / 1_000_000);
      }
    }
  }

  private static List<TableColumn> largeObjectColumns(
      Connection connection, List<TableColumn> columns) throws SQLException {
    List<TableColumn> result = new ArrayList<>();
    // to_regclass and parse_ident read a name as SQL does: quoted as is, otherwise lower-cased.
    try (PreparedStatement statement =
        connection.prepareStatement(
            "select 1 from pg_attribute where attrelid = to_regclass(?)"
                + " and attname = (parse_ident(?))[1]"
                + " and atttypid = 'oid'::regtype and not attisdropped")) {
      for (TableColumn column : columns) {
        statement.setString(1, column.table());
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
}
