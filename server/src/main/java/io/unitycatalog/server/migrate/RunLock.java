package io.unitycatalog.server.migrate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import javax.sql.DataSource;

/**
 * One lock per database for a whole migration run, so two runs never classify or change the schema
 * at the same time. Flyway's own lock is released between steps, so it isn't enough.
 *
 * <ul>
 *   <li>PostgreSQL: {@code pg_advisory_lock} and MySQL: {@code GET_LOCK} on a name derived from the
 *       database's, held on one dedicated connection until {@link #close()}.
 *   <li>H2: an embedded H2 file database is opened by one process, so a lock inside this JVM, keyed
 *       by the database, is enough.
 * </ul>
 */
public final class RunLock implements AutoCloseable {

  /** Any fixed key; Flyway's own advisory lock uses a different one. */
  private static final long POSTGRES_KEY = 0x75635f6d69677261L; // "uc_migra"

  /**
   * The lock name, as SQL. MySQL's locks are global to the server, so the name includes the
   * database; SHA1 keeps it within MySQL's 64-character limit.
   */
  private static final String MYSQL_NAME = "CONCAT('uc_migrate.', SHA1(DATABASE()))";

  private static final ConcurrentMap<String, ReentrantLock> H2_LOCKS = new ConcurrentHashMap<>();

  private final DatabaseKind kind;
  private final Connection connection; // PostgreSQL and MySQL
  private final ReentrantLock h2Lock; // H2

  private RunLock(DatabaseKind kind, Connection connection, ReentrantLock h2Lock) {
    this.kind = kind;
    this.connection = connection;
    this.h2Lock = h2Lock;
  }

  /** Blocks until this run holds the lock. */
  public static RunLock acquire(DataSource dataSource, DatabaseKind kind) {
    try {
      if (kind == DatabaseKind.H2) {
        ReentrantLock lock = H2_LOCKS.computeIfAbsent(h2Key(dataSource), k -> new ReentrantLock());
        lock.lock();
        return new RunLock(kind, null, lock);
      }
      Connection connection = dataSource.getConnection();
      try {
        // Outside a transaction, so the run doesn't keep one open while it holds the lock.
        connection.setAutoCommit(true);
        String sql =
            kind == DatabaseKind.POSTGRESQL
                ? "SELECT pg_advisory_lock(" + POSTGRES_KEY + ")"
                : "SELECT GET_LOCK(" + MYSQL_NAME + ", -1)";
        try (Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql)) {
          rs.next();
          if (kind == DatabaseKind.MYSQL && rs.getInt(1) != 1) {
            throw new IllegalStateException("MySQL did not grant the schema migration lock");
          }
        }
        return new RunLock(kind, connection, null);
      } catch (SQLException | RuntimeException e) {
        connection.close();
        throw e;
      }
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Could not take the schema migration lock: " + e.getMessage(), e);
    }
  }

  /** The database file's path, or the name of an in-memory database. */
  private static String h2Key(DataSource dataSource) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT COALESCE(DATABASE_PATH(), DATABASE())")) {
      rs.next();
      return rs.getString(1);
    }
  }

  @Override
  public void close() {
    if (h2Lock != null) {
      h2Lock.unlock();
      return;
    }
    String sql =
        kind == DatabaseKind.POSTGRESQL
            ? "SELECT pg_advisory_unlock(" + POSTGRES_KEY + ")"
            : "SELECT RELEASE_LOCK(" + MYSQL_NAME + ")";
    try (Connection c = connection;
        Statement statement = c.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Could not release the schema migration lock: " + e.getMessage(), e);
    }
  }
}
