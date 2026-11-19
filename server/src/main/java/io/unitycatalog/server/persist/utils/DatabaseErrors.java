package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.ErrorCode;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.hibernate.SessionFactory;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.exception.ConstraintViolationException;

/**
 * Database errors that map to a specific {@link ErrorCode}, such as a value above a database limit.
 * Each entry names one database dialect, and matches its subclasses too, because databases differ
 * in their limits and in the SQLSTATE they report. Table-name collisions are recognized by their
 * constraint name; unrelated uniqueness failures retain their existing handling.
 */
final class DatabaseErrors {

  private record DatabaseError(
      Class<? extends Dialect> dialect, String sqlState, ErrorCode errorCode) {}

  /**
   * PostgreSQL's program_limit_exceeded, for example an index entry longer than 2,704 bytes. The
   * whole code is mapped, since each of its cases reports a value over a fixed limit. See
   * https://www.postgresql.org/docs/current/errcodes-appendix.html.
   */
  private static final String POSTGRESQL_PROGRAM_LIMIT_EXCEEDED_ERRCODE = "54000";

  private static final List<DatabaseError> DATABASE_ERRORS =
      List.of(
          new DatabaseError(
              PostgreSQLDialect.class,
              POSTGRESQL_PROGRAM_LIMIT_EXCEEDED_ERRCODE,
              ErrorCode.INVALID_ARGUMENT));

  private DatabaseErrors() {}

  /** Returns the error code for {@code error}, or empty if it is not a listed database error. */
  static Optional<ErrorCode> errorCode(SessionFactory sessionFactory, Exception error) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause instanceof ConstraintViolationException violation
          && violation.getConstraintName() != null) {
        String name = violation.getConstraintName().toLowerCase(Locale.ROOT);
        // H2 includes the schema and an index suffix; MySQL may include the table.
        if (name.matches("(?:[a-z0-9_]+\\.)?uk_uc_tables_schema_name(?:_index_[a-z0-9]+)?")) {
          return Optional.of(ErrorCode.TABLE_ALREADY_EXISTS);
        }
      }
    }
    Optional<String> sqlState = sqlState(error);
    if (sqlState.isEmpty()) {
      return Optional.empty();
    }
    Dialect dialect =
        sessionFactory.unwrap(SessionFactoryImplementor.class).getJdbcServices().getDialect();
    return DATABASE_ERRORS.stream()
        .filter(e -> e.sqlState().equals(sqlState.get()) && e.dialect().isInstance(dialect))
        .map(DatabaseError::errorCode)
        .findFirst();
  }

  /** Returns the SQLSTATE of the first SQLException among the causes of {@code error}. */
  private static Optional<String> sqlState(Throwable error) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause instanceof SQLException sqlException) {
        return Optional.ofNullable(sqlException.getSQLState());
      }
    }
    return Optional.empty();
  }
}
