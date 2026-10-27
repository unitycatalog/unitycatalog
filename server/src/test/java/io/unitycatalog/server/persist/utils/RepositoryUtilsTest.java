package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.Session;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.ExceptionConverterImpl;
import org.hibernate.query.SelectionQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

public class RepositoryUtilsTest {

  private static final UUID TABLE_ID = UUID.randomUUID();

  /**
   * A failed pessimistic lock acquisition means a concurrent commit or deletion holds the lock; the
   * caller decides how that surfaces (Delta as the retryable {@code COMMIT_STATE_UNKNOWN}, Iceberg
   * as an {@code UPDATE_REQUIREMENT_CONFLICT}, table deletion as {@code UNAVAILABLE}), never as a
   * leaked ORM exception. The lock query passes the dialect's exception through Hibernate's
   * exception converter, so each failure here is the converter's output for one dialect exception.
   */
  static RuntimeException[] lockAcquisitionFailures() {
    ExceptionConverterImpl converter =
        new ExceptionConverterImpl(
            mock(SharedSessionContractImplementor.class, RETURNS_DEEP_STUBS));
    LockOptions lockOptions = new LockOptions(LockMode.PESSIMISTIC_WRITE);
    return Stream.of(
            // The lock wait failed: PostgreSQL SQLSTATE 55P03, H2 error 50200, or MySQL error 1205.
            new org.hibernate.PessimisticLockException("locked", new SQLException("locked"), "sql"),
            // A deadlock victim: PostgreSQL SQLSTATE 40P01, H2 error 40001,
            // or MySQL SQLSTATE 40001.
            new org.hibernate.exception.LockAcquisitionException(
                "locked", new SQLException("locked")),
            // The lock wait timed out, for example MySQL SQLSTATE 41000.
            new org.hibernate.exception.LockTimeoutException(
                "timed out", new SQLException("timed out")))
        .map(failure -> converter.convert(failure, lockOptions))
        .toArray(RuntimeException[]::new);
  }

  /** The row-lock query that lockTableForCommit runs on {@code session}. */
  @SuppressWarnings("unchecked")
  private static SelectionQuery<TableInfoDAO> lockQuery(Session session) {
    SelectionQuery<TableInfoDAO> query = mock(SelectionQuery.class, RETURNS_SELF);
    when(session.createSelectionQuery(any(String.class), any(Class.class))).thenReturn(query);
    return query;
  }

  @ParameterizedTest
  @MethodSource("lockAcquisitionFailures")
  public void lockFailureRaisesTheCallerSuppliedError(RuntimeException lockFailure) {
    Session session = mock();
    TableInfoDAO dao = mock();
    when(lockQuery(session).getSingleResultOrNull()).thenThrow(lockFailure);

    // Delta callers ask for COMMIT_STATE_UNKNOWN...
    assertThatThrownBy(
            () ->
                RepositoryUtils.lockTableForCommit(
                    session,
                    dao,
                    TABLE_ID,
                    Optional.of("cat.sch.tbl"),
                    ErrorCode.COMMIT_STATE_UNKNOWN))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.COMMIT_STATE_UNKNOWN);

    // ...the Iceberg caller asks for a conflict, and gets exactly that.
    assertThatThrownBy(
            () ->
                RepositoryUtils.lockTableForCommit(
                    session,
                    dao,
                    TABLE_ID,
                    Optional.of("cat.sch.tbl"),
                    ErrorCode.UPDATE_REQUIREMENT_CONFLICT))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.UPDATE_REQUIREMENT_CONFLICT);
  }

  /** The re-read takes the lock too, so its lock failure is reported like the lock query's. */
  @ParameterizedTest
  @MethodSource("lockAcquisitionFailures")
  public void lockFailureOnTheReReadRaisesTheCallerSuppliedError(RuntimeException lockFailure) {
    Session session = mock();
    TableInfoDAO dao = mock();
    when(lockQuery(session).getSingleResultOrNull()).thenReturn(dao);
    doThrow(lockFailure).when(session).refresh(dao, LockMode.PESSIMISTIC_WRITE);

    assertThatThrownBy(
            () ->
                RepositoryUtils.lockTableForCommit(
                    session,
                    dao,
                    TABLE_ID,
                    Optional.of("cat.sch.tbl"),
                    ErrorCode.COMMIT_STATE_UNKNOWN))
        .isInstanceOfSatisfying(
            BaseException.class,
            e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.COMMIT_STATE_UNKNOWN));
  }

  @ParameterizedTest
  @CsvSource({"a.b.c, 3", "a.b, 2"})
  public void splitFullNameAcceptsExactlyTheExpectedNonEmptyParts(String name, int parts) {
    assertThat(RepositoryUtils.splitFullName(name, parts, "Invalid name: " + name))
        .containsExactly(name.split("\\."));
  }

  @ParameterizedTest
  @CsvSource({".b.c, 3", "a..c, 3", "a.b., 3", "a., 2"})
  public void splitFullNameRejectsAnEmptyPart(String name, int parts) {
    assertRejected(name, parts);
  }

  @ParameterizedTest
  @CsvSource({"a.b, 3", "a.b.c.d, 3", "a.b.c., 3", "'', 3", "a.b.c, 2"})
  public void splitFullNameRejectsTheWrongNumberOfParts(String name, int parts) {
    assertRejected(name, parts);
  }

  private static void assertRejected(String name, int parts) {
    assertThatThrownBy(() -> RepositoryUtils.splitFullName(name, parts, "Invalid name: " + name))
        .isInstanceOfSatisfying(
            BaseException.class,
            e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT))
        .hasMessage("Invalid name: " + name);
  }

  /**
   * Any non-lock failure must propagate unchanged rather than be masked as a lock-contention error.
   */
  @Test
  public void nonLockFailurePropagatesUnchanged() {
    Session session = mock();
    TableInfoDAO dao = mock();
    IllegalStateException unexpected = new IllegalStateException("boom");
    when(lockQuery(session).getSingleResultOrNull()).thenThrow(unexpected);

    assertThatThrownBy(
            () ->
                RepositoryUtils.lockTableForCommit(
                    session, dao, TABLE_ID, Optional.empty(), ErrorCode.COMMIT_STATE_UNKNOWN))
        .isSameAs(unexpected);
  }
}
