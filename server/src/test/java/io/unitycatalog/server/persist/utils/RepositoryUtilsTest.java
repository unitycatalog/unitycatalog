package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
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
import org.hibernate.Session;
import org.hibernate.query.SelectionQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class RepositoryUtilsTest {

  private static final UUID TABLE_ID = UUID.randomUUID();

  /**
   * A failed pessimistic lock acquisition means a concurrent commit is in progress; the caller
   * decides how that surfaces (Delta as the retryable {@code COMMIT_STATE_UNKNOWN}, Iceberg as an
   * {@code UPDATE_REQUIREMENT_CONFLICT}), never as a leaked ORM exception. UC bootstraps Hibernate
   * natively, so the failure arrives as a {@code HibernateException} subtype: {@code
   * org.hibernate.PessimisticLockException}, or the JDBC-translated {@code
   * LockAcquisitionException} (deadlock) / {@code LockTimeoutException}. It is never a {@code
   * jakarta.persistence} exception (those only surface under a JPA bootstrap), so those are not
   * covered here.
   */
  static RuntimeException[] lockAcquisitionFailures() {
    return new RuntimeException[] {
      new org.hibernate.PessimisticLockException("locked", new SQLException("locked"), "sql"),
      new org.hibernate.exception.LockAcquisitionException("locked", new SQLException("locked")),
      new org.hibernate.exception.LockTimeoutException("timed out", new SQLException("timed out")),
    };
  }

  /** Makes the row-lock query that lockTableForCommit runs throw {@code failure}. */
  @SuppressWarnings("unchecked")
  private static void lockQueryThrows(Session session, RuntimeException failure) {
    SelectionQuery<TableInfoDAO> query = mock(SelectionQuery.class, RETURNS_SELF);
    doThrow(failure).when(query).getSingleResult();
    when(session.createSelectionQuery(any(String.class), any(Class.class))).thenReturn(query);
  }

  @ParameterizedTest
  @MethodSource("lockAcquisitionFailures")
  public void lockFailureRaisesTheCallerSuppliedError(RuntimeException lockFailure) {
    Session session = mock();
    TableInfoDAO dao = mock();
    lockQueryThrows(session, lockFailure);

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

  /**
   * Any non-lock failure must propagate unchanged rather than be masked as a lock-contention error.
   */
  @Test
  public void nonLockFailurePropagatesUnchanged() {
    Session session = mock();
    TableInfoDAO dao = mock();
    IllegalStateException unexpected = new IllegalStateException("boom");
    lockQueryThrows(session, unexpected);

    assertThatThrownBy(
            () ->
                RepositoryUtils.lockTableForCommit(
                    session, dao, TABLE_ID, Optional.empty(), ErrorCode.COMMIT_STATE_UNKNOWN))
        .isSameAs(unexpected);
  }
}
