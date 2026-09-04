package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.LockMode;
import org.hibernate.Session;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class RepositoryUtilsTest {

  private static final UUID TABLE_ID = UUID.randomUUID();

  /**
   * A failed pessimistic lock acquisition means a concurrent commit is in progress and this
   * request's outcome is unknown (the in-flight attempt may still land). It must surface as the
   * retryable {@code COMMIT_STATE_UNKNOWN}, never as a conflict that would invite a rebase. Both
   * the Hibernate and Jakarta {@code PessimisticLockException}, and the Jakarta {@code
   * LockTimeoutException} a lock-wait can raise instead, are covered.
   */
  static RuntimeException[] lockAcquisitionFailures() {
    return new RuntimeException[] {
      new org.hibernate.PessimisticLockException("locked", new SQLException("locked"), "sql"),
      new jakarta.persistence.PessimisticLockException("locked"),
      new jakarta.persistence.LockTimeoutException("timed out"),
    };
  }

  @ParameterizedTest
  @MethodSource("lockAcquisitionFailures")
  public void lockFailureMapsToCommitStateUnknown(RuntimeException lockFailure) {
    Session session = mock();
    TableInfoDAO dao = mock();
    doThrow(lockFailure).when(session).refresh(any(Object.class), any(LockMode.class));

    assertThatThrownBy(
            () ->
                RepositoryUtils.lockTableForCommit(
                    session, dao, TABLE_ID, Optional.of("cat.sch.tbl")))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.COMMIT_STATE_UNKNOWN);
  }

  /** Any non-lock failure must propagate unchanged rather than be masked as an unknown outcome. */
  @Test
  public void nonLockFailurePropagatesUnchanged() {
    Session session = mock();
    TableInfoDAO dao = mock();
    IllegalStateException unexpected = new IllegalStateException("boom");
    doThrow(unexpected).when(session).refresh(any(Object.class), any(LockMode.class));

    assertThatThrownBy(
            () -> RepositoryUtils.lockTableForCommit(session, dao, TABLE_ID, Optional.empty()))
        .isSameAs(unexpected);
  }
}
