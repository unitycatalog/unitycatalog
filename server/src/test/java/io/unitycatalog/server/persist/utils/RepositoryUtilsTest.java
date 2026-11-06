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
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.Session;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.ExceptionConverterImpl;
import org.hibernate.query.SelectionQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class RepositoryUtilsTest {

  private static final UUID TABLE_ID = UUID.randomUUID();
  private static final Supplier<BaseException> NOT_FOUND =
      () -> new BaseException(ErrorCode.CATALOG_NOT_FOUND, "Catalog not found");

  // The by-key helpers work on any entity with a UUID id; catalogs stand in for every type.
  @TempDatabase private static TestDatabase testDatabase;

  /**
   * A failed pessimistic lock acquisition means a concurrent commit or deletion holds the lock; the
   * caller decides how that surfaces (Delta as the retryable {@code COMMIT_STATE_UNKNOWN}, Iceberg
   * as an {@code UPDATE_REQUIREMENT_CONFLICT}; {@code lockByKey}, used by table deletion, as {@code
   * UNAVAILABLE}), never as a leaked ORM exception. The lock query passes the dialect's exception
   * through Hibernate's exception converter, so each failure here is the converter's output for one
   * dialect exception.
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

  private UUID persistCatalog(String name, String comment) {
    UUID id = UUID.randomUUID();
    inTransaction(
        session -> {
          session.persist(
              CatalogInfoDAO.builder().id(id).name(name).comment(comment).owner("owner").build());
          return null;
        });
    return id;
  }

  private <R> R inTransaction(TransactionManager.DatabaseOperation<R> work) {
    return TransactionManager.executeWithTransaction(
        testDatabase.sessionFactory(), work, "Failed in test", /* readOnly= */ false);
  }

  private CatalogInfoDAO findCatalog(UUID id) {
    try (Session session = testDatabase.sessionFactory().openSession()) {
      return session.get(CatalogInfoDAO.class, id);
    }
  }

  /** A row is found only when its id and every key attribute match. */
  @Test
  public void lockByKeyFindsTheRowOnlyWhenIdAndKeyBothMatch() {
    UUID id = persistCatalog("lock_by_key", "c");
    UUID otherId = persistCatalog("lock_by_key_other", "other");
    Map<String, Object> key = Map.of("name", "lock_by_key", "owner", "owner");

    inTransaction(
        session -> {
          assertThat(
                  RepositoryUtils.lockByKey(
                          session, CatalogInfoDAO.class, id, key, "catalog lock_by_key", NOT_FOUND)
                      .getComment())
              .isEqualTo("c");
          for (Map<String, Object> wrongKey :
              List.of(
                  Map.<String, Object>of("name", "renamed", "owner", "owner"),
                  Map.<String, Object>of("name", "lock_by_key", "owner", "other"),
                  // A key attribute named like the id must not replace the id being locked.
                  Map.<String, Object>of("id", otherId))) {
            assertThatThrownBy(
                    () ->
                        RepositoryUtils.lockByKey(
                            session, CatalogInfoDAO.class, id, wrongKey, "catalog", NOT_FOUND))
                .isInstanceOfSatisfying(
                    BaseException.class,
                    e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.CATALOG_NOT_FOUND));
          }
          assertThatThrownBy(
                  () ->
                      RepositoryUtils.lockByKey(
                          session,
                          CatalogInfoDAO.class,
                          UUID.randomUUID(),
                          key,
                          "catalog",
                          NOT_FOUND))
              .isInstanceOfSatisfying(
                  BaseException.class,
                  e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.CATALOG_NOT_FOUND));
          return null;
        });
  }

  /** A failed lock acquisition is UNAVAILABLE and names the entity, never an ORM exception. */
  @ParameterizedTest
  @MethodSource("lockAcquisitionFailures")
  public void lockByKeyReportsALockFailureAsUnavailable(RuntimeException lockFailure) {
    Session session = mock();
    when(lockQuery(session).getSingleResultOrNull()).thenThrow(lockFailure);

    assertThatThrownBy(
            () ->
                RepositoryUtils.lockByKey(
                    session,
                    TableInfoDAO.class,
                    TABLE_ID,
                    Map.of("name", "tbl"),
                    "table tbl",
                    NOT_FOUND))
        .isInstanceOfSatisfying(
            BaseException.class,
            e -> {
              assertThat(e.getErrorCode()).isEqualTo(ErrorCode.UNAVAILABLE);
              assertThat(e.getMessage())
                  .contains("Concurrent modification in progress on table tbl");
            });
  }

  /** A delete whose key no longer matches changes nothing and reports not found. */
  @Test
  public void deleteByKeyMatchesNothingWhenTheKeyDiffers() {
    UUID id = persistCatalog("by_key", "c");
    inTransaction(
        session -> {
          CatalogInfoDAO entity = session.get(CatalogInfoDAO.class, id);
          assertThatThrownBy(
                  () ->
                      RepositoryUtils.deleteByKey(
                          session, entity, Map.of("name", "renamed"), NOT_FOUND))
              .isInstanceOf(BaseException.class);
          return null;
        });
    assertThat(findCatalog(id).getComment()).isEqualTo("c");

    inTransaction(
        session -> {
          CatalogInfoDAO entity = session.get(CatalogInfoDAO.class, id);
          RepositoryUtils.deleteByKey(session, entity, Map.of("name", "by_key"), NOT_FOUND);
          assertThat(session.contains(entity)).isFalse();
          return null;
        });
    assertThat(findCatalog(id)).isNull();
  }
}
