package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TokenRevocationRepositoryTest {

  @TempDatabase private TestDatabase testDatabase;
  // The cleanups that revoke() started. They run only when a test runs them, so they never race
  // with a test.
  private final List<Runnable> cleanups = new ArrayList<>();
  private TokenRevocationRepository repository;

  @BeforeEach
  void setUpRepository() {
    repository = spy(new TokenRevocationRepository(testDatabase.sessionFactory(), cleanups::add));
  }

  @Test
  public void testRevokeThenIsRevoked() {
    UUID jti = UUID.randomUUID();
    assertThat(repository.isRevoked(jti)).isFalse();

    repository.revoke(jti, fromNow(Duration.ofHours(1)));

    assertThat(repository.isRevoked(jti)).isTrue();
  }

  @Test
  public void testRevokeIsIdempotent() {
    UUID jti = UUID.randomUUID();
    Date expiresAt = fromNow(Duration.ofHours(1));
    repository.revoke(jti, expiresAt);
    // Revoking the same jti again is a no-op rather than failing on the duplicate primary key.
    repository.revoke(jti, expiresAt);

    assertThat(repository.isRevoked(jti)).isTrue();
  }

  @Test
  public void testDuplicateRevokeAfterCleanupFailsOnlyForLiveTokens() {
    // A concurrent logout inserts the row first, and the cleanup purges it before this revoke reads
    // it back. Stubbing the read back as "not revoked" reproduces that order. The purged row was
    // expired, so the failed insert is ignored; a live token's row is never purged, so for it a
    // failed insert that reads back nothing is a real failure.
    UUID expired = UUID.randomUUID();
    UUID live = UUID.randomUUID();
    repository.revoke(expired, fromNow(Duration.ofHours(-1)));
    repository.revoke(live, fromNow(Duration.ofHours(1)));
    doReturn(false).when(repository).isRevoked(any(UUID.class));

    assertThatCode(() -> repository.revoke(expired, fromNow(Duration.ofHours(-1))))
        .doesNotThrowAnyException();
    assertThatThrownBy(() -> repository.revoke(live, fromNow(Duration.ofHours(1))))
        .isExactlyInstanceOf(BaseException.class)
        .hasMessage("Failed to revoke token")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INTERNAL);
  }

  @Test
  public void testFailedReadBackKeepsTheInsertFailure() {
    // The insert fails on the duplicate key, and reading the row back fails too: the insert's
    // failure is thrown, with the read's failure attached, so the log shows both.
    UUID jti = UUID.randomUUID();
    repository.revoke(jti, fromNow(Duration.ofHours(1)));
    BaseException readFailure = new BaseException(ErrorCode.INTERNAL, "read failed");
    doThrow(readFailure).when(repository).isRevoked(any(UUID.class));

    assertThatThrownBy(() -> repository.revoke(jti, fromNow(Duration.ofHours(1))))
        .isExactlyInstanceOf(BaseException.class)
        .hasMessage("Failed to revoke token")
        .satisfies(e -> assertThat(e.getCause()).hasMessageStartingWith("Failed to revoke token: "))
        .satisfies(e -> assertThat(e.getCause().getSuppressed()).containsExactly(readFailure));
  }

  @Test
  public void testCleanupRunsOneAtATime() {
    repository.revoke(UUID.randomUUID(), fromNow(Duration.ofHours(1)));
    repository.revoke(UUID.randomUUID(), fromNow(Duration.ofHours(1)));
    assertThat(cleanups).as("started while one is running").hasSize(1);

    cleanups.get(0).run();
    repository.revoke(UUID.randomUUID(), fromNow(Duration.ofHours(1)));
    assertThat(cleanups).as("started after it finished").hasSize(2);
  }

  @Test
  public void testCleanupStartsAgainAfterItCouldNotStart() {
    List<Runnable> started = new ArrayList<>();
    TokenRevocationRepository rejecting =
        new TokenRevocationRepository(
            testDatabase.sessionFactory(),
            task -> {
              if (started.isEmpty()) {
                started.add(null);
                throw new RejectedExecutionException("rejected");
              }
              started.add(task);
            });

    UUID jti = UUID.randomUUID();
    rejecting.revoke(jti, fromNow(Duration.ofHours(1)));
    assertThat(rejecting.isRevoked(jti)).as("revoked despite the rejected cleanup").isTrue();
    rejecting.revoke(UUID.randomUUID(), fromNow(Duration.ofHours(1)));
    assertThat(started).as("rejected, then started").hasSize(2);
  }

  @Test
  public void testPermanentRevocationSurvivesCleanup() {
    // A null expiry means the token never expires, so its revocation is permanent and cleanup must
    // not remove it.
    UUID jti = UUID.randomUUID();
    repository.revoke(jti, null);

    repository.deleteExpired();

    assertThat(repository.isRevoked(jti)).isTrue();
  }

  @Test
  public void testDeleteExpiredKeepsRowsWithinTheRetentionGrace() {
    // A stored expiry can be off by a time zone offset, so a row is kept for a grace period past
    // its expiry, and only then removed.
    UUID live = UUID.randomUUID();
    UUID recentlyExpired = UUID.randomUUID();
    UUID longExpired = UUID.randomUUID();
    repository.revoke(live, fromNow(Duration.ofHours(1)));
    repository.revoke(recentlyExpired, fromNow(Duration.ofHours(-1)));
    repository.revoke(
        longExpired, fromNow(TokenRevocationRepository.RETENTION_GRACE.plusHours(1).negated()));

    assertThat(repository.deleteExpired()).isEqualTo(1);

    assertThat(repository.isRevoked(live)).isTrue();
    assertThat(repository.isRevoked(recentlyExpired)).isTrue();
    assertThat(repository.isRevoked(longExpired)).isFalse();
  }

  @Test
  public void testDatabaseFailureLeavesOutTheDatabaseMessage() {
    // A failure inside the transaction is thrown with only the repository's message; the
    // database's message stays on the cause, for the server log.
    testDatabase
        .sessionFactory()
        .inTransaction(
            session ->
                session
                    .createNativeMutationQuery(
                        "ALTER TABLE uc_token_revocations ADD CONSTRAINT revoke_failure"
                            + " CHECK (expires_at IS NULL)")
                    .executeUpdate());

    assertThatThrownBy(() -> repository.revoke(UUID.randomUUID(), fromNow(Duration.ofHours(1))))
        .isExactlyInstanceOf(BaseException.class)
        .hasMessage("Failed to revoke token")
        .cause()
        .hasMessageContaining("Failed to revoke token: ");
  }

  private static Date fromNow(Duration offset) {
    return Date.from(Instant.now().plus(offset));
  }
}
