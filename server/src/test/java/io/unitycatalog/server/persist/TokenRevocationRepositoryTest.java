package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TokenRevocationRepositoryTest {

  @TempDatabase private TestDatabase testDatabase;
  private TokenRevocationRepository repository;

  @BeforeEach
  void setUpRepository() {
    // A spy whose cleanup is a no-op, so the asynchronous sweep that revoke() starts never races
    // with a test; tests call the real cleanup through deleteExpiredForTest().
    repository = spy(new TokenRevocationRepository(testDatabase.sessionFactory()));
    doReturn(0).when(repository).deleteExpired();
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
  public void testPermanentRevocationSurvivesCleanup() {
    // A null expiry means the token never expires, so its revocation is permanent and cleanup must
    // not remove it.
    UUID jti = UUID.randomUUID();
    repository.revoke(jti, null);

    deleteExpiredForTest();

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

    assertThat(deleteExpiredForTest()).isEqualTo(1);

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

  /** Runs the real cleanup, which the spy stubs out for revoke(). */
  private int deleteExpiredForTest() {
    return new TokenRevocationRepository(testDatabase.sessionFactory()).deleteExpired();
  }

  private static Date fromNow(Duration offset) {
    return Date.from(Instant.now().plus(offset));
  }
}
