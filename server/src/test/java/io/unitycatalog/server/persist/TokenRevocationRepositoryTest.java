package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TokenRevocationRepositoryTest {

  private static HibernateConfigurator hibernateConfigurator;
  private static TokenRevocationRepository repository;

  @BeforeAll
  public static void setUp() {
    // Use a dedicated in-memory database so the test is hermetic and does not touch the on-disk
    // dev database referenced by etc/conf/hibernate.properties.
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
    properties.setProperty(
        "hibernate.connection.url", "jdbc:h2:mem:token_revocation_test;DB_CLOSE_DELAY=-1");
    properties.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    hibernateConfigurator = new HibernateConfigurator(properties);
    repository = new TokenRevocationRepository(hibernateConfigurator.getSessionFactory());
  }

  @AfterAll
  public static void tearDown() {
    hibernateConfigurator.close();
  }

  @Test
  public void testRevokeThenIsRevoked() {
    UUID jti = UUID.randomUUID();
    assertThat(repository.isRevoked(jti)).isFalse();

    repository.revoke(jti, hoursFromNow(1));

    assertThat(repository.isRevoked(jti)).isTrue();
  }

  @Test
  public void testRevokeIsIdempotent() {
    UUID jti = UUID.randomUUID();
    Date expiresAt = hoursFromNow(1);
    repository.revoke(jti, expiresAt);
    // Revoking the same jti again is a no-op rather than failing on the duplicate primary key.
    repository.revoke(jti, expiresAt);

    assertThat(repository.isRevoked(jti)).isTrue();
  }

  @Test
  public void testDuplicateRevokeAfterCleanupFailsOnlyForLiveTokens() {
    // A concurrent logout inserts the row first, and the cleanup purges it before this revoke reads
    // it back. Stubbing the read back as "not revoked" and the cleanup as a no-op reproduces that
    // order. The purged row was expired, so the failed insert is ignored; a live token's row is
    // never purged, so for it a failed insert that reads back nothing is a real failure.
    TokenRevocationRepository racing =
        spy(new TokenRevocationRepository(hibernateConfigurator.getSessionFactory()));
    doReturn(0).when(racing).deleteExpired();
    UUID expired = UUID.randomUUID();
    UUID live = UUID.randomUUID();
    racing.revoke(expired, hoursFromNow(-1));
    racing.revoke(live, hoursFromNow(1));
    doReturn(false).when(racing).isRevoked(any(UUID.class));

    assertThatCode(() -> racing.revoke(expired, hoursFromNow(-1))).doesNotThrowAnyException();
    assertThatThrownBy(() -> racing.revoke(live, hoursFromNow(1)))
        .isInstanceOf(BaseException.class);
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
  public void testDeleteExpiredRemovesOnlyExpiredEntries() {
    UUID expired = UUID.randomUUID();
    UUID live = UUID.randomUUID();
    repository.revoke(expired, hoursFromNow(-1));
    repository.revoke(live, hoursFromNow(1));

    // Call cleanup explicitly rather than assert on its return count: revoke() also fires an
    // asynchronous best-effort cleanup, which may already have removed the expired row. Only the
    // resulting state is deterministic.
    repository.deleteExpired();

    assertThat(repository.isRevoked(expired)).isFalse();
    assertThat(repository.isRevoked(live)).isTrue();
  }

  private static Date hoursFromNow(long hours) {
    return Date.from(Instant.now().plus(Duration.ofHours(hours)));
  }
}
