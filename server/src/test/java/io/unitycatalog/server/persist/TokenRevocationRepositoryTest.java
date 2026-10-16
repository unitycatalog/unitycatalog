package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TokenRevocationRepositoryTest {

  private static SessionFactory sessionFactory;
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
    sessionFactory = new HibernateConfigurator(properties).getSessionFactory();
    repository = new TokenRevocationRepository(null, sessionFactory);
  }

  @AfterAll
  public static void tearDown() {
    sessionFactory.close();
  }

  private boolean isRevoked(UUID jti) {
    try (Session session = sessionFactory.openSession()) {
      return repository.isRevoked(session, jti);
    }
  }

  @Test
  public void testRevokeThenIsRevoked() {
    UUID jti = UUID.randomUUID();
    assertThat(isRevoked(jti)).isFalse();

    repository.revoke(jti, hoursFromNow(1));

    assertThat(isRevoked(jti)).isTrue();
  }

  @Test
  public void testRevokeIsIdempotent() {
    UUID jti = UUID.randomUUID();
    repository.revoke(jti, hoursFromNow(1));
    // Revoking the same jti again is a no-op rather than failing on the duplicate primary key.
    repository.revoke(jti, hoursFromNow(2));

    assertThat(isRevoked(jti)).isTrue();
  }

  @Test
  public void testPermanentRevocationSurvivesCleanup() {
    // A null expiry means the token never expires, so its revocation is permanent and cleanup must
    // not remove it.
    UUID jti = UUID.randomUUID();
    repository.revoke(jti, null);

    repository.deleteExpired();

    assertThat(isRevoked(jti)).isTrue();
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

    assertThat(isRevoked(expired)).isFalse();
    assertThat(isRevoked(live)).isTrue();
  }

  private static Date hoursFromNow(long hours) {
    return Date.from(Instant.now().plus(Duration.ofHours(hours)));
  }
}
