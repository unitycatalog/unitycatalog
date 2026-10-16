package io.unitycatalog.server.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.utils.LogCapture;
import java.time.Duration;
import java.util.List;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.Test;

/** A policy read that fails is logged as a warning, since revokes stop arriving until it works. */
public class CasbinPolicyRefresherTest {

  @Test
  public void failedPolicyReadsAreWarnings() {
    RuntimeException databaseError = new RuntimeException("connection refused");
    SessionFactory sessionFactory = mock(SessionFactory.class);
    when(sessionFactory.fromSession(any())).thenThrow(databaseError);
    CasbinPolicyRefresher refresher =
        new CasbinPolicyRefresher(() -> {}, sessionFactory, Duration.ZERO);

    try (LogCapture logs = LogCapture.of(CasbinPolicyRefresher.class)) {
      assertThat(refresher.checkAndReload()).isFalse();
      assertThat(refresher.checkAndReloadAfter(System.nanoTime())).isFalse();

      List<LogEvent> warnings = logs.eventsAtLeast(Level.WARN);
      assertThat(warnings).hasSize(2);
      assertThat(warnings).allSatisfy(w -> assertThat(w.getThrown()).isSameAs(databaseError));
    }
  }
}
