package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

public class DbReadinessCheckerTest {

  @Test
  public void initialStateIsNotReady() {
    DbReadinessChecker checker = new DbReadinessChecker(() -> true, Duration.ofSeconds(5));
    assertThat(checker.healthChecker().isHealthy()).isFalse();
  }

  @Test
  public void refreshMarksHealthyWhenProbeSucceeds() {
    DbReadinessChecker checker = new DbReadinessChecker(() -> true, Duration.ofSeconds(5));
    checker.refresh();
    assertThat(checker.healthChecker().isHealthy()).isTrue();
  }

  @Test
  public void refreshMarksUnhealthyWhenProbeReturnsFalse() {
    DbReadinessChecker checker = new DbReadinessChecker(() -> false, Duration.ofSeconds(5));
    checker.refresh();
    assertThat(checker.healthChecker().isHealthy()).isFalse();
  }

  @Test
  public void refreshMarksUnhealthyWhenProbeThrows() {
    DbReadinessChecker checker =
        new DbReadinessChecker(
            () -> {
              throw new RuntimeException("db down");
            },
            Duration.ofSeconds(5));
    checker.refresh();
    assertThat(checker.healthChecker().isHealthy()).isFalse();
  }

  @Test
  public void startRunsSynchronousInitialProbeThenIsHealthy() {
    DbReadinessChecker checker = new DbReadinessChecker(() -> true, Duration.ofSeconds(5));
    checker.start();
    // No sleep: start() probes synchronously before returning.
    assertThat(checker.healthChecker().isHealthy()).isTrue();
    checker.close();
  }
}
