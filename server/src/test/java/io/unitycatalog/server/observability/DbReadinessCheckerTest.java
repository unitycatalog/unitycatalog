package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.observability.DbReadinessChecker.DbProbe;
import java.sql.Connection;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.jdbc.ReturningWork;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DbReadinessCheckerTest {

  @Test
  public void initialStateIsNotReady() {
    DbReadinessChecker checker = new DbReadinessChecker(() -> true, Duration.ofSeconds(5));
    assertThat(checker.healthChecker().isHealthy()).isFalse();
  }

  @ParameterizedTest
  @MethodSource("probeOutcomes")
  public void refreshReflectsProbeOutcome(DbProbe probe, boolean expectedHealthy) {
    DbReadinessChecker checker = new DbReadinessChecker(probe, Duration.ofSeconds(5));
    checker.refresh();
    assertThat(checker.healthChecker().isHealthy()).isEqualTo(expectedHealthy);
  }

  static Stream<Arguments> probeOutcomes() {
    // The Error case guards the catch (Throwable): an Error escaping a scheduled probe would
    // otherwise cancel all future runs and freeze readiness. Built as a local so the throwing
    // lambda does not trip the "no throwing Error" checkstyle rule (which targets production code).
    Error probeError = new StackOverflowError("boom");
    return Stream.of(
        arguments((DbProbe) () -> true, true),
        arguments((DbProbe) () -> false, false),
        arguments(
            (DbProbe)
                () -> {
                  throw new RuntimeException("db down");
                },
            false),
        arguments(
            (DbProbe)
                () -> {
                  throw probeError;
                },
            false));
  }

  @Test
  public void startRunsSynchronousInitialProbeThenIsHealthy() {
    DbReadinessChecker checker = new DbReadinessChecker(() -> true, Duration.ofSeconds(5));
    checker.start();
    // No sleep: start() probes synchronously before returning.
    assertThat(checker.healthChecker().isHealthy()).isTrue();
    checker.close();
  }

  @Test
  public void backgroundSchedulerReprobesAndFlipsStateBothWays() throws InterruptedException {
    AtomicBoolean reachable = new AtomicBoolean(true);
    try (DbReadinessChecker checker =
        new DbReadinessChecker(reachable::get, Duration.ofMillis(50))) {
      checker.start();
      assertThat(checker.healthChecker().isHealthy()).isTrue();

      // The scheduler must keep re-probing: a later DB outage flips readiness to not-ready...
      reachable.set(false);
      pollUntil(() -> !checker.healthChecker().isHealthy(), Duration.ofSeconds(2));
      assertThat(checker.healthChecker().isHealthy()).isFalse();

      // ...and recovery flips it back.
      reachable.set(true);
      pollUntil(() -> checker.healthChecker().isHealthy(), Duration.ofSeconds(2));
      assertThat(checker.healthChecker().isHealthy()).isTrue();
    }
  }

  @Test
  public void startAndCloseAreIdempotent() {
    DbReadinessChecker checker = new DbReadinessChecker(() -> true, Duration.ofSeconds(5));
    checker.start();
    checker.start(); // second start is a no-op, does not schedule a second checker
    assertThat(checker.healthChecker().isHealthy()).isTrue();
    checker.close();
    checker.close(); // second close is a no-op, does not throw
  }

  @Test
  public void forSessionFactoryClampsSubSecondTimeoutToOneSecond() throws Exception {
    // Connection.isValid(0) means "no timeout / wait forever", so a positive sub-second db-timeout
    // must clamp up to 1s, never down to 0.
    assertProbeUsesValidityTimeout(Duration.ofMillis(500), 1);
  }

  @Test
  public void forSessionFactoryPassesWholeSecondTimeout() throws Exception {
    assertProbeUsesValidityTimeout(Duration.ofSeconds(2), 2);
  }

  private static void assertProbeUsesValidityTimeout(Duration dbTimeout, int expectedSeconds)
      throws Exception {
    SessionFactory sessionFactory = mock(SessionFactory.class);
    Session session = mock(Session.class);
    Connection connection = mock(Connection.class);
    when(sessionFactory.openSession()).thenReturn(session);
    when(session.doReturningWork(any()))
        .thenAnswer(
            inv -> {
              ReturningWork<Boolean> work = inv.getArgument(0);
              return work.execute(connection);
            });
    when(connection.isValid(anyInt())).thenReturn(true);

    DbReadinessChecker checker =
        DbReadinessChecker.forSessionFactory(sessionFactory, Duration.ofSeconds(5), dbTimeout);
    checker.refresh();

    verify(connection).isValid(expectedSeconds);
    assertThat(checker.healthChecker().isHealthy()).isTrue();
  }

  private static void pollUntil(BooleanSupplier condition, Duration timeout)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(10);
    }
  }
}
