package io.unitycatalog.server.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class StorageCleanupPollerTest {
  private static final Duration INTERVAL = Duration.ofMillis(1);

  private final StorageCleanupWorker worker = mock(StorageCleanupWorker.class);
  private final StorageCleanupPoller poller = new StorageCleanupPoller(worker);

  @AfterEach
  void closePoller() {
    poller.close();
  }

  @Test
  void startsPollsAndCloses() throws Exception {
    CountDownLatch polled = new CountDownLatch(1);
    when(worker.runOnce())
        .thenAnswer(
            ignored -> {
              polled.countDown();
              return false;
            });

    poller.start(INTERVAL);
    poller.start(INTERVAL);

    assertThat(polled.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(poller.isRunning()).isTrue();
    poller.close();
    assertThat(poller.isRunning()).isFalse();
    verify(worker).close();
  }

  @Test
  void stopsAndRestarts() throws Exception {
    CountDownLatch firstPoll = new CountDownLatch(1);
    CountDownLatch secondPoll = new CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();
    when(worker.runOnce())
        .thenAnswer(
            ignored -> {
              if (calls.getAndIncrement() == 0) {
                firstPoll.countDown();
              } else {
                secondPoll.countDown();
              }
              return false;
            });

    poller.start(INTERVAL);
    assertThat(firstPoll.await(5, TimeUnit.SECONDS)).isTrue();
    poller.stop();
    assertThat(poller.isRunning()).isFalse();

    poller.start(INTERVAL);
    assertThat(secondPoll.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(poller.isRunning()).isTrue();
  }

  @Test
  void stopWaitsForActivePollBeforeRestarting() throws Exception {
    CountDownLatch pollStarted = new CountDownLatch(1);
    CountDownLatch pollInterrupted = new CountDownLatch(1);
    CountDownLatch releasePoll = new CountDownLatch(1);
    CountDownLatch restartAttempted = new CountDownLatch(1);
    when(worker.runOnce())
        .thenAnswer(
            ignored -> {
              pollStarted.countDown();
              try {
                releasePoll.await();
              } catch (InterruptedException e) {
                pollInterrupted.countDown();
                releasePoll.await();
              }
              return false;
            });

    FutureTask<Void> stopping = new FutureTask<>(poller::stop, null);
    Thread stopThread = new Thread(stopping);
    FutureTask<Void> restarting =
        new FutureTask<>(
            () -> {
              restartAttempted.countDown();
              poller.start(INTERVAL);
            },
            null);
    Thread restartThread = new Thread(restarting);
    try {
      poller.start(INTERVAL);
      assertThat(pollStarted.await(5, TimeUnit.SECONDS)).isTrue();
      stopThread.start();

      assertThat(pollInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(stopping.isDone()).isFalse();
      restartThread.start();
      assertThat(restartAttempted.await(5, TimeUnit.SECONDS)).isTrue();
      assertThatThrownBy(() -> restarting.get(1, TimeUnit.SECONDS))
          .isInstanceOf(TimeoutException.class);

      releasePoll.countDown();
      stopping.get(5, TimeUnit.SECONDS);
      restarting.get(5, TimeUnit.SECONDS);
      assertThat(poller.isRunning()).isTrue();
    } finally {
      releasePoll.countDown();
      stopThread.join(5000);
      restartThread.join(5000);
    }
  }

  @Test
  void retriesAfterUnexpectedPollFailure() throws Exception {
    CountDownLatch recovered = new CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();
    when(worker.runOnce())
        .thenAnswer(
            ignored -> {
              if (calls.getAndIncrement() == 0) {
                throw new IllegalStateException("first poll failed");
              }
              recovered.countDown();
              return false;
            });

    poller.start(INTERVAL);

    assertThat(recovered.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(calls).hasValueGreaterThanOrEqualTo(2);
  }

  @Test
  void doesNotOverlapPolls() throws Exception {
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    CountDownLatch secondFinished = new CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger active = new AtomicInteger();
    AtomicInteger maximumActive = new AtomicInteger();
    when(worker.runOnce())
        .thenAnswer(
            ignored -> {
              int call = calls.incrementAndGet();
              maximumActive.accumulateAndGet(active.incrementAndGet(), Math::max);
              try {
                if (call == 1) {
                  firstStarted.countDown();
                  releaseFirst.await();
                } else {
                  secondFinished.countDown();
                }
                return false;
              } finally {
                active.decrementAndGet();
              }
            });

    poller.start(INTERVAL);
    assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
    releaseFirst.countDown();

    assertThat(secondFinished.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(maximumActive).hasValue(1);
  }

  @Test
  void rejectsSubMillisecondIntervals() {
    for (Duration interval : List.of(Duration.ZERO, Duration.ofNanos(1), Duration.ofMillis(-1))) {
      assertThatThrownBy(() -> poller.start(interval))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Cleanup poll interval must be at least one millisecond");
    }
    assertThat(poller.isRunning()).isFalse();
  }
}
