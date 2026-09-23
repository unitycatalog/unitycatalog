package io.unitycatalog.server.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.persist.StorageCleanupTaskRepository;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository.Claim;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository.CleanupFailureReport;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.utils.CooperativeDeadline;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@SuppressWarnings("unchecked")
class StorageCleanupWorkerTest {
  private static final Duration INTERVAL = Duration.ofMillis(1);
  private static final Duration LEASE_DURATION = Duration.ofMinutes(5);
  private static final Duration ATTEMPT_TIMEOUT = Duration.ofSeconds(20);
  private static final Duration INITIAL_DELAY = Duration.ofHours(1);
  private static final Duration RETRY_BACKOFF = Duration.ofMinutes(1);
  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
  private static final UUID RESOURCE_ID = UUID.randomUUID();
  private static final UUID LEASE_TOKEN = UUID.randomUUID();
  private static final NormalizedURL LOCATION =
      NormalizedURL.from("s3://bucket/tables/" + RESOURCE_ID);
  private static final String PREFIX = LOCATION + "/";

  private final StorageCleanupTaskRepository taskRepository =
      mock(StorageCleanupTaskRepository.class);
  private final FileOperations fileOperations = mock(FileOperations.class);
  private final SupportsPrefixOperations fileIO = mock(SupportsPrefixOperations.class);
  private final Clock clock = mock(Clock.class);
  private final ServerProperties serverProperties = mock(ServerProperties.class);
  private StorageCleanupWorker worker;

  @BeforeEach
  void setUp() {
    setClaim(ResourceType.TABLE, LOCATION.toString());
    when(clock.instant()).thenReturn(NOW);
    when(fileOperations.getCleanupFileIO(eq(LOCATION), any())).thenReturn(fileIO);
    when(fileIO.listPrefix(PREFIX)).thenReturn(List.of());
    when(serverProperties.getStorageCleanupPollInterval()).thenReturn(INTERVAL);
    when(serverProperties.getStorageCleanupLeaseDuration()).thenReturn(LEASE_DURATION);
    when(serverProperties.getStorageCleanupInitialDelay()).thenReturn(INITIAL_DELAY);
    when(serverProperties.getStorageCleanupRetryBackoff()).thenReturn(RETRY_BACKOFF);
    worker = newWorker(ATTEMPT_TIMEOUT);
  }

  @AfterEach
  void closeWorker() {
    worker.close();
  }

  @Test
  void returnsWithoutWorkWhenClaimIsEmpty() {
    when(taskRepository.claim(LEASE_DURATION, INITIAL_DELAY)).thenReturn(Optional.empty());
    assertThat(worker.runOnce()).isFalse();
    verifyNoInteractions(fileOperations);
  }

  @Test
  void deletesAndVerifiesOnCallingThread() {
    Thread caller = Thread.currentThread();
    doAnswer(
            ignored -> {
              assertThat(Thread.currentThread()).isSameAs(caller);
              return null;
            })
        .when(fileIO)
        .deletePrefix(PREFIX);

    assertThat(worker.runOnce()).isTrue();

    verify(fileIO).deletePrefix(PREFIX);
    verify(fileIO).listPrefix(PREFIX);
    verify(fileIO).close();
    verify(taskRepository).finish(RESOURCE_ID, LEASE_TOKEN);
    verify(taskRepository, never()).reportFailure(any(), any(), any());
  }

  @Test
  void closesVerificationListing() throws Exception {
    CloseableIterable<FileInfo> listing = mock(CloseableIterable.class);
    when(listing.iterator()).thenReturn(CloseableIterator.empty());
    when(fileIO.listPrefix(PREFIX)).thenReturn(listing);
    assertThat(worker.runOnce()).isTrue();
    verify(listing).close();
  }

  @Test
  void retriesWhenFilesRemain() {
    when(fileIO.listPrefix(PREFIX)).thenReturn(List.of(new FileInfo(PREFIX + "remaining", 1, 1)));
    assertThat(worker.runOnce()).isTrue();
    verifyFailure("Storage cleanup failed: IllegalStateException");
  }

  @Test
  void retriesStorageFailureWithoutItsMessage() {
    doThrow(new IllegalArgumentException("secret")).when(fileIO).deletePrefix(PREFIX);
    assertThat(worker.runOnce()).isTrue();
    verifyFailure("Storage cleanup failed: IllegalArgumentException");
  }

  @Test
  void retriesWhenDeadlineExpiresDuringDeletion() {
    doAnswer(
            ignored -> {
              when(clock.instant()).thenReturn(NOW.plus(ATTEMPT_TIMEOUT));
              return null;
            })
        .when(fileIO)
        .deletePrefix(PREFIX);

    assertThat(worker.runOnce()).isTrue();

    verify(fileIO, never()).listPrefix(PREFIX);
    verify(fileIO).close();
    verifyFailure("Storage cleanup failed: CancellationException");
  }

  @Test
  void checksDeadlineAfterCredentialVending() {
    when(fileOperations.getCleanupFileIO(eq(LOCATION), any()))
        .thenAnswer(
            ignored -> {
              when(clock.instant()).thenReturn(NOW.plus(ATTEMPT_TIMEOUT));
              return fileIO;
            });

    assertThat(worker.runOnce()).isTrue();

    verify(fileIO, never()).deletePrefix(PREFIX);
    verify(fileIO).close();
    verifyFailure("Storage cleanup failed: CancellationException");
  }

  @Test
  void consumesShutdownInterruptBeforeReporting() {
    doAnswer(
            ignored -> {
              Thread.currentThread().interrupt();
              return null;
            })
        .when(fileIO)
        .deletePrefix(PREFIX);
    when(taskRepository.reportFailure(any(), any(), any()))
        .thenAnswer(
            ignored -> {
              assertThat(Thread.currentThread().isInterrupted()).isFalse();
              return true;
            });

    try {
      assertThat(worker.runOnce()).isTrue();
      assertThat(Thread.currentThread().isInterrupted()).isFalse();
      verify(fileIO).close();
      verifyFailure("Storage cleanup failed: CancellationException");
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void stopInterruptsCleanupAndWaitsForReporting() throws Exception {
    CountDownLatch deletionStarted = new CountDownLatch(1);
    when(fileOperations.getCleanupFileIO(eq(LOCATION), any()))
        .thenAnswer(
            invocation -> {
              CooperativeDeadline deadline = invocation.getArgument(1);
              doAnswer(
                      ignored -> {
                        deletionStarted.countDown();
                        try {
                          new CountDownLatch(1).await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                        deadline.checkCancelled();
                        return null;
                      })
                  .when(fileIO)
                  .deletePrefix(PREFIX);
              return fileIO;
            });

    worker.start();
    assertThat(deletionStarted.await(5, TimeUnit.SECONDS)).isTrue();
    worker.stop();

    verify(fileIO).close();
    verifyFailure("Storage cleanup failed: CancellationException");
  }

  @Test
  void finishesSuccessfulCleanupEvenIfBudgetExpiresDuringClose() {
    doAnswer(
            ignored -> {
              when(clock.instant()).thenReturn(NOW.plus(ATTEMPT_TIMEOUT));
              return null;
            })
        .when(fileIO)
        .close();

    assertThat(worker.runOnce()).isTrue();

    verify(taskRepository).finish(RESOURCE_ID, LEASE_TOKEN);
    verify(taskRepository, never()).reportFailure(any(), any(), any());
  }

  @Test
  void validatesTaskBeforeCreatingFileIO() {
    setClaim(ResourceType.TABLE, "s3://bucket/volumes/" + RESOURCE_ID);
    assertThat(worker.runOnce()).isTrue();
    verifyNoInteractions(fileOperations);
    verifyFailure("Storage cleanup failed: IllegalArgumentException");
  }

  @Test
  void processesManagedVolumeCleanupTask() {
    NormalizedURL volumeLocation = NormalizedURL.from("s3://bucket/volumes/" + RESOURCE_ID);
    String volumePrefix = volumeLocation + "/";
    setClaim(ResourceType.VOLUME, volumeLocation.toString());
    when(fileOperations.getCleanupFileIO(eq(volumeLocation), any())).thenReturn(fileIO);
    when(fileIO.listPrefix(volumePrefix)).thenReturn(List.of());

    assertThat(worker.runOnce()).isTrue();

    verify(fileIO).deletePrefix(volumePrefix);
    verify(taskRepository).finish(RESOURCE_ID, LEASE_TOKEN);
    verify(taskRepository, never()).reportFailure(any(), any(), any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"?query", "#fragment"})
  void rejectsQueryAndFragmentLocations(String suffix) {
    setClaim(ResourceType.TABLE, LOCATION + suffix);
    assertThat(worker.runOnce()).isTrue();
    verifyNoInteractions(fileOperations);
    verifyFailure("Storage cleanup failed: IllegalArgumentException");
  }

  @Test
  void rejectsInvalidAttemptTimeoutAndLeaseMargin() {
    assertThatThrownBy(() -> newWorker(Duration.ZERO))
        .hasMessage("Storage cleanup attempt timeout must be at least one millisecond");
    assertThatThrownBy(() -> newWorker(LEASE_DURATION))
        .hasMessage("Storage cleanup lease duration must exceed the attempt timeout");
  }

  @Test
  void startsPollsAndCloses() throws Exception {
    CompletableFuture<Thread> polled = new CompletableFuture<>();
    when(taskRepository.claim(LEASE_DURATION, INITIAL_DELAY))
        .thenAnswer(
            ignored -> {
              polled.complete(Thread.currentThread());
              return Optional.empty();
            });

    worker.start();
    worker.start();

    Thread pollingThread = polled.get(5, TimeUnit.SECONDS);
    assertThat(pollingThread.isDaemon()).isTrue();
    worker.close();
    pollingThread.join(5000);
    assertThat(pollingThread.isAlive()).isFalse();
  }

  @Test
  void stopsAndRestarts() throws Exception {
    CompletableFuture<Thread> firstPoll = new CompletableFuture<>();
    when(taskRepository.claim(LEASE_DURATION, INITIAL_DELAY))
        .thenAnswer(
            ignored -> {
              firstPoll.complete(Thread.currentThread());
              return Optional.empty();
            });

    worker.start();
    Thread firstThread = firstPoll.get(5, TimeUnit.SECONDS);
    worker.stop();
    firstThread.join(5000);
    assertThat(firstThread.isAlive()).isFalse();

    CompletableFuture<Thread> secondPoll = new CompletableFuture<>();
    when(taskRepository.claim(LEASE_DURATION, INITIAL_DELAY))
        .thenAnswer(
            ignored -> {
              secondPoll.complete(Thread.currentThread());
              return Optional.empty();
            });
    worker.start();
    assertThat(secondPoll.get(5, TimeUnit.SECONDS)).isNotSameAs(firstThread);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void stopReturnsWhilePollIsStuckAndPreventsOverlappingRestart(boolean interruptStop)
      throws Exception {
    CompletableFuture<Thread> pollStarted = new CompletableFuture<>();
    CountDownLatch pollInterrupted = new CountDownLatch(1);
    CountDownLatch releasePoll = new CountDownLatch(1);
    when(taskRepository.claim(LEASE_DURATION, INITIAL_DELAY))
        .thenAnswer(
            ignored -> {
              pollStarted.complete(Thread.currentThread());
              try {
                releasePoll.await();
              } catch (InterruptedException e) {
                pollInterrupted.countDown();
                releasePoll.await();
              }
              return Optional.empty();
            });

    FutureTask<Boolean> stopping =
        new FutureTask<>(
            () -> {
              worker.stop();
              worker.close();
              return Thread.currentThread().isInterrupted();
            });
    Thread stopThread = new Thread(stopping);
    try {
      worker.start();
      Thread pollingThread = pollStarted.get(5, TimeUnit.SECONDS);
      stopThread.start();

      assertThat(pollInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
      if (interruptStop) {
        stopThread.interrupt();
      }
      assertThat(stopping.get(10, TimeUnit.SECONDS)).isEqualTo(interruptStop);
      assertThat(pollingThread.isAlive()).isTrue();

      CompletableFuture<Thread> nextPoll = new CompletableFuture<>();
      doAnswer(
              ignored -> {
                nextPoll.complete(Thread.currentThread());
                return Optional.empty();
              })
          .when(taskRepository)
          .claim(LEASE_DURATION, INITIAL_DELAY);
      worker.start();
      assertThatThrownBy(() -> nextPoll.get(100, TimeUnit.MILLISECONDS))
          .isInstanceOf(TimeoutException.class);

      releasePoll.countDown();
      pollingThread.join(5000);
      assertThat(pollingThread.isAlive()).isFalse();
      worker.start();
      assertThat(nextPoll.get(5, TimeUnit.SECONDS)).isNotSameAs(pollingThread);
    } finally {
      releasePoll.countDown();
      stopThread.join(5000);
      Thread pollingThread = pollStarted.getNow(null);
      if (pollingThread != null) {
        pollingThread.join(5000);
      }
    }
  }

  @Test
  void retriesAfterUnexpectedPollFailure() throws Exception {
    CountDownLatch recovered = new CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();
    when(taskRepository.claim(LEASE_DURATION, INITIAL_DELAY))
        .thenAnswer(
            ignored -> {
              if (calls.getAndIncrement() == 0) {
                throw new IllegalStateException("first poll failed");
              }
              recovered.countDown();
              return Optional.empty();
            });

    worker.start();

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
    when(taskRepository.claim(LEASE_DURATION, INITIAL_DELAY))
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
                return Optional.empty();
              } finally {
                active.decrementAndGet();
              }
            });

    worker.start();
    assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
    releaseFirst.countDown();

    assertThat(secondFinished.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(maximumActive).hasValue(1);
  }

  @Test
  void rejectsSubMillisecondIntervals() {
    for (Duration interval : List.of(Duration.ZERO, Duration.ofNanos(1), Duration.ofMillis(-1))) {
      when(serverProperties.getStorageCleanupPollInterval()).thenReturn(interval);
      assertThatThrownBy(() -> newWorker(ATTEMPT_TIMEOUT))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Cleanup poll interval must be at least one millisecond");
    }
    verifyNoInteractions(taskRepository);
  }

  private StorageCleanupWorker newWorker(Duration attemptTimeout) {
    when(serverProperties.getStorageCleanupAttemptTimeout()).thenReturn(attemptTimeout);
    return new StorageCleanupWorker(taskRepository, fileOperations, clock, serverProperties);
  }

  private void setClaim(ResourceType type, String location) {
    when(taskRepository.claim(LEASE_DURATION, INITIAL_DELAY))
        .thenReturn(Optional.of(new Claim(type, RESOURCE_ID, location, LEASE_TOKEN)));
  }

  private void verifyFailure(String error) {
    verify(taskRepository)
        .reportFailure(RESOURCE_ID, LEASE_TOKEN, new CleanupFailureReport(error, RETRY_BACKOFF));
    verify(taskRepository, never()).finish(eq(RESOURCE_ID), eq(LEASE_TOKEN));
  }
}
