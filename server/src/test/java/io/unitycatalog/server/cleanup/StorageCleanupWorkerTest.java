package io.unitycatalog.server.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.persist.StorageCleanupTaskRepository;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository.Claim;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository.CleanupFailureReport;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.utils.NormalizedURL;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private static final Duration LEASE_DURATION = Duration.ofMinutes(5);
  private static final Duration SOCKET_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration ATTEMPT_TIMEOUT = Duration.ofSeconds(20);
  private static final Duration INITIAL_DELAY = Duration.ofHours(1);
  private static final Duration RETRY_BACKOFF = Duration.ofMinutes(1);
  private static final UUID RESOURCE_ID = UUID.randomUUID();
  private static final UUID LEASE_TOKEN = UUID.randomUUID();
  private static final NormalizedURL LOCATION =
      NormalizedURL.from("s3://bucket/tables/" + RESOURCE_ID);
  private static final String PREFIX = LOCATION + "/";

  private final StorageCleanupTaskRepository taskRepository =
      mock(StorageCleanupTaskRepository.class);
  private final FileOperations fileOperations = mock(FileOperations.class);
  private final SupportsPrefixOperations fileIO = mock(SupportsPrefixOperations.class);
  private StorageCleanupWorker worker;

  @BeforeEach
  void setUp() {
    setClaim(ResourceType.TABLE, LOCATION.toString());
    when(fileOperations.getCleanupFileIO(LOCATION, SOCKET_TIMEOUT)).thenReturn(fileIO);
    when(fileIO.listPrefix(PREFIX)).thenReturn(List.of());
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
  void deletesPrefixAndVerifiesItIsEmpty() {
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
  void interruptsAttemptAtTimeout() throws Exception {
    CountDownLatch deletionStarted = new CountDownLatch(1);
    CountDownLatch deletionInterrupted = new CountDownLatch(1);
    blockDeletionUntilInterrupted(deletionStarted, deletionInterrupted);
    worker.close();
    worker = newWorker(Duration.ofMillis(200));
    FutureTask<Boolean> result = new FutureTask<>(worker::runOnce);
    Thread caller = new Thread(result);
    try {
      caller.start();
      assertThat(deletionStarted.await(5, TimeUnit.SECONDS)).isTrue();

      assertThat(result.get(5, TimeUnit.SECONDS)).isTrue();
      assertThat(deletionInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
      verifyFailureAfterCancellation();
    } finally {
      caller.interrupt();
    }
  }

  @Test
  void callerInterruptCancelsAttemptAndRestoresInterrupt() throws Exception {
    CountDownLatch deletionStarted = new CountDownLatch(1);
    CountDownLatch deletionInterrupted = new CountDownLatch(1);
    blockDeletionUntilInterrupted(deletionStarted, deletionInterrupted);
    AtomicBoolean interruptRestored = new AtomicBoolean();
    FutureTask<Boolean> result =
        new FutureTask<>(
            () -> {
              boolean processed = worker.runOnce();
              interruptRestored.set(Thread.currentThread().isInterrupted());
              return processed;
            });
    Thread caller = new Thread(result);
    try {
      caller.start();
      assertThat(deletionStarted.await(5, TimeUnit.SECONDS)).isTrue();
      caller.interrupt();

      assertThat(result.get(5, TimeUnit.SECONDS)).isTrue();
      assertThat(interruptRestored).isTrue();
      assertThat(deletionInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
      verifyFailureAfterCancellation();
    } finally {
      caller.interrupt();
    }
  }

  @Test
  void doesNotQueueAnotherAttemptWhileTimedOutAttemptIsStopping() throws Exception {
    CountDownLatch deletionStarted = new CountDownLatch(1);
    CountDownLatch releaseDeletion = new CountDownLatch(1);
    doAnswer(
            ignored -> {
              deletionStarted.countDown();
              while (true) {
                try {
                  releaseDeletion.await();
                  throw new CancellationException();
                } catch (InterruptedException ignoredInterrupt) {
                  // Keep the attempt active until the test releases it.
                }
              }
            })
        .when(fileIO)
        .deletePrefix(PREFIX);
    worker.close();
    worker = newWorker(Duration.ofMillis(200));
    try {
      assertThat(worker.runOnce()).isTrue();
      assertThat(deletionStarted.await(5, TimeUnit.SECONDS)).isTrue();

      assertThat(worker.runOnce()).isFalse();
      verify(taskRepository, times(1)).claim(LEASE_DURATION, INITIAL_DELAY);

      releaseDeletion.countDown();
      verifyFailureAfterCancellation();
    } finally {
      releaseDeletion.countDown();
    }
  }

  @Test
  void validatesTaskBeforeCreatingFileIO() {
    setClaim(ResourceType.TABLE, "s3://bucket/volumes/" + RESOURCE_ID);

    assertThat(worker.runOnce()).isTrue();

    verifyNoInteractions(fileOperations);
    verifyFailure("Storage cleanup failed: IllegalArgumentException");
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
        .hasMessage(
            "Storage cleanup lease duration must exceed the attempt timeout plus socket timeout");
  }

  private StorageCleanupWorker newWorker(Duration attemptTimeout) {
    return new StorageCleanupWorker(
        taskRepository,
        fileOperations,
        LEASE_DURATION,
        SOCKET_TIMEOUT,
        attemptTimeout,
        INITIAL_DELAY,
        RETRY_BACKOFF);
  }

  private void blockDeletionUntilInterrupted(
      CountDownLatch deletionStarted, CountDownLatch deletionInterrupted) {
    doAnswer(
            ignored -> {
              deletionStarted.countDown();
              try {
                new CountDownLatch(1).await();
                return null;
              } catch (InterruptedException exception) {
                deletionInterrupted.countDown();
                throw new CancellationException();
              }
            })
        .when(fileIO)
        .deletePrefix(PREFIX);
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

  private void verifyFailureAfterCancellation() {
    verify(taskRepository, timeout(5000))
        .reportFailure(
            RESOURCE_ID,
            LEASE_TOKEN,
            new CleanupFailureReport(
                "Storage cleanup failed: CancellationException", RETRY_BACKOFF));
    verify(taskRepository, never()).finish(eq(RESOURCE_ID), eq(LEASE_TOKEN));
  }
}
