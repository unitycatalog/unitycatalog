package io.unitycatalog.server.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.persist.StorageCleanupTaskRepository;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class StorageCleanupWorkerTest {
  private static final Instant CLAIMED_AT = Instant.parse("2026-09-12T12:00:00Z");
  private static final Instant FINISHED_AT = CLAIMED_AT.plusSeconds(30);
  private static final Duration LEASE_DURATION = Duration.ofMinutes(5);
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration RETRY_BACKOFF = Duration.ofMinutes(1);
  private static final UUID RESOURCE_ID = UUID.randomUUID();
  private static final UUID LEASE_TOKEN = UUID.randomUUID();

  private final StorageCleanupTaskRepository taskRepository =
      mock(StorageCleanupTaskRepository.class);
  private final StorageCleanupAdapterFactory adapterFactory =
      mock(StorageCleanupAdapterFactory.class);
  private final StorageCleanupAttempt cleanupAttempt = mock(StorageCleanupAttempt.class);
  private final StorageCleanupAdapter adapter = mock(StorageCleanupAdapter.class);
  private final Clock clock = mock(Clock.class);
  private final StorageCleanupTaskDAO task =
      StorageCleanupTaskDAO.builder()
          .resourceType(ResourceType.TABLE)
          .resourceId(RESOURCE_ID)
          .storageLocation("s3://bucket/tables/" + RESOURCE_ID)
          .cleanableAt(CLAIMED_AT)
          .build();

  @BeforeEach
  void setUpClaimedTask() {
    when(clock.instant()).thenReturn(CLAIMED_AT, FINISHED_AT);
    when(taskRepository.findReadyTask(CLAIMED_AT)).thenReturn(Optional.of(task));
    when(taskRepository.claimTask(RESOURCE_ID, CLAIMED_AT, LEASE_DURATION))
        .thenReturn(Optional.of(LEASE_TOKEN));
    when(adapterFactory.create(task, REQUEST_TIMEOUT)).thenReturn(adapter);
  }

  @Test
  void returnsWithoutWorkWhenNoTaskIsReady() {
    when(taskRepository.findReadyTask(CLAIMED_AT)).thenReturn(Optional.empty());

    assertThat(worker().runOnce()).isFalse();

    verify(taskRepository).findReadyTask(CLAIMED_AT);
    verifyNoInteractions(adapterFactory, cleanupAttempt);
  }

  @Test
  void returnsWithoutWorkWhenCompetingClaimWins() {
    when(taskRepository.claimTask(RESOURCE_ID, CLAIMED_AT, LEASE_DURATION))
        .thenReturn(Optional.empty());

    assertThat(worker().runOnce()).isFalse();

    verifyNoInteractions(adapterFactory, cleanupAttempt);
  }

  @Test
  void completesTaskUsingCurrentEndTime() {
    when(cleanupAttempt.run(adapter)).thenReturn(StorageCleanupAttempt.Result.COMPLETE);

    assertThat(worker().runOnce()).isTrue();

    verify(taskRepository).completeTask(RESOURCE_ID, LEASE_TOKEN, FINISHED_AT);
    verify(taskRepository, never()).releaseIncomplete(RESOURCE_ID, LEASE_TOKEN, FINISHED_AT);
  }

  @Test
  void releasesPartialTaskUsingCurrentEndTime() {
    when(cleanupAttempt.run(adapter)).thenReturn(StorageCleanupAttempt.Result.PARTIAL);

    assertThat(worker().runOnce()).isTrue();

    verify(taskRepository).releaseIncomplete(RESOURCE_ID, LEASE_TOKEN, FINISHED_AT);
    verify(taskRepository, never()).completeTask(RESOURCE_ID, LEASE_TOKEN, FINISHED_AT);
  }

  @Test
  void recordsCredentialFailure() {
    when(adapterFactory.create(task, REQUEST_TIMEOUT))
        .thenThrow(new IllegalStateException("credential failed"));

    assertThat(worker().runOnce()).isTrue();

    verifyFailure("Storage cleanup failed: IllegalStateException");
  }

  @Test
  void recordsStorageFailure() {
    when(cleanupAttempt.run(adapter)).thenThrow(new IllegalArgumentException("storage failed"));

    assertThat(worker().runOnce()).isTrue();

    verifyFailure("Storage cleanup failed: IllegalArgumentException");
  }

  @Test
  void doesNotRetryWhenAStaleLeaseCannotSave() {
    when(cleanupAttempt.run(adapter)).thenReturn(StorageCleanupAttempt.Result.COMPLETE);
    when(taskRepository.completeTask(RESOURCE_ID, LEASE_TOKEN, FINISHED_AT)).thenReturn(false);

    assertThat(worker().runOnce()).isTrue();

    verify(taskRepository).completeTask(RESOURCE_ID, LEASE_TOKEN, FINISHED_AT);
    verify(taskRepository, never())
        .recordFailure(eq(RESOURCE_ID), eq(LEASE_TOKEN), any(), eq(RETRY_BACKOFF), anyString());
  }

  @Test
  void storesBoundedErrorWithoutTheExceptionMessage() {
    when(adapterFactory.create(task, REQUEST_TIMEOUT))
        .thenThrow(new RuntimeException("secret\n" + "x".repeat(3000)) {});
    ArgumentCaptor<String> error = ArgumentCaptor.forClass(String.class);

    assertThat(worker().runOnce()).isTrue();

    verify(taskRepository)
        .recordFailure(
            eq(RESOURCE_ID), eq(LEASE_TOKEN), eq(FINISHED_AT), eq(RETRY_BACKOFF), error.capture());
    assertThat(error.getValue()).isEqualTo("Storage cleanup failed").doesNotContain("secret");
    assertThat(error.getValue().length()).isLessThanOrEqualTo(2048);
  }

  private StorageCleanupWorker worker() {
    return new StorageCleanupWorker(
        taskRepository,
        adapterFactory,
        cleanupAttempt,
        clock,
        LEASE_DURATION,
        REQUEST_TIMEOUT,
        RETRY_BACKOFF);
  }

  private void verifyFailure(String error) {
    verify(taskRepository)
        .recordFailure(RESOURCE_ID, LEASE_TOKEN, FINISHED_AT, RETRY_BACKOFF, error);
    verify(taskRepository, never()).completeTask(RESOURCE_ID, LEASE_TOKEN, FINISHED_AT);
    verify(taskRepository, never()).releaseIncomplete(RESOURCE_ID, LEASE_TOKEN, FINISHED_AT);
  }
}
