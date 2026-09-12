package io.unitycatalog.server.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.utils.NormalizedURL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

class StorageCleanupAttemptTest {
  private static final int BATCH_SIZE = 2;
  private static final Duration TIME_SLICE = Duration.ofNanos(100);
  private static final Duration REQUEST_TIMEOUT = Duration.ofNanos(30);

  @TempDir Path tempDir;

  @Test
  void deletesBatchesUntilStorageIsEmpty() {
    StorageCleanupAdapter adapter = mock(StorageCleanupAdapter.class);
    when(adapter.listBatch(BATCH_SIZE))
        .thenReturn(List.of("a", "b"))
        .thenReturn(List.of("c"))
        .thenReturn(List.of());

    StorageCleanupAttempt.Result result = attempt(() -> 0L).run(adapter);

    assertThat(result).isEqualTo(StorageCleanupAttempt.Result.COMPLETE);
    InOrder calls = inOrder(adapter);
    calls.verify(adapter).listBatch(BATCH_SIZE);
    calls.verify(adapter).deleteBatch(List.of("a", "b"));
    calls.verify(adapter).listBatch(BATCH_SIZE);
    calls.verify(adapter).deleteBatch(List.of("c"));
    calls.verify(adapter).listBatch(BATCH_SIZE);
    calls.verify(adapter).close();
    verifyNoMoreInteractions(adapter);
  }

  @Test
  void doesNotListWithoutTimeForListAndDelete() {
    StorageCleanupAdapter adapter = mock(StorageCleanupAdapter.class);

    assertThat(attempt(times(0, 41)).run(adapter)).isEqualTo(StorageCleanupAttempt.Result.PARTIAL);
    verify(adapter, never()).listBatch(BATCH_SIZE);
    verify(adapter).close();
  }

  @Test
  void doesNotDeleteWithoutTimeForOneRequest() {
    StorageCleanupAdapter adapter = mock(StorageCleanupAdapter.class);
    when(adapter.listBatch(BATCH_SIZE)).thenReturn(List.of("a"));

    assertThat(attempt(times(0, 0, 71)).run(adapter))
        .isEqualTo(StorageCleanupAttempt.Result.PARTIAL);
    verify(adapter, never()).deleteBatch(List.of("a"));
    verify(adapter).close();
  }

  @Test
  void successfulDeleteDoesNotExtendTheTimeSlice() {
    StorageCleanupAdapter adapter = mock(StorageCleanupAdapter.class);
    when(adapter.listBatch(BATCH_SIZE)).thenReturn(List.of("a"));

    assertThat(attempt(times(0, 0, 0, 41)).run(adapter))
        .isEqualTo(StorageCleanupAttempt.Result.PARTIAL);
    verify(adapter).deleteBatch(List.of("a"));
    verify(adapter).close();
  }

  @Test
  void propagatesStorageFailureAndClosesAdapter() {
    StorageCleanupAdapter adapter = mock(StorageCleanupAdapter.class);
    when(adapter.listBatch(BATCH_SIZE)).thenReturn(List.of("a"));
    doThrow(new IllegalStateException("delete failed")).when(adapter).deleteBatch(List.of("a"));

    assertThatThrownBy(() -> attempt(() -> 0L).run(adapter))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("delete failed");
    verify(adapter).close();
  }

  @Test
  void propagatesListingFailureAndClosesAdapter() {
    StorageCleanupAdapter adapter = mock(StorageCleanupAdapter.class);
    when(adapter.listBatch(BATCH_SIZE)).thenThrow(new IllegalStateException("list failed"));

    assertThatThrownBy(() -> attempt(() -> 0L).run(adapter))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("list failed");
    verify(adapter).close();
  }

  @Test
  void rejectsInvalidLimits() {
    assertThatThrownBy(() -> new StorageCleanupAttempt(0, TIME_SLICE, REQUEST_TIMEOUT, () -> 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cleanup batch size must be positive");
    assertThatThrownBy(
            () -> new StorageCleanupAttempt(BATCH_SIZE, Duration.ZERO, REQUEST_TIMEOUT, () -> 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cleanup time slice must be positive");
    assertThatThrownBy(
            () -> new StorageCleanupAttempt(BATCH_SIZE, TIME_SLICE, Duration.ofNanos(51), () -> 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cleanup time slice must allow one list and delete request");
  }

  @Test
  void localAdapterDeletesBoundedBatches() throws Exception {
    Files.createDirectories(tempDir.resolve("nested"));
    Files.writeString(tempDir.resolve("a"), "a");
    Files.writeString(tempDir.resolve("nested/b"), "b");
    Files.writeString(tempDir.resolve("nested/c"), "c");

    LocalStorageCleanupAdapter adapter =
        new LocalStorageCleanupAdapter(NormalizedURL.from(tempDir.toUri().toString()));
    assertThat(adapter.listBatch(BATCH_SIZE)).hasSize(BATCH_SIZE);

    StorageCleanupAttempt.Result result = attempt(() -> 0L).run(adapter);

    assertThat(result).isEqualTo(StorageCleanupAttempt.Result.COMPLETE);
    assertThat(tempDir.resolve("a")).doesNotExist();
    assertThat(tempDir.resolve("nested/b")).doesNotExist();
    assertThat(tempDir.resolve("nested/c")).doesNotExist();
  }

  @Test
  void localAdapterTreatsMissingStorageAndFilesAsDeleted() throws Exception {
    LocalStorageCleanupAdapter missing =
        new LocalStorageCleanupAdapter(
            NormalizedURL.from(tempDir.resolve("missing").toUri().toString()));
    assertThat(attempt(() -> 0L).run(missing)).isEqualTo(StorageCleanupAttempt.Result.COMPLETE);

    Path file = tempDir.resolve("file");
    Files.writeString(file, "data");
    LocalStorageCleanupAdapter adapter =
        new LocalStorageCleanupAdapter(NormalizedURL.from(tempDir.toUri().toString()));
    List<String> batch = adapter.listBatch(1);
    Files.delete(file);
    assertThatCode(() -> adapter.deleteBatch(batch)).doesNotThrowAnyException();
  }

  @Test
  void localAdapterRejectsCloudLocation() {
    assertThatThrownBy(() -> new LocalStorageCleanupAdapter(NormalizedURL.from("s3://bucket/path")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Local cleanup requires a local storage location");
  }

  @Test
  void localAdapterRejectsInvalidBatchLimits() {
    LocalStorageCleanupAdapter adapter =
        new LocalStorageCleanupAdapter(NormalizedURL.from(tempDir.toUri().toString()));

    for (int maxFiles : List.of(0, -1)) {
      assertThatThrownBy(() -> adapter.listBatch(maxFiles))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Maximum files must be positive");
    }
  }

  private static StorageCleanupAttempt attempt(LongSupplier nanoTime) {
    return new StorageCleanupAttempt(BATCH_SIZE, TIME_SLICE, REQUEST_TIMEOUT, nanoTime);
  }

  private static LongSupplier times(long... values) {
    AtomicInteger next = new AtomicInteger();
    return () -> values[Math.min(next.getAndIncrement(), values.length - 1)];
  }
}
