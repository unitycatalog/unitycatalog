package io.unitycatalog.server.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.HttpStorageOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageException;
import io.unitycatalog.server.utils.NormalizedURL;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class GCSStorageCleanupAdapterTest {
  private static final String BUCKET = "bucket";
  private static final String KEY = "root/table";
  private static final String PREFIX = KEY + "/";
  private static final String LOCATION = "gs://" + BUCKET + "/" + KEY;

  private Storage storage;
  private StorageBatch batch;
  private GCSStorageCleanupAdapter adapter;

  @BeforeEach
  void setUp() {
    storage = mock(Storage.class);
    batch = mock(StorageBatch.class);
    adapter = new GCSStorageCleanupAdapter(storage, NormalizedURL.from(LOCATION));
  }

  @Test
  void listsOnlyTheCurrentBoundedPage() {
    Page<Blob> page = page(PREFIX + "a", PREFIX + "b", PREFIX + "c");
    when(storage.list(eq(BUCKET), any(BlobListOption[].class))).thenReturn(page);

    assertThat(adapter.listBatch(2)).containsExactly(LOCATION + "/a", LOCATION + "/b");

    assertThat(captureListOptions())
        .containsExactly(BlobListOption.prefix(PREFIX), BlobListOption.pageSize(2));
    verify(page).getValues();
    verify(page, never()).iterateAll();
    verify(page, never()).getNextPage();
    verify(page, never()).getNextPageToken();
  }

  @Test
  void capsAndValidatesListBatchSize() {
    Page<Blob> page = page(PREFIX + "a");
    when(storage.list(eq(BUCKET), any(BlobListOption[].class))).thenReturn(page);

    assertThat(adapter.listBatch(101)).containsExactly(LOCATION + "/a");
    assertThat(captureListOptions())
        .containsExactly(BlobListOption.prefix(PREFIX), BlobListOption.pageSize(100));
    for (int limit : List.of(0, -1)) {
      assertThatThrownBy(() -> adapter.listBatch(limit))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Cleanup batch size must be positive");
    }
    verify(storage).list(eq(BUCKET), any(BlobListOption[].class));
  }

  @Test
  void deletesDescendantsWithOneStorageBatch() {
    StorageBatchResult<Boolean> result = deleteResult(true);

    adapter.deleteBatch(List.of(LOCATION + "/a", LOCATION + "/nested/b"));

    ArgumentCaptor<BlobId> ids = ArgumentCaptor.forClass(BlobId.class);
    verify(batch, times(2)).delete(ids.capture());
    assertThat(ids.getAllValues())
        .containsExactly(BlobId.of(BUCKET, PREFIX + "a"), BlobId.of(BUCKET, PREFIX + "nested/b"));
    verify(batch).submit();
    verify(result, times(2)).get();
  }

  @Test
  void deletesExactKeyAfterDescendants() {
    Page<Blob> descendants = page(PREFIX + "child");
    Page<Blob> empty = page();
    when(storage.list(eq(BUCKET), any(BlobListOption[].class)))
        .thenReturn(descendants)
        .thenReturn(empty);
    deleteResult(true);

    StorageCleanupAttempt.Result result =
        new StorageCleanupAttempt(10, Duration.ofSeconds(30), Duration.ofSeconds(3)).run(adapter);

    assertThat(result).isEqualTo(StorageCleanupAttempt.Result.COMPLETE);
    ArgumentCaptor<BlobId> ids = ArgumentCaptor.forClass(BlobId.class);
    verify(batch, times(2)).delete(ids.capture());
    assertThat(ids.getAllValues())
        .containsExactly(BlobId.of(BUCKET, PREFIX + "child"), BlobId.of(BUCKET, KEY));
    verify(storage, times(2)).list(eq(BUCKET), any(BlobListOption[].class));
  }

  @Test
  void treatsMissingExactKeyAsDeleted() {
    Page<Blob> empty = page();
    when(storage.list(eq(BUCKET), any(BlobListOption[].class))).thenReturn(empty);
    deleteResult(false);

    assertThat(adapter.listBatch(10)).containsExactly(LOCATION);
    assertThatCode(() -> adapter.deleteBatch(List.of(LOCATION))).doesNotThrowAnyException();
    assertThat(adapter.listBatch(10)).isEmpty();

    verify(batch).delete(BlobId.of(BUCKET, KEY));
    verify(storage).list(eq(BUCKET), any(BlobListOption[].class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void newAttemptRepeatsExactKeyDeleteAfterFailure() {
    Page<Blob> empty = page();
    when(storage.list(eq(BUCKET), any(BlobListOption[].class))).thenReturn(empty);
    StorageBatchResult<Boolean> failed = mock(StorageBatchResult.class);
    StorageBatchResult<Boolean> retried = mock(StorageBatchResult.class);
    when(storage.batch()).thenReturn(batch);
    when(batch.delete(any(BlobId.class))).thenReturn(failed).thenReturn(retried);
    StorageException failure = new StorageException(500, "failed");
    when(failed.get()).thenThrow(failure);
    when(retried.get()).thenReturn(true);

    List<String> firstBatch = adapter.listBatch(10);
    assertThatThrownBy(() -> adapter.deleteBatch(firstBatch)).isSameAs(failure);
    GCSStorageCleanupAdapter retryAdapter =
        new GCSStorageCleanupAdapter(storage, NormalizedURL.from(LOCATION));
    List<String> retryBatch = retryAdapter.listBatch(10);
    retryAdapter.deleteBatch(retryBatch);

    assertThat(firstBatch).containsExactly(LOCATION);
    assertThat(retryBatch).containsExactly(LOCATION);
    verify(batch, times(2)).delete(BlobId.of(BUCKET, KEY));
  }

  @Test
  void rejectsDeletionOutsideExactPrefix() {
    for (String location :
        List.of("gs://bucket/root/table-other/file", "gs://other/root/table/file")) {
      assertThatThrownBy(() -> adapter.deleteBatch(List.of(location)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("GCS cleanup cannot delete outside its task prefix");
    }
    assertThatThrownBy(() -> adapter.deleteBatch(Collections.nCopies(101, LOCATION + "/file")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("GCS cleanup batch cannot exceed 100 objects");
    verify(storage, never()).batch();
  }

  @Test
  @SuppressWarnings("unchecked")
  void rejectsListedObjectOutsideExactPrefix() {
    Page<Blob> page = mock(Page.class);
    Blob outside = blob("other", PREFIX + "file");
    when(page.getValues()).thenReturn(List.of(outside));
    when(storage.list(eq(BUCKET), any(BlobListOption[].class))).thenReturn(page);

    assertThatThrownBy(() -> adapter.listBatch(1))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("GCS listed an object outside the cleanup prefix");
  }

  @Test
  void propagatesStorageFailures() {
    StorageException listFailure = new StorageException(500, "list failed");
    when(storage.list(eq(BUCKET), any(BlobListOption[].class))).thenThrow(listFailure);
    assertThatThrownBy(() -> adapter.listBatch(1)).isSameAs(listFailure);

    StorageBatchResult<Boolean> result = deleteResult(true);
    StorageException deleteFailure = new StorageException(500, "delete failed");
    when(result.get()).thenThrow(deleteFailure);
    assertThatThrownBy(() -> adapter.deleteBatch(List.of(LOCATION))).isSameAs(deleteFailure);
  }

  @Test
  void configuresRetryAndHttpTimeouts() {
    Credentials credentials = mock(Credentials.class);

    HttpStorageOptions options =
        GCSStorageCleanupAdapter.storageOptions(credentials, Duration.ofSeconds(3));

    assertThat(options.getCredentials()).isSameAs(credentials);
    RetrySettings retry = options.getRetrySettings();
    org.threeten.bp.Duration timeout = org.threeten.bp.Duration.ofSeconds(3);
    assertThat(retry.getInitialRpcTimeout()).isEqualTo(timeout);
    assertThat(retry.getMaxRpcTimeout()).isEqualTo(timeout);
    assertThat(retry.getRpcTimeoutMultiplier()).isEqualTo(1.0);
    assertThat(retry.getTotalTimeout()).isEqualTo(timeout);
    HttpTransportOptions transport = (HttpTransportOptions) options.getTransportOptions();
    assertThat(transport.getConnectTimeout()).isEqualTo(3000);
    assertThat(transport.getReadTimeout()).isEqualTo(3000);
  }

  @Test
  void rejectsInvalidTimeouts() {
    Credentials credentials = mock(Credentials.class);
    for (Duration timeout :
        List.of(
            Duration.ZERO,
            Duration.ofMillis(-1),
            Duration.ofNanos(999_999),
            Duration.ofMillis((long) Integer.MAX_VALUE + 1))) {
      assertThatThrownBy(() -> GCSStorageCleanupAdapter.storageOptions(credentials, timeout))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("GCS request timeout must be between 1 and 2147483647 milliseconds");
    }
    assertThatThrownBy(
            () ->
                GCSStorageCleanupAdapter.storageOptions(
                    credentials, Duration.ofSeconds(Long.MAX_VALUE)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("GCS request timeout is too large");
  }

  @Test
  void rejectsInvalidLocationAndDoesNotCloseStorage() throws Exception {
    assertThatThrownBy(
            () -> new GCSStorageCleanupAdapter(storage, NormalizedURL.from("s3://bucket/path")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("GCS cleanup requires a GCS storage location");
    assertThatThrownBy(
            () -> new GCSStorageCleanupAdapter(storage, NormalizedURL.from("gs://bucket")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("GCS cleanup requires a bucket and object prefix");

    adapter.close();
    verify(storage, never()).close();
  }

  private BlobListOption[] captureListOptions() {
    ArgumentCaptor<BlobListOption[]> options = ArgumentCaptor.forClass(BlobListOption[].class);
    verify(storage).list(eq(BUCKET), options.capture());
    return options.getValue();
  }

  @SuppressWarnings("unchecked")
  private Page<Blob> page(String... names) {
    List<Blob> blobs = java.util.Arrays.stream(names).map(this::blob).toList();
    Page<Blob> page = mock(Page.class);
    when(page.getValues()).thenReturn(blobs);
    return page;
  }

  private Blob blob(String name) {
    return blob(BUCKET, name);
  }

  private Blob blob(String bucket, String name) {
    Blob blob = mock(Blob.class);
    when(blob.getBlobId()).thenReturn(BlobId.of(bucket, name));
    return blob;
  }

  @SuppressWarnings("unchecked")
  private StorageBatchResult<Boolean> deleteResult(boolean deleted) {
    StorageBatchResult<Boolean> result = mock(StorageBatchResult.class);
    when(storage.batch()).thenReturn(batch);
    when(batch.delete(any(BlobId.class))).thenReturn(result);
    when(result.get()).thenReturn(deleted);
    return result;
  }
}
