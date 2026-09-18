package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import io.unitycatalog.server.utils.CooperativeDeadline;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.stream.IntStream;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class InterruptiblePrefixOperationsTest {
  private static final String PREFIX = "s3://bucket/tables/id/";
  private static final String LOCATION = "s3://bucket/tables/id";

  private final DelegateFileIO fileIO = mock(DelegateFileIO.class);
  private final Clock clock = mock(Clock.class);
  private final Instant expiresAt = Instant.parse("2026-01-01T00:00:20Z");
  private final CooperativeDeadline deadline = new CooperativeDeadline(clock, expiresAt);
  private final InterruptiblePrefixOperations operations =
      new InterruptiblePrefixOperations(fileIO, PREFIX, deadline);

  @BeforeEach
  void setUp() {
    when(clock.instant()).thenReturn(expiresAt.minusSeconds(20));
  }

  @Test
  void deletesOneListingInBatchesOfAtMostOneThousand() {
    List<FileInfo> listing =
        IntStream.range(0, 1001).mapToObj(index -> new FileInfo(PREFIX + index, 1, 1)).toList();
    List<String> firstBatch = listing.subList(0, 1000).stream().map(FileInfo::location).toList();
    List<String> secondBatch = List.of(listing.get(1000).location());
    when(fileIO.listPrefix(PREFIX)).thenReturn(listing);

    operations.deletePrefix(PREFIX);

    verify(fileIO).deleteFiles(firstBatch);
    verify(fileIO).deleteFiles(secondBatch);
    verify(fileIO).listPrefix(PREFIX);
    verify(fileIO, never()).deleteFile(any(String.class));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void deletesDirectoryMarkerOnlyWhenListed(boolean hasDirectoryMarker) {
    when(fileIO.listPrefix(PREFIX))
        .thenReturn(hasDirectoryMarker ? List.of(new FileInfo(PREFIX, 0, 1)) : List.of());

    operations.deletePrefix(PREFIX);

    if (hasDirectoryMarker) {
      verify(fileIO).deleteFiles(List.of(PREFIX));
    } else {
      verify(fileIO, never()).deleteFiles(any());
    }
    verify(fileIO, never()).deleteFile(any(String.class));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void gcsDeletionUsesObjectKeysAndChecksDeadlineBetweenBatches(boolean expireDeadline) {
    String location = "gs://bucket/tables/id";
    String prefix = location + "/";
    Storage storage = mock(Storage.class);
    Page<Blob> page = mock(Page.class);
    List<Blob> blobs =
        IntStream.range(0, 1001)
            .mapToObj(
                index -> {
                  Blob blob = mock(Blob.class);
                  when(blob.getBucket()).thenReturn("bucket");
                  when(blob.getName()).thenReturn("tables/id/" + index);
                  when(blob.getSize()).thenReturn(1L);
                  return blob;
                })
            .toList();
    when(storage.list("bucket", Storage.BlobListOption.prefix("tables/id/"))).thenReturn(page);
    when(page.streamAll()).thenAnswer(ignored -> blobs.stream());
    List<BlobId> deleted = new ArrayList<>();
    when(storage.delete(anyIterable()))
        .thenAnswer(
            invocation -> {
              Iterable<BlobId> batch = invocation.getArgument(0);
              batch.forEach(deleted::add);
              if (expireDeadline) {
                when(clock.instant()).thenReturn(expiresAt);
              }
              return List.of();
            });
    try (InterruptiblePrefixOperations gcs =
        new InterruptiblePrefixOperations(new GCSFileIO(() -> storage), prefix, deadline)) {
      if (expireDeadline) {
        assertThatThrownBy(() -> gcs.deletePrefix(prefix))
            .isInstanceOf(CancellationException.class);
      } else {
        gcs.deletePrefix(prefix);
      }
    }
    verify(storage, never()).delete(any(BlobId.class));
    assertThat(deleted)
        .containsExactlyElementsOf(
            IntStream.range(0, expireDeadline ? 1000 : 1001)
                .mapToObj(index -> BlobId.of("bucket", "tables/id/" + index))
                .toList());
  }

  @Test
  void clearsInterruptBeforeListingSoThreadCanBeReused() {
    Thread.currentThread().interrupt();
    try {
      assertThatThrownBy(() -> operations.deletePrefix(PREFIX))
          .isInstanceOf(CancellationException.class)
          .hasMessage("Operation interrupted");
      verifyNoInteractions(fileIO);
      assertThat(Thread.currentThread().isInterrupted()).isFalse();

      when(fileIO.listPrefix(PREFIX)).thenReturn(List.of());
      operations.deletePrefix(PREFIX);
      verify(fileIO).listPrefix(PREFIX);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void stopsBeforeDeletingWhenListingWasInterrupted() {
    when(fileIO.listPrefix(PREFIX))
        .thenReturn(
            () ->
                List.of(new FileInfo(PREFIX + "file", 1, 1)).stream()
                    .peek(ignored -> Thread.currentThread().interrupt())
                    .iterator());

    try {
      assertThatThrownBy(() -> operations.deletePrefix(PREFIX))
          .isInstanceOf(CancellationException.class);
      assertThat(Thread.currentThread().isInterrupted()).isFalse();
    } finally {
      Thread.interrupted();
    }

    verify(fileIO, never()).deleteFiles(any());
  }

  @Test
  void rejectsPrefixesOutsideBoundLocation() {
    assertThatThrownBy(() -> new InterruptiblePrefixOperations(fileIO, null, deadline))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("boundPrefix");
    assertThatThrownBy(
            () -> new InterruptiblePrefixOperations(fileIO, "s3://bucket/tables/id", deadline))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Bound prefix must end with '/'");
    for (String prefix : List.of("s3://bucket/tables/id", "s3://bucket/tables/id2/")) {
      assertThatThrownBy(() -> operations.listPrefix(prefix))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Prefix does not match bound prefix");
      assertThatThrownBy(() -> operations.deletePrefix(prefix))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Prefix does not match bound prefix");
    }
    verifyNoInteractions(fileIO);
  }

  @Test
  void rejectsOperationsOutsideBoundPrefix() {
    for (Runnable operation :
        List.<Runnable>of(
            () -> operations.newInputFile(PREFIX + "file"),
            () -> operations.newOutputFile(PREFIX + "file"),
            () -> operations.deleteFile(PREFIX + "file"),
            () -> operations.initialize(Map.of()))) {
      assertThatThrownBy(operation::run)
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessage("Only bound prefix operations are supported");
    }
    verifyNoInteractions(fileIO);
  }

  @Test
  void stopsAfterOneBatchWhenDeadlineExpires() {
    List<FileInfo> listing =
        IntStream.range(0, 1001).mapToObj(index -> new FileInfo(PREFIX + index, 1, 1)).toList();
    List<String> firstBatch = listing.subList(0, 1000).stream().map(FileInfo::location).toList();
    when(fileIO.listPrefix(PREFIX)).thenReturn(listing);
    doAnswer(
            ignored -> {
              when(clock.instant()).thenReturn(expiresAt);
              return null;
            })
        .when(fileIO)
        .deleteFiles(firstBatch);

    assertThatThrownBy(() -> operations.deletePrefix(PREFIX))
        .isInstanceOf(CancellationException.class)
        .hasMessage("Operation deadline reached");

    verify(fileIO).deleteFiles(firstBatch);
    verify(fileIO, never()).deleteFiles(List.of(listing.get(1000).location()));
    verify(fileIO, never()).deleteFile(LOCATION);
  }

  @Test
  void rejectsExpiredListingBeforeStorageAccess() {
    when(clock.instant()).thenReturn(expiresAt);
    assertThatThrownBy(() -> operations.listPrefix(PREFIX))
        .isInstanceOf(CancellationException.class);
    verifyNoInteractions(fileIO);
  }

  @Test
  void closesDelegate() {
    operations.close();

    verify(fileIO).close();
  }
}
