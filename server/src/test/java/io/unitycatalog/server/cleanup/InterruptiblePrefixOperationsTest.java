package io.unitycatalog.server.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.stream.IntStream;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.junit.jupiter.api.Test;

class InterruptiblePrefixOperationsTest {
  private static final String PREFIX = "s3://bucket/tables/id/";
  private static final String LOCATION = "s3://bucket/tables/id";

  private final DelegateFileIO fileIO = mock(DelegateFileIO.class);
  private final InterruptiblePrefixOperations operations =
      new InterruptiblePrefixOperations(fileIO, PREFIX);

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
    verify(fileIO).deleteFile(LOCATION);
  }

  @Test
  void clearsInterruptBeforeListingSoThreadCanBeReused() {
    Thread.currentThread().interrupt();
    try {
      assertThatThrownBy(() -> operations.deletePrefix(PREFIX))
          .isInstanceOf(CancellationException.class)
          .hasMessage("Prefix deletion interrupted");
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
  void rejectsPrefixesOutsideCleanupLocation() {
    assertThatThrownBy(() -> new InterruptiblePrefixOperations(fileIO, "s3://bucket/tables/id"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cleanup prefix must end with '/'");
    for (String prefix : List.of("s3://bucket/tables/id", "s3://bucket/tables/id2/")) {
      assertThatThrownBy(() -> operations.listPrefix(prefix))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Prefix does not match cleanup location");
      assertThatThrownBy(() -> operations.deletePrefix(prefix))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Prefix does not match cleanup location");
    }
    verifyNoInteractions(fileIO);
  }

  @Test
  void rejectsOperationsOutsideBoundPrefixCleanup() {
    for (Runnable operation :
        List.<Runnable>of(
            () -> operations.newInputFile(PREFIX + "file"),
            () -> operations.newOutputFile(PREFIX + "file"),
            () -> operations.deleteFile(PREFIX + "file"),
            () -> operations.initialize(Map.of()))) {
      assertThatThrownBy(operation::run)
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessage("Cleanup supports only bound prefix operations");
    }
    verifyNoInteractions(fileIO);
  }

  @Test
  void closesDelegate() {
    operations.close();

    verify(fileIO).close();
  }
}
