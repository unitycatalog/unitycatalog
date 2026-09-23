package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.utils.CooperativeDeadline;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

public class SimpleLocalFileIOTest {

  private final SimpleLocalFileIO fileIO = new SimpleLocalFileIO();

  @AfterEach
  public void closeFileIO() {
    fileIO.close();
  }

  private static String uri(Path path) {
    return path.toUri().toString();
  }

  @SneakyThrows
  private void write(String location, String content) {
    OutputFile outputFile = fileIO.newOutputFile(location);
    try (OutputStream out = outputFile.createOrOverwrite()) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
  }

  @SneakyThrows
  private String read(String location) {
    InputFile inputFile = fileIO.newInputFile(location);
    try (InputStream in = inputFile.newStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  @Test
  public void writesAndReadsBackThroughFileUris(@TempDir Path tempDir) {
    String location = uri(tempDir.resolve("data.txt"));
    write(location, "hello");
    assertThat(read(location)).isEqualTo("hello");
  }

  @Test
  public void newOutputFileCreatesMissingParentDirectories(@TempDir Path tempDir) {
    String location = uri(tempDir.resolve("nested/deeper/data.txt"));
    write(location, "content");
    assertThat(Files.exists(tempDir.resolve("nested/deeper/data.txt"))).isTrue();
    assertThat(read(location)).isEqualTo("content");
  }

  @Test
  public void deleteFileRemovesTheFile(@TempDir Path tempDir) {
    Path file = tempDir.resolve("data.txt");
    String location = uri(file);
    write(location, "x");

    fileIO.deleteFile(location);
    assertThat(Files.exists(file)).isFalse();
  }

  @Test
  public void deleteFileThrowsWhenTheFileIsMissing(@TempDir Path tempDir) {
    assertThatThrownBy(() -> fileIO.deleteFile(uri(tempDir.resolve("missing.txt"))))
        .isInstanceOf(UncheckedIOException.class);
  }

  @Test
  public void deleteFilesDeletesEveryPathThatExists(@TempDir Path tempDir) {
    String a = uri(tempDir.resolve("a.txt"));
    String b = uri(tempDir.resolve("b.txt"));
    write(a, "a");
    write(b, "b");

    fileIO.deleteFiles(List.of(a, b));
    assertThat(Files.exists(tempDir.resolve("a.txt"))).isFalse();
    assertThat(Files.exists(tempDir.resolve("b.txt"))).isFalse();
  }

  @Test
  public void deleteFilesReportsFailuresAsBulkDeletionFailure(@TempDir Path tempDir) {
    String existing = uri(tempDir.resolve("a.txt"));
    String missing = uri(tempDir.resolve("missing.txt"));
    write(existing, "a");

    assertThatThrownBy(() -> fileIO.deleteFiles(List.of(existing, missing)))
        .isInstanceOf(BulkDeletionFailureException.class);
    // The deletable file is still removed before the missing one fails.
    assertThat(Files.exists(tempDir.resolve("a.txt"))).isFalse();
  }

  @SneakyThrows
  @Test
  public void listPrefixReturnsRegularFilesRecursivelyAndExcludesDirectories(
      @TempDir Path tempDir) {
    write(uri(tempDir.resolve("top.txt")), "1234");
    write(uri(tempDir.resolve("sub/child.txt")), "56");

    try (CloseableIterable<FileInfo> listed = fileIO.listPrefix(uri(tempDir))) {
      List<FileInfo> files =
          java.util.stream.StreamSupport.stream(listed.spliterator(), false)
              .collect(Collectors.toList());
      assertThat(files).hasSize(2);
      assertThat(files.stream().map(FileInfo::location))
          .containsExactlyInAnyOrder(
              uri(tempDir.resolve("top.txt")), uri(tempDir.resolve("sub/child.txt")));
      assertThat(files.stream().map(FileInfo::size)).containsExactlyInAnyOrder(4L, 2L);
    }
  }

  @Test
  public void deletePrefixRemovesTheEntireTree(@TempDir Path tempDir) {
    Path root = tempDir.resolve("table");
    write(uri(root.resolve("metadata/v1.json")), "m");
    write(uri(root.resolve("data/part-0")), "d");

    fileIO.deletePrefix(uri(root));
    assertThat(Files.exists(root)).isFalse();
  }

  @Test
  public void deletePrefixAllowsMissingDirectory(@TempDir Path tempDir) {
    assertThatCode(() -> fileIO.deletePrefix(uri(tempDir.resolve("absent"))))
        .doesNotThrowAnyException();
  }

  @Test
  public void deletePrefixClearsInterruptSoThreadCanBeReused(@TempDir Path tempDir) {
    Path root = tempDir.resolve("table");
    write(uri(root.resolve("data")), "d");

    Thread.currentThread().interrupt();
    try {
      assertThatThrownBy(() -> fileIO.deletePrefix(uri(root)))
          .isInstanceOf(java.util.concurrent.CancellationException.class);
      assertThat(Files.exists(root)).isTrue();
      assertThat(Thread.currentThread().isInterrupted()).isFalse();

      fileIO.deletePrefix(uri(root));
      assertThat(Files.exists(root)).isFalse();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void deletePrefixStopsDuringTraversalAtDeadline(@TempDir Path tempDir) throws Exception {
    Path root = tempDir.resolve("table");
    write(uri(root.resolve("first")), "a");
    write(uri(root.resolve("second")), "b");
    Clock clock = mock(Clock.class);
    Instant deadline = Instant.parse("2026-01-01T00:00:20Z");
    // Enter the directory and delete one file, then expire before visiting the next file.
    when(clock.instant()).thenReturn(deadline.minusSeconds(1), deadline.minusSeconds(1), deadline);
    SimpleLocalFileIO cleanupIO = new SimpleLocalFileIO(new CooperativeDeadline(clock, deadline));

    assertThatThrownBy(() -> cleanupIO.deletePrefix(uri(root)))
        .isInstanceOf(CancellationException.class)
        .hasMessage("Operation deadline reached");
    try (var remaining = fileIO.listPrefix(uri(root))) {
      assertThat(remaining).hasSize(1);
    }

    fileIO.deletePrefix(uri(root));
    assertThat(root).doesNotExist();
  }

  @Test
  public void listPrefixOnMissingDirectoryIsEmpty(@TempDir Path tempDir) {
    assertThatCode(
            () -> {
              try (CloseableIterable<FileInfo> listed =
                  fileIO.listPrefix(uri(tempDir.resolve("absent")))) {
                assertThat(listed.iterator().hasNext()).isFalse();
              }
            })
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(strings = {"partial", "iteration failure", "cancellation", "explicit close"})
  public void closeReleasesListings(String scenario, @TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("data.txt");
    AtomicBoolean closed = new AtomicBoolean();
    try (Stream<Path> walk = Stream.of(file).onClose(() -> closed.set(true));
        MockedStatic<Files> files = mockStatic(Files.class)) {
      files.when(() -> Files.exists(tempDir)).thenReturn(true);
      files.when(() -> Files.walk(tempDir)).thenReturn(walk);
      files.when(() -> Files.isRegularFile(file)).thenReturn(true);
      files
          .when(() -> Files.readAttributes(file, BasicFileAttributes.class))
          .thenThrow(new IOException("Cannot read file attributes"));

      try (SupportsPrefixOperations operations = new SimpleLocalFileIO()) {
        Iterable<FileInfo> listing = operations.listPrefix(uri(tempDir));
        var iterator = listing.iterator();
        assertThat(iterator.hasNext()).isTrue();
        switch (scenario) {
          case "iteration failure" ->
              assertThatThrownBy(iterator::next).isInstanceOf(UncheckedIOException.class);
          case "cancellation" -> {
            Thread.currentThread().interrupt();
            try {
              assertThatThrownBy(CooperativeDeadline.NO_DEADLINE::checkCancelled)
                  .isInstanceOf(CancellationException.class)
                  .hasMessage("Operation interrupted");
              assertThat(Thread.currentThread().isInterrupted()).isFalse();
            } finally {
              Thread.interrupted();
            }
          }
          case "explicit close" -> CloseableIterable.of(listing).close();
          default -> {
            /* Leave the partially consumed listing open. */
          }
        }
        assertThat(closed.get()).isEqualTo(scenario.equals("explicit close"));
      }
      assertThat(closed).isTrue();
    }
  }
}
