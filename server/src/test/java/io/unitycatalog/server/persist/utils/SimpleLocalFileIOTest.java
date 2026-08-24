package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SimpleLocalFileIOTest {

  private final SimpleLocalFileIO fileIO = new SimpleLocalFileIO();

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
  public void deleteDirectoryThrowsFileNotFoundWhenPrefixIsMissing(@TempDir Path tempDir) {
    assertThatThrownBy(() -> SimpleLocalFileIO.deleteDirectory(uri(tempDir.resolve("absent"))))
        .isInstanceOf(UncheckedIOException.class)
        .hasCauseInstanceOf(java.io.FileNotFoundException.class);
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
}
