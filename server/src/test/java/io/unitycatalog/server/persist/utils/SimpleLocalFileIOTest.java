package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.CooperativeDeadline;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
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
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

public class SimpleLocalFileIOTest {

  // The root every FileIO in this suite is bound to; each test works under it.
  @TempDir Path tempDir;
  // A directory outside the root, as the target of links.
  @TempDir Path outside;
  private SimpleLocalFileIO fileIO;

  @BeforeEach
  public void createFileIO() {
    fileIO = new SimpleLocalFileIO(tempDir);
  }

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
  public void writesAndReadsBackThroughFileUris() {
    String location = uri(tempDir.resolve("data.txt"));
    write(location, "hello");
    assertThat(read(location)).isEqualTo("hello");
  }

  @Test
  public void newOutputFileCreatesMissingParentDirectories() {
    String location = uri(tempDir.resolve("nested/deeper/data.txt"));
    write(location, "content");
    assertThat(Files.exists(tempDir.resolve("nested/deeper/data.txt"))).isTrue();
    assertThat(read(location)).isEqualTo("content");
  }

  @Test
  public void deleteFileRemovesTheFile() {
    Path file = tempDir.resolve("data.txt");
    String location = uri(file);
    write(location, "x");

    fileIO.deleteFile(location);
    assertThat(Files.exists(file)).isFalse();
  }

  @Test
  public void deleteFileThrowsWhenTheFileIsMissing() {
    assertThatThrownBy(() -> fileIO.deleteFile(uri(tempDir.resolve("missing.txt"))))
        .isInstanceOf(UncheckedIOException.class);
  }

  @Test
  public void deleteFilesDeletesEveryPathThatExists() {
    String a = uri(tempDir.resolve("a.txt"));
    String b = uri(tempDir.resolve("b.txt"));
    write(a, "a");
    write(b, "b");

    fileIO.deleteFiles(List.of(a, b));
    assertThat(Files.exists(tempDir.resolve("a.txt"))).isFalse();
    assertThat(Files.exists(tempDir.resolve("b.txt"))).isFalse();
  }

  @Test
  public void deleteFilesReportsFailuresAsBulkDeletionFailure() {
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
  public void listPrefixReturnsRegularFilesRecursivelyAndExcludesDirectoriesAndLinks() {
    write(uri(tempDir.resolve("top.txt")), "1234");
    write(uri(tempDir.resolve("sub/child.txt")), "56");
    // Links to a file and to a directory outside the root are neither listed nor descended into.
    Files.writeString(outside.resolve("target.txt"), "outside");
    Files.createSymbolicLink(tempDir.resolve("fileLink"), outside.resolve("target.txt"));
    Files.createSymbolicLink(tempDir.resolve("sub/dirLink"), outside);

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

  @SneakyThrows
  @Test
  public void deletePrefixRemovesTheEntireTreeButNotLinkTargets() {
    Path root = tempDir.resolve("table");
    write(uri(root.resolve("metadata/v1.json")), "m");
    write(uri(root.resolve("data/part-0")), "d");
    Files.writeString(outside.resolve("target.txt"), "outside");
    Files.createSymbolicLink(root.resolve("data/dirLink"), outside);

    fileIO.deletePrefix(uri(root));
    assertThat(Files.exists(root)).isFalse();
    // The link was removed, not followed.
    assertThat(outside.resolve("target.txt")).hasContent("outside");
  }

  @Test
  public void deletePrefixAllowsMissingDirectory() {
    assertThatCode(() -> fileIO.deletePrefix(uri(tempDir.resolve("absent"))))
        .doesNotThrowAnyException();
  }

  @Test
  public void deletePrefixClearsInterruptSoThreadCanBeReused() {
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
  public void deletePrefixStopsDuringTraversalAtDeadline() throws Exception {
    Path root = tempDir.resolve("table");
    write(uri(root.resolve("first")), "a");
    write(uri(root.resolve("second")), "b");
    Clock clock = mock(Clock.class);
    Instant deadline = Instant.parse("2026-01-01T00:00:20Z");
    // Enter the directory and delete one file, then expire before visiting the next file.
    when(clock.instant()).thenReturn(deadline.minusSeconds(1), deadline.minusSeconds(1), deadline);
    SimpleLocalFileIO cleanupIO =
        new SimpleLocalFileIO(tempDir, new CooperativeDeadline(clock, deadline));

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
  public void listPrefixOnMissingDirectoryIsEmpty() {
    assertThatCode(
            () -> {
              try (CloseableIterable<FileInfo> listed =
                  fileIO.listPrefix(uri(tempDir.resolve("absent")))) {
                assertThat(listed.iterator().hasNext()).isFalse();
              }
            })
        .doesNotThrowAnyException();
  }

  @Test
  public void operationsThroughALinkUnderTheRootAreRejected() throws IOException {
    Files.writeString(outside.resolve("target.txt"), "outside");
    Files.createDirectories(outside.resolve("sub"));
    Files.writeString(outside.resolve("sub/keep.txt"), "keep");
    Files.createSymbolicLink(tempDir.resolve("dirLink"), outside);
    Files.createSymbolicLink(tempDir.resolve("fileLink"), outside.resolve("target.txt"));
    // A link whose target does not exist yet: writing through it would create the target.
    Files.createSymbolicLink(tempDir.resolve("danglingLink"), outside.resolve("new.json"));
    String throughDirLink = uri(tempDir.resolve("dirLink/metadata/v1.json"));
    String fileLink = uri(tempDir.resolve("fileLink"));

    assertLinkRejected(() -> fileIO.newOutputFile(throughDirLink));
    assertLinkRejected(() -> fileIO.newOutputFile(fileLink));
    assertLinkRejected(() -> fileIO.newOutputFile(uri(tempDir.resolve("danglingLink"))));
    assertLinkRejected(() -> fileIO.newInputFile(fileLink));
    assertLinkRejected(() -> fileIO.deleteFile(fileLink));
    assertLinkRejected(() -> fileIO.listPrefix(uri(tempDir.resolve("dirLink"))));
    assertLinkRejected(() -> fileIO.deletePrefix(uri(tempDir.resolve("dirLink/sub"))));
    // Nothing was created, changed, or deleted outside the root.
    assertThat(outside.resolve("metadata")).doesNotExist();
    assertThat(outside.resolve("new.json")).doesNotExist();
    assertThat(outside.resolve("target.txt")).hasContent("outside");
    assertThat(outside.resolve("sub/keep.txt")).hasContent("keep");
  }

  @Test
  public void pathOutsideTheRootIsRejected() {
    assertThatThrownBy(() -> fileIO.newOutputFile(uri(outside.resolve("v1.json"))))
        .isInstanceOf(IllegalArgumentException.class);
    // An escape that decodes to a separator and a dot segment is rejected before it is decoded.
    String escaped = uri(tempDir) + "sub/..%2F..%2Fescaped.json";
    assertThatThrownBy(() -> fileIO.newInputFile(escaped))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INVALID_ARGUMENT);
    assertThatThrownBy(() -> fileIO.newInputFile(uri(tempDir.resolve("../escaped.json"))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void linkAtTheRootIsRejectedButNotAboveIt() throws IOException {
    Files.writeString(outside.resolve("target.txt"), "outside");
    // The root itself is a link, e.g. a table location replaced by a link to another directory.
    Path linkedRoot = Files.createSymbolicLink(tempDir.resolve("linkedRoot"), outside);
    try (SimpleLocalFileIO linked = new SimpleLocalFileIO(linkedRoot)) {
      assertLinkRejected(() -> linked.newOutputFile(uri(linkedRoot.resolve("metadata/v1.json"))));
      assertLinkRejected(() -> linked.newInputFile(uri(linkedRoot.resolve("target.txt"))));
      assertLinkRejected(() -> linked.listPrefix(uri(linkedRoot)));
      assertLinkRejected(() -> linked.deletePrefix(uri(linkedRoot)));
    }
    assertThat(outside.resolve("metadata")).doesNotExist();
    assertThat(outside.resolve("target.txt")).hasContent("outside");

    // Ancestors of the root are not checked, so a root under a linked directory works.
    Path rootUnderLink =
        Files.createSymbolicLink(tempDir.resolve("linkedParent"), outside).resolve("table");
    try (SimpleLocalFileIO underLink = new SimpleLocalFileIO(rootUnderLink)) {
      OutputFile file = underLink.newOutputFile(uri(rootUnderLink.resolve("metadata/v1.json")));
      try (OutputStream out = file.create()) {
        out.write('x');
      }
    }
    assertThat(outside.resolve("table/metadata/v1.json")).hasContent("x");
  }

  private static void assertLinkRejected(ThrowingCallable call) {
    assertThatThrownBy(call)
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("symbolic link")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.PERMISSION_DENIED);
  }

  @ParameterizedTest
  @ValueSource(strings = {"partial", "iteration failure", "cancellation", "explicit close"})
  public void closeReleasesListings(String scenario) throws Exception {
    Path file = tempDir.resolve("data.txt");
    AtomicBoolean closed = new AtomicBoolean();
    // The root's link check reads the real directory's attributes, read before Files is mocked.
    BasicFileAttributes rootAttributes =
        Files.readAttributes(tempDir, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    try (Stream<Path> walk = Stream.of(file).onClose(() -> closed.set(true));
        MockedStatic<Files> files = mockStatic(Files.class)) {
      files.when(() -> Files.exists(tempDir, LinkOption.NOFOLLOW_LINKS)).thenReturn(true);
      files
          .when(
              () ->
                  Files.readAttributes(
                      tempDir, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS))
          .thenReturn(rootAttributes);
      files.when(() -> Files.walk(tempDir)).thenReturn(walk);
      files.when(() -> Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)).thenReturn(true);
      files
          .when(() -> Files.readAttributes(file, BasicFileAttributes.class))
          .thenThrow(new IOException("Cannot read file attributes"));

      try (SupportsPrefixOperations operations = new SimpleLocalFileIO(tempDir)) {
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
