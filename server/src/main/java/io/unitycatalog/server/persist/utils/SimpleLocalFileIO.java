package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.utils.CooperativeDeadline;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.stream.Stream;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal Iceberg {@link DelegateFileIO} for local (file://) paths, implemented over {@code
 * java.nio} with reads delegating to iceberg-core's {@link org.apache.iceberg.Files}.
 *
 * <p>Iceberg's {@link org.apache.iceberg.io.ResolvingFileIO} resolves the file:// scheme to
 * HadoopFileIO, which requires hadoop-client-runtime on the classpath. This wrapper keeps local
 * Iceberg REST metadata reads and writes on the lightweight iceberg-core adapter.
 *
 * <p>It implements {@link DelegateFileIO} (rather than plain {@code FileIO}) for the {@link
 * #deletePrefix(String)} operation that backs directory deletion for managed tables/volumes.
 */
public class SimpleLocalFileIO implements DelegateFileIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleLocalFileIO.class);
  private final CooperativeDeadline deadline;
  private final CloseableGroup listings = new CloseableGroup();

  /** Creates local operations with interrupt checks and no deadline. */
  public SimpleLocalFileIO() {
    this(CooperativeDeadline.NO_DEADLINE);
  }

  /** Creates local operations sharing the given deadline and interruption checks. */
  public SimpleLocalFileIO(CooperativeDeadline deadline) {
    this.deadline = Objects.requireNonNull(deadline, "deadline");
  }

  @Override
  public InputFile newInputFile(String path) {
    return org.apache.iceberg.Files.localInput(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    // Local Iceberg REST tables write metadata through the same FileIO abstraction as cloud
    // tables. The core local adapter supplies the required atomic create/overwrite semantics.
    Path filePath = toPath(path);
    Path parent = filePath.getParent();
    if (parent != null) {
      try {
        Files.createDirectories(parent);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to create parent directories for: " + path, e);
      }
    }
    return org.apache.iceberg.Files.localOutput(filePath.toFile());
  }

  @Override
  public void deleteFile(String path) {
    try {
      Files.delete(toPath(path));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to delete " + path, e);
    }
  }

  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    int failures = 0;
    for (String path : pathsToDelete) {
      try {
        deleteFile(path);
      } catch (RuntimeException e) {
        // BulkDeletionFailureException only carries a count, so log the per-file cause here to
        // keep failures diagnosable rather than silently swallowed.
        failures++;
        LOGGER.warn("Failed to delete {}", path, e);
      }
    }
    if (failures > 0) {
      throw new BulkDeletionFailureException(failures);
    }
  }

  /**
   * Lists regular files recursively under the prefix without collecting the full listing. The
   * directory stream opens when this method is called; callers may close the returned iterable
   * early, and closing this FileIO releases any remaining streams. Directories are excluded, per
   * the Iceberg {@code FileInfo} listing contract.
   */
  @Override
  public CloseableIterable<FileInfo> listPrefix(String prefix) {
    deadline.checkCancelled();
    return CloseableIterable.transform(walkPrefix(prefix), SimpleLocalFileIO::toFileInfo);
  }

  @Override
  public void deletePrefix(String prefix) {
    deleteDirectory(prefix, deadline);
  }

  /** Closes directory listings, including listings whose iteration stopped early. */
  @Override
  public void close() {
    try {
      listings.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close local directory listings", e);
    }
  }

  /**
   * Recursively deletes everything under {@code prefix}, including the prefix directory itself.
   * Exposed as a static so callers with only a local path (and no FileIO instance) can reuse this
   * logic; the instance {@link #deletePrefix(String)} delegates here. A missing prefix is already
   * clean and returns successfully.
   *
   * @throws CancellationException if the current thread is interrupted. Detection clears the
   *     interrupt status before throwing so an executor can reuse the thread.
   */
  public static void deleteDirectory(String prefix) {
    new SimpleLocalFileIO().deletePrefix(prefix);
  }

  private static void deleteDirectory(String prefix, CooperativeDeadline deadline) {
    try {
      Files.walkFileTree(
          toPath(prefix),
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(
                Path directory, BasicFileAttributes attributes) {
              deadline.checkCancelled();
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
                throws IOException {
              deadline.checkCancelled();
              Files.deleteIfExists(file);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path directory, IOException failure)
                throws IOException {
              deadline.checkCancelled();
              if (failure != null && !(failure instanceof NoSuchFileException)) {
                throw failure;
              }
              Files.deleteIfExists(directory);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException failure)
                throws IOException {
              deadline.checkCancelled();
              if (failure instanceof NoSuchFileException) {
                return FileVisitResult.CONTINUE;
              }
              throw failure;
            }
          });
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to delete directory " + prefix, e);
    }
  }

  /**
   * Opens a lazy walk of regular files, owned by this FileIO and optionally closed earlier through
   * the returned iterable. Returns an empty iterable if the prefix does not exist.
   */
  private CloseableIterable<Path> walkPrefix(String prefix) {
    Path dirPath = toPath(prefix);
    if (!Files.exists(dirPath)) {
      return CloseableIterable.empty();
    }
    Stream<Path> walk;
    try {
      walk = Files.walk(dirPath);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to walk " + prefix, e);
    }
    listings.addCloseable(walk);
    Stream<Path> entries = walk.filter(Files::isRegularFile);
    return CloseableIterable.combine(entries::iterator, walk::close);
  }

  private static FileInfo toFileInfo(Path path) {
    try {
      // Fetch size and mtime in a single stat rather than one syscall each.
      BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
      return new FileInfo(
          path.toUri().toString(), attrs.size(), attrs.lastModifiedTime().toMillis());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to stat " + path, e);
    }
  }

  private static Path toPath(String path) {
    return Paths.get(URI.create(path));
  }
}
