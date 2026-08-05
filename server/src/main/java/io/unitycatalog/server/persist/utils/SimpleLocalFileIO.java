package io.unitycatalog.server.persist.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.stream.Stream;
import org.apache.iceberg.io.BulkDeletionFailureException;
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
 * HadoopFileIO, which requires hadoop-client-runtime on the classpath. Since the server only needs
 * read + directory operations on local paths (the Iceberg REST path is read-only), this wrapper
 * avoids that heavy runtime dependency by using only {@code java.nio} and iceberg-core.
 *
 * <p>It implements {@link DelegateFileIO} (rather than plain {@code FileIO}) for the {@link
 * #deletePrefix(String)} operation that backs directory deletion for managed tables/volumes.
 */
public class SimpleLocalFileIO implements DelegateFileIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleLocalFileIO.class);

  @Override
  public InputFile newInputFile(String path) {
    return org.apache.iceberg.Files.localInput(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    // SimpleLocalFileIO is read-only: the Iceberg REST path only reads local metadata, and managed
    // storage writes go through other code paths. Writes are intentionally unsupported.
    throw new UnsupportedOperationException("SimpleLocalFileIO is read-only: " + path);
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
   * Lists all regular files (recursively) under the given prefix. Returns a lazy {@link
   * CloseableIterable}: the underlying directory stream is opened when iteration starts and
   * released on {@link CloseableIterable#close()}, so callers must close the result (e.g. via
   * try-with-resources). Directories are intentionally excluded, per the Iceberg {@code FileInfo}
   * listing contract.
   */
  @Override
  public CloseableIterable<FileInfo> listPrefix(String prefix) {
    return CloseableIterable.transform(
        walkPrefix(prefix, false /* includeDirectories */, false /* bottomUp */),
        SimpleLocalFileIO::toFileInfo);
  }

  @Override
  public void deletePrefix(String prefix) {
    deleteDirectory(prefix);
  }

  /**
   * Recursively deletes everything under {@code prefix}, including the prefix directory itself.
   * Exposed as a static so callers with only a local path (and no FileIO instance) can reuse this
   * logic; the instance {@link #deletePrefix(String)} delegates here.
   *
   * @throws UncheckedIOException wrapping a {@link FileNotFoundException} if the prefix does not
   *     exist.
   */
  public static void deleteDirectory(String prefix) {
    // Include directories so the emptied tree is removed too, bottom-up (children before parents)
    // so each directory is empty by the time it is deleted.
    boolean deletedAny = false;
    try (CloseableIterable<Path> paths =
        walkPrefix(prefix, true /* includeDirectories */, true /* bottomUp */)) {
      for (Path path : paths) {
        delete(path);
        deletedAny = true;
      }
    } catch (IOException e) {
      // Per-path delete failures throw UncheckedIOException from delete() and propagate directly;
      // this only catches the checked IOException from closing the directory walk.
      throw new UncheckedIOException("Failed to close directory walk for " + prefix, e);
    }
    if (!deletedAny) {
      // With includeDirectories=true, walkPrefix yields the prefix directory itself when it
      // exists, so an empty walk means the prefix does not exist.
      throw new UncheckedIOException(
          new FileNotFoundException("Directory does not exist: " + prefix));
    }
  }

  /**
   * Walks the tree under {@code prefix} and returns a lazy, close-safe view of its entries. When
   * {@code includeDirectories} is false, only regular files are returned (the {@link #listPrefix}
   * contract); when true, directories are included as well. When {@code bottomUp} is true, entries
   * are returned in reverse lexicographic order (children before parents) so callers can delete
   * bottom-up. Returns an empty iterable if the prefix does not exist. Callers must close the
   * result to release the underlying directory stream.
   */
  private static CloseableIterable<Path> walkPrefix(
      String prefix, boolean includeDirectories, boolean bottomUp) {
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
    Stream<Path> entries = walk;
    if (!includeDirectories) {
      entries = entries.filter(Files::isRegularFile);
    }
    if (bottomUp) {
      // reverseOrder puts children before parents; .sorted() also buffers the full tree before
      // emitting, so deletePrefix can delete during iteration without disturbing the walk.
      entries = entries.sorted(Comparator.reverseOrder());
    }
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

  private static void delete(Path path) {
    try {
      Files.delete(path);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to delete " + path, e);
    }
  }

  private static Path toPath(String path) {
    return Paths.get(URI.create(path));
  }
}
