package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.CooperativeDeadline;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ValidationUtils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
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
 *
 * <p>Each instance is bound to the root directory of one data entity (e.g. a table location) and
 * only operates at or under it. The server writes with its own OS identity, and clients that share
 * the local file system can create symbolic links in the entity's directories, or make the entity
 * directory itself a link, which could otherwise redirect a server operation outside the entity.
 * So:
 *
 * <ul>
 *   <li>an operation whose path goes through a link at or below the root, when the operation
 *       starts, is rejected, except deleting the root itself;
 *   <li>listings skip links, and deletion removes a link itself rather than its target;
 *   <li>the ancestors of the root are not checked.
 * </ul>
 *
 * <p>The check is not atomic with the file access: a link created after it (including before a
 * returned {@link InputFile} or {@link OutputFile} is opened) is not detected.
 */
public class SimpleLocalFileIO implements DelegateFileIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleLocalFileIO.class);
  private final Path root;
  private final CooperativeDeadline deadline;
  private final CloseableGroup listings = new CloseableGroup();

  /**
   * Creates local operations for one data entity, with interrupt checks and no deadline.
   *
   * @param root the root directory of the data entity, as a local path
   */
  public SimpleLocalFileIO(Path root) {
    this(root, CooperativeDeadline.NO_DEADLINE);
  }

  /**
   * Creates local operations for one data entity, sharing the given deadline and interruption
   * checks.
   *
   * @param root the root directory of the data entity, as a local path
   * @param deadline the cancellation checks shared with the caller
   */
  public SimpleLocalFileIO(Path root, CooperativeDeadline deadline) {
    this.root = Objects.requireNonNull(root, "root").normalize();
    this.deadline = Objects.requireNonNull(deadline, "deadline");
  }

  @Override
  public InputFile newInputFile(String path) {
    return org.apache.iceberg.Files.localInput(resolve(path).toFile());
  }

  @Override
  public OutputFile newOutputFile(String path) {
    // Local Iceberg REST tables write metadata through the same FileIO abstraction as cloud
    // tables. The core local adapter supplies the required atomic create/overwrite semantics.
    Path filePath = resolve(path);
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
      Files.delete(resolve(path));
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
    Path dir = toPath(prefix).normalize();
    // The walk does not follow a link at the path it starts from, but the OS follows links in the
    // components before it. At the root, a link is removed itself, so cleanup of an entity whose
    // directory was replaced by a link can finish; below it, check the components in between.
    deleteDirectory(dir.equals(root) ? dir : resolve(prefix), deadline);
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
    new SimpleLocalFileIO(toPath(prefix)).deletePrefix(prefix);
  }

  // The walk does not follow links: a link is visited as a file, so deleting it removes the
  // link and leaves its target alone.
  private static void deleteDirectory(Path prefix, CooperativeDeadline deadline) {
    try {
      Files.walkFileTree(
          prefix,
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
    Path dirPath = resolve(prefix);
    if (!Files.exists(dirPath, LinkOption.NOFOLLOW_LINKS)) {
      return CloseableIterable.empty();
    }
    Stream<Path> walk;
    try {
      walk = Files.walk(dirPath);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to walk " + prefix, e);
    }
    listings.addCloseable(walk);
    // The walk does not descend into linked directories; NOFOLLOW_LINKS also skips linked files.
    Stream<Path> entries = walk.filter(p -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS));
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

  /**
   * Returns the local path of a location under the root, rejecting it if the root itself or any
   * existing component below it is a symbolic link. Components that do not exist yet are not links.
   *
   * @param path a file URI at or under the root
   * @throws BaseException with {@code PERMISSION_DENIED} if the path goes through a link
   * @throws BaseException with {@code INVALID_ARGUMENT} if the path is not at or under the root
   * @throws UncheckedIOException if a component's attributes cannot be read
   */
  private Path resolve(String path) {
    Path filePath = toPath(path).normalize();
    ValidationUtils.checkArgument(
        filePath.startsWith(root), "Local path %s is not under %s", filePath, root);
    // The root itself, then each component below it, until one does not exist yet.
    if (!existsRejectingLink(root, path) || filePath.equals(root)) {
      return filePath;
    }
    Path current = root;
    for (Path name : root.relativize(filePath)) {
      current = current.resolve(name);
      if (!existsRejectingLink(current, path)) {
        break;
      }
    }
    return filePath;
  }

  /**
   * Returns whether something exists at a path, without following a link at it.
   *
   * @param path the component to check
   * @param location the location being resolved, for the error message
   * @return false if nothing exists at the path
   * @throws BaseException with {@code PERMISSION_DENIED} if the path is a symbolic link
   * @throws UncheckedIOException if the attributes cannot be read
   */
  static boolean existsRejectingLink(Path path, String location) {
    BasicFileAttributes attributes;
    try {
      attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    } catch (NoSuchFileException e) {
      return false;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read attributes of " + path, e);
    }
    if (attributes.isSymbolicLink()) {
      throw new BaseException(
          ErrorCode.PERMISSION_DENIED,
          String.format(
              "Local path %s goes through a symbolic link at %s, which the server does not follow",
              location, path));
    }
    return true;
  }

  /**
   * Returns the local path the file system opens for a location. The location is normalized first,
   * which rejects an escape that would change the path when decoded (e.g. {@code ..%2F}), so the
   * path checked is the path opened.
   *
   * @throws BaseException with {@code INVALID_ARGUMENT} if the location is not one plain path
   */
  private static Path toPath(String path) {
    return Paths.get(NormalizedURL.from(path).toUri());
  }
}
