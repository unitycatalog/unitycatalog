package io.unitycatalog.server.persist.utils;

import com.azure.storage.common.Utility;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import io.unitycatalog.server.utils.CooperativeDeadline;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;

/**
 * Deletes one ADLS directory tree on the calling thread, with cooperative cancellation. Direct SDK
 * operations preserve directory information and propagate failures so a later attempt can retry.
 */
final class ADLSPrefixOperations implements SupportsPrefixOperations {
  private final ADLSFileIO fileIO;
  private final String boundPrefix;
  private final String directory;
  private final String containerRoot;
  private final CooperativeDeadline deadline;

  /**
   * @param fileIO credentialed FileIO owned and closed by these prefix operations
   * @param boundPrefix absolute ADLS directory URL, including its trailing slash
   * @param deadline shared attempt deadline and shutdown interruption check
   */
  ADLSPrefixOperations(ADLSFileIO fileIO, String boundPrefix, CooperativeDeadline deadline) {
    URI uri = URI.create(boundPrefix);
    String path = uri.getPath();
    if (!("abfs".equals(uri.getScheme()) || "abfss".equals(uri.getScheme()))
        || uri.getRawAuthority() == null
        || uri.getRawQuery() != null
        || uri.getRawFragment() != null
        || path == null
        || path.chars().allMatch(character -> character == '/')
        || !path.endsWith("/")
        || !uri.normalize().equals(uri)
        || hasDotSegment(path)) {
      throw new IllegalArgumentException(
          "Bound prefix must identify an ADLS directory ending in '/'");
    }
    this.fileIO = Objects.requireNonNull(fileIO, "fileIO");
    this.boundPrefix = boundPrefix;
    this.directory = path.substring(1, path.length() - 1);
    this.containerRoot = uri.getScheme() + "://" + uri.getRawAuthority() + "/";
    this.deadline = Objects.requireNonNull(deadline, "deadline");
  }

  /**
   * Lists files lazily as absolute ADLS URLs; a missing directory is already empty.
   *
   * @param prefix the exact bound prefix
   */
  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    checkPrefix(prefix);
    return () -> {
      deadline.checkCancelled();
      return StreamSupport.stream(
              Spliterators.spliteratorUnknownSize(
                  listPaths(fileIO.client(boundPrefix), directory, true), Spliterator.ORDERED),
              false)
          .filter(item -> !item.isDirectory())
          .map(
              item ->
                  new FileInfo(
                      containerRoot + Utility.urlEncode(item.getName()).replace("%2F", "/"),
                      item.getContentLength(),
                      item.getCreationTime() == null
                          ? 0
                          : item.getCreationTime().toInstant().toEpochMilli()))
          .iterator();
    };
  }

  /**
   * Deletes files before their parent directories, stopping between listings and deletes. An
   * in-flight SDK call uses provider defaults and may outlast the deadline.
   *
   * @param prefix the exact bound prefix
   */
  @Override
  public void deletePrefix(String prefix) {
    checkPrefix(prefix);
    deleteTree(fileIO.client(boundPrefix), directory);
  }

  private void deleteTree(DataLakeFileSystemClient client, String directory) {
    Iterator<PathItem> children = listPaths(client, directory, false);
    while (children.hasNext()) {
      PathItem child = children.next();
      deadline.checkCancelled();
      if (child.isDirectory()) {
        deleteTree(client, child.getName());
      } else {
        // Use the SDK directly: Iceberg's bulk delete uses a shared thread pool and its
        // single-file delete logs failures instead of propagating them to the worker.
        client.getFileClient(child.getName()).deleteIfExists();
      }
    }
    deadline.checkCancelled();
    // This is non-recursive. A concurrent write leaves a non-empty directory and triggers a retry.
    client.getDirectoryClient(directory).deleteIfExists();
  }

  /** Checks cancellation around lazy page reads, including a 404 from a later page. */
  private Iterator<PathItem> listPaths(
      DataLakeFileSystemClient client, String directory, boolean recursive) {
    return new Iterator<>() {
      private Iterator<PathItem> paths;
      private boolean missing;

      @Override
      public boolean hasNext() {
        deadline.checkCancelled();
        if (missing) {
          return false;
        }
        try {
          if (paths == null) {
            paths =
                client
                    .listPaths(
                        new ListPathsOptions()
                            .setPath(directory)
                            .setRecursive(recursive)
                            .setMaxResults(1000),
                        null)
                    .iterator();
          }
          boolean hasNext = paths.hasNext();
          deadline.checkCancelled();
          return hasNext;
        } catch (DataLakeStorageException e) {
          if (e.getStatusCode() != 404) {
            throw e;
          }
          missing = true;
          return false;
        }
      }

      @Override
      public PathItem next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        PathItem item = paths.next();
        deadline.checkCancelled();
        if (!item.getName().startsWith(directory + "/") || hasDotSegment(item.getName())) {
          throw new IllegalArgumentException("Listed path is outside the bound directory");
        }
        return item;
      }
    };
  }

  private static boolean hasDotSegment(String path) {
    return Arrays.stream(path.split("/")).anyMatch(part -> part.equals(".") || part.equals(".."));
  }

  private void checkPrefix(String prefix) {
    if (!boundPrefix.equals(prefix)) {
      throw new IllegalArgumentException("Prefix does not match bound prefix");
    }
    deadline.checkCancelled();
  }

  @Override
  public InputFile newInputFile(String path) {
    throw new UnsupportedOperationException("Only bound prefix operations are supported");
  }

  @Override
  public OutputFile newOutputFile(String path) {
    throw new UnsupportedOperationException("Only bound prefix operations are supported");
  }

  @Override
  public void deleteFile(String path) {
    throw new UnsupportedOperationException("Only bound prefix operations are supported");
  }

  @Override
  public void initialize(Map<String, String> properties) {
    throw new UnsupportedOperationException("Only bound prefix operations are supported");
  }

  @Override
  public Map<String, String> properties() {
    return fileIO.properties();
  }

  @Override
  public void close() {
    fileIO.close();
  }
}
