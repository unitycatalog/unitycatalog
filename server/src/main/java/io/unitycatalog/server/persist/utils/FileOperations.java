package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.CooperativeDeadline;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsPrefixOperations;

/**
 * Single entry point for all storage/file access in the server. Covers both directory lifecycle
 * management for managed storage locations (create/delete) and credential-vended Iceberg {@link
 * FileIO} construction used by the Iceberg REST catalog. The default implementation is {@link
 * FileOperationsImpl}; tests may wrap it to redirect cloud storage to the local filesystem.
 */
public interface FileOperations {

  /** Delete entire directory recursively. Note that currently it does nothing for cloud FS */
  static void deleteDirectory(NormalizedURL url) {
    switch (UriScheme.fromURI(url.toUri())) {
      // Directory deletion for local paths is handled by SimpleLocalFileIO.
      case FILE, NULL -> SimpleLocalFileIO.deleteDirectory(url.toString());
      // Currently we can NOT delete the path in cloud storage. We will update this in future
      // when UC OSS begins using the hadoopfs libraries.
      case S3 -> {}
      case GS -> {}
      case ABFS, ABFSS -> {}
    }
  }

  /** Create a directory for storage location. Note that currently it does nothing for cloud FS */
  static void createStorageLocationDir(NormalizedURL url) {
    switch (UriScheme.fromURI(url.toUri())) {
      case FILE, NULL -> createLocalDirectory(url);
      // Currently we can NOT create the directory in cloud storage. We will update this in future
      // when UC OSS begins using the hadoopfs libraries.
      case S3 -> {}
      case GS -> {}
      case ABFS, ABFSS -> {}
    }
  }

  private static void createLocalDirectory(NormalizedURL url) {
    Path dirPath = Paths.get(url.toUri());
    // Check if directory already exists
    if (Files.exists(dirPath)) {
      throw new BaseException(ErrorCode.ALREADY_EXISTS, "Directory already exists: " + dirPath);
    }
    // Create the directory
    try {
      Files.createDirectories(dirPath);
    } catch (IOException e) {
      throw new BaseException(ErrorCode.INTERNAL, "Failed to create directory: " + dirPath, e);
    }
  }

  /**
   * Returns an Iceberg {@link FileIO} for reading the given location. Cloud paths are served with
   * credentials vended for the location; the concrete FileIO is chosen by the implementation.
   */
  default FileIO getFileIO(NormalizedURL path) {
    return getFileIO(path, CredentialContext.READ_ONLY);
  }

  /** Returns a FileIO configured for the requested storage privileges. */
  FileIO getFileIO(NormalizedURL path, Set<CredentialContext.Privilege> privileges);

  /**
   * Returns fresh, write-enabled prefix operations sharing the attempt's cancellation checks.
   *
   * <p>Cloud cleanup uses the provider's default request settings. Cancellation is checked between
   * batches and does not impose a timeout on an in-flight storage call.
   */
  SupportsPrefixOperations getCleanupFileIO(NormalizedURL path, CooperativeDeadline deadline);

  /**
   * Builds the Iceberg FileIO configuration (credentials, region, token expiry) for the given
   * location by vending temporary storage credentials for it. Returns an empty map for local
   * (file://) paths, which need no cloud credentials.
   *
   * @param path the normalized storage location to vend credentials and build config for
   */
  default Map<String, String> getFileIOConfig(NormalizedURL path) {
    return getFileIOConfig(path, CredentialContext.READ_ONLY);
  }

  /** Builds FileIO configuration using the requested storage privileges. */
  default Map<String, String> getFileIOConfig(
      NormalizedURL path, Set<CredentialContext.Privilege> privileges) {
    return getFileIOConfig(path, privileges, Optional.empty());
  }

  /**
   * Builds FileIO configuration that also tells the client where to renew the vended credentials.
   *
   * <p>The endpoint is only meaningful in a configuration handed to a REST client: Iceberg's
   * clients resolve it against their catalog URI, which the server's own FileIO does not have, so
   * the server's internal callers must leave it empty.
   *
   * @param credentialsEndpoint path of the loadCredentials endpoint for this table, relative to the
   *     catalog URI, or empty for a configuration the server builds for itself
   */
  Map<String, String> getFileIOConfig(
      NormalizedURL path,
      Set<CredentialContext.Privilege> privileges,
      Optional<String> credentialsEndpoint);
}
