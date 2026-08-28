package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;

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
   * Returns an Iceberg {@link FileIO} for reading the given location. Local paths use {@link
   * SimpleLocalFileIO}; cloud paths use a credential-vended {@link ResolvingFileIO}.
   */
  default FileIO getFileIO(NormalizedURL path) {
    return getFileIO(path, CredentialContext.READ_ONLY);
  }

  /** Returns a FileIO configured for the requested storage privileges. */
  FileIO getFileIO(NormalizedURL path, Set<CredentialContext.Privilege> privileges);

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
  Map<String, String> getFileIOConfig(
      NormalizedURL path, Set<CredentialContext.Privilege> privileges);
}
