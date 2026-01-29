package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static io.unitycatalog.server.utils.Constants.MANAGED_STORAGE_CATALOG_PREFIX;
import static io.unitycatalog.server.utils.Constants.MANAGED_STORAGE_SCHEMA_PREFIX;

public class FileOperations {

  private final ServerProperties serverProperties;

  public FileOperations(ServerProperties serverProperties) {
    this.serverProperties = serverProperties;
  }

  private static URI createURI(String uri) {
    if (uri.startsWith("s3://") || uri.startsWith("file:")) {
      return URI.create(uri);
    } else {
      return Paths.get(uri).toUri();
    }
  }

  public void deleteDirectory(String path) {
    URI directoryUri = createURI(path);
    UriUtils.validateURI(directoryUri);
    if (directoryUri.getScheme() == null || directoryUri.getScheme().equals("file")) {
      try {
        deleteLocalDirectory(Paths.get(directoryUri));
      } catch (RuntimeException | IOException e) {
        throw new BaseException(ErrorCode.INTERNAL, "Failed to delete directory: " + path, e);
      }
    } else {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Unsupported URI scheme: " + directoryUri.getScheme());
    }
  }

  public static void deleteLocalDirectory(Path dirPath) throws IOException {
    if (Files.exists(dirPath)) {
      try (Stream<Path> walk = Files.walk(dirPath, FileVisitOption.FOLLOW_LINKS)) {
        walk.sorted(Comparator.reverseOrder())
            .forEach(
                path -> {
                  try {
                    Files.delete(path);
                  } catch (IOException e) {
                    throw new RuntimeException("Failed to delete " + path, e);
                  }
                });
      }
    } else {
      throw new FileNotFoundException("Directory does not exist: " + dirPath);
    }
  }

  private static NormalizedURL getManagedLocationForEntity(
      NormalizedURL storageRoot, String prefix, UUID entityId) {
    return NormalizedURL.from(
        String.join("/", List.of(storageRoot.toString(), prefix, entityId.toString())));
  }

  public static NormalizedURL getManagedLocationForSchema(
      NormalizedURL storageRoot, UUID schemaId) {
    return getManagedLocationForEntity(storageRoot, MANAGED_STORAGE_SCHEMA_PREFIX, schemaId);
  }

  public static NormalizedURL getManagedLocationForCatalog(
      NormalizedURL storageRoot, UUID catalogId) {
    return getManagedLocationForEntity(storageRoot, MANAGED_STORAGE_CATALOG_PREFIX, catalogId);
  }

  // The following methods do not add a __unitystorage prefix because the storageRoot
  // is expected to be the storageLocation of a catalog or schema, which already includes
  // the __unitystorage prefix.

  public static NormalizedURL getManagedLocationForTable(
      NormalizedURL parentStorageLocation, UUID tableId) {
    return getManagedLocationForEntity(parentStorageLocation, "tables", tableId);
  }

  public static NormalizedURL getManagedLocationForVolume(
      NormalizedURL parentStorageLocation, UUID volumeId) {
    return getManagedLocationForEntity(parentStorageLocation, "volumes", volumeId);
  }

  public static NormalizedURL getManagedLocationForModel(
      NormalizedURL parentStorageLocation, UUID modelId) {
    return getManagedLocationForEntity(parentStorageLocation, "models", modelId);
  }

  public static NormalizedURL getManagedLocationForModelVersion(
      NormalizedURL parentModelStorageLocation, UUID modelVersionId) {
    return getManagedLocationForEntity(parentModelStorageLocation, "versions", modelVersionId);
  }
}
