package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.GenerateTemporaryModelVersionCredentialsResponse;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UriUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(UriUtils.class);
  private static final ServerPropertiesUtils properties = ServerPropertiesUtils.getInstance();

  private enum Operation {
    CREATE,
    DELETE
  }

  private UriUtils() {}

  private static String modelStorageRootCached;
  private static String modelStorageRootPropertyCached;

  private static void reset() {
    modelStorageRootPropertyCached = null;
    modelStorageRootCached = null;
  }

  // Model specific storage root handlers and convenience methods
  private static String getModelStorageRoot() {
    String currentModelStorageRoot = properties.getProperty("storage-root.models");
    if (modelStorageRootPropertyCached != currentModelStorageRoot) {
      // This means the property has been updated from the previous read, or this is the first time
      // reading it
      reset();
    }
    if (modelStorageRootCached != null) {
      return modelStorageRootCached;
    }
    String modelStorageRoot = currentModelStorageRoot;
    if (modelStorageRoot == null) {
      // If the model storage root is empty, use the CWD
      modelStorageRoot = System.getProperty("user.dir");
    }
    // If the model storage root is not a valid URI, make it one
    if (!isValidURI(modelStorageRoot)) {
      // Convert to an absolute path
      modelStorageRoot = Paths.get(modelStorageRoot).toUri().toString();
    }
    // Check if the modelStorageRoot ends with a slash and remove it if it does
    while (modelStorageRoot.endsWith("/")) {
      modelStorageRoot = modelStorageRoot.substring(0, modelStorageRoot.length() - 1);
    }
    modelStorageRootCached = modelStorageRoot;
    modelStorageRootPropertyCached = currentModelStorageRoot;
    return modelStorageRoot;
  }

  private static String getModelDirectoryURI(String entityFullName) {
    return getModelStorageRoot() + "/" + entityFullName.replace(".", "/");
  }

  public static String getModelStorageLocation(String catalogId, String schemaId, String modelId) {
    return getModelDirectoryURI(catalogId + "." + schemaId + ".models." + modelId);
  }

  public static String getModelVersionStorageLocation(
      String catalogId, String schemaId, String modelId, String versionId) {
    return getModelDirectoryURI(
        catalogId + "." + schemaId + ".models." + modelId + ".versions." + versionId);
  }

  public static String createStorageLocationPath(String uri) {
    return updateDirectoryFromUri(uri, Operation.CREATE, Optional.empty()).toString();
  }

  public static String deleteStorageLocationPath(String uri) {
    return updateDirectoryFromUri(uri, Operation.DELETE, Optional.empty()).toString();
  }

  private static URI updateDirectoryFromUri(
      String uri,
      Operation op,
      Optional<GenerateTemporaryModelVersionCredentialsResponse> credentials) {
    URI parsedUri = URI.create(uri);
    validateURI(parsedUri);
    if (!parsedUri.getScheme().equals("file") && credentials.isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Empty credentials passed for a non-file based URI");
    }
    try {
      if (parsedUri.getScheme().equals("file")) {
        return updateLocalDirectory(parsedUri, op);
      } else if (parsedUri.getScheme().equals("s3")
          && credentials.get().getAwsTempCredentials() != null) {
        // For v0.2, we will NOT create the path in cloud storage since MLflow uses the native cloud
        // clients and not the hadoopfs libraries.  We will update this in v0.3 when UC OSS begins
        // using
        // the hadoopfs libraries.
        /* return updateS3Directory(parsedUri, op, credentials.get().getAwsTempCredentials()); */
        return parsedUri;
      } else if (parsedUri.getScheme().equals("gc")
          && credentials.get().getGcpOauthToken() != null) {
        // For v0.2, we will NOT create the path in cloud storage since MLflow uses the native cloud
        // clients and not the hadoopfs libraries.  We will update this in v0.3 when UC OSS begins
        // using
        // the hadoopfs libraries.
        /* return updateGcDirectory(parsedUri, op, credentials.get().getGcpOauthToken()); */
        return parsedUri;
      } else if (parsedUri.getScheme().equals("abfs")
          && credentials.get().getAzureUserDelegationSas() != null) {
        // For v0.2, we will NOT create the path in cloud storage since MLflow uses the native cloud
        // clients and not the hadoopfs libraries.  We will update this in v0.3 when UC OSS begins
        // using
        // the hadoopfs libraries.
        /* return updateAbsDirectory(parsedUri, op, credentials.get().getAzureUserDelegationSas()); */
        return parsedUri;
      }
    } catch (Exception e) {
      throw new BaseException(
          ErrorCode.INTERNAL,
          "Error attempting to "
              + op.name()
              + " URI "
              + parsedUri.toString()
              + ": "
              + e.getMessage());
    }
    throw new BaseException(
        ErrorCode.INVALID_ARGUMENT, "Unknown scheme detected: " + parsedUri.getScheme());
  }

  private static boolean isValidURI(String uri) {
    try {
      URI testURI = new URI(uri);
      if (testURI.getScheme() != null && testURI.getPath() != null) {
        return true;
      }
      return false;
    } catch (URISyntaxException e) {
      return false;
    }
  }

  private static URI updateLocalDirectory(URI parsedUri, Operation op) throws IOException {
    Path dirPath = Paths.get(parsedUri);
    if (op == Operation.CREATE) {
      // Check if directory already exists
      if (Files.exists(dirPath)) {
        throw new BaseException(ErrorCode.ALREADY_EXISTS, "Directory already exists: " + dirPath);
      }
      // Create the directory
      try {
        Files.createDirectories(dirPath);
        LOGGER.debug("Directory created successfully: " + dirPath);
      } catch (Exception e) {
        throw new BaseException(ErrorCode.INTERNAL, "Failed to create directory: " + dirPath, e);
      }
    }
    if (op == Operation.DELETE) {
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
        } catch (IOException e) {
          throw new RuntimeException("Failed to delete " + dirPath, e);
        }
      } else {
        throw new IOException("Directory does not exist: " + dirPath);
      }
    }
    return parsedUri;
  }

  private static URI updateS3Directory(URI parsedUri, Operation op, AwsCredentials awsCredentials) {
    throw new BaseException(ErrorCode.UNIMPLEMENTED, "Aws cloud storage updates unimplemented");
  }

  private static URI updateGcDirectory(URI parsedURI, Operation op, GcpOauthToken token) {
    throw new BaseException(ErrorCode.UNIMPLEMENTED, "Google cloud storage updates unimplemented");
  }

  private static URI updateAbsDirectory(
      URI parsedURI, Operation op, AzureUserDelegationSAS credential) {
    throw new BaseException(ErrorCode.UNIMPLEMENTED, "Azure blob storage updates unimplemented");
  }

  private static void validateURI(URI uri) {
    if (uri.getScheme() == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid path: " + uri.getPath());
    }
    URI normalized = uri.normalize();
    if (!normalized.getPath().startsWith(uri.getPath())) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Normalization failed: " + uri.getPath());
    }
  }
}
