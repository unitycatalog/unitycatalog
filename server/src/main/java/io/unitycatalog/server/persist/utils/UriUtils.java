package io.unitycatalog.server.persist.utils;

import static io.unitycatalog.server.utils.Constants.URI_SCHEME_ABFS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_ABFSS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_GS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_S3;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.TemporaryCredentials;
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

  private enum Operation {
    CREATE,
    DELETE
  }

  private UriUtils() {}

  public static String createStorageLocationPath(String uri) {
    return updateDirectoryFromUri(uri, Operation.CREATE, Optional.empty()).toString();
  }

  public static String deleteStorageLocationPath(String uri) {
    return updateDirectoryFromUri(uri, Operation.DELETE, Optional.empty()).toString();
  }

  private static URI updateDirectoryFromUri(
      String uri, Operation op, Optional<TemporaryCredentials> credentials) {
    URI parsedUri = URI.create(uri);
    validateURI(parsedUri);
    try {
      if (parsedUri.getScheme().equals("file")) {
        return updateLocalDirectory(parsedUri, op);
      } else if (parsedUri.getScheme().equals(URI_SCHEME_S3)) {
        // For v0.2, we will NOT create the path in cloud storage since MLflow uses the native cloud
        // clients and not the hadoopfs libraries.  We will update this in v0.3 when UC OSS begins
        // using the hadoopfs libraries.
        /* return updateS3Directory(parsedUri, op, credentials.get().getAwsTempCredentials()); */
        return parsedUri;
      } else if (parsedUri.getScheme().equals(URI_SCHEME_GS)) {
        // For v0.2, we will NOT create the path in cloud storage since MLflow uses the native cloud
        // clients and not the hadoopfs libraries.  We will update this in v0.3 when UC OSS begins
        // using the hadoopfs libraries.
        /* return updateGcDirectory(parsedUri, op, credentials.get().getGcpOauthToken()); */
        return parsedUri;
      } else if (parsedUri.getScheme().equals(URI_SCHEME_ABFS)
          || parsedUri.getScheme().equals(URI_SCHEME_ABFSS)) {
        // For v0.2, we will NOT create the path in cloud storage since MLflow uses the native cloud
        // clients and not the hadoopfs libraries.  We will update this in v0.3 when UC OSS begins
        // using the hadoopfs libraries.
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

  public static boolean isValidURI(String uri) {
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
        LOGGER.debug("Directory created successfully: {}", dirPath);
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
