package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.CredentialOperations;
import io.unitycatalog.server.service.iceberg.FileIOFactory;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.ServerProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

public class FileUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);
  private static final ServerProperties properties = ServerProperties.getInstance();
  private static final CredentialOperations credentialOps = new CredentialOperations();
  private static final FileIOFactory fileIOFactory = new FileIOFactory(credentialOps);

  private FileUtils() {}

  private static String getStorageRoot() {
    // Use local tmp directory as default storage root
    return properties.getProperty("storageRoot", "file:/tmp");
  }

  public static String createTableDirectory(String tableId) {
    String directoryUriString = toStandardizedURIString(getStorageRoot() + "/tables/" + tableId);
    URI directoryUri = URI.create(directoryUriString);
    validateURI(directoryUri);
    FileIO fileIO = fileIOFactory.getFileIO(directoryUri);
    if (fileExists(fileIO, directoryUri)) {
      throw new BaseException(ErrorCode.ALREADY_EXISTS, "Table directory already exists: " + directoryUri);
    }
    return directoryUriString;
  }

  public static boolean fileExists(FileIO fileIO, URI fileUri) {
    InputFile inputFile = fileIO.newInputFile(fileUri.getPath());
    return inputFile.exists(); // Returns true if the file exists, false otherwise
  }

  public static void deleteDirectory(String path) {
    URI directoryUri = URI.create(toStandardizedURIString(path));
    validateURI(directoryUri);
    FileIO fileIO = fileIOFactory.getFileIO(directoryUri);
    fileIO.deleteFile(directoryUri.getPath());
    LOGGER.info("Directory deleted: " + directoryUri);
  }

  private static URI adjustLocalFileURI(URI fileUri) {
    String uriString = fileUri.toString();
    // Ensure the URI starts with "file:///" for absolute paths
    if (uriString.startsWith("file:/") && !uriString.startsWith("file:///")) {
      uriString = "file://" + uriString.substring(5);
    }
    return URI.create(uriString);
  }


  /**
   * Converts a given input path or URI into a standardized URI string.
   * This method ensures that local file paths are correctly formatted as file URIs
   * and that URIs for different storage providers (e.g., S3, Azure, GCS) are handled appropriately.
   *
   * <p>If the input is a valid URI with a recognized scheme (e.g., "file", "s3", "abfs", etc.),
   * the method returns a standardized version of the URI. If the input is not a valid URI,
   * it treats the input as a local file path and converts it to a "file://" URI.</p>
   *
   * @param inputPath the input path or URI to be standardized.
   * @return the standardized URI string.
   * @throws BaseException if the input path has an unsupported URI scheme.
   * @throws URISyntaxException if the input path is an invalid URI and cannot be parsed.
   *
   * <p>Examples of input and output:</p>
   *
   * <pre>
   * // Local File System Example:
   * "file:/tmp/myfile"         -> "file:///tmp/myfile"
   *
   * // AWS S3 Example:
   * "s3://my-bucket/my-file"   -> "s3://my-bucket/my-file"
   *
   * // Azure Blob Storage Example:
   * "abfs://my-container@my-storage.dfs.core.windows.net/my-file"
   *                          -> "abfs://my-container@my-storage.dfs.core.windows.net/my-file"
   *
   * // Google Cloud Storage Example:
   * "gs://my-bucket/my-file"   -> "gs://my-bucket/my-file"
   *
   * // Invalid Path Example (treated as a file path):
   * "/local/path/to/file"      -> "file:///local/path/to/file"
   *
   * // Unsupported Scheme Example:
   * "ftp://example.com/file"   -> Throws BaseException with message: "Unsupported URI scheme: ftp"
   * </pre>
   */
  public static String toStandardizedURIString(String inputPath) {
    try {
      // Check if the path is already a URI with a valid scheme
      URI uri = new URI(inputPath);
      // If it's a file URI, standardize it
      if (uri.getScheme() != null) {
        return switch (uri.getScheme()) {
          case Constants.URI_SCHEME_FILE -> adjustLocalFileURI(uri).toString();
          case Constants.URI_SCHEME_S3, Constants.URI_SCHEME_ABFS, Constants.URI_SCHEME_ABFSS, Constants.URI_SCHEME_GS ->
                  uri.toString();
          default -> throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Unsupported URI scheme: " + uri.getScheme());
        };
      }
    } catch (URISyntaxException e) {
      // Not a valid URI, treat it as a file path
    }
    return Paths.get(inputPath).toUri().toString();
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
