package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.AwsUtils;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

public class FileOperations {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileOperations.class);
  private final ServerProperties serverProperties;
  private static String modelStorageRootCached;
  private static String modelStorageRootPropertyCached;

  public FileOperations(ServerProperties serverProperties) {
    this.serverProperties = serverProperties;
  }

  /**
   * TODO: Deprecate this method once unit tests are self contained and this class gets
   * re-instantiated with each test. Property updates shouldn't affect the instantiated class and we
   * should require a server restart if the properties file is updated.
   */
  private static void reset() {
    modelStorageRootPropertyCached = null;
    modelStorageRootCached = null;
  }

  // Model specific storage root handlers and convenience methods
  private String getModelStorageRoot() {
    String currentModelStorageRoot = serverProperties.get(Property.MODEL_STORAGE_ROOT);
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
    if (!UriUtils.isValidURI(modelStorageRoot)) {
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

  private String getModelDirectoryURI(String entityFullName) {
    return getModelStorageRoot() + "/" + entityFullName.replace(".", "/");
  }

  public String getModelStorageLocation(String catalogId, String schemaId, String modelId) {
    return getModelDirectoryURI(catalogId + "." + schemaId + ".models." + modelId);
  }

  public String getModelVersionStorageLocation(
      String catalogId, String schemaId, String modelId, String versionId) {
    return getModelDirectoryURI(
        catalogId + "." + schemaId + ".models." + modelId + ".versions." + versionId);
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
    } else if (directoryUri.getScheme().equals("s3")) {
      modifyS3Directory(directoryUri, false);
    } else {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Unsupported URI scheme: " + directoryUri.getScheme());
    }
  }

  private static void deleteLocalDirectory(Path dirPath) throws IOException {
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
      throw new IOException("Directory does not exist: " + dirPath);
    }
  }

  private URI modifyS3Directory(URI parsedUri, boolean createOrDelete) {
    String bucketName = parsedUri.getHost();
    String path = parsedUri.getPath().substring(1); // Remove leading '/'
    String accessKey = serverProperties.get(Property.AWS_S3_ACCESS_KEY);
    String secretKey = serverProperties.get(Property.AWS_S3_SECRET_KEY);
    String sessionToken = serverProperties.get(Property.AWS_S3_SESSION_TOKEN);
    String region = serverProperties.get(Property.AWS_REGION);
    String endpointUrl = serverProperties.get(Property.AWS_S3_ENDPOINT_URL);

    AwsCredentialsProvider awsCredentialsProvider =
        AwsUtils.getAwsCredentialsProvider(accessKey, secretKey, sessionToken);
    S3Client s3Client = AwsUtils.getS3Client(awsCredentialsProvider, region, endpointUrl);

    if (createOrDelete) {

      if (!path.endsWith("/")) {
        path += "/";
      }
      if (AwsUtils.doesObjectExist(s3Client, bucketName, path)) {
        throw new BaseException(ErrorCode.ALREADY_EXISTS, "Directory already exists: " + path);
      }
      try {
        // Create a zero-byte object to represent the directory
        s3Client.putObject(
            PutObjectRequest.builder().bucket(bucketName).key(path).build(), RequestBody.empty());
        LOGGER.debug("Directory created successfully: {}", path);
        return URI.create(String.format("s3://%s/%s", bucketName, path));
      } catch (Exception e) {
        throw new BaseException(ErrorCode.INTERNAL, "Failed to create directory: " + path, e);
      }
    } else {
      ListObjectsV2Request req =
          ListObjectsV2Request.builder().bucket(bucketName).prefix(path).build();
      s3Client.listObjectsV2Paginator(req).stream()
          .flatMap(r -> r.contents().stream())
          .forEach(
              object -> {
                DeleteObjectRequest deleteRequest =
                    DeleteObjectRequest.builder().bucket(bucketName).key(object.key()).build();
                s3Client.deleteObject(deleteRequest);
              });

      return URI.create(String.format("s3://%s/%s", bucketName, path));
    }
  }

  /**
   * This helper function adjusts local file URI that starts with file:/ or file:// but not with
   * file:///. This function makes sure these URIs must begin with file:/// in order to be a valid
   * local file URI.
   */
  private static URI adjustLocalFileURI(URI fileUri) {
    String uriString = fileUri.toString();
    // Ensure the URI starts with "file:///" for absolute paths
    if (uriString.startsWith("file:/") && !uriString.startsWith("file:///")) {
      uriString = "file://" + uriString.substring(5);
    }
    return URI.create(uriString);
  }

  public static void assertValidLocation(String location) {
    UriUtils.validateURI(URI.create(location));
  }

  /**
   * Converts a given input path or URI into a standardized URI string. This method ensures that
   * local file paths are correctly formatted as file URIs and that URIs for different storage
   * providers (e.g., S3, Azure, GCS) are handled appropriately.
   *
   * <p>If the input is a valid URI with a recognized scheme (e.g., "file", "s3", "abfs", etc.), the
   * method returns a standardized version of the URI. If the input is not a valid URI, it treats
   * the input as a local file path and converts it to a "file://" URI.
   *
   * @param inputPath the input path or URI to be standardized.
   * @return the standardized URI string.
   * @throws BaseException if the input path has an unsupported URI scheme.
   *     <p>Examples of input and output:
   *     <pre>
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
    // Check if the path is already a URI with a valid scheme
    URI uri;
    try {
      uri = new URI(inputPath);
    } catch (URISyntaxException e) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Unsupported path: " + inputPath);
    }
    // If it's a file URI, standardize it
    if (uri.getScheme() != null) {
      if (uri.getScheme().equals(Constants.URI_SCHEME_FILE)) {
        return adjustLocalFileURI(uri).toString();
      } else if (Constants.SUPPORTED_CLOUD_SCHEMES.contains(uri.getScheme())) {
        return uri.toString();
      } else {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT, "Unsupported URI scheme: " + uri.getScheme());
      }
    }
    String localUri = Paths.get(inputPath).toUri().toString();
    if (!inputPath.endsWith("/") && localUri.endsWith("/")) {
      // A special case where the local inputPath is a directory already exist, generated localUri
      // will have an extra trailing slash. Remove it to make it consistent.
      localUri = localUri.substring(0, localUri.length() - 1);
    }
    return localUri;
  }

  private String getManagedTablesStorageRoot() {
    // Use local tmp directory as default storage root
    return serverProperties.get(Property.TABLE_STORAGE_ROOT);
  }

  /**
   * This function does not actually create a directory. But it only returns the constructed path.
   */
  public String createTableDirectory(String tableId) {
    String directoryUriString = getManagedTablesStorageRoot() + "/tables/" + tableId;
    return toStandardizedURIString(directoryUriString);
  }
}
