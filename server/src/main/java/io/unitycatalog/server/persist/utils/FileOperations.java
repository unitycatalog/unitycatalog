package io.unitycatalog.server.persist.utils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    validateURI(directoryUri);
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
    String accessKey = serverProperties.getProperty("aws.s3.accessKey");
    String secretKey = serverProperties.getProperty("aws.s3.secretKey");
    String sessionToken = serverProperties.getProperty("aws.s3.sessionToken");
    String region = serverProperties.getProperty("aws.region");

    BasicSessionCredentials sessionCredentials =
        new BasicSessionCredentials(accessKey, secretKey, sessionToken);
    AmazonS3 s3Client =
        AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
            .withRegion(region)
            .build();

    if (createOrDelete) {

      if (!path.endsWith("/")) {
        path += "/";
      }
      if (s3Client.doesObjectExist(bucketName, path)) {
        throw new BaseException(ErrorCode.ALREADY_EXISTS, "Directory already exists: " + path);
      }
      try {
        // Create empty content
        byte[] emptyContent = new byte[0];
        ByteArrayInputStream emptyContentStream = new ByteArrayInputStream(emptyContent);

        // Set metadata for the empty content
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        s3Client.putObject(new PutObjectRequest(bucketName, path, emptyContentStream, metadata));
        LOGGER.debug("Directory created successfully: {}", path);
        return URI.create(String.format("s3://%s/%s", bucketName, path));
      } catch (Exception e) {
        throw new BaseException(ErrorCode.INTERNAL, "Failed to create directory: " + path, e);
      }
    } else {
      ObjectListing listing;
      ListObjectsRequest req = new ListObjectsRequest().withBucketName(bucketName).withPrefix(path);
      do {
        listing = s3Client.listObjects(req);
        listing
            .getObjectSummaries()
            .forEach(
                object -> {
                  s3Client.deleteObject(bucketName, object.getKey());
                });
        req.setMarker(listing.getNextMarker());
      } while (listing.isTruncated());
      return URI.create(String.format("s3://%s/%s", bucketName, path));
    }
  }

  private static URI adjustFileUri(URI fileUri) {
    String uriString = fileUri.toString();
    // Ensure the URI starts with "file:///" for absolute paths
    if (uriString.startsWith("file:/") && !uriString.startsWith("file:///")) {
      uriString = "file://" + uriString.substring(5);
    }
    return URI.create(uriString);
  }

  public static String convertRelativePathToURI(String url) {
    if (url == null) {
      return null;
    }
    if (isSupportedCloudStorageUri(url)) {
      return url;
    } else {
      return adjustFileUri(createURI(url)).toString();
    }
  }

  public static boolean isSupportedCloudStorageUri(String url) {
    String scheme = URI.create(url).getScheme();
    return scheme != null && Constants.SUPPORTED_SCHEMES.contains(scheme);
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

  public static void assertValidLocation(String location) {
    validateURI(URI.create(location));
  }
}
