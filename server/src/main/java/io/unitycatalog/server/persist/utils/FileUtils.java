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

public class FileUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);
  private static final ServerPropertiesUtils properties = ServerPropertiesUtils.getInstance();

  private FileUtils() {}

  private static String getStorageRoot() {
    return properties.getProperty("storageRoot");
  }

  // This is temporary until storageRoot is fully supported
  private static String getModelStorageRoot() {
    return properties.getProperty("registeredModelStorageRoot");
  }

  private static String getDirectoryURI(String entityFullName) {
    return getStorageRoot() + "/" + entityFullName.replace(".", "/");
  }

  private static String getModelDirectoryURI(String entityFullName) {
    return getModelStorageRoot() + "/" + entityFullName.replace(".", "/");
  }

  public static String getModelStorageLocation(
      String catalogName, String schemaName, String modelName) {
    return getModelDirectoryURI(catalogName + "." + schemaName + ".models." + modelName);
  }

  public static String createVolumeDirectory(String volumeName) {
    String absoluteUri = getDirectoryURI(volumeName);
    return createDirectory(absoluteUri).toString();
  }

  public static String createTableDirectory(
      String catalogName, String schemaName, String tableName) {
    String absoluteUri = getDirectoryURI(catalogName + "." + schemaName + ".tables." + tableName);
    return createDirectory(absoluteUri).toString();
  }

  public static String createRegisteredModelDirectory(
      String catalogName, String schemaName, String modelName) {
    String absoluteUri =
        getModelDirectoryURI(catalogName + "." + schemaName + ".models." + modelName);
    return createDirectory(absoluteUri).toString();
  }

  public static String createModelVersionDirectory(
      String catalogName, String schemaName, String modelName) {
    String absoluteUri =
        getModelDirectoryURI(catalogName + "." + schemaName + ".models." + modelName + ".versions");
    return createDirectory(absoluteUri).toString();
  }

  private static URI createDirectory(String uri) {
    URI parsedUri = createURI(uri);
    validateURI(parsedUri);
    if (uri.startsWith("s3://")) {
      return modifyS3Directory(parsedUri, true);
    } else {
      return adjustFileUri(createLocalDirectory(Paths.get(parsedUri)));
    }
  }

  private static URI createURI(String uri) {
    if (uri.startsWith("s3://") || uri.startsWith("file:")) {
      return URI.create(uri);
    } else {
      return Paths.get(uri).toUri();
    }
  }

  private static URI createLocalDirectory(Path dirPath) {
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
    return dirPath.toUri();
  }

  public static void deleteDirectory(String path) {
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

  private static URI modifyS3Directory(URI parsedUri, boolean createOrDelete) {
    String bucketName = parsedUri.getHost();
    String path = parsedUri.getPath().substring(1); // Remove leading '/'
    String accessKey = ServerPropertiesUtils.getInstance().getProperty("aws.s3.accessKey");
    String secretKey = ServerPropertiesUtils.getInstance().getProperty("aws.s3.secretKey");
    String sessionToken = ServerPropertiesUtils.getInstance().getProperty("aws.s3.sessionToken");
    String region = ServerPropertiesUtils.getInstance().getProperty("aws.region");

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
        LOGGER.debug("Directory created successfully: " + path);
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
    if (url.startsWith("s3://")) {
      return url;
    } else {
      return adjustFileUri(createURI(url)).toString();
    }
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
