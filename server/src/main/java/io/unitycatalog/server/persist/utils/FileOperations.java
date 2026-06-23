package io.unitycatalog.server.persist.utils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.UriScheme;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
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

  public FileOperations(ServerProperties serverProperties) {
    this.serverProperties = serverProperties;
  }

  /** Delete entire directory recursively. */
  public void deleteDirectory(NormalizedURL url) throws IOException {
    switch (UriScheme.fromURI(url.toUri())) {
      case FILE, NULL -> deleteLocalDirectory(url);
      case S3 -> modifyS3Directory(url, false);
      // Currently we can NOT delete the path in cloud storage. We will update this in future
      // when UC OSS begins using the hadoopfs libraries.
      case GS -> {}
      case ABFS, ABFSS -> {}
    }
  }

  private static void deleteLocalDirectory(NormalizedURL url) throws IOException {
    Path dirPath = Paths.get(url.toUri());
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

  /** Create a directory for storage location. */
  public void createStorageLocationDir(NormalizedURL url) {
    switch (UriScheme.fromURI(url.toUri())) {
      case FILE, NULL -> createLocalDirectory(url);
      case S3 -> modifyS3Directory(url, true);
      // Currently we can NOT create the directory in cloud storage. We will update this in future
      // when UC OSS begins using the hadoopfs libraries.
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

  private void modifyS3Directory(NormalizedURL url, boolean createOrDelete) {
    URI parsedUri = url.toUri();
    String bucketName = parsedUri.getHost();
    String path = parsedUri.getPath();
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    AmazonS3 s3Client = buildS3Client(url);

    if (createOrDelete) {
      if (!path.isEmpty() && !path.endsWith("/")) {
        path += "/";
      }
      if (s3Client.doesObjectExist(bucketName, path)) {
        throw new BaseException(ErrorCode.ALREADY_EXISTS, "Directory already exists: " + url);
      }
      try {
        byte[] emptyContent = new byte[0];
        ByteArrayInputStream emptyContentStream = new ByteArrayInputStream(emptyContent);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        s3Client.putObject(new PutObjectRequest(bucketName, path, emptyContentStream, metadata));
        LOGGER.debug("Directory created successfully: {}", url);
      } catch (Exception e) {
        throw new BaseException(ErrorCode.INTERNAL, "Failed to create directory: " + url, e);
      }
    } else {
      ListObjectsRequest req = new ListObjectsRequest().withBucketName(bucketName).withPrefix(path);
      ObjectListing listing;
      do {
        listing = s3Client.listObjects(req);
        listing
            .getObjectSummaries()
            .forEach(objectSummary -> s3Client.deleteObject(bucketName, objectSummary.getKey()));
        req.setMarker(listing.getNextMarker());
      } while (listing.isTruncated());
    }
  }

  private AmazonS3 buildS3Client(NormalizedURL url) {
    S3StorageConfig config = resolveS3Config(url);
    String accessKey = config.getAccessKey();
    String secretKey = config.getSecretKey();
    String sessionToken = config.getSessionToken();
    String region = config.getRegion();
    if (accessKey == null || secretKey == null) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "S3 credentials not configured for " + url.getStorageBase());
    }
    if (region == null || region.isEmpty()) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION, "S3 region not configured for " + url.getStorageBase());
    }

    BasicSessionCredentials sessionCredentials =
        new BasicSessionCredentials(accessKey, secretKey, sessionToken);
    AmazonS3ClientBuilder s3ClientBuilder =
        AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
            .withRegion(region);

    String endpointUrl = resolveEndpointUrl(config);
    if (endpointUrl != null && !endpointUrl.isEmpty()) {
      s3ClientBuilder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(endpointUrl, region));
    }
    return s3ClientBuilder.build();
  }

  private S3StorageConfig resolveS3Config(NormalizedURL url) {
    S3StorageConfig perBucketConfig =
        serverProperties.getS3Configurations().get(url.getStorageBase());
    if (perBucketConfig != null) {
      return perBucketConfig;
    }
    return serverProperties.getS3MasterRoleConfiguration();
  }

  private String resolveEndpointUrl(S3StorageConfig config) {
    if (config.getEndpointUrl() != null && !config.getEndpointUrl().isEmpty()) {
      return config.getEndpointUrl();
    }
    String endpointUrl = serverProperties.get(Property.AWS_S3_ENDPOINT_URL);
    if (endpointUrl != null && !endpointUrl.isEmpty()) {
      return endpointUrl;
    }
    return serverProperties.get(Property.AWS_ENDPOINT_URL);
  }
}
