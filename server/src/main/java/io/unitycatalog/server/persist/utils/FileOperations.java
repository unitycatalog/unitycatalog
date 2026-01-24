package io.unitycatalog.server.persist.utils;

import static io.unitycatalog.server.utils.Constants.MANAGED_STORAGE_CATALOG_PREFIX;
import static io.unitycatalog.server.utils.Constants.MANAGED_STORAGE_SCHEMA_PREFIX;

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
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.io.ByteArrayInputStream;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileOperations {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileOperations.class);
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
    } else if (directoryUri.getScheme().equals("s3")) {
      modifyS3Directory(directoryUri, false);
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

  private URI modifyS3Directory(URI parsedUri, boolean createOrDelete) {
    String bucketName = parsedUri.getHost();
    String path = parsedUri.getPath().substring(1); // Remove leading '/'
    String accessKey = serverProperties.get(Property.AWS_S3_ACCESS_KEY);
    String secretKey = serverProperties.get(Property.AWS_S3_SECRET_KEY);
    String sessionToken = serverProperties.get(Property.AWS_S3_SESSION_TOKEN);
    String region = serverProperties.get(Property.AWS_REGION);

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

  private NormalizedURL createManagedEntityDirectory(
      NormalizedURL storageRoot, String prefix, UUID entityId) {
    return NormalizedURL.from(
        String.join("/", List.of(storageRoot.toString(), prefix, entityId.toString())));
  }

  public NormalizedURL createManagedSchemaDirectory(NormalizedURL storageRoot, UUID schemaId) {
    return createManagedEntityDirectory(storageRoot, MANAGED_STORAGE_SCHEMA_PREFIX, schemaId);
  }

  public NormalizedURL createManagedCatalogDirectory(NormalizedURL storageRoot, UUID catalogId) {
    return createManagedEntityDirectory(storageRoot, MANAGED_STORAGE_CATALOG_PREFIX, catalogId);
  }

  // The following methods do not add a __unitystorage prefix because the storageRoot
  // is expected to be the storageLocation of a catalog or schema, which already includes
  // the __unitystorage prefix.

  public NormalizedURL createManagedTableDirectory(NormalizedURL storageRoot, UUID tableId) {
    return createManagedEntityDirectory(storageRoot, "tables", tableId);
  }

  public NormalizedURL createManagedVolumeDirectory(NormalizedURL storageRoot, UUID volumeId) {
    return createManagedEntityDirectory(storageRoot, "volumes", volumeId);
  }

  public NormalizedURL createManagedModelDirectory(NormalizedURL storageRoot, UUID modelId) {
    return createManagedEntityDirectory(storageRoot, "models", modelId);
  }
}
