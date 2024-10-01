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
import io.unitycatalog.server.service.credential.CredentialOperations;
import io.unitycatalog.server.service.iceberg.FileIOFactory;
import io.unitycatalog.server.utils.Constants;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);
  private static final ServerPropertiesUtils properties = ServerPropertiesUtils.getInstance();
  private static final CredentialOperations credentialOps = new CredentialOperations();
  private static final FileIOFactory fileIOFactory = new FileIOFactory(credentialOps);

  private FileUtils() {}

  private static String getStorageRoot() {
    return properties.getProperty("storageRoot");
  }

  public static String createEntityDirectory(String entityId) {
    URI standardURI = URI.create(toStandardizedURIString(getStorageRoot() + "/" + entityId));
    validateURI(standardURI);
    return createDirectory(standardURI).toString();
  }

  public static boolean fileExists(FileIO fileIO, URI fileUri) {
    try {
      InputFile inputFile = fileIO.newInputFile(fileUri.toString());
      return inputFile.exists(); // Returns true if the file exists, false otherwise
    } catch (Exception e) {
      // Optionally log or handle exceptions
      return false;
    }
  }

  public static URI createDirectory(URI uri) {
    FileIO fileIO = fileIOFactory.getFileIO(uri);
    if (fileExists(fileIO, uri)) {
      throw new BaseException(ErrorCode.ALREADY_EXISTS, "Directory already exists: " + uri);
    }
    try {
      // Add a trailing slash to represent the directory if not present
      String dirPath = uri.toString();
      if (!dirPath.endsWith("/")) {
        dirPath += "/";
      }

      // Create a zero-byte file to represent the directory
      OutputFile outputFile = fileIO.newOutputFile(dirPath + ".dir");
      outputFile.createOrOverwrite().close();
      System.out.println("Directory created: " + dirPath);
      return URI.create(dirPath);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create directory: " + uri, e);
    }
  }

  public static void deleteDirectory(String path) {
    URI directoryUri = URI.create(toStandardizedURIString(path));
    validateURI(directoryUri);
    if (directoryUri.getScheme() == null || directoryUri.getScheme().equals("file")) {
//      try {
//        deleteLocalDirectory(Paths.get(directoryUri));
//      } catch (RuntimeException | IOException e) {
//        throw new BaseException(ErrorCode.INTERNAL, "Failed to delete directory: " + path, e);
//      }
    } else if (directoryUri.getScheme().equals("s3")) {
      modifyS3Directory(directoryUri, false);
    } else {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Unsupported URI scheme: " + directoryUri.getScheme());
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

  private static URI adjustLocalFileURI(URI fileUri) {
    String uriString = fileUri.toString();
    // Ensure the URI starts with "file:///" for absolute paths
    if (uriString.startsWith("file:/") && !uriString.startsWith("file:///")) {
      uriString = "file://" + uriString.substring(5);
    }
    return URI.create(uriString);
  }

  public static String toStandardizedURIString(String inputPath) {
    try {
      // Check if the path is already a URI with a valid scheme
      URI uri = new URI(inputPath);
      // If it's a file URI, standardize it
      if (uri.getScheme() != null) {
        return switch (uri.getScheme()) {
          case "file" -> adjustLocalFileURI(uri).toString();
          case Constants.URI_SCHEME_S3, Constants.URI_SCHEME_ABFS, Constants.URI_SCHEME_ABFSS, Constants.URI_SCHEME_GS ->
                  uri.toString();
          default -> throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT, "Unsupported URI scheme: " + uri.getScheme());
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
