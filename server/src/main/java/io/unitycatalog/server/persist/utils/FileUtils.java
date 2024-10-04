package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.CredentialOperations;
import io.unitycatalog.server.service.iceberg.FileIOFactory;
import io.unitycatalog.server.utils.Constants;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

public class FileUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);
  private static final ServerPropertiesUtils properties = ServerPropertiesUtils.getInstance();
  private static final CredentialOperations credentialOps = new CredentialOperations();
  private static final FileIOFactory fileIOFactory = new FileIOFactory(credentialOps);

  private FileUtils() {}

  private static String getStorageRoot() {
    return properties.getProperty("storageRoot");
  }

  public static String createTableDirectory(String tableId) {
    URI standardURI = URI.create(toStandardizedURIString(getStorageRoot() + "/tables/" + tableId));
    return toStandardizedURIString(createDirectory(standardURI).toString());
  }

  public static boolean fileExists(FileIO fileIO, URI fileUri) {
    try {
      InputFile inputFile = fileIO.newInputFile(fileUri.getPath());
      return inputFile.exists(); // Returns true if the file exists, false otherwise
    } catch (Exception e) {
      return false;
    }
  }

  public static URI createDirectory(URI uri) {
    validateURI(uri);
    FileIO fileIO = fileIOFactory.getFileIO(uri);
    if (fileExists(fileIO, uri)) {
      throw new BaseException(ErrorCode.ALREADY_EXISTS, "Directory already exists: " + uri);
    }
    try {
      // Add a trailing slash to represent the directory if not present
      String dirPath = uri.getPath();
//      if (!dirPath.endsWith("/")) {
//        dirPath += "/";
//      }

      // Create a zero-byte file to represent the directory
//      OutputFile outputFile = fileIO.newOutputFile(dirPath);
//      outputFile.createOrOverwrite().close();
      LOGGER.info("Directory created: " + dirPath);
      return URI.create(dirPath);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create directory: " + uri, e);
    }
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
