package io.unitycatalog.cli.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

public class FileOperations {

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
   * @throws CliException if the input path has an unsupported URI scheme.
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
      throw new CliException(
          "Storage location must be a valid URL or local filesystem path: " + inputPath);
    }
    // If it's a file URI, standardize it
    if (uri.getScheme() != null) {
      if (uri.getScheme().equals(Constants.URI_SCHEME_FILE)) {
        return adjustLocalFileURI(uri).toString();
      } else if (Constants.SUPPORTED_CLOUD_SCHEMES.contains(uri.getScheme())) {
        return uri.toString();
      } else {
        throw new CliException("Unsupported URI scheme: " + uri.getScheme());
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
}
