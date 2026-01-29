package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import lombok.EqualsAndHashCode;

/**
 * A type-safe wrapper for normalized URLs that ensures consistent URL formatting across the Unity
 * Catalog system.
 *
 * <p>This class provides automatic normalization of URLs at construction time, guaranteeing that:
 *
 * <ul>
 *   <li>Local file paths are converted to proper file:/// URIs
 *   <li>Cloud storage URLs (s3://, gs://, abfs://) have consistent formatting
 *   <li>Trailing slashes are removed
 *   <li>Multiple consecutive slashes are collapsed
 * </ul>
 *
 * <p>By using this class instead of plain strings, the type system helps prevent bugs from
 * comparing unnormalized URLs that should be considered equal. For example, "s3://bucket/path/" and
 * "s3://bucket/path" represent the same location and will be normalized to the same form.
 *
 * <p>Usage example:
 *
 * <pre>
 * NormalizedURL url1 = NormalizedURL.from("s3://bucket/path/");
 * NormalizedURL url2 = NormalizedURL.from("s3://bucket/path");
 * assert url1.equals(url2); // true - both normalized to "s3://bucket/path"
 * </pre>
 *
 * <p>This class is immutable and thread-safe.
 *
 * @see UriScheme for supported URI schemes
 */
@EqualsAndHashCode
public final class NormalizedURL {

  private final String url;

  /**
   * Creates a normalized URL from the given input path or URI.
   *
   * @param url the input path or URI to normalize
   */
  private NormalizedURL(String url) {
    this.url = normalize(url);
  }

  /**
   * Creates a normalized URL from the given input path or URI. Returns null if the input is null.
   *
   * @param url the input path or URI to normalize
   */
  public static NormalizedURL from(String url) {
    if (url == null) {
      return null;
    } else {
      return new NormalizedURL(url);
    }
  }

  /**
   * Returns the normalized URL as a string.
   *
   * @return the normalized URL string, never null
   */
  @Override
  public String toString() {
    return url;
  }

  /**
   * Converts the normalized URL to a URI object.
   *
   * @return the URI representation of this normalized URL
   * @throws IllegalArgumentException if the URL string violates RFC 2396
   */
  public URI toUri() {
    return URI.create(url);
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
  public static String normalize(String inputPath) {
    if (inputPath == null || inputPath.isBlank()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Path cannot be null or empty");
    }
    if (!inputPath.contains("/")) {
      // A path containing no / is very likely a malformed path or it's intended to be a name.
      // In any case we reject it.
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Ambiguous path: " + inputPath);
    }
    // Check if the path is already a URI with a valid scheme
    URI uri;
    try {
      uri = new URI(inputPath).normalize();
    } catch (URISyntaxException e) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Unsupported path: " + inputPath);
    }
    UriScheme scheme = UriScheme.fromURI(uri);
    return switch (scheme) {
      // It's a local path without file://. Construct a file:// URI using Path.
      case NULL -> localFileURIToString(Paths.get(inputPath).toAbsolutePath().toUri().normalize());
      case FILE -> localFileURIToString(uri);
      case S3, GS, ABFS, ABFSS -> removeExtraSlashes(uri.toString());
    };
  }

  /**
   * This helper function adjusts local file URI that starts with file:/ or file:// but not with
   * file:///. This function makes sure these URIs must begin with file:/// in order to be a valid
   * local file URI.
   */
  private static String localFileURIToString(URI fileUri) {
    String uriString = fileUri.toString();
    // Ensure the URI starts with "file:///" for absolute paths
    if (uriString.startsWith("file:/")) {
      String path = uriString.substring(5);
      uriString = "file://" + removeExtraSlashes(path);
    }
    return uriString;
  }

  /**
   * Removes leading and trailing slashes from a path string, keeping only one leading slash if
   * present.
   *
   * <p>This method normalizes paths by:
   *
   * <ul>
   *   <li>Reducing multiple leading slashes to a single slash
   *   <li>Removing all trailing slashes
   *   <li>Preserving the internal structure of the path
   * </ul>
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>"///a/b///" -> "/a/b"
   *   <li>"///////" -> "/"
   *   <li>"a/b////" -> "a/b"
   *   <li>"" -> ""
   *   <li>null -> null
   * </ul>
   *
   * @param path The path string to normalize, may be null
   * @return The normalized path, or null if input is null
   */
  private static String removeExtraSlashes(String path) {
    if (path == null || path.length() <= 1) {
      return path;
    }

    // 1. find the first non-slash
    int start = 0;
    while (start < path.length() && path.charAt(start) == '/') {
      start++;
    }
    // Keep the first slash if any
    if (start > 0) {
      start--;
    }

    // 2. find the last non-slash. stop before when reaching start.
    int end = path.length() - 1;
    while (end > start && path.charAt(end) == '/') {
      end--;
    }
    // Advance end to point to the first trailing slashes to remove, or path.length() if nothing to
    // remove
    end++;

    return path.substring(start, end);
  }

  public NormalizedURL getStorageBase() {
    URI uri = URI.create(url);
    return NormalizedURL.from(uri.getScheme() + "://" + uri.getAuthority());
  }
}
