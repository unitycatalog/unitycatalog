package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import org.apache.hadoop.fs.Path;

/**
 * A type-safe wrapper for normalized URLs that ensures consistent URL formatting across the Unity
 * Catalog system.
 *
 * <p>This class provides automatic normalization of URLs at construction time, guaranteeing that:
 *
 * <ul>
 *   <li>Local file paths are converted to proper file:/// URIs
 *   <li>Local file URIs whose escapes would change the path they name are rejected
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
   * Creates a normalized URL from the given URI. Returns null if the input is null. A cloud URI's
   * decoded path is the literal object name, so {@code s3://b/team%20reports} names {@code team
   * reports}. A cloud URI with a query or fragment is rejected.
   *
   * @param uri the URI to normalize
   */
  public static NormalizedURL from(URI uri) {
    if (uri == null) {
      return null;
    }
    // A cloud URI escapes its path, but the stored location is the literal path string.
    return switch (UriScheme.fromURI(uri)) {
      case S3, GS, ABFS, ABFSS -> {
        if (uri.getRawQuery() != null || uri.getRawFragment() != null) {
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT,
              "Cloud storage location cannot contain '?' or '#': " + uri);
        }
        yield new NormalizedURL(
            uri.getScheme() + "://" + Objects.toString(uri.getRawAuthority(), "") + uri.getPath());
      }
      case FILE, NULL -> new NormalizedURL(uri.toString());
    };
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
   * <p>For a cloud location this is the URI Hadoop builds from the path string, so its decoded path
   * is the literal object name.
   *
   * @return the URI representation of this normalized URL
   * @throws IllegalArgumentException if a non-cloud URL string violates RFC 2396
   */
  public URI toUri() {
    URI cloudUri = cloudUri(url);
    return cloudUri != null ? cloudUri : URI.create(url);
  }

  /** Returns the URI Hadoop builds for a cloud location, or null for any other location. */
  private static URI cloudUri(String location) {
    URI uri;
    try {
      uri = new Path(location).toUri();
    } catch (IllegalArgumentException e) {
      return null;
    }
    return switch (UriScheme.fromURI(uri)) {
      case S3, GS, ABFS, ABFSS -> uri;
      case FILE, NULL -> null;
    };
  }

  /**
   * Returns the path as interpreted by storage clients, with a leading slash when it is nonempty.
   *
   * <p>Cloud object names keep percent sequences literally, matching the stored location and Hadoop
   * string paths. Local file paths use URI decoding to identify the file-system path.
   */
  public String getStoragePath() {
    return toUri().getPath();
  }

  /**
   * Returns whether this URL identifies an entire cloud storage bucket or container.
   *
   * <p>A cloud root's literal path is empty or only slashes; an encoded {@code %2F} is part of an
   * object name, not a separator.
   */
  public boolean isCloudStorageRoot() {
    URI uri = toUri();
    return switch (UriScheme.fromURI(uri)) {
      case S3, GS, ABFS, ABFSS -> {
        String path = getStoragePath();
        yield path == null || path.chars().allMatch(character -> character == '/');
      }
      default -> false;
    };
  }

  /**
   * Converts a given input path or URI into a standardized URI string. This method ensures that
   * local file paths are correctly formatted as file URIs and that URIs for different storage
   * providers (e.g., S3, Azure, GCS) are handled appropriately.
   *
   * <p>A cloud location ("s3", "gs", "abfs", "abfss") is a literal path string, as in Hadoop: a
   * space or '%' stays part of the object name, so the result need not be a valid URI. Any other
   * input with a recognized scheme is returned as a standardized URI. If the input is not a valid
   * URI, it treats the input as a local file path and converts it to a "file://" URI.
   *
   * @param inputPath the input path or URI to be standardized.
   * @return the standardized location string.
   * @throws BaseException if the input path has an unsupported URI scheme, or is a cloud location
   *     containing '?' or '#' or an invalid bucket or container.
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
    // A cloud object name is literal, as in Hadoop: a space or '%' is part of the name.
    URI cloudUri = cloudUri(inputPath);
    if (cloudUri != null) {
      // Iceberg's S3 and GCS FileIO cut a path at '?' or '#', so such a name can't be addressed
      // safely; ADLS rejects them too, for one rule across clouds.
      if (inputPath.indexOf('?') >= 0 || inputPath.indexOf('#') >= 0) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Cloud storage location cannot contain '?' or '#': " + inputPath);
      }
      // Only the object path is literal: the bucket or container must still be a valid authority.
      String root = cloudUri.getScheme() + "://" + Objects.toString(cloudUri.getAuthority(), "");
      if (cloudUri.getAuthority() != null) {
        try {
          new URI(root);
        } catch (URISyntaxException e) {
          throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Unsupported path: " + inputPath);
        }
      }
      return removeExtraSlashes(root + cloudUri.getPath());
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
      case NULL ->
          validateLocalFileURI(
              localFileURIToString(Paths.get(inputPath).toAbsolutePath().toUri().normalize()),
              inputPath);
      // A host is read as the first directory (file://tmp/x is /tmp/x): check after folding it in.
      case FILE -> validateLocalFileURI(localFileURIToString(uri), inputPath);
      case S3, GS, ABFS, ABFSS -> removeExtraSlashes(uri.toString());
    };
  }

  /**
   * Normalizes a local file URI and rejects it if it does not name one plain path. The file system
   * decodes the path, so an encoded '/' or NUL, or an encoded dot segment, would make the server
   * read or write a different path than the one checked: file:///data/root/%2e%2e/etc is /data/etc.
   * A ".." above the root (file:///../etc), a query, a fragment, or an opaque URI (file:a/b) is not
   * a plain path either.
   *
   * @return the URI as a file:/// string
   */
  private static String validateLocalFileURI(String fileUri, String inputPath) {
    URI uri;
    try {
      uri = URI.create(fileUri).normalize();
    } catch (IllegalArgumentException e) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Unsupported local path: " + inputPath, e);
    }
    if (uri.isOpaque()
        || uri.getRawQuery() != null
        || uri.getRawFragment() != null
        || uri.getPath().indexOf('\0') >= 0
        || hasEncodedSeparatorOrDotSegment(uri)) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Unsupported local path: " + inputPath);
    }
    return localFileURIToString(uri);
  }

  private static boolean hasEncodedSeparatorOrDotSegment(URI uri) {
    String[] segments = uri.getPath().split("/", -1);
    // A decoded '/' adds a segment.
    if (segments.length != uri.getRawPath().split("/", -1).length) {
      return true;
    }
    // normalize() collapsed the rest, so a "." or ".." left is escaped or above the root.
    for (String segment : segments) {
      if (segment.equals(".") || segment.equals("..")) {
        return true;
      }
    }
    return false;
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
    URI uri = toUri();
    return NormalizedURL.from(uri.getScheme() + "://" + uri.getAuthority());
  }
}
