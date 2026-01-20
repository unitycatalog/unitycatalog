package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Enum representing supported URI schemes for storage locations in Unity Catalog.
 *
 * <p>This enum provides a type-safe way to handle different cloud storage and file system URI
 * schemes, including Azure Blob Storage (abfs/abfss), Google Cloud Storage (gs), Amazon S3 (s3),
 * and local file systems (file).
 *
 * <p>Use {@link #fromURI(URI)} to convert a URI to its corresponding scheme enum value.
 */
public enum UriScheme {
  ABFS("abfs"),
  ABFSS("abfss"),
  GS("gs"),
  S3("s3"),
  FILE("file"),
  NULL(null);

  private final String scheme;

  /**
   * Pre-computed map for O(1) scheme string to enum lookup. This avoids iterating through all enum
   * values on each {@link #fromString(String)} call, providing constant-time lookups instead of
   * linear-time iteration. In this map, null scheme (local path like /tmp) is also mapped to NULL.
   */
  private static final Map<String, UriScheme> LOOKUP_MAP = new HashMap<>();

  // Populate the lookup map statically
  static {
    for (UriScheme s : values()) {
      LOOKUP_MAP.put(s.scheme, s);
    }
  }

  UriScheme(String scheme) {
    this.scheme = scheme;
  }

  private static UriScheme fromString(String scheme) {
    // null is also converted into NULL
    UriScheme res = LOOKUP_MAP.get(scheme);
    if (res == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Unsupported URI scheme: " + scheme);
    }
    return res;
  }

  public static UriScheme fromURI(URI uri) {
    return fromString(uri.getScheme());
  }

  @Override
  public String toString() {
    return scheme;
  }
}
