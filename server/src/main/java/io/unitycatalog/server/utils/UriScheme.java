package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public enum UriScheme {
  ABFS("abfs"),
  ABFSS("abfss"),
  GS("gs"),
  S3("s3"),
  FILE("file"),
  NULL(null);

  private final String scheme;

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
