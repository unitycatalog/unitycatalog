package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.auth.decorator.AuthorizeValueExtractor;
import io.unitycatalog.server.delta.model.DeltaCreateTableRequest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.Path;

/** Converts locations at the Delta API boundary, where Delta clients exchange URIs. */
public final class DeltaLocations {

  private DeltaLocations() {}

  /** Decodes a location a Delta client sent; returns null if the input is null. */
  public static NormalizedURL fromDelta(String location) {
    NormalizedURL literal = NormalizedURL.from(location);
    if (literal == null || !isCloud(literal)) {
      return literal;
    }
    try {
      return NormalizedURL.from(new URI(location));
    } catch (URISyntaxException e) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Cloud storage location is not a valid URI: " + location);
    }
  }

  /** Encodes a non-null stored location for a Delta client. */
  public static String toDelta(String location) {
    return NormalizedURL.from(location).toUri().toString();
  }

  /**
   * Formats a non-null stored staging location for a Delta client. Delta passes it to Spark as the
   * table location, and Spark reads that as a Hadoop path, so it is a path, not a URI.
   */
  public static String toDeltaPath(String location) {
    return new Path(NormalizedURL.from(location).toUri()).toString();
  }

  private static boolean isCloud(NormalizedURL url) {
    return switch (UriScheme.fromURI(url.toUri())) {
      case S3, GS, ABFS, ABFSS -> true;
      case FILE, NULL -> false;
    };
  }

  /** The decoded {@code location} of a Delta create-table request, for authorization. */
  public static class CreateTableLocationExtractor implements AuthorizeValueExtractor {

    @Override
    public Object extract(Object body) {
      String location = ((DeltaCreateTableRequest) body).getLocation();
      // A value without '/' is an external location name; leave it as it is.
      return location == null || !location.contains("/")
          ? location
          : fromDelta(location).toString();
    }
  }
}
