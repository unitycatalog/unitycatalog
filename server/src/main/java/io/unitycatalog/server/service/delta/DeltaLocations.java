package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.auth.decorator.AuthorizeValueExtractor;
import io.unitycatalog.server.delta.model.DeltaCreateTableRequest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Converts locations at the Delta API boundary. Delta clients exchange a location as a URI ({@code
 * new Path(p).toUri().toString()} when sending, {@code new URI(location)} when reading), while UC
 * stores a cloud location as the literal path string: folder {@code team reports} is stored as
 * {@code s3://b/team reports} and sent as {@code s3://b/team%20reports}. Local file locations are
 * already URIs and pass through unchanged.
 */
public final class DeltaLocations {

  private DeltaLocations() {}

  /**
   * Parses a location a Delta client sent. A cloud location is decoded once into its literal path.
   * Returns null if the input is null.
   *
   * @throws BaseException with {@code INVALID_ARGUMENT} if a cloud location is not a valid URI, is
   *     opaque ({@code s3:bucket/x}), or contains '?' or '#'
   */
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

  /**
   * Formats a non-null stored location for a Delta client: a cloud location becomes its encoded
   * URI.
   */
  public static String toDelta(String location) {
    return NormalizedURL.from(location).toUri().toString();
  }

  private static boolean isCloud(NormalizedURL url) {
    return switch (UriScheme.fromURI(url.toUri())) {
      case S3, GS, ABFS, ABFSS -> true;
      case FILE, NULL -> false;
    };
  }

  /**
   * The literal storage path of a Delta create-table request's {@code location}, so authorization
   * resolves the same path the table is created at.
   */
  public static class CreateTableLocationExtractor implements AuthorizeValueExtractor {

    @Override
    public Object extract(Object body) {
      String location = ((DeltaCreateTableRequest) body).getLocation();
      // KeyMapper reads a value without '/' as an external location name; leave those as they are.
      return location == null || !location.contains("/")
          ? location
          : fromDelta(location).toString();
    }
  }
}
