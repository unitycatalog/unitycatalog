package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.auth.decorator.AuthorizeValueExtractor;
import io.unitycatalog.server.delta.model.DeltaCreateTableRequest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import java.net.URI;
import java.net.URISyntaxException;

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
      // KeyMapper reads a value without '/' as an external location name; leave those as they are.
      return location == null || !location.contains("/")
          ? location
          : fromDelta(location).toString();
    }
  }
}
