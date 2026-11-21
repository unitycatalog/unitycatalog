package io.unitycatalog.server.service.credential;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import io.unitycatalog.server.utils.NormalizedURL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/** Object name limits of cloud storage that apply to every object stored under a location. */
public final class StorageLimits {

  /**
   * The longest object name that S3, GCS and Azure Blob Storage allow.
   *
   * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html">S3 object
   *     keys</a>
   * @see <a href="https://cloud.google.com/storage/docs/objects">GCS object names</a>
   * @see <a
   *     href="https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata">
   *     Azure blob names</a>
   */
  public static final int MAX_OBJECT_NAME_LENGTH = 1024;

  /**
   * The most path segments of a blob in a hierarchical namespace account, counting the account and
   * the container.
   */
  public static final int MAX_AZURE_PATH_SEGMENTS = 63;

  public enum Unit {
    UTF8_BYTES("bytes"),
    CHARACTERS("characters");

    private final String label;

    Unit(String label) {
      this.label = label;
    }

    int length(String name) {
      return this == UTF8_BYTES ? name.getBytes(StandardCharsets.UTF_8).length : name.length();
    }
  }

  private StorageLimits() {}

  /**
   * The object name of a location in its bucket or container: its path without the leading slash.
   */
  public static String objectName(NormalizedURL location) {
    return CharMatcher.is('/').trimLeadingFrom(location.toUri().getPath());
  }

  /**
   * Checks that an object can be stored under each location. The shortest name of such an object is
   * the location's name, a slash and one character.
   *
   * @param kind what the storage calls an object name, for the message
   * @return why no object fits under one of the locations, or empty
   */
  public static Optional<String> objectNameViolation(
      List<NormalizedURL> locations, String kind, Unit unit) {
    return locations.stream()
        .mapToInt(location -> unit.length(objectName(location)) + 2)
        .max()
        .stream()
        .filter(shortestChildName -> shortestChildName > MAX_OBJECT_NAME_LENGTH)
        .mapToObj(
            shortestChildName ->
                String.format(
                    "%s names under it are at least %d %s, and the limit is %d",
                    kind, shortestChildName, unit.label, MAX_OBJECT_NAME_LENGTH))
        .findFirst();
  }

  /**
   * Checks that an object can be stored under each location of a hierarchical namespace account,
   * whose blobs have at most {@link #MAX_AZURE_PATH_SEGMENTS} path segments.
   *
   * @return why no object fits under one of the locations, or empty
   */
  public static Optional<String> azurePathSegmentViolation(List<NormalizedURL> locations) {
    return locations.stream()
        .mapToInt(
            location ->
                2
                    + Splitter.on('/').omitEmptyStrings().splitToList(objectName(location)).size()
                    + 1)
        .max()
        .stream()
        .filter(segments -> segments > MAX_AZURE_PATH_SEGMENTS)
        .mapToObj(
            segments ->
                String.format(
                    "Azure blob paths under it have at least %d segments, counting the account and"
                        + " container, and the limit is %d",
                    segments, MAX_AZURE_PATH_SEGMENTS))
        .findFirst();
  }
}
