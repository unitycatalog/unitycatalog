package io.unitycatalog.server.service.credential;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import io.unitycatalog.server.model.DataSourceFormat;
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
   * The most path segments a blob name can have. An account with a hierarchical namespace allows
   * fewer, 63 counting the account and container, but nothing the server is given says whether an
   * account has one, so that limit isn't checked.
   */
  public static final int MAX_AZURE_PATH_SEGMENTS = 254;

  /** How the length of a name is counted. */
  public enum Unit {
    UTF8_BYTES("bytes"),
    UTF16_CODE_UNITS("characters"),
    CODE_POINTS("characters");

    private final String label;

    Unit(String label) {
      this.label = label;
    }

    public int length(String name) {
      return switch (this) {
        case UTF8_BYTES -> name.getBytes(StandardCharsets.UTF_8).length;
        case UTF16_CODE_UNITS -> name.length();
        case CODE_POINTS -> name.codePointCount(0, name.length());
      };
    }

    public String label() {
      return this == UTF8_BYTES ? "bytes in UTF-8" : label;
    }
  }

  /** Whether objects are stored under a location, or the location may itself name one. */
  public enum Extent {
    /**
     * Objects are stored under the location. The shortest name of such an object is the location's
     * name, a slash and one character, one path segment more.
     */
    PREFIX("/x"),
    /**
     * A Delta table. Its longest file name of a fixed length is a staged commit, {@code
     * _delta_log/_staged_commits/<version>.<uuid>.json} with the version padded to 20 digits, three
     * path segments more.
     *
     * @see <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md">Delta protocol</a>
     */
    DELTA_TABLE(
        "/_delta_log/_staged_commits/"
            + "00000000000000000000.00000000-0000-0000-0000-000000000000.json"),
    /** The location may itself name an object, such as a CSV table that is a single file. */
    OBJECT_OR_PREFIX("");

    private final int extraNameLength;
    private final int extraSegments;

    Extent(String longestSuffix) {
      this.extraNameLength = longestSuffix.length();
      this.extraSegments = CharMatcher.is('/').countIn(longestSuffix);
    }

    /**
     * The extent of a table location. Delta and Iceberg tables are directories of data and metadata
     * files, and a table in another format, such as CSV, may be a single file.
     *
     * @param format the table's format, or null if it has none
     */
    public static Extent ofTable(DataSourceFormat format) {
      if (format == DataSourceFormat.DELTA) {
        return DELTA_TABLE;
      }
      return format == DataSourceFormat.ICEBERG ? PREFIX : OBJECT_OR_PREFIX;
    }

    private String describe(String kind, String measure) {
      return switch (this) {
        case PREFIX -> String.format("%s names under it have at least %s", kind, measure);
        case DELTA_TABLE ->
            String.format("the %s names of its Delta staged commit files have %s", kind, measure);
        case OBJECT_OR_PREFIX -> String.format("its %s name has %s", kind, measure);
      };
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
   * Checks that each location can hold an object: under it, or for {@link Extent#OBJECT_OR_PREFIX}
   * also as the location itself.
   *
   * @param kind what the storage calls an object, for the message
   * @return why no object fits at one of the locations, or empty
   */
  public static Optional<String> objectNameViolation(
      List<NormalizedURL> locations, String kind, Unit unit, Extent extent) {
    return locations.stream()
        .mapToInt(location -> unit.length(objectName(location)) + extent.extraNameLength)
        .max()
        .stream()
        .filter(shortestName -> shortestName > MAX_OBJECT_NAME_LENGTH)
        .mapToObj(
            shortestName ->
                String.format(
                    "%s, and the limit is %d",
                    extent.describe(kind, shortestName + " " + unit.label), MAX_OBJECT_NAME_LENGTH))
        .findFirst();
  }

  /**
   * Checks that each location can hold a blob whose name has at most {@link
   * #MAX_AZURE_PATH_SEGMENTS} path segments, under it or, for {@link Extent#OBJECT_OR_PREFIX}, as
   * the location itself.
   *
   * @return why no blob fits at one of the locations, or empty
   */
  public static Optional<String> azurePathSegmentViolation(
      List<NormalizedURL> locations, Extent extent) {
    return locations.stream()
        .mapToInt(
            location ->
                Splitter.on('/').omitEmptyStrings().splitToList(objectName(location)).size()
                    + extent.extraSegments)
        .max()
        .stream()
        .filter(segments -> segments > MAX_AZURE_PATH_SEGMENTS)
        .mapToObj(
            segments ->
                String.format(
                    "%s, and the limit is %d",
                    extent.describe("Azure blob", segments + " path segments"),
                    MAX_AZURE_PATH_SEGMENTS))
        .findFirst();
  }
}
