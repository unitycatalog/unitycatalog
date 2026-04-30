package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.delta.model.UniformMetadata;
import io.unitycatalog.server.delta.model.UniformMetadataIceberg;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.DeltaCommit;
import io.unitycatalog.server.model.DeltaUniform;
import io.unitycatalog.server.model.DeltaUniformIceberg;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ValidationUtils;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Hub for UniForm-Iceberg cross-cutting logic. Shared between the create-time path ({@link
 * DeltaCreateTableMapper}, {@code TableRepository.createTableForDelta}) and the commit-time path
 * ({@code DeltaCommitRepository}). Centralized so the two cannot drift on what counts as a valid
 * UniForm state or on how UniForm fields are written to the DAO. Owns:
 *
 * <ul>
 *   <li>{@link #isIcebergEnabled} -- "is UniForm-Iceberg on?" predicate over a properties map.
 *   <li>{@link #validateConsistency} -- property/block presence consistency.
 *   <li>{@link #getUniformFields(UniformMetadata)} -- create-time wire-shape extract.
 *   <li>{@link #getUniformFields(DeltaCommit)} -- commit-time extract + shape + atomicity.
 *   <li>{@link #validateCreate} -- create-time atomicity + sequential + subpath check.
 *   <li>{@link #requireBasicShape} -- presence + size, shared by both extractors.
 *   <li>{@link #requireMetadataLocationSubpath} -- subpath rule, shared with the commit-time path.
 *   <li>{@link UniformIcebergFields} -- type-safe carrier for the validated fields
 *       (metadata-location, version, timestamp-ms, optional base-version) so no field is normalized
 *       or parsed twice.
 *   <li>{@link #applyToDao} -- writes the triple to the DAO; no-op when absent.
 * </ul>
 */
public final class DeltaUniformUtils {

  private DeltaUniformUtils() {}

  /**
   * Maximum byte length the server accepts for {@code uniform.iceberg.metadata-location}. Matches
   * the DAO column ({@code uniform_iceberg_metadata_location} length=65535) so a request that
   * passes here cannot fail later at persist time.
   */
  public static final int MAX_METADATA_LOCATION_BYTES = 65535;

  /**
   * UniForm Iceberg fields, normalized once at the request boundary. Callers normalize the
   * metadata-location (using {@link NormalizedURL#from(String)} or {@link NormalizedURL#from(URI)})
   * and convert the timestamp to epoch ms (parsing the RFC 3339 string once on the commit-side
   * path), then assemble this record. After {@link #requireBasicShape} succeeds the first three
   * fields are non-null and the location is within the size bound, so downstream code (subpath
   * check, DAO write) reads them without re-checking. {@code baseConvertedDeltaVersion} is the
   * wire-side optional (a sequential-validation hook for incremental conversion) carried through so
   * {@link #validateCreate} can reject it without re-reading the wire object; modeled as {@link
   * Optional} since the wire shape itself marks it optional. Holding the values as a single carrier
   * guarantees nothing is normalized or parsed twice.
   */
  public record UniformIcebergFields(
      NormalizedURL metadataLocation,
      Long convertedDeltaVersion,
      Long convertedDeltaTimestampMs,
      Optional<Long> baseConvertedDeltaVersion) {}

  /**
   * True when {@code properties} declares the table as UniForm-Iceberg-enabled, i.e. property
   * {@link TableProperties#UNIVERSAL_FORMAT_ENABLED_FORMATS} contains {@link
   * DeltaConsts#UNIVERSAL_FORMAT_ICEBERG} as one of its comma-separated entries. Per Delta spec the
   * value is a list (today {@code iceberg} and {@code hudi}); a value like {@code "iceberg,hudi"}
   * still has Iceberg enabled, so a plain equality check would miss it. The single source of truth
   * for "is UniForm-Iceberg on?" -- callers must not re-derive this from the map directly so that
   * the property key/value strings live in one place.
   */
  public static boolean isIcebergEnabled(Map<String, String> properties) {
    if (properties == null) {
      return false;
    }
    String value = properties.get(TableProperties.UNIVERSAL_FORMAT_ENABLED_FORMATS);
    if (value == null || value.isEmpty()) {
      return false;
    }
    for (String format : value.split(",")) {
      if (DeltaConsts.UNIVERSAL_FORMAT_ICEBERG.equals(format)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Validate that the {@code uniform} block is present iff {@code properties} declares the table as
   * UniForm-Iceberg-enabled (via {@code delta.universalFormat.enabledFormats=iceberg}). Either both
   * or neither must be set; any other combination would leave the table in a state the next commit
   * would reject.
   *
   * <p>{@code uniformPresent} is supplied as a boolean rather than the uniform object itself so
   * that the create path (delta.yaml's {@code UniformMetadata}) and the commit path (all.yaml's
   * {@code DeltaUniform}) can share this validator without coupling it to either wire type.
   */
  public static void validateConsistency(Map<String, String> properties, boolean uniformPresent) {
    boolean enabledViaProperty = isIcebergEnabled(properties);
    if (enabledViaProperty && !uniformPresent) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Uniform metadata must be set when '"
              + TableProperties.UNIVERSAL_FORMAT_ENABLED_FORMATS
              + "' includes '"
              + DeltaConsts.UNIVERSAL_FORMAT_ICEBERG
              + "'.");
    }
    if (!enabledViaProperty && uniformPresent) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Uniform metadata must not be set unless '"
              + TableProperties.UNIVERSAL_FORMAT_ENABLED_FORMATS
              + "' includes '"
              + DeltaConsts.UNIVERSAL_FORMAT_ICEBERG
              + "'.");
    }
  }

  /**
   * Extract and shape-validate the create-time UniForm Iceberg fields from a request. Runs the
   * wire-shape checks that the commit path also runs:
   *
   * <ul>
   *   <li>{@code uniform.iceberg} is not null;
   *   <li>basic shape (presence + size) via {@link #requireBasicShape}.
   * </ul>
   *
   * <p>Create-time-specific rules ({@code converted-delta-version} must be 0 or 1, {@code
   * base-converted-delta-version} must be unset, subpath) live in {@link #validateCreate}, where
   * the table location is also available. Returns {@code Optional.empty()} when no UniForm metadata
   * was supplied; otherwise returns a record whose metadata-location has been normalized exactly
   * once and is reused downstream by {@link #applyToDao}.
   */
  public static Optional<UniformIcebergFields> getUniformFields(UniformMetadata uniform) {
    if (uniform == null) {
      return Optional.empty();
    }
    UniformMetadataIceberg iceberg = uniform.getIceberg();
    if (iceberg == null) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "uniform.iceberg is required when uniform is set.");
    }
    UniformIcebergFields fields =
        new UniformIcebergFields(
            NormalizedURL.from(iceberg.getMetadataLocation()),
            iceberg.getConvertedDeltaVersion(),
            iceberg.getConvertedDeltaTimestamp(),
            Optional.ofNullable(iceberg.getBaseConvertedDeltaVersion()));
    requireBasicShape(fields);
    return Optional.of(fields);
  }

  /**
   * Extract and shape-validate the commit-time UniForm Iceberg fields. Co-located with the
   * create-time overload so the two paths can't drift on which fields are required, how the
   * location is normalized, or how the timestamp is parsed. Runs the validations that don't need
   * the table's stored URL:
   *
   * <ul>
   *   <li>{@code uniform.iceberg} is not null;
   *   <li>basic shape (presence + size) via {@link #requireBasicShape};
   *   <li>commit-version atomicity: {@code converted-delta-version} must equal {@code
   *       commit.commitInfo.version} when commit-info is present.
   * </ul>
   *
   * <p>The metadata-location is normalized exactly once and the RFC 3339 timestamp parsed exactly
   * once; both are returned in {@link UniformIcebergFields} so the subpath check ({@code
   * DeltaCommitRepository.validateTableForCommit}) and the DAO write ({@link #applyToDao}) reuse
   * them without re-doing the work. Returns {@code Optional.empty()} when no UniForm metadata was
   * supplied.
   */
  public static Optional<UniformIcebergFields> getUniformFields(DeltaCommit commit) {
    DeltaUniform uniform = commit.getUniform();
    if (uniform == null) {
      return Optional.empty();
    }
    ValidationUtils.checkArgument(
        uniform.getIceberg() != null, "Field cannot be null in uniform: iceberg");
    DeltaUniformIceberg iceberg = uniform.getIceberg();
    String timestampStr = iceberg.getConvertedDeltaTimestamp();
    UniformIcebergFields fields =
        new UniformIcebergFields(
            NormalizedURL.from(iceberg.getMetadataLocation()),
            iceberg.getConvertedDeltaVersion(),
            timestampStr == null ? null : Instant.parse(timestampStr).toEpochMilli(),
            Optional.ofNullable(iceberg.getBaseConvertedDeltaVersion()));
    requireBasicShape(fields);
    if (commit.getCommitInfo() != null) {
      ValidationUtils.checkArgument(
          fields.convertedDeltaVersion().equals(commit.getCommitInfo().getVersion()),
          "uniform.iceberg.converted-delta-version (%d) must equal commit version (%d)",
          fields.convertedDeltaVersion(),
          commit.getCommitInfo().getVersion());
    }
    return Optional.of(fields);
  }

  /**
   * Run the create-time validations that {@link #getUniformFields(UniformMetadata)} cannot:
   *
   * <ul>
   *   <li>create-time atomicity: {@code converted-delta-version} is {@code 0} or {@code 1} (V2 or
   *       V3-catalog-managed);
   *   <li>create-time sequential: {@code base-converted-delta-version} must be unset, since there
   *       is no prior stored {@code converted-delta-version} for an incremental conversion to
   *       reference;
   *   <li>create-time subpath: the {@code metadata-location} must be inside the table's storage
   *       root (mirrors the commit-time {@code DeltaCommitRepository.validateTableForCommit}).
   * </ul>
   *
   * <p>No-op when {@code uniformFields} is empty. Property/block consistency is the caller's
   * responsibility (see {@link #validateConsistency}); wire-shape is {@link
   * #getUniformFields(UniformMetadata)}'s.
   */
  public static void validateCreate(
      Optional<UniformIcebergFields> uniformFields, NormalizedURL tableLocation) {
    if (uniformFields.isEmpty()) {
      return;
    }
    UniformIcebergFields fields = uniformFields.get();
    long version = fields.convertedDeltaVersion();
    if (version != 0L && version != 1L) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          String.format(
              "uniform.iceberg.converted-delta-version must be 0 or 1 at create time, got %d.",
              version));
    }
    if (fields.baseConvertedDeltaVersion().isPresent()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "uniform.iceberg.base-converted-delta-version must not be set at create time:"
              + " the table has no prior converted-delta-version for it to reference.");
    }
    requireMetadataLocationSubpath(fields.metadataLocation(), tableLocation);
  }

  /**
   * Validate the basic shape of an Iceberg block (presence + size). Callers extract their wire
   * shape into a {@link UniformIcebergFields} -- normalizing the metadata-location and parsing the
   * timestamp to epoch ms -- and pass the assembled record here. After this method returns, every
   * field is non-null and the location is within the size bound, so downstream code (subpath check,
   * DAO write) can read the record without re-checking. Shared between {@link
   * #getUniformFields(UniformMetadata)} (create-time) and {@link #getUniformFields(DeltaCommit)}
   * (commit-time).
   */
  public static void requireBasicShape(UniformIcebergFields fields) {
    if (fields.metadataLocation() == null) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "uniform.iceberg.metadata-location is required when uniform is set.");
    }
    if (fields.convertedDeltaVersion() == null) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "uniform.iceberg.converted-delta-version is required when uniform is set.");
    }
    if (fields.convertedDeltaTimestampMs() == null) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "uniform.iceberg.converted-delta-timestamp is required when uniform is set.");
    }
    int locationBytes =
        fields
            .metadataLocation()
            .toString()
            .getBytes(java.nio.charset.StandardCharsets.UTF_8)
            .length;
    if (locationBytes > MAX_METADATA_LOCATION_BYTES) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          String.format(
              "uniform.iceberg.metadata-location byte size (%d) exceeds the maximum allowed (%d).",
              locationBytes, MAX_METADATA_LOCATION_BYTES));
    }
  }

  /**
   * Throw {@code INVALID_ARGUMENT} unless {@code metadataLocation} is strictly inside {@code
   * tableLocation}. Both inputs are typed {@link NormalizedURL} so callers normalize once at the
   * boundary rather than this helper re-normalizing on every call. The Delta REST Catalog spec
   * requires the Iceberg sidecar metadata to live under the table's storage root so table-level
   * credential vending and lifecycle (delete/rename) cover it -- a path outside the root would be
   * orphaned when the table is dropped. Shared between create-time ({@link #validateCreate}) and
   * commit-time ({@code DeltaCommitRepository.validateTableForCommit}) call sites.
   */
  public static void requireMetadataLocationSubpath(
      NormalizedURL metadataLocation, NormalizedURL tableLocation) {
    if (!metadataLocation.toString().startsWith(tableLocation.toString() + "/")) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          String.format(
              "uniform.iceberg.metadata-location ('%s') must be a subpath of the table location"
                  + " ('%s').",
              metadataLocation, tableLocation));
    }
  }

  /**
   * Apply pre-validated, pre-normalized UniForm Iceberg fields onto a {@link TableInfoDAO}. No-op
   * when {@code fields} is empty (no UniForm metadata supplied for this request). All
   * presence/shape validation must already have been done by the caller via {@link #validateCreate}
   * or commit-time validation; this method only writes.
   */
  public static void applyToDao(TableInfoDAO dao, Optional<UniformIcebergFields> fields) {
    fields.ifPresent(
        f ->
            dao.updateUniformIcebergMetadata(
                f.metadataLocation(), f.convertedDeltaVersion(), f.convertedDeltaTimestampMs()));
  }
}
