package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageLimits.Extent;
import io.unitycatalog.server.service.credential.StorageLimits.Unit;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rejects storage locations that their database column, or the cloud storage they are on, can't
 * hold. Cloud limits are the ones {@link CloudCredentialVendor#limitViolation} checks: object name
 * limits, and the limits of the credentials vended for a location.
 *
 * <p>A location given in a request is checked against its column in characters, counted the way the
 * database counts them. A location the server builds, a storage root and the managed locations
 * under it, is checked in UTF-8 bytes, which is stricter and keeps the indexed staging table
 * location within PostgreSQL's 2,704 byte index entry limit.
 */
public class StorageLocationValidator {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageLocationValidator.class);

  /** Finds the storage credential of the external location that covers a location. */
  @FunctionalInterface
  public interface CredentialLookup {
    Optional<CredentialDAO> find(Session session, NormalizedURL location);
  }

  /** How the server uses a location, which decides the credentials vended for it. */
  private enum Use {
    /**
     * A location given in a request may only be read. A write vend whose policy is too long fails
     * with INVALID_ARGUMENT before STS is called.
     */
    REQUESTED(Set.of(CredentialContext.Privilege.SELECT)),
    /** The server reads and writes objects under the managed locations it builds. */
    MANAGED(Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE));

    private final Set<CredentialContext.Privilege> privileges;

    Use(Set<CredentialContext.Privilege> privileges) {
      this.privileges = privileges;
    }
  }

  // The S3 and GCS limit checks depend on the location's storage credential. S3 builds its policy
  // from it. GCS builds an access boundary only for a location without one, since it can't vend
  // with a storage credential yet.
  private static final Set<String> CREDENTIAL_SCHEMES =
      Set.of(UriScheme.S3.toString(), UriScheme.GS.toString());

  // Managed locations are laid out the same for every id.
  private static final UUID ANY_ID = new UUID(0, 0);

  private record ManagedLocation(String kind, NormalizedURL location, int maxBytes) {}

  /** The storage credential a vend for a location would use. */
  private sealed interface VendCredential {}

  /** The covering credential can't be resolved, so the vend fails and reports that instead. */
  private record Unresolvable() implements VendCredential {}

  /** The credential, or empty when the vend uses none, such as with no covering location. */
  private record Resolved(Optional<CredentialDAO> credential) implements VendCredential {}

  private final CloudCredentialVendor cloudCredentialVendor;
  private final CredentialLookup credentialLookup;
  private final Supplier<Unit> columnUnit;

  /**
   * @param columnUnit how the database counts the length of a varchar column
   */
  public StorageLocationValidator(
      CloudCredentialVendor cloudCredentialVendor,
      CredentialLookup credentialLookup,
      Supplier<Unit> columnUnit) {
    this.cloudCredentialVendor = cloudCredentialVendor;
    this.credentialLookup = credentialLookup;
    this.columnUnit = columnUnit;
  }

  /**
   * Validates a location given in a request, such as an external table or volume location.
   *
   * @param maxLength the width of the column the location is stored in, in characters
   * @param extent {@link Extent#OBJECT_OR_PREFIX} only for a location that may be a single file,
   *     such as a CSV table, otherwise {@link Extent#PREFIX}
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} if the location is too long
   */
  public void validateLocation(
      Session session, NormalizedURL location, int maxLength, Extent extent) {
    validateLocation(
        location,
        maxLength,
        columnUnit.get(),
        Use.REQUESTED,
        extent,
        credentialFor(session, location));
  }

  /**
   * Validates the URL of an external location, whose storage credential is already known. Objects
   * are stored under an external location, so one has to fit under it.
   *
   * @param maxLength the width of the column the URL is stored in, in characters
   * @param credential the location's storage credential, or empty if it was force-deleted. A vend
   *     for an S3 or GCS URL then fails on the missing credential, so only the column width is
   *     checked for it.
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} if the URL is too long
   */
  public void validateExternalLocationUrl(
      NormalizedURL url, int maxLength, Optional<CredentialDAO> credential) {
    VendCredential vendCredential =
        credential.isPresent() || !CREDENTIAL_SCHEMES.contains(url.toUri().getScheme())
            ? new Resolved(credential)
            : new Unresolvable();
    validateLocation(
        url, maxLength, columnUnit.get(), Use.REQUESTED, Extent.PREFIX, vendCredential);
  }

  /**
   * Validates a location the server derived for a new managed entity.
   *
   * @param maxBytes the width of the column the location is stored in, counted in UTF-8 bytes
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} if the location is too long
   */
  public void validateManagedLocation(Session session, NormalizedURL location, int maxBytes) {
    validateLocation(
        location,
        maxBytes,
        Unit.UTF8_BYTES,
        Use.MANAGED,
        Extent.PREFIX,
        credentialFor(session, location));
  }

  /**
   * Validates the storage root of a new catalog: every kind of managed location the server creates
   * under it has to fit.
   *
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} if the root is too long
   */
  public void validateCatalogStorageRoot(Session session, NormalizedURL storageRoot) {
    validateStorageRoot(
        session,
        storageRoot,
        ExternalLocationUtils.getManagedLocationForCatalog(storageRoot, ANY_ID));
  }

  /**
   * Validates the storage root of a new schema: every kind of managed location the server creates
   * under it has to fit.
   *
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} if the root is too long
   */
  public void validateSchemaStorageRoot(Session session, NormalizedURL storageRoot) {
    validateStorageRoot(
        session,
        storageRoot,
        ExternalLocationUtils.getManagedLocationForSchema(storageRoot, ANY_ID));
  }

  /**
   * Validates the location of a new registered model and of the versions the server creates under
   * it.
   *
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} if a location is too long
   */
  public void validateModelLocation(Session session, NormalizedURL modelLocation) {
    VendCredential credential = credentialFor(session, modelLocation);
    validateLocation(
        modelLocation,
        Constants.MAX_MODEL_LOCATION_LENGTH,
        Unit.UTF8_BYTES,
        Use.MANAGED,
        Extent.PREFIX,
        credential);
    NormalizedURL versionLocation =
        ExternalLocationUtils.getManagedLocationForModelVersion(modelLocation, ANY_ID);
    violation(
            versionLocation,
            Constants.MAX_MODEL_LOCATION_LENGTH,
            Unit.UTF8_BYTES,
            Use.MANAGED,
            Extent.PREFIX,
            credential)
        .ifPresent(
            violation -> {
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT,
                  String.format(
                      "Registered model location is too long. For the location of a model version"
                          + " under it, %s.",
                      violation));
            });
  }

  /**
   * Validates a value stored in a column of {@code maxLength} characters, such as a model version
   * source.
   *
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} if the value is too long
   */
  public void validateLength(String field, String value, int maxLength) {
    lengthViolation(value, maxLength, columnUnit.get())
        .ifPresent(
            violation -> {
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT,
                  String.format("%s is too long: %s.", field, violation));
            });
  }

  private void validateStorageRoot(
      Session session, NormalizedURL storageRoot, NormalizedURL container) {
    List<ManagedLocation> managedLocations = managedLocationsUnder(container).toList();
    int excessBytes =
        managedLocations.stream()
            .mapToInt(
                managed ->
                    Unit.UTF8_BYTES.length(managed.location().toString()) - managed.maxBytes())
            .max()
            .getAsInt();
    if (excessBytes > 0) {
      int rootBytes = Unit.UTF8_BYTES.length(storageRoot.toString());
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          String.format(
              "Storage root is %d bytes long in UTF-8, but it can be at most %d bytes so that "
                  + "managed table and volume locations under it fit in %d bytes.",
              rootBytes, rootBytes - excessBytes, Constants.MAX_STORAGE_LOCATION_LENGTH));
    }
    VendCredential credential = credentialFor(session, storageRoot);
    managedLocations.forEach(
        managed ->
            cloudLimitViolation(managed.location(), Use.MANAGED, Extent.PREFIX, credential)
                .ifPresent(
                    violation -> {
                      throw new BaseException(
                          ErrorCode.INVALID_ARGUMENT,
                          String.format(
                              "Storage root is too long. For the location of a managed %s under"
                                  + " it, %s.",
                              managed.kind(), violation));
                    }));
  }

  private static Stream<ManagedLocation> managedLocationsUnder(NormalizedURL container) {
    NormalizedURL model = ExternalLocationUtils.getManagedLocationForModel(container, ANY_ID);
    return Stream.of(
        new ManagedLocation(
            "table",
            ExternalLocationUtils.getManagedLocationForTable(container, ANY_ID),
            Constants.MAX_STORAGE_LOCATION_LENGTH),
        new ManagedLocation(
            "volume",
            ExternalLocationUtils.getManagedLocationForVolume(container, ANY_ID),
            Constants.MAX_STORAGE_LOCATION_LENGTH),
        new ManagedLocation("registered model", model, Constants.MAX_MODEL_LOCATION_LENGTH),
        new ManagedLocation(
            "model version",
            ExternalLocationUtils.getManagedLocationForModelVersion(model, ANY_ID),
            Constants.MAX_MODEL_LOCATION_LENGTH));
  }

  /**
   * The storage credential a vend for the location would use. It is unresolvable when the lookup
   * fails, such as when the covering external location's credential was force-deleted.
   */
  private VendCredential credentialFor(Session session, NormalizedURL location) {
    if (!CREDENTIAL_SCHEMES.contains(location.toUri().getScheme())) {
      return new Resolved(Optional.empty());
    }
    try {
      return new Resolved(credentialLookup.find(session, location));
    } catch (BaseException e) {
      LOGGER.warn("Skipping cloud limit checks for {}: credential lookup failed", location, e);
      return new Unresolvable();
    }
  }

  private void validateLocation(
      NormalizedURL location,
      int maxLength,
      Unit unit,
      Use use,
      Extent extent,
      VendCredential credential) {
    violation(location, maxLength, unit, use, extent, credential)
        .ifPresent(
            violation -> {
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT,
                  String.format("Storage location is too long: %s.", violation));
            });
  }

  private static Optional<String> lengthViolation(String value, int maxLength, Unit unit) {
    int length = unit.length(value);
    return length <= maxLength
        ? Optional.empty()
        : Optional.of(
            String.format("it is %d %s, and the limit is %d", length, unit.label(), maxLength));
  }

  private Optional<String> violation(
      NormalizedURL location,
      int maxLength,
      Unit unit,
      Use use,
      Extent extent,
      VendCredential credential) {
    return lengthViolation(location.toString(), maxLength, unit)
        .or(() -> cloudLimitViolation(location, use, extent, credential));
  }

  private Optional<String> cloudLimitViolation(
      NormalizedURL location, Use use, Extent extent, VendCredential credential) {
    if (!(credential instanceof Resolved resolved)) {
      return Optional.empty();
    }
    return cloudCredentialVendor.limitViolation(
        CredentialContext.create(location, use.privileges, resolved.credential()), extent);
  }
}
