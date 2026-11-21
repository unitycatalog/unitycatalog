package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
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
 * limits for objects under the location, and the limits of the credentials vended for it.
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

  // A location given in a request may only be read, so only its read policy has to fit. A write
  // vend whose policy is too long fails with INVALID_ARGUMENT before STS is called.
  private static final Set<CredentialContext.Privilege> READ =
      Set.of(CredentialContext.Privilege.SELECT);

  // The server writes to the managed locations it builds, so their read/write policy has to fit.
  private static final Set<CredentialContext.Privilege> READ_WRITE =
      Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE);

  // Only the S3 and GCS vendors build credentials from a location's storage credential.
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
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} if the location is too long
   */
  public void validateLocation(Session session, NormalizedURL location, int maxLength) {
    validateLocation(location, maxLength, columnUnit.get(), READ, credentialFor(session, location));
  }

  /**
   * Validates a location given in a request whose storage credential is already known, such as the
   * URL of an external location.
   *
   * @param maxLength the width of the column the location is stored in, in characters
   * @param credential the location's storage credential, or empty if it has none
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} if the location is too long
   */
  public void validateLocation(
      NormalizedURL location, int maxLength, Optional<CredentialDAO> credential) {
    validateLocation(location, maxLength, columnUnit.get(), READ, new Resolved(credential));
  }

  /**
   * Validates a location the server derived for a new managed entity.
   *
   * @param maxBytes the width of the column the location is stored in, counted in UTF-8 bytes
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} if the location is too long
   */
  public void validateManagedLocation(Session session, NormalizedURL location, int maxBytes) {
    validateLocation(
        location, maxBytes, Unit.UTF8_BYTES, READ_WRITE, credentialFor(session, location));
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
        READ_WRITE,
        credential);
    NormalizedURL versionLocation =
        ExternalLocationUtils.getManagedLocationForModelVersion(modelLocation, ANY_ID);
    violation(
            versionLocation,
            Constants.MAX_MODEL_LOCATION_LENGTH,
            Unit.UTF8_BYTES,
            READ_WRITE,
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
            cloudLimitViolation(managed.location(), READ_WRITE, credential)
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
      Set<CredentialContext.Privilege> privileges,
      VendCredential credential) {
    violation(location, maxLength, unit, privileges, credential)
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
      Set<CredentialContext.Privilege> privileges,
      VendCredential credential) {
    return lengthViolation(location.toString(), maxLength, unit)
        .or(() -> cloudLimitViolation(location, privileges, credential));
  }

  private Optional<String> cloudLimitViolation(
      NormalizedURL location,
      Set<CredentialContext.Privilege> privileges,
      VendCredential credential) {
    if (!(credential instanceof Resolved resolved) || location.isCloudStorageRoot()) {
      return Optional.empty();
    }
    return cloudCredentialVendor.limitViolation(
        CredentialContext.create(location, privileges, resolved.credential()));
  }
}
