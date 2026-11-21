package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.NormalizedURL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.hibernate.Session;

/**
 * Rejects storage locations that their database column, or the cloud storage they are on, can't
 * hold. Cloud limits are the ones {@link CloudCredentialVendor#limitViolation} checks: object name
 * limits for objects under the location, and the limits of the credentials vended for it.
 */
public class StorageLocationValidator {

  /** Finds the storage credential of the external location that covers a location. */
  @FunctionalInterface
  public interface CredentialLookup {
    Optional<CredentialDAO> find(Session session, NormalizedURL location);
  }

  /** The column width of registered model and model version locations. */
  public static final int MAX_MODEL_LOCATION_LENGTH = 4096;

  /** The column width of external location URLs. */
  public static final int MAX_EXTERNAL_LOCATION_URL_LENGTH = 255;

  // The most a vend asks for, so the longest credential policy is checked.
  private static final Set<CredentialContext.Privilege> ALL_PRIVILEGES =
      Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE);

  // Managed locations are laid out the same for every id.
  private static final UUID ANY_ID = new UUID(0, 0);

  private record ManagedLocation(String kind, NormalizedURL location, int maxBytes) {}

  private final CloudCredentialVendor cloudCredentialVendor;
  private final CredentialLookup credentialLookup;

  public StorageLocationValidator(
      CloudCredentialVendor cloudCredentialVendor, CredentialLookup credentialLookup) {
    this.cloudCredentialVendor = cloudCredentialVendor;
    this.credentialLookup = credentialLookup;
  }

  /**
   * Validates a location given in a request or derived for a new managed entity.
   *
   * @param maxBytes the width of the column the location is stored in, counted in UTF-8 bytes
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} if the location is too long
   */
  public void validateLocation(Session session, NormalizedURL location, int maxBytes) {
    validateLocation(location, maxBytes, credentialLookup.find(session, location));
  }

  /**
   * Validates a location whose storage credential is already known, such as the URL of a new
   * external location.
   *
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} if the location is too long
   */
  public void validateLocation(
      NormalizedURL location, int maxBytes, Optional<CredentialDAO> credential) {
    violation(location, maxBytes, credential)
        .ifPresent(
            violation -> {
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT,
                  String.format("Storage location is too long: %s.", violation));
            });
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

  private void validateStorageRoot(
      Session session, NormalizedURL storageRoot, NormalizedURL container) {
    List<ManagedLocation> managedLocations = managedLocationsUnder(container).toList();
    int excessBytes =
        managedLocations.stream()
            .mapToInt(managed -> utf8Length(managed.location().toString()) - managed.maxBytes())
            .max()
            .getAsInt();
    if (excessBytes > 0) {
      int rootBytes = utf8Length(storageRoot.toString());
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          String.format(
              "Storage root is %d bytes long in UTF-8, but it can be at most %d bytes so that "
                  + "managed table and volume locations under it fit in %d bytes.",
              rootBytes, rootBytes - excessBytes, Constants.MAX_STORAGE_LOCATION_LENGTH));
    }
    Optional<CredentialDAO> credential = credentialLookup.find(session, storageRoot);
    managedLocations.forEach(
        managed ->
            cloudLimitViolation(managed.location(), credential)
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

  /**
   * Validates the location of a new registered model and of the versions the server creates under
   * it.
   *
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} if a location is too long
   */
  public void validateModelLocation(Session session, NormalizedURL modelLocation) {
    Optional<CredentialDAO> credential = credentialLookup.find(session, modelLocation);
    validateLocation(modelLocation, MAX_MODEL_LOCATION_LENGTH, credential);
    NormalizedURL versionLocation =
        ExternalLocationUtils.getManagedLocationForModelVersion(modelLocation, ANY_ID);
    violation(versionLocation, MAX_MODEL_LOCATION_LENGTH, credential)
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
        new ManagedLocation("registered model", model, MAX_MODEL_LOCATION_LENGTH),
        new ManagedLocation(
            "model version",
            ExternalLocationUtils.getManagedLocationForModelVersion(model, ANY_ID),
            MAX_MODEL_LOCATION_LENGTH));
  }

  /**
   * Validates a value stored in a column of {@code maxBytes}, such as a model version source.
   *
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} if the value is too long
   */
  public static void validateLength(String field, String value, int maxBytes) {
    lengthViolation(value, maxBytes)
        .ifPresent(
            violation -> {
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT,
                  String.format("%s is too long: %s.", field, violation));
            });
  }

  private static int utf8Length(String value) {
    return value.getBytes(StandardCharsets.UTF_8).length;
  }

  private static Optional<String> lengthViolation(String value, int maxBytes) {
    int bytes = utf8Length(value);
    return bytes <= maxBytes
        ? Optional.empty()
        : Optional.of(
            String.format("it is %d bytes in UTF-8, and the limit is %d", bytes, maxBytes));
  }

  private Optional<String> violation(
      NormalizedURL location, int maxBytes, Optional<CredentialDAO> credential) {
    return lengthViolation(location.toString(), maxBytes)
        .or(() -> cloudLimitViolation(location, credential));
  }

  private Optional<String> cloudLimitViolation(
      NormalizedURL location, Optional<CredentialDAO> credential) {
    if (location.isCloudStorageRoot()) {
      return Optional.empty();
    }
    return cloudCredentialVendor.limitViolation(
        CredentialContext.create(location, ALL_PRIVILEGES, credential));
  }
}
