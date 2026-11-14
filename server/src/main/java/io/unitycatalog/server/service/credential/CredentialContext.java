package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;

/**
 * Encapsulates the context needed for generating cloud storage credentials.This class captures all
 * information required by cloud credential vendors to generate appropriately scoped temporary
 * credentials.
 */
@Builder
@Getter
public class CredentialContext {
  public enum Privilege {
    SELECT,
    UPDATE
  }

  // Common privilege sets: SELECT alone vends read-only storage credentials, SELECT + UPDATE vends
  // read/write credentials.
  public static final Set<Privilege> READ_ONLY = Set.of(Privilege.SELECT);
  public static final Set<Privilege> READ_WRITE = Set.of(Privilege.SELECT, Privilege.UPDATE);

  // The storage scheme (S3, GCS, ABFS) to determine which cloud vendor to use
  private final UriScheme storageScheme;
  // The storage base (e.g., s3://bucket) for looking up per-bucket configurations
  private final NormalizedURL storageBase;
  // The privileges (SELECT, UPDATE) that determine read/write permissions
  private final Set<Privilege> privileges;
  // The specific locations that credentials should grant access to. This is a list of locations to
  // be a little future-proofing when a table could have more than 1 location where files belonging
  // to table are located.
  private final List<NormalizedURL> locations;
  // If a storage credential of external location for this path is found, the credential will
  // be generated according to it.
  private final Optional<CredentialDAO> credentialDAO;
  // The S3 endpoint of the external location for this path, set for S3-compatible storage.
  @Builder.Default private final Optional<String> s3Endpoint = Optional.empty();
  // The region to sign requests to that S3 endpoint with, set together with s3Endpoint.
  @Builder.Default private final Optional<String> s3Region = Optional.empty();
  // Whether the credentials are for the server's own storage access rather than for a client. The
  // server always needs usable keys, even when its credential does not vend keys to clients.
  private final boolean isServerAccess;

  /**
   * Creates a CredentialContext for a single storage location that is not on S3-compatible storage.
   *
   * @param location the storage location URL (e.g., s3://bucket/path)
   * @param privileges the set of privileges required (SELECT for read, UPDATE for write)
   * @param credentialDAO optional storage credential from an external location
   * @return a new CredentialContext instance
   */
  public static CredentialContext create(
      NormalizedURL location, Set<Privilege> privileges, Optional<CredentialDAO> credentialDAO) {
    return create(
        location,
        privileges,
        credentialDAO,
        Optional.empty(),
        Optional.empty(),
        /* isServerAccess= */ false);
  }

  /**
   * Creates a CredentialContext for a single storage location.
   *
   * @param location the storage location URL (e.g., s3://bucket/path)
   * @param privileges the set of privileges required (SELECT for read, UPDATE for write)
   * @param credentialDAO optional storage credential from an external location; if present, the
   *     credential vendor will use it to generate credential instead of using per-bucket
   *     configuration.
   * @param s3Endpoint the external location's S3 endpoint for S3-compatible storage, empty for AWS
   * @param s3Region the region of the storage at {@code s3Endpoint}, empty for AWS
   * @param isServerAccess whether the credentials are for the server's own storage access
   * @return a new CredentialContext instance
   */
  public static CredentialContext create(
      NormalizedURL location,
      Set<Privilege> privileges,
      Optional<CredentialDAO> credentialDAO,
      Optional<String> s3Endpoint,
      Optional<String> s3Region,
      boolean isServerAccess) {
    URI locationURI = location.toUri();
    return CredentialContext.builder()
        .privileges(privileges)
        .storageScheme(UriScheme.fromURI(locationURI))
        .storageBase(location.getStorageBase())
        .locations(List.of(location))
        .credentialDAO(credentialDAO)
        .s3Endpoint(s3Endpoint)
        .s3Region(s3Region)
        .isServerAccess(isServerAccess)
        .build();
  }
}
