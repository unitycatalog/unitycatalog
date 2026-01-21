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

  /**
   * Creates a CredentialContext for a single storage location.
   *
   * @param location the storage location URL (e.g., s3://bucket/path)
   * @param privileges the set of privileges required (SELECT for read, UPDATE for write)
   * @param credentialDAO optional storage credential from an external location; if present, the
   *     credential vendor will use it to generate credential instead of using per-bucket
   *     configuration.
   * @return a new CredentialContext instance
   */
  public static CredentialContext create(
      NormalizedURL location, Set<Privilege> privileges, Optional<CredentialDAO> credentialDAO) {
    URI locationURI = location.toUri();
    return CredentialContext.builder()
        .privileges(privileges)
        .storageScheme(UriScheme.fromURI(locationURI))
        .storageBase(location.getStorageBase())
        .locations(List.of(location))
        .credentialDAO(credentialDAO)
        .build();
  }
}
