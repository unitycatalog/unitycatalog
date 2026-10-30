package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** Vends temporary cloud storage credentials for accessing data at specific paths. */
public interface StorageCredentialVendor {
  /**
   * Vends temporary credentials for accessing the specified storage path. Authorization must
   * already be checked by the caller.
   *
   * @param path the normalized storage location URL (e.g., s3://bucket/path)
   * @param privileges the set of privileges required (SELECT, UPDATE)
   * @return temporary credentials scoped to the requested path and privileges
   */
  TemporaryCredentials vendCredential(
      NormalizedURL path, Set<CredentialContext.Privilege> privileges);

  /**
   * Vends temporary credentials, as the two-argument form does. {@code purpose} and {@code
   * resourceId} only label the audit record; they do not change what is vended. The two-argument
   * form is for {@link Purpose#CLIENT} and names no resource.
   *
   * @param resourceId the ID of the object the credential is for, when the caller knows it
   */
  default TemporaryCredentials vendCredential(
      NormalizedURL path,
      Set<CredentialContext.Privilege> privileges,
      Purpose purpose,
      Optional<UUID> resourceId) {
    return vendCredential(path, privileges);
  }
}
