package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.audit.AuditRecord.Purpose;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.Set;

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
   * Vends temporary credentials, as the two-argument form does. {@code purpose} only labels the
   * audit record; it does not change what is vended. The two-argument form is for {@link
   * Purpose#CLIENT}.
   */
  default TemporaryCredentials vendCredential(
      NormalizedURL path, Set<CredentialContext.Privilege> privileges, Purpose purpose) {
    return vendCredential(path, privileges);
  }
}
