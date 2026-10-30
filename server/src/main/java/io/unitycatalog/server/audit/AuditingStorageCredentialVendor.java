package io.unitycatalog.server.audit;

import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.Purpose;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** Records every vended credential, including ones served from the credential cache below. */
final class AuditingStorageCredentialVendor implements StorageCredentialVendor {
  private final StorageCredentialVendor delegate;
  private final Auditor auditor;

  AuditingStorageCredentialVendor(StorageCredentialVendor delegate, Auditor auditor) {
    this.delegate = delegate;
    this.auditor = auditor;
  }

  @Override
  public TemporaryCredentials vendCredential(
      NormalizedURL path, Set<CredentialContext.Privilege> privileges) {
    TemporaryCredentials credential = delegate.vendCredential(path, privileges);
    auditor.recordCredential(path, privileges, Purpose.CLIENT, Optional.empty(), credential);
    return credential;
  }

  @Override
  public TemporaryCredentials vendCredential(
      NormalizedURL path,
      Set<CredentialContext.Privilege> privileges,
      Purpose purpose,
      Optional<UUID> resourceId) {
    TemporaryCredentials credential =
        delegate.vendCredential(path, privileges, purpose, resourceId);
    auditor.recordCredential(path, privileges, purpose, resourceId, credential);
    return credential;
  }
}
