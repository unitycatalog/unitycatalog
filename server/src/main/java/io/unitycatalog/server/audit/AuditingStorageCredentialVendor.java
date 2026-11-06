package io.unitycatalog.server.audit;

import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.CredentialUse;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.Set;

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
      NormalizedURL path, Set<CredentialContext.Privilege> privileges, CredentialUse use) {
    TemporaryCredentials credential = delegate.vendCredential(path, privileges, use);
    auditor.recordCredential(path, privileges, use, credential);
    return credential;
  }
}
