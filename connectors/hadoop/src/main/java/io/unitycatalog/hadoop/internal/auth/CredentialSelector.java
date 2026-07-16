package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.hadoop.internal.id.CredId;
import io.unitycatalog.hadoop.internal.id.DeltaStagingTableCredId;
import io.unitycatalog.hadoop.internal.id.DeltaTableCredId;

/**
 * Selects the {@link GenericCredential} from a {@link CredentialBundle} that applies to a given
 * credential scope. Behavior depends on the credential type {@link CredId}.
 */
public interface CredentialSelector {

  GenericCredential select(CredentialBundle bundle, CredId credId);

  /** Returns the selection policy that applies to {@code credId}'s type. */
  static CredentialSelector forCredId(CredId credId) {
    if (credId instanceof DeltaTableCredId || credId instanceof DeltaStagingTableCredId) {
      return new PrefixCredentialSelector();
    }
    return new SingleCredentialSelector();
  }
}
