package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.hadoop.internal.id.CredId;

/** Selection policy for single credential sources. Returns the sole credential in the bundle. */
final class SingleCredentialSelector implements CredentialSelector {

  @Override
  public GenericCredential select(CredentialBundle bundle, CredId credId) {
    return bundle.single();
  }
}
