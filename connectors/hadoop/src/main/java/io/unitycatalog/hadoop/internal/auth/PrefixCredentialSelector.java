package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.StorageLocationUtil;
import io.unitycatalog.hadoop.internal.id.CredId;

/**
 * Selection policy for prefix-scoped credentials. A response may carry several credentials each
 * scoped to a storage-path prefix; this selects the one whose prefix covers the requested location,
 * preferring the longest match. Coverage is always validated, even when the bundle holds a single
 * credential.
 */
final class PrefixCredentialSelector implements CredentialSelector {

  @Override
  public GenericCredential select(CredentialBundle bundle, CredId credId) {
    String location = credId.location();
    Preconditions.checkArgument(
        location != null && !location.isEmpty(),
        "Prefix-scoped credential selection requires a location, but the credId has none.");

    GenericCredential best =
        StorageLocationUtil.longestCovering(location, bundle.all(), GenericCredential::prefix);
    Preconditions.checkArgument(best != null, "No credential matched location '%s'.", location);
    return best;
  }
}
