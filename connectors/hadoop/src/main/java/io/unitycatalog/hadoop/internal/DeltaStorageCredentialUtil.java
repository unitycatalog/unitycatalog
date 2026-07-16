package io.unitycatalog.hadoop.internal;

import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.auth.CredentialBundle;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import java.util.ArrayList;
import java.util.List;

/** Internal utility for UC Delta storage credentials. */
public final class DeltaStorageCredentialUtil {
  private DeltaStorageCredentialUtil() {}

  /**
   * Maps every storage credential in a UC Delta response into a {@link CredentialBundle},
   * preserving order.
   */
  public static CredentialBundle toCredentialBundle(List<DeltaStorageCredential> creds) {
    Preconditions.checkArgument(
        creds != null && !creds.isEmpty(), "UC Delta API response has no storage credentials.");
    List<GenericCredential> out = new ArrayList<>(creds.size());
    for (DeltaStorageCredential cred : creds) {
      Preconditions.checkNotNull(cred, "UC Delta API response contains a null storage credential.");
      out.add(toGenericCredential(cred));
    }
    return CredentialBundle.of(out);
  }

  /**
   * Converts one UC Delta storage credential into a {@link GenericCredential} scoped to its prefix.
   *
   * <p>The credential's config must contain exactly one cloud's values; a missing config, multiple
   * clouds, or a partially-populated cloud is rejected.
   */
  public static GenericCredential toGenericCredential(DeltaStorageCredential cred) {
    DeltaStorageCredentialConfig config = requireSingleCloudConfig(cred);
    long expiry = cred.getExpirationTimeMs() == null ? Long.MAX_VALUE : cred.getExpirationTimeMs();
    GenericCredential result;
    if (isS3Config(config)) {
      result =
          GenericCredential.forAws(
              field(config.getS3AccessKeyId(), cred, "S3 access key"),
              field(config.getS3SecretAccessKey(), cred, "S3 secret key"),
              field(config.getS3SessionToken(), cred, "S3 session token"),
              expiry);
    } else if (isAzureConfig(config)) {
      result =
          GenericCredential.forAzure(
              field(config.getAzureSasToken(), cred, "Azure SAS token"), expiry);
    } else {
      result =
          GenericCredential.forGcs(
              field(config.getGcsOauthToken(), cred, "GCS OAuth token"), expiry);
    }
    return result.withPrefix(cred.getPrefix());
  }

  private static DeltaStorageCredentialConfig requireSingleCloudConfig(
      DeltaStorageCredential cred) {
    Preconditions.checkNotNull(cred, "storageCredential cannot be null");
    DeltaStorageCredentialConfig c = cred.getConfig();
    Preconditions.checkArgument(
        c != null, "UC Delta credential for '%s' is missing config.", cred.getPrefix());
    int clouds =
        (isS3Config(c) ? 1 : 0)
            + (c.getAzureSasToken() != null ? 1 : 0)
            + (c.getGcsOauthToken() != null ? 1 : 0);
    Preconditions.checkArgument(
        clouds == 1,
        "UC Delta credential for '%s' must contain exactly one cloud credential config.",
        cred.getPrefix());
    return c;
  }

  private static boolean isS3Config(DeltaStorageCredentialConfig c) {
    return c.getS3AccessKeyId() != null
        || c.getS3SecretAccessKey() != null
        || c.getS3SessionToken() != null;
  }

  private static boolean isAzureConfig(DeltaStorageCredentialConfig c) {
    return c.getAzureSasToken() != null;
  }

  private static String field(String value, DeltaStorageCredential cred, String label) {
    Preconditions.checkArgument(
        value != null && !value.isEmpty(),
        "UC Delta credential for '%s' is missing %s.",
        cred.getPrefix(),
        label);
    return value;
  }
}
