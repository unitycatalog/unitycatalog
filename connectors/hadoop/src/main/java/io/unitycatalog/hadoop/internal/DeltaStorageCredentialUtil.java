package io.unitycatalog.hadoop.internal;

import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import java.util.List;

/** Internal utility for UC Delta storage credentials. */
public final class DeltaStorageCredentialUtil {
  private DeltaStorageCredentialUtil() {}

  /** Converts one UC Delta storage credential into an internal {@link GenericCredential}. */
  public static GenericCredential toGenericCredential(DeltaStorageCredential cred) {
    DeltaStorageCredentialConfig config = requireSingleCloudConfig(cred);
    long expiry = cred.getExpirationTimeMs() == null ? Long.MAX_VALUE : cred.getExpirationTimeMs();
    String prefix = cred.getPrefix();

    if (isS3Config(config)) {
      return GenericCredential.forAws(
          CredentialUtil.field(
              config.getS3AccessKeyId(),
              "UC Delta credential for '%s' is missing S3 access key.",
              prefix),
          CredentialUtil.field(
              config.getS3SecretAccessKey(),
              "UC Delta credential for '%s' is missing S3 secret key.",
              prefix),
          CredentialUtil.field(
              config.getS3SessionToken(),
              "UC Delta credential for '%s' is missing S3 session token.",
              prefix),
          expiry);
    } else if (isAzureConfig(config)) {
      return GenericCredential.forAzure(
          CredentialUtil.field(
              config.getAzureSasToken(),
              "UC Delta credential for '%s' is missing Azure SAS token.",
              prefix),
          expiry);
    } else {
      return GenericCredential.forGcs(
          CredentialUtil.field(
              config.getGcsOauthToken(),
              "UC Delta credential for '%s' is missing GCS OAuth token.",
              prefix),
          expiry);
    }
  }

  /** Selects the credential whose prefix covers the requested location. */
  public static DeltaStorageCredential selectForLocation(
      String location, List<DeltaStorageCredential> creds) {
    Preconditions.checkArgument(
        creds != null && !creds.isEmpty(),
        "UC Delta API response for '%s' has no storage credentials.",
        location);
    if (creds.size() == 1) {
      Preconditions.checkNotNull(
          creds.get(0), "UC Delta API response for '%s' contains null.", location);
    }
    DeltaStorageCredential best = null;
    int bestLen = -1;
    for (DeltaStorageCredential c : creds) {
      if (c == null || c.getPrefix() == null || !prefixCovers(location, c.getPrefix())) {
        continue;
      }
      int len = stripTrailingSlashes(c.getPrefix()).length();
      if (len > bestLen) {
        best = c;
        bestLen = len;
      }
    }
    Preconditions.checkArgument(
        best != null, "No UC Delta credential matched location '%s'.", location);
    return best;
  }

  static boolean prefixCovers(String location, String prefix) {
    String l = stripTrailingSlashes(location);
    String p = stripTrailingSlashes(prefix);
    return !p.isEmpty() && (l.equals(p) || (l.startsWith(p) && l.charAt(p.length()) == '/'));
  }

  private static String stripTrailingSlashes(String value) {
    int end = value.length();
    int min = value.indexOf("://");
    min = min >= 0 ? min + 3 : 1;
    while (end > min && value.charAt(end - 1) == '/') {
      end--;
    }
    return value.substring(0, end);
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
}
