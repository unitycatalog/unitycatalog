package io.unitycatalog.hadoop.internal;

import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.auth.ScopedCredential;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/** Internal utility for UC Delta storage credentials. */
public final class DeltaStorageCredentialUtil {
  private DeltaStorageCredentialUtil() {}

  /** Converts one UC Delta storage credential into standard UC temporary credentials. */
  public static TemporaryCredentials toTemporaryCredentials(DeltaStorageCredential cred) {
    DeltaStorageCredentialConfig config = requireSingleCloudConfig(cred);
    long expiry = cred.getExpirationTimeMs() == null ? Long.MAX_VALUE : cred.getExpirationTimeMs();
    TemporaryCredentials out = new TemporaryCredentials().expirationTime(expiry);
    if (isS3Config(config)) {
      out.awsTempCredentials(
          new AwsCredentials()
              .accessKeyId(field(config.getS3AccessKeyId(), cred, "S3 access key"))
              .secretAccessKey(field(config.getS3SecretAccessKey(), cred, "S3 secret key"))
              .sessionToken(field(config.getS3SessionToken(), cred, "S3 session token")));
    } else if (isAzureConfig(config)) {
      out.azureUserDelegationSas(
          new AzureUserDelegationSAS()
              .sasToken(field(config.getAzureSasToken(), cred, "Azure SAS token")));
    } else {
      out.gcpOauthToken(
          new GcpOauthToken()
              .oauthToken(field(config.getGcsOauthToken(), cred, "GCS OAuth token")));
    }
    return out;
  }

  /** Cloud schemes the connector can translate into Hadoop credential properties. */
  private static boolean isSupportedCloudScheme(String scheme) {
    return scheme != null
        && (scheme.startsWith("s3")
            || scheme.equals("gs")
            || scheme.equals("abfs")
            || scheme.equals("abfss"));
  }

  /**
   * Converts every credential in {@code creds} other than {@code primary} into a {@link
   * ScopedCredential}, skipping prefixes the connector cannot translate.
   */
  public static List<ScopedCredential> toAdditionalScopedCredentials(
      DeltaStorageCredential primary, List<DeltaStorageCredential> creds) {
    List<ScopedCredential> scoped = new ArrayList<>();
    for (DeltaStorageCredential cred : creds) {
      if (cred == null || cred == primary || cred.getPrefix() == null) {
        continue;
      }
      if (!isSupportedCloudScheme(URI.create(cred.getPrefix()).getScheme())) {
        continue;
      }
      String operation = cred.getOperation() == null ? "READ" : cred.getOperation().getValue();
      scoped.add(new ScopedCredential(cred.getPrefix(), operation, toTemporaryCredentials(cred)));
    }
    return scoped;
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

  private static String field(String value, DeltaStorageCredential cred, String label) {
    Preconditions.checkArgument(
        value != null && !value.isEmpty(),
        "UC Delta credential for '%s' is missing %s.",
        cred.getPrefix(),
        label);
    return value;
  }
}
