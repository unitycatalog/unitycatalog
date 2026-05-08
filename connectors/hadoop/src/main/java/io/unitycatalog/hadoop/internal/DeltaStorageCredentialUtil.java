package io.unitycatalog.hadoop.internal;

import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.delta.model.StorageCredential;
import io.unitycatalog.client.delta.model.StorageCredentialConfig;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.util.List;

/** Internal utility for UC Delta storage credentials. */
public final class DeltaStorageCredentialUtil {
  private DeltaStorageCredentialUtil() {}

  /** Converts a UC Delta credential operation to the equivalent standard UC table operation. */
  public static TableOperation toTableOperation(CredentialOperation op) {
    Preconditions.checkArgument(op != null, "StorageCredential.operation is required");
    switch (op) {
      case READ:
        return TableOperation.READ;
      case READ_WRITE:
        return TableOperation.READ_WRITE;
      default:
        throw new IllegalArgumentException(
            "Unsupported UC Delta table credential operation: " + op);
    }
  }

  /** Converts one UC Delta storage credential into standard UC temporary credentials. */
  public static TemporaryCredentials toTemporaryCredentials(StorageCredential cred) {
    StorageCredentialConfig config = requireSingleCloudConfig(cred);
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

  /** Selects the credential whose prefix covers the requested location. */
  public static StorageCredential selectForLocation(
      String location, List<StorageCredential> creds) {
    Preconditions.checkArgument(
        creds != null && !creds.isEmpty(),
        "UC Delta API response for '%s' has no storage credentials.",
        location);
    if (creds.size() == 1) {
      Preconditions.checkNotNull(
          creds.get(0), "UC Delta API response for '%s' contains null.", location);
    }
    StorageCredential best = null;
    int bestLen = -1;
    for (StorageCredential c : creds) {
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

  private static StorageCredentialConfig requireSingleCloudConfig(StorageCredential cred) {
    Preconditions.checkNotNull(cred, "storageCredential cannot be null");
    StorageCredentialConfig c = cred.getConfig();
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

  private static boolean isS3Config(StorageCredentialConfig c) {
    return c.getS3AccessKeyId() != null
        || c.getS3SecretAccessKey() != null
        || c.getS3SessionToken() != null;
  }

  private static boolean isAzureConfig(StorageCredentialConfig c) {
    return c.getAzureSasToken() != null;
  }

  private static String field(String value, StorageCredential cred, String label) {
    Preconditions.checkArgument(
        value != null && !value.isEmpty(),
        "UC Delta credential for '%s' is missing %s.",
        cred.getPrefix(),
        label);
    return value;
  }
}
