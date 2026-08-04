package io.unitycatalog.hadoop.internal;

import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.AzureCredential;
import io.unitycatalog.hadoop.internal.auth.GcsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import java.util.List;

/** Internal utilities for building and selecting {@link GenericCredential}s. */
public final class CredentialUtil {
  private CredentialUtil() {}

  /** Converts a UC SDK {@link TemporaryCredentials} into an internal {@link GenericCredential}. */
  public static GenericCredential toGenericCredential(TemporaryCredentials tempCred) {
    long expiry =
        tempCred.getExpirationTime() == null ? Long.MAX_VALUE : tempCred.getExpirationTime();
    if (tempCred.getAwsTempCredentials() != null) {
      AwsCredentials aws = tempCred.getAwsTempCredentials();
      return new AwsCredential(
          aws.getAccessKeyId(),
          aws.getSecretAccessKey(),
          aws.getSessionToken(),
          expiry,
          tempCred.getUrl());
    } else if (tempCred.getAzureUserDelegationSas() != null) {
      return new AzureCredential(
          tempCred.getAzureUserDelegationSas().getSasToken(), expiry, tempCred.getUrl());
    } else if (tempCred.getGcpOauthToken() != null) {
      return new GcsCredential(
          tempCred.getGcpOauthToken().getOauthToken(), expiry, tempCred.getUrl());
    }
    throw new IllegalArgumentException("UC temporary credentials contained no cloud credential");
  }

  /** Converts one UC Delta storage credential into an internal {@link GenericCredential}. */
  public static GenericCredential toGenericCredential(DeltaStorageCredential cred) {
    DeltaStorageCredentialConfig config = requireSingleCloudConfig(cred);
    long expiry = cred.getExpirationTimeMs() == null ? Long.MAX_VALUE : cred.getExpirationTimeMs();

    if (isS3Config(config)) {
      return new AwsCredential(
          config.getS3AccessKeyId(),
          config.getS3SecretAccessKey(),
          config.getS3SessionToken(),
          expiry,
          cred.getPrefix());
    } else if (isAzureConfig(config)) {
      return new AzureCredential(config.getAzureSasToken(), expiry, cred.getPrefix());
    } else {
      return new GcsCredential(config.getGcsOauthToken(), expiry, cred.getPrefix());
    }
  }

  /** Selects the credential whose location covers {@code location} (longest match wins). */
  public static GenericCredential selectForLocation(
      String location, List<GenericCredential> creds) {
    GenericCredential best = null;
    int bestLen = -1;
    for (GenericCredential cred : creds) {
      String prefix = cred.prefix();
      if (prefix == null || !prefixCovers(location, prefix)) {
        continue;
      }
      int len = stripTrailingSlashes(prefix).length();
      if (len > bestLen) {
        best = cred;
        bestLen = len;
      }
    }
    Preconditions.checkArgument(
        best != null, "No vended credential covers location '%s'.", location);
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
