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
import java.util.Locale;

/** Internal utilities for building and selecting {@link GenericCredential}s. */
public final class CredentialUtil {
  private CredentialUtil() {}

  /** Converts a UC SDK {@link TemporaryCredentials} into an internal {@link GenericCredential}. */
  public static GenericCredential toGenericCredential(TemporaryCredentials tempCred) {
    Long expiration = tempCred.getExpirationTime();
    if (tempCred.getAwsTempCredentials() != null) {
      AwsCredentials aws = tempCred.getAwsTempCredentials();
      return new AwsCredential(
          aws.getAccessKeyId(), aws.getSecretAccessKey(), aws.getSessionToken(), expiration, null);
    } else if (tempCred.getAzureUserDelegationSas() != null) {
      return new AzureCredential(
          tempCred.getAzureUserDelegationSas().getSasToken(), expiration, null);
    } else if (tempCred.getGcpOauthToken() != null) {
      return new GcsCredential(tempCred.getGcpOauthToken().getOauthToken(), expiration, null);
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
      int len = canonicalLocation(c.getPrefix()).length();
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
    String l = canonicalLocation(location);
    String p = canonicalLocation(prefix);
    return !p.isEmpty() && (l.equals(p) || (l.startsWith(p) && l.charAt(p.length()) == '/'));
  }

  /** The comparable form of a location: canonical scheme with trailing slashes stripped. */
  static String canonicalLocation(String location) {
    return stripTrailingSlashes(normalizeScheme(location));
  }

  /**
   * Rewrites a known cloud scheme (any alias or letter case) to its canonical form via {@link
   * CloudType}; a scheme-less or unknown-scheme location is returned unchanged.
   */
  private static String normalizeScheme(String location) {
    int idx = location.indexOf("://");
    if (idx < 0) {
      return location;
    }
    String scheme = location.substring(0, idx);
    return CloudType.fromScheme(scheme.toLowerCase(Locale.ROOT))
        .map(type -> type.canonicalScheme() + location.substring(idx))
        .orElse(location);
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
