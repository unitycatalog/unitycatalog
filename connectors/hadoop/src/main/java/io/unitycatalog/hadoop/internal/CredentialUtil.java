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
import java.util.function.Function;

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

  /** Selects the credential whose location covers {@code location} (longest match wins). */
  public static GenericCredential selectForLocation(
      String location, List<GenericCredential> creds) {
    int index = longestCoveringIndex(location, creds, GenericCredential::location);
    Preconditions.checkArgument(index >= 0, "No vended credential covers location '%s'.", location);
    return creds.get(index);
  }

  /**
   * Returns the index of the item whose prefix (via {@code prefixOf}) covers {@code location} by
   * the longest match, or {@code -1} if none does. Items with a null prefix are skipped.
   */
  private static <T> int longestCoveringIndex(
      String location, List<T> items, Function<T, String> prefixOf) {
    int best = -1;
    int bestLen = -1;
    for (int i = 0; i < items.size(); i++) {
      String prefix = prefixOf.apply(items.get(i));
      if (prefix == null || !prefixCovers(location, prefix)) {
        continue;
      }
      int len = canonicalLocation(prefix).length();
      if (len > bestLen) {
        best = i;
        bestLen = len;
      }
    }
    return best;
  }

  /**
   * Whether {@code prefix} covers {@code location}: they denote the same storage location or {@code
   * prefix} is an ancestor path of it. Both sides are reduced to a canonical form (canonical
   * scheme, no trailing slashes) first, so scheme aliases like {@code s3a} and {@code s3} match.
   */
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
