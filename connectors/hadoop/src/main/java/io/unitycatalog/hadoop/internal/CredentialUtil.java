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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Function;

/** Internal utilities for building and selecting {@link GenericCredential}s. */
public final class CredentialUtil {
  private static final Charset UTF_8 = StandardCharsets.UTF_8;

  private CredentialUtil() {}

  public static String[] encodeMultiCredPrefixes(List<String> prefixes) {
    Preconditions.checkArgument(
        prefixes != null && !prefixes.isEmpty(),
        "List of credential prefixes cannot be null or empty");
    List<String> encodedPrefixes = new ArrayList<>(prefixes.size());
    Base64.Encoder encoder = Base64.getEncoder();
    for (String prefix : prefixes) {
      Preconditions.checkArgument(
          prefix != null && !prefix.isEmpty(), "Credential prefixes cannot be null or empty");
      encodedPrefixes.add(encoder.encodeToString(prefix.getBytes(UTF_8)));
    }
    return encodedPrefixes.toArray(String[]::new);
  }

  public static List<String> decodeMultiCredPrefixes(String[] prefixes) {
    Preconditions.checkArgument(
        prefixes != null && prefixes.length > 0,
        "Encoded credential prefixes cannot be null or empty");
    Base64.Decoder decoder = Base64.getDecoder();
    List<String> decodedPrefixes = new ArrayList<>(prefixes.length);
    for (String prefix : prefixes) {
      Preconditions.checkArgument(prefix != null, "Encoded credential prefixes cannot be null");
      String decodedPrefix = new String(decoder.decode(prefix), UTF_8);
      Preconditions.checkArgument(!decodedPrefix.isEmpty(), "Credential prefixes cannot be empty");
      decodedPrefixes.add(decodedPrefix);
    }
    return decodedPrefixes;
  }

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
    Preconditions.checkArgument(
        creds != null && creds.size() > 1,
        "Credential selection requires multiple storage credentials.");
    int selected = longestCoveringIndex(location, creds, GenericCredential::prefix);
    Preconditions.checkArgument(
        selected >= 0, "No vended credential covers location '%s'.", location);
    return creds.get(selected);
  }

  /**
   * Returns the index of the prefix that covers {@code location} by the longest match, or {@code
   * -1} if none does. Null prefixes are skipped.
   */
  public static int longestCoveringIndex(String location, List<String> prefixes) {
    Preconditions.checkNotNull(prefixes, "List of prefixes cannot be null.");
    return longestCoveringIndex(location, prefixes, Function.identity());
  }

  private static <T> int longestCoveringIndex(
      String location, List<T> values, Function<T, String> prefixExtractor) {
    int best = -1;
    int bestLen = -1;
    for (int i = 0; i < values.size(); i++) {
      T value = values.get(i);
      if (value == null) {
        continue;
      }
      String prefix = prefixExtractor.apply(value);
      if (prefix == null || !prefixCovers(location, prefix)) {
        continue;
      }
      int len = stripTrailingSlashes(prefix).length();
      if (len > bestLen) {
        best = i;
        bestLen = len;
      }
    }
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
