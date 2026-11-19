package io.unitycatalog.hadoop.internal.props;

import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.util.S3Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;

/** Builds S3 credential properties. */
final class S3CredPropsBuilder extends CredPropsBuilder {
  private static final String AWS_VENDED_TOKEN_PROVIDER_CLASS =
      "io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider";
  private static final String S3A_ACCESS_KEY = "fs.s3a.access.key";
  private static final String S3A_SECRET_KEY = "fs.s3a.secret.key";
  private static final String S3A_SESSION_TOKEN = "fs.s3a.session.token";

  S3CredPropsBuilder(Configuration hadoopConf) {
    super(hadoopConf);
    set("fs.s3a.path.style.access", "true");
    set("fs.s3.impl.disable.cache", "true");
    set("fs.s3a.impl.disable.cache", "true");
  }

  @Override
  protected void setCredScopedFsKeys() {
    saveAndOverride("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem", CRED_SCOPED_FS_CLASS);
    saveAndOverride("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem", CRED_SCOPED_FS_CLASS);
    saveAndOverride(
        "fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A", CRED_SCOPED_AFS_CLASS);
    saveAndOverride(
        "fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A", CRED_SCOPED_AFS_CLASS);
  }

  @Override
  protected void setS3EndpointKeys(List<GenericCredential> credentials) {
    // S3A takes one endpoint and region per bucket, so all credentials for a bucket must agree,
    // including AWS S3 ones (no endpoint): an endpoint set for the bucket would also take their
    // paths to the other storage.
    Map<String, AwsCredential> byBucket = new HashMap<>();
    // Without renewal only a single credential's keys are set, so several with keys for
    // S3-compatible storage would reach it with the engine's own keys; and only a
    // credential-scoped file system tells the vended provider which of them covers its path.
    Preconditions.checkArgument(
        (isRenewCredEnabled() && isCredScopedFsEnabled())
            || credentials.size() == 1
            || credentials.stream()
                .map(AwsCredential.class::cast)
                .noneMatch(aws -> aws.s3Endpoint() != null && aws.hasKeys()),
        "Several credentials with keys for S3-compatible storage need credential renewal and"
            + " credential-scoped file systems; got %s",
        credentials.size());
    for (GenericCredential cred : credentials) {
      AwsCredential aws = (AwsCredential) cred;
      if (aws.s3Endpoint() == null) {
        // A prefix that names no bucket is left to the rest of the connector.
        S3Paths.bucketIfAny(cred.prefix())
            .ifPresent(
                bucket -> {
                  checkSameStorage(byBucket, bucket, aws);
                  recordVendedStorage(bucket, aws, credentials.size());
                });
        continue;
      }
      Preconditions.checkArgument(
          cred.prefix() != null,
          "S3 endpoint %s has no storage path to apply to",
          aws.s3Endpoint());
      String bucket = S3Paths.bucketOf(cred.prefix());
      checkSameStorage(byBucket, bucket, aws);
      recordVendedStorage(bucket, aws, credentials.size());
      // Per-bucket, so other buckets read in the same session keep their own S3 endpoint and
      // region.
      String bucketPrefix = "fs.s3a.bucket." + bucket + ".endpoint";
      set(bucketPrefix, aws.s3Endpoint());
      if (aws.s3Region() != null) {
        set(bucketPrefix + ".region", aws.s3Region());
      }
      // At bucket scope too, since a bucket option in the engine's configuration overrides the
      // global one; the engine's own keys stay in use when no keys are vended.
      String bucketScope = "fs.s3a.bucket." + bucket;
      set(bucketScope + ".path.style.access", "true");
      // As the global keys: the vended provider with renewal, otherwise a single credential's keys.
      if (aws.hasKeys()) {
        if (isRenewCredEnabled()) {
          set(bucketScope + ".aws.credentials.provider", AWS_VENDED_TOKEN_PROVIDER_CLASS);
        } else if (credentials.size() == 1) {
          set(bucketScope + ".access.key", aws.accessKeyId());
          set(bucketScope + ".secret.key", aws.secretAccessKey());
          set(bucketScope + ".session.token", aws.sessionToken() != null ? aws.sessionToken() : "");
          // A provider that reads these keys, rather than one the engine configured for its own.
          set(bucketScope + ".aws.credentials.provider", fixedKeysProvider(aws.sessionToken()));
        }
      }
    }
    // S3-compatible keys serve only their buckets, set above: a global vended provider would also
    // send them to any other bucket the file system opens, on AWS S3 by default. One the engine's
    // own configuration lists is dropped from its list, which keeps its other providers; an empty
    // list is S3A's default chain.
    if (credentials.stream().map(AwsCredential.class::cast).allMatch(a -> a.s3Endpoint() != null)) {
      String engineProviders = hadoopConf().get(UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER, "");
      if (engineProviders.contains(AWS_VENDED_TOKEN_PROVIDER_CLASS)) {
        set(
            UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER,
            Arrays.stream(engineProviders.split(","))
                .map(String::trim)
                .filter(provider -> !provider.isEmpty())
                .filter(provider -> !provider.equals(AWS_VENDED_TOKEN_PROVIDER_CLASS))
                .collect(Collectors.joining(",")));
      } else {
        unset(UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER);
      }
    }
  }

  private static void checkSameStorage(
      Map<String, AwsCredential> byBucket, String bucket, AwsCredential aws) {
    AwsCredential other = byBucket.putIfAbsent(bucket, aws);
    Preconditions.checkArgument(
        other == null
            || (Objects.equals(other.s3Endpoint(), aws.s3Endpoint())
                && Objects.equals(other.s3Region(), aws.s3Region())),
        "Paths %s and %s of bucket %s are on different storage (%s and %s); one session can"
            + " reach a bucket name on only one storage",
        other == null ? null : other.prefix(),
        aws.prefix(),
        bucket,
        other == null ? null : storageName(other),
        storageName(aws));
  }

  /**
   * Records the storage vended for a bucket, which renewal checks against, and whether S3A signs
   * the bucket's requests with the vended keys, which the file system cache key reads.
   */
  private void recordVendedStorage(String bucket, AwsCredential aws, int credentialCount) {
    set(
        UCHadoopConfConstants.UC_VENDED_S3_ENDPOINT_PREFIX + bucket,
        Objects.toString(aws.s3Endpoint(), ""));
    set(
        UCHadoopConfConstants.UC_VENDED_S3_REGION_PREFIX + bucket,
        Objects.toString(aws.s3Region(), ""));
    set(
        UCHadoopConfConstants.UC_VENDED_S3_KEYS_PREFIX + bucket,
        String.valueOf(vendedKeysSign(bucket, aws, credentialCount)));
  }

  /**
   * Whether S3A signs the bucket's requests with the vended keys, rather than with the engine's own
   * settings: the keys reach the bucket only through the provider or keys set above, and the
   * engine's bucket options override the global ones.
   *
   * @param bucket the bucket
   * @param aws the credential vended for it
   * @param credentialCount how many credentials the table load vended
   * @return whether the vended keys sign the bucket's requests
   */
  private boolean vendedKeysSign(String bucket, AwsCredential aws, int credentialCount) {
    if (!aws.hasKeys() || (!isRenewCredEnabled() && credentialCount > 1)) {
      // Without renewal, keys are set only for a single credential.
      return false;
    }
    String bucketScope = "fs.s3a.bucket." + bucket + ".";
    // With S3A delegation tokens, the current user's token signs, whatever provider is set.
    if (engineSets(bucketScope + "delegation.token.binding")
        || engineSets("fs.s3a.delegation.token.binding")) {
      return false;
    }
    if (isRenewCredEnabled()) {
      // The vended provider reads its keys itself. It is set at the bucket's scope for
      // S3-compatible storage; for AWS S3 globally, where an engine provider for the bucket wins.
      return aws.s3Endpoint() != null || !engineSets(bucketScope + "aws.credentials.provider");
    }
    // Without renewal S3A's providers read the keys through Hadoop's credential stores first, which
    // may hold other keys, per user too.
    if (Stream.of(
            bucketScope + "security.credential.provider.path",
            "fs.s3a.security.credential.provider.path",
            "hadoop.security.credential.provider.path")
        .anyMatch(this::engineSets)) {
      return false;
    }
    if (aws.s3Endpoint() != null) {
      // The keys and their provider are set at the bucket's scope, over the engine's options.
      return true;
    }
    // AWS S3 keys are set globally, so the engine's own provider or keys for the bucket, or its own
    // global provider, win.
    return !engineSets(bucketScope + "aws.credentials.provider")
        && !engineSets(bucketScope + "access.key")
        && !engineSets(UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER);
  }

  /**
   * @return the S3A provider that reads a fixed key pair, with its session token if it has one
   */
  static String fixedKeysProvider(String sessionToken) {
    return sessionToken != null
        ? "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
        : "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
  }

  private static String storageName(AwsCredential aws) {
    if (aws.s3Endpoint() == null) {
      return "AWS S3";
    }
    return aws.s3Region() == null ? aws.s3Endpoint() : aws.s3Endpoint() + " in " + aws.s3Region();
  }

  @Override
  protected void setVendedProviderKeys() {
    set(UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER, AWS_VENDED_TOKEN_PROVIDER_CLASS);
  }

  @Override
  protected void setInitRenewableCredKeys(GenericCredential cred) {
    Preconditions.checkArgument(
        cred instanceof AwsCredential,
        "Expected AwsCredential, but got %s",
        cred.getClass().getSimpleName());
    AwsCredential aws = (AwsCredential) cred;
    set(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, aws.accessKeyId());
    set(UCHadoopConfConstants.S3A_INIT_SECRET_KEY, aws.secretAccessKey());
    // A key pair vended for S3-compatible storage has no session token: empty, so that one already
    // configured is not paired with it, and the endpoint tells the provider, which reads these keys
    // back, that one is not needed.
    set(
        UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN,
        aws.sessionToken() != null ? aws.sessionToken() : "");
    // Empty when absent, so that a value already configured is not used.
    set(UCHadoopConfConstants.S3A_INIT_S3_ENDPOINT, Objects.toString(aws.s3Endpoint(), ""));
    set(UCHadoopConfConstants.S3A_INIT_S3_REGION, Objects.toString(aws.s3Region(), ""));
    // Expiration may be absent (e.g. a static token provider), so write the key only when set.
    if (aws.expirationTimeMillis() != null) {
      set(
          UCHadoopConfConstants.S3A_INIT_CRED_EXPIRED_TIME,
          String.valueOf(aws.expirationTimeMillis()));
    }
  }

  @Override
  protected void setInitFixedCredKeys(GenericCredential cred) {
    Preconditions.checkArgument(
        cred instanceof AwsCredential,
        "Expected AwsCredential, but got %s",
        cred.getClass().getSimpleName());
    AwsCredential aws = (AwsCredential) cred;
    if (!aws.hasKeys() || aws.s3Endpoint() != null) {
      // Only the S3 endpoint is vended, so the engine's own keys stay in place; or the keys are
      // for S3-compatible storage, set at the scope of its bucket only.
      return;
    }
    set(S3A_ACCESS_KEY, aws.accessKeyId());
    set(S3A_SECRET_KEY, aws.secretAccessKey());
    // Empty for a key pair, so that a session token already in the configuration is not paired
    // with the vended keys.
    set(S3A_SESSION_TOKEN, aws.sessionToken() != null ? aws.sessionToken() : "");
  }
}
