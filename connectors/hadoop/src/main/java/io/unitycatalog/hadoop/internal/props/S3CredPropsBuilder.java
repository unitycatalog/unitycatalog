package io.unitycatalog.hadoop.internal.props;

import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.util.S3Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    // S3-compatible storage would reach it with the engine's own keys.
    Preconditions.checkArgument(
        isRenewCredEnabled()
            || credentials.size() == 1
            || credentials.stream()
                .map(AwsCredential.class::cast)
                .noneMatch(aws -> aws.s3Endpoint() != null && aws.hasKeys()),
        "Several credentials with keys for S3-compatible storage need credential renewal; got %s",
        credentials.size());
    for (GenericCredential cred : credentials) {
      AwsCredential aws = (AwsCredential) cred;
      if (aws.s3Endpoint() == null) {
        // A prefix that names no bucket is left to the rest of the connector.
        S3Paths.bucketIfAny(cred.prefix())
            .ifPresent(
                bucket -> {
                  checkSameStorage(byBucket, bucket, aws);
                  recordVendedStorage(bucket, aws);
                });
        continue;
      }
      Preconditions.checkArgument(
          cred.prefix() != null,
          "S3 endpoint %s has no storage path to apply to",
          aws.s3Endpoint());
      String bucket = S3Paths.bucketOf(cred.prefix());
      checkSameStorage(byBucket, bucket, aws);
      recordVendedStorage(bucket, aws);
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

  /** Records the storage vended for a bucket, which renewal checks against. */
  private void recordVendedStorage(String bucket, AwsCredential aws) {
    set(
        UCHadoopConfConstants.UC_VENDED_S3_ENDPOINT_PREFIX + bucket,
        Objects.toString(aws.s3Endpoint(), ""));
    set(
        UCHadoopConfConstants.UC_VENDED_S3_REGION_PREFIX + bucket,
        Objects.toString(aws.s3Region(), ""));
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
    if (aws.s3Endpoint() != null) {
      set(UCHadoopConfConstants.S3A_INIT_S3_ENDPOINT, aws.s3Endpoint());
    }
    if (aws.s3Region() != null) {
      set(UCHadoopConfConstants.S3A_INIT_S3_REGION, aws.s3Region());
    }
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
    if (!aws.hasKeys()) {
      // Only the S3 endpoint is vended: leave the engine's own keys in place.
      return;
    }
    set(S3A_ACCESS_KEY, aws.accessKeyId());
    set(S3A_SECRET_KEY, aws.secretAccessKey());
    // Empty for a key pair, so that a session token already in the configuration is not paired
    // with the vended keys.
    set(S3A_SESSION_TOKEN, aws.sessionToken() != null ? aws.sessionToken() : "");
  }
}
