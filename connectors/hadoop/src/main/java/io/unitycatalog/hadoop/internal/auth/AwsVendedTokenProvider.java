package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.util.S3Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Preconditions;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/** Hadoop S3A credentials provider backed by Unity Catalog temporary credentials. */
public class AwsVendedTokenProvider extends GenericCredentialProvider
    implements AwsCredentialsProvider {

  private final Configuration conf;

  // The last credential checkSameStorage accepted.
  private volatile AwsCredential checkedCredential;

  /**
   * Constructor for the hadoop's CredentialProviderListFactory#buildAWSProviderList to initialize.
   */
  public AwsVendedTokenProvider(Configuration conf) {
    this.conf = conf;
    initialize(conf);
  }

  @Override
  public GenericCredential initGenericCredential(Configuration conf) {
    String accessKey = conf.get(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY);
    String secretKey = conf.get(UCHadoopConfConstants.S3A_INIT_SECRET_KEY);
    // Empty for a key pair without a session token, so that one already configured is not used.
    String sessionToken = conf.get(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN);
    if (sessionToken != null && sessionToken.isEmpty()) {
      sessionToken = null;
    }
    String s3Endpoint = emptyToNull(conf.get(UCHadoopConfConstants.S3A_INIT_S3_ENDPOINT));
    // Only S3-compatible storage vends a key pair without a session token; otherwise the provider
    // fetches the credential from Unity Catalog.
    if (accessKey != null && secretKey != null && (sessionToken != null || s3Endpoint != null)) {

      long expiredTimeMillis =
          conf.getLong(UCHadoopConfConstants.S3A_INIT_CRED_EXPIRED_TIME, Long.MAX_VALUE);
      Preconditions.checkState(
          expiredTimeMillis > 0,
          "Expired time %s must be greater than 0, " + "please check configure key '%s'",
          expiredTimeMillis,
          UCHadoopConfConstants.S3A_INIT_CRED_EXPIRED_TIME);

      return new AwsCredential(
          accessKey,
          secretKey,
          sessionToken,
          expiredTimeMillis,
          conf.get(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY),
          s3Endpoint,
          emptyToNull(conf.get(UCHadoopConfConstants.S3A_INIT_S3_REGION)));
    } else {
      return null;
    }
  }

  @Override
  public AwsCredentials resolveCredentials() {
    AwsCredential aws = (AwsCredential) accessCredentials();
    Preconditions.checkState(
        aws.hasKeys(),
        "Unity Catalog now vends only the storage endpoint for %s, with no access keys, for"
            + " example after a change of its vending mode; load the table again.",
        aws.prefix());
    // The SDK resolves credentials for every request; a credential is checked once.
    if (aws != checkedCredential) {
      checkSameStorage(aws);
      checkedCredential = aws;
    }
    // A static key pair, renewed alongside session credentials of other locations, has no token.
    if (aws.sessionToken() == null) {
      return AwsBasicCredentials.create(aws.accessKeyId(), aws.secretAccessKey());
    }
    return AwsSessionCredentials.builder()
        .accessKeyId(aws.accessKeyId())
        .secretAccessKey(aws.secretAccessKey())
        .sessionToken(aws.sessionToken())
        .build();
  }

  /**
   * Refuses a renewed credential for other storage than the table load vended for its bucket: the
   * file system keeps the endpoint and region it was set up with, so the keys would go there, for
   * example from AWS S3 to S3-compatible storage or back. A bucket without a record was not routed
   * by the table load and keeps the engine's own routing, so it takes only AWS S3 renewals.
   */
  private void checkSameStorage(AwsCredential aws) {
    Optional<String> renewedBucket = S3Paths.bucketIfAny(aws.prefix());
    // The bucket the file system was set up for, from the prefix it serves; the renewed
    // credential's own prefix may name another, for a table recreated elsewhere.
    // Without a prefix, as for a single credential without credential-scoped file systems, the
    // table's only recorded bucket.
    Optional<String> bucket =
        S3Paths.bucketIfAny(conf.get(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY))
            .or(this::onlyRecordedBucket)
            .or(() -> renewedBucket);
    if (bucket.isEmpty()) {
      // No storage path to check against; the file system can only have been set up for AWS S3.
      Preconditions.checkState(
          aws.s3Endpoint() == null,
          "Unity Catalog now vends credentials for S3 endpoint %s, with no storage path to check"
              + " them against; load the table again.",
          aws.s3Endpoint());
      return;
    }
    Preconditions.checkState(
        renewedBucket.isEmpty() || renewedBucket.equals(bucket),
        "Unity Catalog now vends credentials for %s rather than bucket %s; load the table again.",
        aws.prefix(),
        bucket.get());
    String endpoint = conf.get(UCHadoopConfConstants.UC_VENDED_S3_ENDPOINT_PREFIX + bucket.get());
    if (endpoint == null) {
      // A table load records every bucket it routes, AWS S3 ones with an empty endpoint; one
      // without a record keeps the engine's own routing.
      Preconditions.checkState(
          aws.s3Endpoint() == null,
          "The storage of %s changed in Unity Catalog since the table was loaded, from AWS S3 to"
              + " %s; load the table again.",
          aws.prefix(),
          aws.s3Endpoint());
      return;
    }
    String region = conf.get(UCHadoopConfConstants.UC_VENDED_S3_REGION_PREFIX + bucket.get(), "");
    String renewedEndpoint = Objects.toString(aws.s3Endpoint(), "");
    String renewedRegion = Objects.toString(aws.s3Region(), "");
    // The endpoint decides where the keys go. The region only signs requests, and the storage may
    // report it after the server first used a fallback, so a renewal that changes only the region
    // is taken.
    Preconditions.checkState(
        endpoint.equals(renewedEndpoint),
        "The storage of %s changed in Unity Catalog since the table was loaded, from %s to %s;"
            + " load the table again.",
        aws.prefix(),
        describeStorage(endpoint, region),
        describeStorage(renewedEndpoint, renewedRegion));
  }

  private Optional<String> onlyRecordedBucket() {
    Set<String> buckets =
        conf.getPropsWithPrefix(UCHadoopConfConstants.UC_VENDED_S3_ENDPOINT_PREFIX).keySet();
    return buckets.size() == 1 ? buckets.stream().findFirst() : Optional.empty();
  }

  private static String emptyToNull(String value) {
    return value == null || value.isEmpty() ? null : value;
  }

  private static String describeStorage(String endpoint, String region) {
    return endpoint.isEmpty() ? "AWS S3" : endpoint + " (region " + region + ")";
  }
}
