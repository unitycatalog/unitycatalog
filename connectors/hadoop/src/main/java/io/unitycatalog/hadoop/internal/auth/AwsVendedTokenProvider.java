package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.util.S3Paths;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Preconditions;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/** Hadoop S3A credentials provider backed by Unity Catalog temporary credentials. */
public class AwsVendedTokenProvider extends GenericCredentialProvider
    implements AwsCredentialsProvider {

  /**
   * Constructor for the hadoop's CredentialProviderListFactory#buildAWSProviderList to initialize.
   */
  private final Configuration conf;

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
    String s3Endpoint = conf.get(UCHadoopConfConstants.S3A_INIT_S3_ENDPOINT);
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
          conf.get(UCHadoopConfConstants.S3A_INIT_S3_REGION));
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
    checkSameStorage(aws);
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
   * example from AWS S3 to S3-compatible storage or back. A bucket without a record is not checked.
   */
  private void checkSameStorage(AwsCredential aws) {
    Optional<String> bucket = S3Paths.bucketIfAny(aws.prefix());
    String endpoint =
        bucket
            .map(b -> conf.get(UCHadoopConfConstants.UC_VENDED_S3_ENDPOINT_PREFIX + b))
            .orElse(null);
    if (endpoint == null) {
      return;
    }
    String region = conf.get(UCHadoopConfConstants.UC_VENDED_S3_REGION_PREFIX + bucket.get(), "");
    String renewedEndpoint = Objects.toString(aws.s3Endpoint(), "");
    String renewedRegion = Objects.toString(aws.s3Region(), "");
    Preconditions.checkState(
        endpoint.equals(renewedEndpoint) && region.equals(renewedRegion),
        "The storage of %s changed in Unity Catalog since the table was loaded, from %s to %s;"
            + " load the table again.",
        aws.prefix(),
        describeStorage(endpoint, region),
        describeStorage(renewedEndpoint, renewedRegion));
  }

  private static String describeStorage(String endpoint, String region) {
    return endpoint.isEmpty() ? "AWS S3" : endpoint + " (region " + region + ")";
  }
}
