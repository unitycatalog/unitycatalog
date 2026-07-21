package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Preconditions;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/** Hadoop S3A credentials provider backed by Unity Catalog temporary credentials. */
public class AwsVendedTokenProvider extends GenericCredentialProvider
    implements AwsCredentialsProvider {

  /**
   * Constructor for the hadoop's CredentialProviderListFactory#buildAWSProviderList to initialize.
   */
  public AwsVendedTokenProvider(Configuration conf) {
    initialize(conf);
  }

  @Override
  public GenericCredential initGenericCredential(Configuration conf) {
    if (conf.get(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY) != null
        && conf.get(UCHadoopConfConstants.S3A_INIT_SECRET_KEY) != null
        && conf.get(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN) != null) {

      String accessKey = conf.get(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY);
      String secretKey = conf.get(UCHadoopConfConstants.S3A_INIT_SECRET_KEY);
      String sessionToken = conf.get(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN);

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
          conf.get(UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY));
    } else {
      return null;
    }
  }

  @Override
  public AwsCredentials resolveCredentials() {
    AwsCredential aws = (AwsCredential) accessCredentials();

    return AwsSessionCredentials.builder()
        .accessKeyId(aws.accessKeyId())
        .secretAccessKey(aws.secretAccessKey())
        .sessionToken(aws.sessionToken())
        .build();
  }
}
