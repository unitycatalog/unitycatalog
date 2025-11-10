package io.unitycatalog.spark.auth;

import io.unitycatalog.spark.UCHadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.sparkproject.guava.base.Preconditions;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class AwsVendedTokenProvider extends GenericCredentialProvider
    implements AwsCredentialsProvider {

  /**
   * Constructor for the hadoop's CredentialProviderListFactory#buildAWSProviderList to initialize.
   */
  public AwsVendedTokenProvider(Configuration conf) {
    super(conf);
  }

  @Override
  public GenericCredential initGenericCredential(Configuration conf) {
    if (conf.get(UCHadoopConf.S3A_INIT_ACCESS_KEY) != null
        && conf.get(UCHadoopConf.S3A_INIT_SECRET_KEY) != null
        && conf.get(UCHadoopConf.S3A_INIT_SESSION_TOKEN) != null
        && conf.get(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME) != null) {

      String accessKey = conf.get(UCHadoopConf.S3A_INIT_ACCESS_KEY);
      String secretKey = conf.get(UCHadoopConf.S3A_INIT_SECRET_KEY);
      String sessionToken = conf.get(UCHadoopConf.S3A_INIT_SESSION_TOKEN);

      long expiredTimeMillis = conf.getLong(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME, 0L);
      Preconditions.checkState(expiredTimeMillis > 0, "Expired time %s must be greater than 0, " +
          "please check configure key '%s'", expiredTimeMillis, UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME);

      return GenericCredential.forAws(accessKey, secretKey, sessionToken, expiredTimeMillis);
    } else {
      return null;
    }
  }

  @Override
  public AwsCredentials resolveCredentials() {
    GenericCredential generic = accessCredentials();

    // Wrap the GenericCredential as an AwsCredentials.
    io.unitycatalog.client.model.AwsCredentials awsTempCred = generic
        .temporaryCredentials()
        .getAwsTempCredentials();
    Preconditions.checkNotNull(awsTempCred,
        "AWS temp credential of generic credentials cannot be null");

    return AwsSessionCredentials.builder()
        .accessKeyId(awsTempCred.getAccessKeyId())
        .secretAccessKey(awsTempCred.getSecretAccessKey())
        .sessionToken(awsTempCred.getSessionToken())
        .build();
  }
}
