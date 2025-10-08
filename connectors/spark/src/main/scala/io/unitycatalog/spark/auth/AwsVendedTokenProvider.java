package io.unitycatalog.spark.auth;

import io.unitycatalog.spark.UCHadoopConf;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class AwsVendedTokenProvider extends GeneralCredentialProvider
    implements AwsCredentialsProvider {

  public AwsVendedTokenProvider(URI uri, Configuration conf) {
    super(uri, conf);
  }

  @Override
  public GeneralCredential initGeneralCredential(Configuration conf) {
    if (conf.get(UCHadoopConf.S3A_INIT_ACCESS_KEY) != null
        && conf.get(UCHadoopConf.S3A_INIT_SECRET_KEY) != null
        && conf.get(UCHadoopConf.S3A_INIT_SESSION_TOKEN) != null
        && conf.get(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME) != null) {

      String accessKey = conf.get(UCHadoopConf.S3A_INIT_ACCESS_KEY);
      String secretKey = conf.get(UCHadoopConf.S3A_INIT_SECRET_KEY);
      String sessionToken = conf.get(UCHadoopConf.S3A_INIT_SESSION_TOKEN);
      long expiredTimeMillis = conf.getLong(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME, 0L);

      return GeneralCredential.forAws(accessKey, secretKey, sessionToken, expiredTimeMillis);
    } else {
      return null;
    }
  }

  @Override
  public AwsCredentials resolveCredentials() {
    GeneralCredential general = accessCredentials();
    io.unitycatalog.client.model.AwsCredentials awsTempCred = general
        .temporaryCredentials()
        .getAwsTempCredentials();

    return AwsSessionCredentials.builder()
        .accessKeyId(awsTempCred.getAccessKeyId())
        .secretAccessKey(awsTempCred.getSecretAccessKey())
        .sessionToken(awsTempCred.getSessionToken())
        .build();
  }
}
