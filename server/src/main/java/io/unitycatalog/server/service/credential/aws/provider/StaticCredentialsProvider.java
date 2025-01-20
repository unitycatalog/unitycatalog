package io.unitycatalog.server.service.credential.aws.provider;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;

public class StaticCredentialsProvider implements AwsCredentialsProvider {

  public static final String SESSION_TOKEN = "sessiontoken";

  private String accessKey;
  private  String secretKey;
  private String sessionToken;

  @Override
  public AwsCredentialsProvider configure(AwsCredentialsProviderConfig config) {
    accessKey = config.get(AwsCredentialsProviderConfig.ACCESS_KEY_ID);
    secretKey = config.get(AwsCredentialsProviderConfig.SECRET_ACCESS_KEY);
    sessionToken = config.get(SESSION_TOKEN);
    return this;
  }

  @Override
  public AwsCredentials generateCredentials(
      CredentialContext context, S3StorageConfig s3StorageConfig) {
    return new AwsCredentials()
            .accessKeyId(accessKey)
            .secretAccessKey(secretKey)
            .sessionToken(sessionToken)
            .endpoint(s3StorageConfig.getEndpoint());
  }
}
