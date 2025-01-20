package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.provider.AwsCredentialsProvider;
import io.unitycatalog.server.service.credential.aws.provider.AwsCredentialsProviderFactory;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;

public class AwsCredentialVendor {

  private final Map<String, S3StorageConfig> s3Configurations;
  private final Map<String, AwsCredentialsProvider> s3CredentialProviders;

  public AwsCredentialVendor(ServerProperties serverProperties) {
    this.s3Configurations = serverProperties.getS3Configurations();
    this.s3CredentialProviders =
        new AwsCredentialsProviderFactory().loadProviders(serverProperties);
  }

  public AwsCredentials vendAwsCredentials(CredentialContext context) {
    S3StorageConfig s3StorageConfig = s3Configurations.get(context.getStorageBase());
    if (s3StorageConfig == null) {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "S3 bucket configuration not found.");
    }

    final AwsCredentialsProvider provider =
        s3CredentialProviders.get(context.getStorageBase());
    if (provider == null) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "S3 credentials provider " + s3StorageConfig.getProvider() + " not found");
    }

    return provider.generateCredentials(context, s3StorageConfig);
  }
}
