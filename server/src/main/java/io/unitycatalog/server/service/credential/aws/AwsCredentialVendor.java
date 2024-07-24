package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.utils.ServerPropertiesUtils;
import io.unitycatalog.server.service.credential.CredentialContext;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.util.Map;

public class AwsCredentialVendor {

  private final Map<String, S3StorageConfig> s3Configurations;

  public AwsCredentialVendor() {
    this.s3Configurations = ServerPropertiesUtils.getInstance().getS3Configurations();
  }

  // TODO: proper downscoping
  public AwsSessionCredentials vendAwsCredentials(CredentialContext context) {
    S3StorageConfig s3StorageConfig = s3Configurations.get(context.getStorageBasePath());
    if (s3StorageConfig == null) {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "S3 bucket configuration not found.");
    }
    return AwsSessionCredentials.create(
      s3StorageConfig.getAccessKey(),s3StorageConfig.getSecretKey(),s3StorageConfig.getSessionToken());
  }
}
