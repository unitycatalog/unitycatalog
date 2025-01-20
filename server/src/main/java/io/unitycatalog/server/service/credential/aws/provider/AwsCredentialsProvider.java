package io.unitycatalog.server.service.credential.aws.provider;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;

public interface AwsCredentialsProvider {
  AwsCredentialsProvider configure(AwsCredentialsProviderConfig configuration);

  AwsCredentials generateCredentials(CredentialContext context, S3StorageConfig s3StorageConfig);
}
