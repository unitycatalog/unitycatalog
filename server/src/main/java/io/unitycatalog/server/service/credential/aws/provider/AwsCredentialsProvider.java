package io.unitycatalog.server.service.credential.aws.provider;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;

/**
 * Interface for AWS credentials provider.
 */
public interface AwsCredentialsProvider {
  /**
   * Configure the provider with the given configuration. Will receive key-value pairs from server.properties with stripped `aws.credentials.provider.<provider_name>` prefix.
   *
   * @param configuration the configuration
   * @return the provider
   */
  AwsCredentialsProvider configure(AwsCredentialsProviderConfig configuration);

  AwsCredentials generateCredentials(CredentialContext context, S3StorageConfig s3StorageConfig);
}
