package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.Credentials;

public class AwsCredentialVendor {

  private static final Logger LOGGER = LoggerFactory.getLogger(AwsCredentialVendor.class);
  private final Map<String, S3StorageConfig> s3Configurations;

  public AwsCredentialVendor(ServerProperties serverProperties) {
    this.s3Configurations = serverProperties.getS3Configurations();
  }

  public S3StorageConfig getS3StorageConfig(String storageBase) {
    return s3Configurations.get(storageBase);
  }

  public Credentials vendAwsCredentials(CredentialContext context) {
    S3StorageConfig s3StorageConfig = s3Configurations.get(context.getStorageBase());
    if (s3StorageConfig == null) {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "S3 bucket configuration not found.");
    }

    if (s3StorageConfig.getSessionToken() != null && !s3StorageConfig.getSessionToken().isEmpty()) {
      // if a session token was supplied, then we will just return static session credentials
      return Credentials.builder()
          .accessKeyId(s3StorageConfig.getAccessKey())
          .secretAccessKey(s3StorageConfig.getSecretKey())
          .sessionToken(s3StorageConfig.getSessionToken())
          .build();
    }

    // TODO: cache sts client
    StsClient stsClient = getStsClientForStorageConfig(s3StorageConfig);

    // TODO: Update this with relevant user/role type info once available
    String roleSessionName = "uc-%s".formatted(UUID.randomUUID());
    String awsPolicy =
        AwsPolicyGenerator.generatePolicy(context.getPrivileges(), context.getLocations());

    return stsClient
        .assumeRole(
            r ->
                r.roleArn(s3StorageConfig.getAwsRoleArn())
                    .policy(awsPolicy)
                    .roleSessionName(roleSessionName)
                    .durationSeconds((int) Duration.ofHours(1).toSeconds()))
        .credentials();
  }

  private StsClient getStsClientForStorageConfig(S3StorageConfig s3StorageConfig) {
    AwsCredentialsProvider credentialsProvider;
    if (s3StorageConfig.getSecretKey() != null && !s3StorageConfig.getAccessKey().isEmpty()) {
      credentialsProvider =
          StaticCredentialsProvider.create(
              AwsBasicCredentials.create(
                  s3StorageConfig.getAccessKey(), s3StorageConfig.getSecretKey()));
    } else {
      credentialsProvider = DefaultCredentialsProvider.create();
    }

    StsClientBuilder stsClientBuilder =
        StsClient.builder()
            .credentialsProvider(credentialsProvider)
            .region(Region.of(s3StorageConfig.getRegion()));

    // Support custom STS endpoint for 3rd-party S3 services like MinIO
    if (s3StorageConfig.getStsEndpoint() != null && !s3StorageConfig.getStsEndpoint().isEmpty()) {
      try {
        URI stsEndpointUri = URI.create(s3StorageConfig.getStsEndpoint());
        stsClientBuilder.endpointOverride(stsEndpointUri);
        LOGGER.info("Using custom STS endpoint: {}", s3StorageConfig.getStsEndpoint());
      } catch (IllegalArgumentException e) {
        LOGGER.error("Invalid STS endpoint URL: {}", s3StorageConfig.getStsEndpoint(), e);
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Invalid STS endpoint URL: " + s3StorageConfig.getStsEndpoint());
      }
    }

    return stsClientBuilder.build();
  }
}
