package io.unitycatalog.server.service.credential.aws;

import com.amazonaws.SdkClientException;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.utils.ServerPropertiesUtils;
import io.unitycatalog.server.service.credential.CredentialContext;
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
import software.amazon.awssdk.services.sts.model.Credentials;

public class AwsCredentialVendor {

  private static final Logger LOGGER = LoggerFactory.getLogger(AwsCredentialVendor.class);

  private final Map<String, S3StorageConfig> s3Configurations;
  private final String serverRegion;

  public AwsCredentialVendor() {
    ServerPropertiesUtils utils = ServerPropertiesUtils.getInstance();
    this.s3Configurations = utils.getS3Configurations();

    String awsRegion = utils.getProperty("aws.region");

    if (awsRegion == null || awsRegion.isEmpty()) {
      try {
        awsRegion = new DefaultAwsRegionProviderChain().getRegion();
      } catch (SdkClientException e) {
        LOGGER.warn(e.getMessage());
      }
    }

    this.serverRegion = awsRegion;
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

    return StsClient.builder()
        .credentialsProvider(credentialsProvider)
        .region(resolveRegion(s3StorageConfig))
        .build();
  }

  private Region resolveRegion(S3StorageConfig s3StorageConfig) {
    if (serverRegion == null) {
      // couldn't determine uc server region, falling back to what the storage region is
      return Region.of(s3StorageConfig.getRegion());
    } else {
      return Region.of(serverRegion);
    }
  }
}
