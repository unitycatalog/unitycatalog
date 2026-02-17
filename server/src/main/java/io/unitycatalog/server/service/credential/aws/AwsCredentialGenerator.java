package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.service.credential.CredentialContext;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Credentials;

/**
 * Generates credentials based on the provided {@link CredentialContext}.
 *
 * <p>Currently supported implementations include:
 *
 * <ul>
 *   <li>{@link StaticAwsCredentialGenerator}: returns fixed credentials defined in configuration.
 *   <li>{@link StsAwsCredentialGenerator}: retrieves temporary, expiring credentials from AWS STS.
 * </ul>
 *
 * <p>Test scenarios can provide custom {@link AwsCredentialGenerator} implementations. For example,
 * a time-based generator can be used in end-to-end tests to validate credential renewal behavior.
 */
public interface AwsCredentialGenerator {
  Credentials generate(CredentialContext ctx);

  class StaticAwsCredentialGenerator implements AwsCredentialGenerator {
    private final String accessKeyId;
    private final String secretKey;
    private final String sessionToken;

    public StaticAwsCredentialGenerator(S3StorageConfig config) {
      this.accessKeyId = config.getAccessKey();
      this.secretKey = config.getSecretKey();
      this.sessionToken = config.getSessionToken();
    }

    @Override
    public Credentials generate(CredentialContext ctx) {
      return Credentials.builder()
          .accessKeyId(accessKeyId)
          .secretAccessKey(secretKey)
          .sessionToken(sessionToken)
          .build();
    }
  }

  class StsAwsCredentialGenerator implements AwsCredentialGenerator {
    private final StsClient stsClient;
    // This is the role ARN to assume for the per-bucket config. Otherwise, the CredentialContext
    // contains a CredentialDAO and it will assume the role ARN in CredentialDAO instead.
    private final String staticAwsRoleArn;

    public StsAwsCredentialGenerator(StsClientBuilder builder, S3StorageConfig config) {
      // Get STS region
      Region region;
      if (config.getRegion() != null && !config.getRegion().isEmpty()) {
        region = Region.of(config.getRegion());
      } else {
        try {
          region = DefaultAwsRegionProviderChain.builder().build().getRegion();
        } catch (SdkClientException e) {
          region = Region.AWS_GLOBAL;
        }
      }

      AwsCredentialsProvider credentialsProvider;
      if (config.getAccessKey() != null && !config.getAccessKey().isEmpty()) {
        // Build STS client using keys if they are specified in the config.
        credentialsProvider =
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(config.getAccessKey(), config.getSecretKey()));
      } else {
        // Otherwise just defer to DefaultCredentialsProvider
        credentialsProvider = DefaultCredentialsProvider.create();
      }

      this.stsClient = builder.region(region).credentialsProvider(credentialsProvider).build();
      this.staticAwsRoleArn = config.getAwsRoleArn();
    }

    @Override
    public Credentials generate(CredentialContext ctx) {
      Optional<AwsIamRoleResponse> awsIamRole =
          ctx.getCredentialDAO().map(CredentialDAO::getAwsIamRoleResponse);
      // Assume the role along with the externalId specified in CredentialDAO. If there's no
      // CredentialDAO, assume staticAwsRoleArn from the per-bucket config.
      String roleArn = awsIamRole.map(AwsIamRoleResponse::getRoleArn).orElse(staticAwsRoleArn);
      // externalId is only from CredentialDAO. Per-bucket config does not need it.
      Optional<String> externalId = awsIamRole.map(AwsIamRoleResponse::getExternalId);

      String awsPolicy = AwsPolicyGenerator.generatePolicy(ctx.getPrivileges(), ctx.getLocations());
      String roleSessionName = "uc-%s".formatted(UUID.randomUUID());

      AssumeRoleRequest.Builder roleRequestBuilder =
          AssumeRoleRequest.builder()
              .roleArn(roleArn)
              .policy(awsPolicy)
              .roleSessionName(roleSessionName)
              .durationSeconds((int) Duration.ofHours(1).toSeconds());
      externalId.ifPresent(roleRequestBuilder::externalId);

      return stsClient.assumeRole(roleRequestBuilder.build()).credentials();
    }
  }
}
