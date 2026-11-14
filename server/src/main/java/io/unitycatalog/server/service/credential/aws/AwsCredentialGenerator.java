package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.service.credential.CredentialContext;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
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
  /** How long vended credentials last: STS sessions, and the refetch time of a vend without one. */
  Duration SESSION_DURATION = Duration.ofHours(1);

  /** The longest an AssumeRole call to the STS of S3-compatible storage may take, with retries. */
  Duration S3_COMPATIBLE_STS_TIMEOUT = Duration.ofSeconds(30);

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

  class StsAwsCredentialGenerator implements AwsCredentialGenerator, AutoCloseable {
    private final StsClient stsClient;
    // This is the role ARN to assume for the per-bucket config. Otherwise, the CredentialContext
    // contains a CredentialDAO and it will assume the role ARN in CredentialDAO instead.
    private final String staticAwsRoleArn;
    // Same region the STS client uses; used when the assumed role ARN does not carry a partition.
    private final Region awsRegion;
    // Whether AssumeRole goes to S3-compatible STS (an endpoint override) rather than AWS STS.
    private final boolean isS3Compatible;

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
      } else if (config.getStsEndpoint() != null) {
        // S3-compatible STS: each request is signed with its credential's key pair (see
        // generate(ctx, roleArn, signer)), so the client holds no keys.
        credentialsProvider = AnonymousCredentialsProvider.create();
      } else {
        // Otherwise just defer to DefaultCredentialsProvider
        credentialsProvider = DefaultCredentialsProvider.create();
      }

      builder.region(region).credentialsProvider(credentialsProvider);
      this.isS3Compatible = config.getStsEndpoint() != null;
      if (isS3Compatible) {
        // The endpoint is set through the credential API, so a slow one does not hold a request
        // for the SDK's socket timeout and retries.
        builder
            .endpointOverride(URI.create(config.getStsEndpoint()))
            .overrideConfiguration(c -> c.apiCallTimeout(S3_COMPATIBLE_STS_TIMEOUT));
      }
      this.stsClient = builder.build();
      this.staticAwsRoleArn = config.getAwsRoleArn();
      this.awsRegion = region;
    }

    /** Closes the STS client, releasing its connections. */
    @Override
    public void close() {
      stsClient.close();
    }

    @Override
    public Credentials generate(CredentialContext ctx) {
      // Only an aws_iam_role credential names the role; an s3_compatible one configures it.
      Optional<AwsIamRoleResponse> awsIamRole =
          ctx.getCredentialDAO()
              .filter(dao -> dao.getCredentialType() == CredentialDAO.CredentialType.AWS_IAM_ROLE)
              .map(CredentialDAO::getAwsIamRoleResponse);
      // Assume the role along with the externalId specified in CredentialDAO. If there's no
      // CredentialDAO, assume staticAwsRoleArn from the per-bucket config.
      String roleArn = awsIamRole.map(AwsIamRoleResponse::getRoleArn).orElse(staticAwsRoleArn);
      // externalId is only from CredentialDAO. Per-bucket config does not need it.
      Optional<String> externalId = awsIamRole.map(AwsIamRoleResponse::getExternalId);
      return assumeRole(ctx, roleArn, externalId, Optional.empty());
    }

    /**
     * Assumes a role on S3-compatible STS, signing the request with the credential's key pair
     * rather than with the client's identity.
     *
     * @param ctx the path and privileges to scope the session to
     * @param roleArn the role to assume
     * @param signer the key pair of the credential version the request read
     * @return the session's credentials
     */
    public Credentials generate(
        CredentialContext ctx, String roleArn, AwsCredentialsProvider signer) {
      return assumeRole(ctx, roleArn, Optional.empty(), Optional.of(signer));
    }

    private Credentials assumeRole(
        CredentialContext ctx,
        String roleArn,
        Optional<String> externalId,
        Optional<AwsCredentialsProvider> signer) {
      String awsPolicy =
          AwsPolicyGenerator.generatePolicy(
              ctx.getPrivileges(), ctx.getLocations(), roleArn, awsRegion, isS3Compatible);
      String roleSessionName = "uc-%s".formatted(UUID.randomUUID());

      AssumeRoleRequest.Builder roleRequestBuilder =
          AssumeRoleRequest.builder()
              .roleArn(roleArn)
              .policy(awsPolicy)
              .roleSessionName(roleSessionName)
              .durationSeconds((int) SESSION_DURATION.toSeconds());
      externalId.ifPresent(roleRequestBuilder::externalId);
      signer.ifPresent(
          provider ->
              roleRequestBuilder.overrideConfiguration(o -> o.credentialsProvider(provider)));

      Credentials credentials;
      try {
        credentials = stsClient.assumeRole(roleRequestBuilder.build()).credentials();
      } catch (SdkException e) {
        // The SDK failure is the cause, which the exception handler logs. This names the failed
        // step more precisely than StorageExceptions' general message.
        throw new BaseException(
            ErrorCode.INTERNAL, "Failed to vend temporary storage credentials.", e);
      }
      if (credentials == null
          || isBlank(credentials.accessKeyId())
          || isBlank(credentials.secretAccessKey())
          || isBlank(credentials.sessionToken())
          || credentials.expiration() == null) {
        // An STS other than AWS may answer without credentials, or with an incomplete element,
        // which must not leave a client to its default credentials or a session it never renews.
        throw new BaseException(
            ErrorCode.INTERNAL, "Failed to vend temporary storage credentials.");
      }
      return credentials;
    }

    private static boolean isBlank(String value) {
      return value == null || value.isBlank();
    }
  }
}
