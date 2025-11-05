package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sts.model.Credentials;

public class AwsCredentialVendor {

  private static final Logger LOGGER = LoggerFactory.getLogger(AwsCredentialVendor.class);

  private final Map<String, S3StorageConfig> s3Configurations;
  private final Map<String, CredentialsGenerator> credGenerators = new ConcurrentHashMap<>();

  public AwsCredentialVendor(ServerProperties serverProperties) {
    this.s3Configurations = serverProperties.getS3Configurations();
  }

  private CredentialsGenerator createCredentialsGenerator(S3StorageConfig config) {
    // Dynamically load and initialize the generator if it's intentionally configured.
    if (config.getCredentialsGenerator() != null) {
      try {
        return (CredentialsGenerator)
            Class.forName(config.getCredentialsGenerator()).getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    if (config.getSessionToken() != null && !config.getSessionToken().isEmpty()) {
      // if a session token was supplied, then we will just return static session credentials
      return new CredentialsGenerator.StaticCredentialsGenerator(
          config.getAccessKey(), config.getSecretKey(), config.getSessionToken());
    }

    // Parse service endpoint URI if provided (for S3-compatible services like MinIO)
    URI endpointOverride = null;
    if (config.getServiceEndpoint() != null && !config.getServiceEndpoint().isEmpty()) {
      endpointOverride = URI.create(config.getServiceEndpoint());
      LOGGER.debug(
          "Using custom STS endpoint for bucket {}: {}",
          config.getBucketPath(),
          config.getServiceEndpoint());
    } else {
      LOGGER.debug("Using AWS STS endpoint for bucket: {}", config.getBucketPath());
    }

    if (config.getAccessKey() != null && !config.getAccessKey().isEmpty()) {
      return new CredentialsGenerator.StsCredentialsGenerator(
          config.getRegion(),
          config.getAccessKey(),
          config.getSecretKey(),
          config.getAwsRoleArn(),
          endpointOverride);
    } else {
      return new CredentialsGenerator.StsCredentialsGenerator(
          config.getRegion(), config.getAwsRoleArn(), endpointOverride);
    }
  }

  public Credentials vendAwsCredentials(CredentialContext context) {
    S3StorageConfig config = s3Configurations.get(context.getStorageBase());
    if (config == null) {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "S3 bucket configuration not found.");
    }

    CredentialsGenerator generator =
        credGenerators.compute(
            context.getStorageBase(),
            (storageBase, credGenerator) ->
                credGenerator == null ? createCredentialsGenerator(config) : credGenerator);

    return generator.generate(context);
  }

  /**
   * Retrieves the service endpoint URL for the given storage context.
   *
   * @param context The credential context containing storage information
   * @return The service endpoint URL if configured, null otherwise
   */
  public String getServiceEndpoint(CredentialContext context) {
    S3StorageConfig config = s3Configurations.get(context.getStorageBase());
    if (config == null) {
      return null;
    }
    return config.getServiceEndpoint();
  }
}
