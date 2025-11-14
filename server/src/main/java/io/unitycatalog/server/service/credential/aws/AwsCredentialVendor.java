package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.services.sts.model.Credentials;

public class AwsCredentialVendor {

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

    if (config.getAccessKey() != null && !config.getAccessKey().isEmpty()) {
      return new CredentialsGenerator.StsCredentialsGenerator(
          config.getRegion(), config.getAccessKey(), config.getSecretKey(), config.getAwsRoleArn());
    } else {
      return new CredentialsGenerator.StsCredentialsGenerator(
          config.getRegion(), config.getAwsRoleArn());
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
}
