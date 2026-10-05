package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.Credentials;

/**
 * Vends AWS credentials for accessing S3 storage.
 *
 * <p>This class supports these modes of credential vending:
 *
 * <ol>
 *   <li><b>External Location + STS:</b> When a storage credential is {@link
 *       CredentialDAO.CredentialType#AWS_IAM_ROLE} with a real IAM role ARN, the master role STS
 *       client assumes that role (see {@link AwsIamRoleRequest} / {@link AwsIamRoleResponse}).
 *   <li><b>External Location + static access key:</b> When the credential is {@link
 *       CredentialDAO.CredentialType#S3_ACCESS_KEY}, UC reads the secret configured under {@code
 *       s3.static.secretKey.<accessKeyId>} for that credential's access key id. Soft expiry comes
 *       from {@code s3.static.credentialTtlSeconds}. Secrets are never stored in the catalog
 *       database.
 *   <li><b>Per-Bucket Configuration:</b> Legacy mode using per-bucket S3 configurations in
 *       server.properties when no external location covers the path.
 * </ol>
 */
public class AwsCredentialVendor {

  private static final Logger LOGGER = LoggerFactory.getLogger(AwsCredentialVendor.class);

  private final ServerProperties serverProperties;
  private final Map<NormalizedURL, S3StorageConfig> perBucketS3Configs;
  private final Map<NormalizedURL, AwsCredentialGenerator> perBucketCredGenerators =
      new ConcurrentHashMap<>();

  // Supplier to create new StsClientBuilder instances.
  private final Supplier<StsClientBuilder> stsClientBuilderSupplier;

  // This config is used to construct a master role STS client to assume storage roles. It may
  // contain keys of a UC master user, or no key at all. If there's no key in this config, the
  // StsClient will be constructed to use DefaultCredentialsProvider to figure out the correct
  // key/token to use.
  // The awsRoleArn in this config is always null as the storage role ARN to assume is defined in
  // CredentialDAO instead.
  private final S3StorageConfig awsS3MasterRoleConfig;

  // This StsCredentialGenerator holds credential of a UC master role/user, and it assumes storage
  // roles defined in CredentialDAO to create temporary storage access credentials.
  // It's lazy initialized (just like perBucketCredGenerators) for two reasons:
  // 1. Some tests do not provide a proper ServerProperties so that it can be initialized
  // 2. In real production, the server may be running on other clouds and doesn't care about AWS at
  //  all. So this generator and perBucketCredGenerators can remain empty until they are needed.
  @Getter(lazy = true)
  private final AwsCredentialGenerator awsS3MasterRoleStsGenerator =
      createStsCredentialGenerator(awsS3MasterRoleConfig); // Lazy initialized

  public AwsCredentialVendor(ServerProperties serverProperties) {
    this(serverProperties, StsClient::builder);
  }

  /**
   * Constructor for injecting a test StsClientBuilder supplier.
   *
   * @param serverProperties the server properties containing S3 configurations
   * @param stsClientBuilderSupplier supplier that creates new StsClientBuilder instances
   */
  public AwsCredentialVendor(
      ServerProperties serverProperties, Supplier<StsClientBuilder> stsClientBuilderSupplier) {
    this.serverProperties = serverProperties;
    this.stsClientBuilderSupplier = stsClientBuilderSupplier;
    this.perBucketS3Configs = serverProperties.getS3Configurations();
    // awsS3MasterRoleConfig.awsRoleArn is null. The ARN to assume comes from credential securable.
    this.awsS3MasterRoleConfig = serverProperties.getS3MasterRoleConfiguration();
  }

  private AwsCredentialGenerator createPerBucketCredentialGenerator(S3StorageConfig config) {
    // Dynamically load and initialize the generator if it's intentionally configured.
    if (config.getCredentialGenerator() != null) {
      try {
        return (AwsCredentialGenerator)
            Class.forName(config.getCredentialGenerator()).getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    if (config.getSessionToken() != null && !config.getSessionToken().isEmpty()) {
      // if a session token was supplied, then we will just return static session credentials
      return new AwsCredentialGenerator.StaticAwsCredentialGenerator(config);
    }

    return createStsCredentialGenerator(config);
  }

  private AwsCredentialGenerator createStsCredentialGenerator(S3StorageConfig config) {
    return new AwsCredentialGenerator.StsAwsCredentialGenerator(
        stsClientBuilderSupplier.get(), config);
  }

  public Credentials vendAwsCredentials(CredentialContext context) {
    AwsCredentialGenerator generator;
    if (context.getCredentialDAO().isPresent()) {
      CredentialDAO credentialDAO = context.getCredentialDAO().get();
      if (credentialDAO.getCredentialType() == CredentialDAO.CredentialType.S3_ACCESS_KEY) {
        LOGGER.debug(
            "Vending S3 credentials using static access key: credential={}, storageBase={}",
            credentialDAO.getName(),
            context.getStorageBase());
        generator =
            createStaticAccessKeyGenerator(
                credentialDAO.getAwsS3AccessKeyResponse().getAccessKeyId());
      } else if (credentialDAO.getCredentialType() == CredentialDAO.CredentialType.AWS_IAM_ROLE) {
        LOGGER.debug(
            "Vending S3 credentials using STS AssumeRole: credential={}, roleArn={},"
                + " storageBase={}",
            credentialDAO.getName(),
            credentialDAO.getAwsIamRoleResponse().getRoleArn(),
            context.getStorageBase());
        generator = getAwsS3MasterRoleStsGenerator();
      } else {
        throw new BaseException(
            ErrorCode.FAILED_PRECONDITION,
            "Unsupported credential type for S3: " + credentialDAO.getCredentialType());
      }
    } else {
      // No credential dao. Use the per bucket config
      S3StorageConfig config = perBucketS3Configs.get(context.getStorageBase());
      if (config == null) {
        throw new BaseException(
            ErrorCode.FAILED_PRECONDITION, "S3 bucket configuration not found.");
      }
      generator =
          perBucketCredGenerators.computeIfAbsent(
              context.getStorageBase(), storageBase -> createPerBucketCredentialGenerator(config));
    }
    return generator.generate(context);
  }

  public Optional<String> resolveS3EndpointUrl(CredentialContext context) {
    if (context.getStorageBase() != null) {
      S3StorageConfig config = perBucketS3Configs.get(context.getStorageBase());
      Optional<String> perBucketEndpoint = resolveConfiguredS3Endpoint(config);
      if (perBucketEndpoint.isPresent()) {
        return perBucketEndpoint;
      }
    }
    return resolveConfiguredS3Endpoint(awsS3MasterRoleConfig);
  }

  private Optional<String> resolveConfiguredS3Endpoint(S3StorageConfig config) {
    if (config == null) {
      return Optional.empty();
    }
    if (config.getS3EndpointUrl() != null && !config.getS3EndpointUrl().isEmpty()) {
      return Optional.of(config.getS3EndpointUrl());
    }
    if (config.getEndpointUrl() != null && !config.getEndpointUrl().isEmpty()) {
      return Optional.of(config.getEndpointUrl());
    }
    return Optional.empty();
  }

  /**
   * @param accessKeyId access key id stored on the storage credential; the matching secret comes
   *     from {@code s3.static.secretKey.<accessKeyId>} in server configuration
   */
  private AwsCredentialGenerator createStaticAccessKeyGenerator(String accessKeyId) {
    if (accessKeyId == null || accessKeyId.isBlank()) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "Storage credential has no S3 access key id. Recreate it with aws_s3_access_key.");
    }
    S3StorageConfig config =
        serverProperties
            .resolveS3StaticAccessKeyConfiguration(accessKeyId)
            .orElseThrow(
                () ->
                    new BaseException(
                        ErrorCode.FAILED_PRECONDITION,
                        "No static S3 secret configured for access key id '"
                            + accessKeyId
                            + "'. Set s3.static.secretKey."
                            + accessKeyId
                            + " in server.properties."));
    return new AwsCredentialGenerator.StaticAwsCredentialGenerator(
        config, serverProperties.getS3StaticCredentialTtl());
  }
}
