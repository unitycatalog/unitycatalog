package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import lombok.Getter;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.Credentials;

/**
 * Vends AWS credentials for accessing S3 storage.
 *
 * <p>This class supports two modes of credential vending:
 *
 * <ol>
 *   <li><b>External Location Credentials:</b> When a storage credential (CredentialDAO) is
 *       associated with an external location, the master role STS client assumes the storage role
 *       specified in the credential with the appropriate external ID.
 *   <li><b>Per-Bucket Configuration:</b> Legacy mode using per-bucket S3 configurations defined in
 *       server.properties (s3.bucketPath.*, s3.accessKey.*, etc.).
 * </ol>
 */
public class AwsCredentialVendor {

  private final Map<NormalizedURL, S3StorageConfig> perBucketS3Configs;
  private final Map<NormalizedURL, CredentialsGenerator> perBucketCredGenerators =
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

  // This StsCredentialsGenerator holds credential of a UC master role/user, and it assumes storage
  // roles defined in CredentialDAO to create temporary storage access credentials.
  // It's lazy initialized (just like perBucketCredGenerators) for two reasons:
  // 1. Some tests do not provide a proper ServerProperties so that it can be initialized
  // 2. In real production, the server may be running on other clouds and doesn't care about AWS at
  //  all. So this generator and perBucketCredGenerators can remain empty until they are needed.
  @Getter(lazy = true)
  private final CredentialsGenerator awsS3MasterRoleStsGenerator =
      createStsCredentialsGenerator(awsS3MasterRoleConfig); // Lazy initialized

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
    this.stsClientBuilderSupplier = stsClientBuilderSupplier;
    this.perBucketS3Configs = serverProperties.getS3Configurations();
    // awsS3MasterRoleConfig.awsRoleArn is null. The ARN to assume comes from credential securable.
    this.awsS3MasterRoleConfig = serverProperties.getS3MasterRoleConfiguration();
  }

  private CredentialsGenerator createPerBucketCredentialsGenerator(S3StorageConfig config) {
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
      return new CredentialsGenerator.StaticCredentialsGenerator(config);
    }

    return createStsCredentialsGenerator(config);
  }

  private CredentialsGenerator createStsCredentialsGenerator(S3StorageConfig config) {
    return new CredentialsGenerator.StsCredentialsGenerator(stsClientBuilderSupplier.get(), config);
  }

  public Credentials vendAwsCredentials(CredentialContext context) {
    CredentialsGenerator generator;
    if (context.getCredentialDAO().isPresent()) {
      // Use the master role STS generator
      generator = getAwsS3MasterRoleStsGenerator();
    } else {
      // No credential dao. Use the per bucket config
      S3StorageConfig config = perBucketS3Configs.get(context.getStorageBase());
      if (config == null) {
        throw new BaseException(
            ErrorCode.FAILED_PRECONDITION, "S3 bucket configuration not found.");
      }
      generator =
          perBucketCredGenerators.computeIfAbsent(
              context.getStorageBase(), storageBase -> createPerBucketCredentialsGenerator(config));
    }
    return generator.generate(context);
  }
}
