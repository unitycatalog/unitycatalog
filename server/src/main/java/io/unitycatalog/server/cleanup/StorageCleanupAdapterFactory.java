package io.unitycatalog.server.cleanup;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.UriScheme;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** Creates a fresh storage adapter for one claimed cleanup task. */
public final class StorageCleanupAdapterFactory {
  private final StorageCredentialVendor storageCredentialVendor;
  private final Map<NormalizedURL, String> s3BucketRegions;

  public StorageCleanupAdapterFactory(
      StorageCredentialVendor storageCredentialVendor, ServerProperties serverProperties) {
    this.storageCredentialVendor =
        Objects.requireNonNull(storageCredentialVendor, "storageCredentialVendor");
    this.s3BucketRegions =
        Objects.requireNonNull(serverProperties, "serverProperties")
            .getS3Configurations()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().getRegion() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getRegion()));
  }

  /** Validates the task identity, vends credentials when needed, and creates its adapter. */
  public StorageCleanupAdapter create(StorageCleanupTaskDAO task, Duration requestTimeout) {
    NormalizedURL location = validateTask(task);
    UriScheme scheme = UriScheme.fromURI(location.toUri());
    if (scheme == UriScheme.FILE || scheme == UriScheme.NULL) {
      return new LocalStorageCleanupAdapter(location);
    }
    if (scheme != UriScheme.S3) {
      throw new IllegalArgumentException("Storage cleanup supports only local files and S3");
    }

    TemporaryCredentials credentials =
        storageCredentialVendor.vendCredential(location, CredentialContext.READ_WRITE);
    return createS3Adapter(location, requestTimeout, credentials);
  }

  private StorageCleanupAdapter createS3Adapter(
      NormalizedURL location, Duration requestTimeout, TemporaryCredentials credentials) {
    requireCredentialKind(credentials, UriScheme.S3);
    AwsCredentials aws = credentials.getAwsTempCredentials();
    String accessKey = requireValue(aws.getAccessKeyId(), UriScheme.S3);
    String secretKey = requireValue(aws.getSecretAccessKey(), UriScheme.S3);
    AwsCredentialsProvider provider =
        aws.getSessionToken() == null || aws.getSessionToken().isBlank()
            ? StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
            : StaticCredentialsProvider.create(
                AwsSessionCredentials.create(accessKey, secretKey, aws.getSessionToken()));
    String region = s3BucketRegions.get(location.getStorageBase());
    if (region == null || region.isBlank()) {
      throw new IllegalStateException(
          "S3 cleanup requires a configured region for the task bucket");
    }
    S3Client client =
        S3Client.builder().credentialsProvider(provider).region(Region.of(region)).build();
    return new S3StorageCleanupAdapter(client, location, requestTimeout);
  }

  private static void requireCredentialKind(TemporaryCredentials credentials, UriScheme scheme) {
    boolean matches =
        credentials != null
            && credentials.getAwsTempCredentials() != null
            && credentials.getGcpOauthToken() == null
            && credentials.getAzureUserDelegationSas() == null;
    if (!matches) {
      throw invalidCredentials(scheme);
    }
  }

  private static String requireValue(String value, UriScheme scheme) {
    if (value == null || value.isBlank()) {
      throw invalidCredentials(scheme);
    }
    return value;
  }

  private static IllegalStateException invalidCredentials(UriScheme scheme) {
    return new IllegalStateException(
        "Credential vendor returned invalid credentials for " + scheme + " cleanup");
  }

  private static NormalizedURL validateTask(StorageCleanupTaskDAO task) {
    Objects.requireNonNull(task, "task");
    ResourceType resourceType = Objects.requireNonNull(task.getResourceType(), "resourceType");
    String segment =
        switch (resourceType) {
          case TABLE, STAGING_TABLE -> "tables";
          case VOLUME -> "volumes";
          case REGISTERED_MODEL -> "models";
          case MODEL_VERSION -> "versions";
        };
    NormalizedURL location = NormalizedURL.from(task.getStorageLocation());
    String path = location.toUri().getPath();
    if (task.getResourceId() == null
        || path == null
        || !path.endsWith("/" + segment + "/" + task.getResourceId())) {
      throw new IllegalArgumentException(
          "Cleanup task location does not match its " + resourceType + " resource id");
    }
    return location;
  }
}
