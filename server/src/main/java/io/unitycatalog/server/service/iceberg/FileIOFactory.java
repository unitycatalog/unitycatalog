package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSLocationUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.UriScheme;
import lombok.SneakyThrows;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.io.FileIO;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Map;
import java.util.Set;

public class FileIOFactory {

  private final StorageCredentialVendor storageCredentialVendor;
  private final Map<NormalizedURL, S3StorageConfig> s3Configurations;
  // FIXME!! privileges are defaulted to READ only here for now as Iceberg REST impl doesn't
  //  support write
  private final Set<CredentialContext.Privilege> privileges =
      Set.of(CredentialContext.Privilege.SELECT);

  public FileIOFactory(
      StorageCredentialVendor storageCredentialVendor, ServerProperties serverProperties) {
    this.storageCredentialVendor = storageCredentialVendor;
    this.s3Configurations = serverProperties.getS3Configurations();
  }

  // TODO: Cache fileIOs
  public FileIO getFileIO(NormalizedURL location) {
    return switch (UriScheme.fromURI(location.toUri())) {
      case ABFS, ABFSS -> getADLSFileIO(location);
      case GS -> getGCSFileIO(location);
      case S3 -> getS3FileIO(location);
      case FILE, NULL -> new SimpleLocalFileIO();
    };
  }

  protected ADLSFileIO getADLSFileIO(NormalizedURL location) {
    AzureUserDelegationSAS credential =
        storageCredentialVendor.vendCredential(location, privileges).getAzureUserDelegationSas();
    ADLSLocationUtils.ADLSLocationParts locationParts = ADLSLocationUtils.parseLocation(location);

    // NOTE: when fileio caching is implemented, need to set/deal with expiry here
    Map<String, String> properties =
        Map.of(AzureProperties.ADLS_SAS_TOKEN_PREFIX + locationParts.account(),
            credential.getSasToken());

    ADLSFileIO result = new ADLSFileIO();
    result.initialize(properties);
    return result;
  }

  @SneakyThrows
  protected GCSFileIO getGCSFileIO(NormalizedURL location) {
    GcpOauthToken gcpToken =
        storageCredentialVendor.vendCredential(location, privileges).getGcpOauthToken();

    // NOTE: when fileio caching is implemented, need to set/deal with expiry here
    Map<String, String> properties =
        Map.of(GCPProperties.GCS_OAUTH2_TOKEN, gcpToken.getOauthToken());

    GCSFileIO result = new GCSFileIO();
    result.initialize(properties);
    return result;
  }

  protected S3FileIO getS3FileIO(NormalizedURL location) {
    S3StorageConfig s3StorageConfig = s3Configurations.get(location.getStorageBase());

    S3FileIO s3FileIO =
        new S3FileIO(() -> getS3Client(getAwsCredentialsProvider(location),
            s3StorageConfig.getRegion()));

    s3FileIO.initialize(Map.of());

    return s3FileIO;
  }

  protected S3Client getS3Client(AwsCredentialsProvider awsCredentialsProvider, String region) {
    return S3Client.builder()
        .region(Region.of(region))
        .credentialsProvider(awsCredentialsProvider)
        .forcePathStyle(false)
        .build();
  }

  private AwsCredentialsProvider getAwsCredentialsProvider(NormalizedURL location) {
    try {
      AwsCredentials awsSessionCredentials =
          storageCredentialVendor.vendCredential(location, privileges).getAwsTempCredentials();
      return StaticCredentialsProvider.create(
          AwsSessionCredentials.create(
              awsSessionCredentials.getAccessKeyId(),
              awsSessionCredentials.getSecretAccessKey(),
              awsSessionCredentials.getSessionToken()));
    } catch (BaseException e) {
      return DefaultCredentialsProvider.create();
    }
  }
}
