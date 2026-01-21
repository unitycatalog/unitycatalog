package io.unitycatalog.server.service.iceberg;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSLocationUtils;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
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
import software.amazon.awssdk.services.sts.model.Credentials;

import java.util.Map;
import java.util.Set;

public class FileIOFactory {

  private final CloudCredentialVendor cloudCredentialVendor;
  private final Map<NormalizedURL, S3StorageConfig> s3Configurations;

  public FileIOFactory(CloudCredentialVendor cloudCredentialVendor,
      ServerProperties serverProperties) {
    this.cloudCredentialVendor = cloudCredentialVendor;
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
    CredentialContext credentialContext = getCredentialContextFromTableLocation(location);
    AzureCredential credential = cloudCredentialVendor.vendAzureCredential(credentialContext);
    ADLSLocationUtils.ADLSLocationParts locationParts =
        ADLSLocationUtils.parseLocation(location);

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
    CredentialContext credentialContext = getCredentialContextFromTableLocation(location);
    AccessToken gcpToken = cloudCredentialVendor.vendGcpToken(credentialContext);

    // NOTE: when fileio caching is implemented, need to set/deal with expiry here
    Map<String, String> properties =
        Map.of(GCPProperties.GCS_OAUTH2_TOKEN, gcpToken.getTokenValue());

    GCSFileIO result = new GCSFileIO();
    result.initialize(properties);
    return result;
  }

  protected S3FileIO getS3FileIO(NormalizedURL location) {
    CredentialContext context = getCredentialContextFromTableLocation(location);
    S3StorageConfig s3StorageConfig = s3Configurations.get(context.getStorageBase());

    S3FileIO s3FileIO =
        new S3FileIO(() -> getS3Client(getAwsCredentialsProvider(context),
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

  private AwsCredentialsProvider getAwsCredentialsProvider(CredentialContext context) {
    try {
      Credentials awsSessionCredentials = cloudCredentialVendor.vendAwsCredential(context);
      return StaticCredentialsProvider.create(
          AwsSessionCredentials.create(
              awsSessionCredentials.accessKeyId(),
              awsSessionCredentials.secretAccessKey(),
              awsSessionCredentials.sessionToken()));
    } catch (BaseException e) {
      return DefaultCredentialsProvider.create();
    }
  }

  private CredentialContext getCredentialContextFromTableLocation(NormalizedURL location) {
    // FIXME!! privileges are defaulted to READ only here for now as Iceberg REST impl doesn't
    // support write
    return CredentialContext.create(location.toUri(),
        Set.of(CredentialContext.Privilege.SELECT));
  }
}

