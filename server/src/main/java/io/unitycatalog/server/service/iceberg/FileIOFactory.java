package io.unitycatalog.server.service.iceberg;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.CredentialOperations;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSLocationUtils;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
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

import java.net.URI;
import java.util.Map;
import java.util.Set;

import static io.unitycatalog.server.utils.Constants.URI_SCHEME_ABFS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_ABFSS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_GS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_S3;

public class FileIOFactory {

  private final CredentialOperations credentialOps;
  private final Map<String, S3StorageConfig> s3Configurations;

  public FileIOFactory(CredentialOperations credentialOps, ServerProperties serverProperties) {
    this.credentialOps = credentialOps;
    this.s3Configurations = serverProperties.getS3Configurations();
  }

  // TODO: Cache fileIOs
  public FileIO getFileIO(URI tableLocationUri) {
    return switch (tableLocationUri.getScheme()) {
      case URI_SCHEME_ABFS, URI_SCHEME_ABFSS -> getADLSFileIO(tableLocationUri);
      case URI_SCHEME_GS -> getGCSFileIO(tableLocationUri);
      case URI_SCHEME_S3 -> getS3FileIO(tableLocationUri);
      // TODO: should we default/fallback to HadoopFileIO ?
      default -> new SimpleLocalFileIO();
    };
  }

  protected ADLSFileIO getADLSFileIO(URI tableLocationUri) {
    CredentialContext credentialContext = getCredentialContextFromTableLocation(tableLocationUri);
    AzureCredential credential = credentialOps.vendAzureCredential(credentialContext);
    ADLSLocationUtils.ADLSLocationParts locationParts = ADLSLocationUtils.parseLocation(tableLocationUri.toString());

    // NOTE: when fileio caching is implemented, need to set/deal with expiry here
    Map<String, String> properties =
      Map.of(AzureProperties.ADLS_SAS_TOKEN_PREFIX + locationParts.account(), credential.getSasToken());

    ADLSFileIO result = new ADLSFileIO();
    result.initialize(properties);
    return result;
  }

  @SneakyThrows
  protected GCSFileIO getGCSFileIO(URI tableLocationUri) {
    CredentialContext credentialContext = getCredentialContextFromTableLocation(tableLocationUri);
    AccessToken gcpToken = credentialOps.vendGcpToken(credentialContext);

    // NOTE: when fileio caching is implemented, need to set/deal with expiry here
    Map<String, String> properties =
      Map.of(
        GCPProperties.GCS_OAUTH2_TOKEN, gcpToken.getTokenValue());

    GCSFileIO result = new GCSFileIO();
    result.initialize(properties);
    return result;
  }

  protected S3FileIO getS3FileIO(URI tableLocationUri) {
    CredentialContext context = getCredentialContextFromTableLocation(tableLocationUri);
    S3StorageConfig s3StorageConfig = s3Configurations.get(context.getStorageBase());

    S3FileIO s3FileIO =
        new S3FileIO(() -> getS3Client(getAwsCredentialsProvider(context), s3StorageConfig.getRegion()));

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
      Credentials awsSessionCredentials = credentialOps.vendAwsCredential(context);
      return StaticCredentialsProvider.create(
        AwsSessionCredentials.create(awsSessionCredentials.accessKeyId(), awsSessionCredentials.secretAccessKey(), awsSessionCredentials.sessionToken()));
    } catch (BaseException e) {
      return DefaultCredentialsProvider.create();
    }
  }

  private CredentialContext getCredentialContextFromTableLocation(URI tableLocationUri) {
    // FIXME!! privileges are defaulted to READ only here for now as Iceberg REST impl doesn't support write
    return CredentialContext.create(tableLocationUri, Set.of(CredentialContext.Privilege.SELECT));
  }
}
