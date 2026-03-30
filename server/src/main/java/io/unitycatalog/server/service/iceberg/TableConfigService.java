package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSLocationUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.UriScheme;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;

import java.util.Map;
import java.util.Set;

public class TableConfigService {

  private final StorageCredentialVendor storageCredentialVendor;
  private final Map<NormalizedURL, S3StorageConfig> s3Configurations;
  // FIXME!! privileges are defaulted to READ only here for now as Iceberg REST impl doesn't
  // support write
  private final Set<CredentialContext.Privilege> privileges =
      Set.of(CredentialContext.Privilege.SELECT);

  public TableConfigService(
      StorageCredentialVendor storageCredentialVendor, ServerProperties serverProperties) {
    this.s3Configurations = serverProperties.getS3Configurations();
    this.storageCredentialVendor = storageCredentialVendor;
  }

  public Map<String, String> getTableConfig(TableMetadata tableMetadata) {
    NormalizedURL location = NormalizedURL.from(tableMetadata.location());
    UriScheme scheme = UriScheme.fromURI(location.toUri());

    return switch(scheme) {
      case ABFS, ABFSS -> getADLSConfig(location);
      case GS -> getGCSConfig(location);
      case S3 -> getS3Config(location);
      case FILE, NULL -> Map.of();
    };
  }

  private Map<String, String> getADLSConfig(NormalizedURL location) {
    ADLSLocationUtils.ADLSLocationParts locationParts = ADLSLocationUtils.parseLocation(location);

    AzureUserDelegationSAS azureCredential =
        storageCredentialVendor.vendCredential(location, privileges).getAzureUserDelegationSas();

    return Map.of(
        AzureProperties.ADLS_SAS_TOKEN_PREFIX + locationParts.account(),
        azureCredential.getSasToken());
  }

  private Map<String, String> getGCSConfig(NormalizedURL location) {
    TemporaryCredentials credential = storageCredentialVendor.vendCredential(location, privileges);
    GcpOauthToken token = credential.getGcpOauthToken();

    return Map.of(
        GCPProperties.GCS_OAUTH2_TOKEN, token.getOauthToken(),
        GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
        Long.toString(credential.getExpirationTime()));
  }

  private Map<String, String> getS3Config(NormalizedURL location) {
    S3StorageConfig s3StorageConfig = s3Configurations.get(location.getStorageBase());
    AwsCredentials awsCredential =
        storageCredentialVendor.vendCredential(location, privileges).getAwsTempCredentials();

    return Map.of(
        S3FileIOProperties.ACCESS_KEY_ID, awsCredential.getAccessKeyId(),
        S3FileIOProperties.SECRET_ACCESS_KEY, awsCredential.getSecretAccessKey(),
        S3FileIOProperties.SESSION_TOKEN, awsCredential.getSessionToken(),
        AwsClientProperties.CLIENT_REGION, s3StorageConfig.getRegion());
  }
}
