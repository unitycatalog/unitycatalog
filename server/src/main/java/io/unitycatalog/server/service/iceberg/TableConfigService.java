package io.unitycatalog.server.service.iceberg;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSLocationUtils;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.net.URI;
import java.util.Map;
import java.util.Set;

import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_ABFS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_ABFSS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_GS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_S3;

public class TableConfigService {

  private final CloudCredentialVendor cloudCredentialVendor;
  private final Map<String, S3StorageConfig> s3Configurations;

  public TableConfigService(CloudCredentialVendor cloudCredentialVendor, ServerProperties serverProperties) {
    this.s3Configurations = serverProperties.getS3Configurations();
    this.cloudCredentialVendor = cloudCredentialVendor;
  }

  public Map<String, String> getTableConfig(TableMetadata tableMetadata) {
    URI locationURI = URI.create(tableMetadata.location());
    String scheme = locationURI.getScheme();

    CredentialContext context = CredentialContext.create(locationURI, Set.of(SELECT));

    return switch(scheme) {
      case URI_SCHEME_ABFS, URI_SCHEME_ABFSS -> getADLSConfig(context);
      case URI_SCHEME_GS -> getGCSConfig(context);
      case URI_SCHEME_S3 -> getS3Config(context);
      default -> Map.of();
    };
  }

  private Map<String, String> getADLSConfig(CredentialContext context) {
    ADLSLocationUtils.ADLSLocationParts locationParts =
      ADLSLocationUtils.parseLocation(context.getStorageBase());

    AzureCredential azureCredential = cloudCredentialVendor.vendAzureCredential(context);

    return Map.of(AzureProperties.ADLS_SAS_TOKEN_PREFIX + locationParts.account(), azureCredential.getSasToken());
  }

  private Map<String, String> getGCSConfig(CredentialContext context) {
    AccessToken token = cloudCredentialVendor.vendGcpToken(context);

    return Map.of(
      GCPProperties.GCS_OAUTH2_TOKEN, token.getTokenValue(),
      GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, Long.toString(token.getExpirationTime().getTime()));
  }

  private Map<String, String> getS3Config(CredentialContext context) {
    S3StorageConfig s3StorageConfig = s3Configurations.get(context.getStorageBase());
    Credentials awsCredential = cloudCredentialVendor.vendAwsCredential(context);

    var configMap = new java.util.HashMap<String, String>();
    configMap.put(S3FileIOProperties.ACCESS_KEY_ID, awsCredential.accessKeyId());
    configMap.put(S3FileIOProperties.SECRET_ACCESS_KEY, awsCredential.secretAccessKey());
    configMap.put(S3FileIOProperties.SESSION_TOKEN, awsCredential.sessionToken());
    configMap.put(AwsClientProperties.CLIENT_REGION, s3StorageConfig.getRegion());
    
    // Add custom S3 endpoint configuration if specified
    if (s3StorageConfig.getS3Endpoint() != null && !s3StorageConfig.getS3Endpoint().isEmpty()) {
      configMap.put(S3FileIOProperties.ENDPOINT, s3StorageConfig.getS3Endpoint());
    }
    
    // Configure path style access if specified
    if (s3StorageConfig.getPathStyleAccess() != null && s3StorageConfig.getPathStyleAccess()) {
      configMap.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
    }
    
    return configMap;
  }
}
