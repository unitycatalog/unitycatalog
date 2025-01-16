package io.unitycatalog.server.service.credential.azure;

import static java.lang.String.format;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.core.util.Context;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.file.datalake.DataLakeServiceAsyncClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.implementation.util.DataLakeSasImplUtil;
import com.azure.storage.file.datalake.models.UserDelegationKey;
import com.azure.storage.file.datalake.sas.DataLakeServiceSasSignatureValues;
import com.azure.storage.file.datalake.sas.PathSasPermission;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;

public class AzureCredentialVendor {

  private final Map<String, ADLSStorageConfig> adlsConfigurations;

  public AzureCredentialVendor(ServerProperties serverProperties) {
    this.adlsConfigurations = serverProperties.getAdlsConfigurations();
  }

  public AzureCredential vendAzureCredential(CredentialContext context) {
    ADLSLocationUtils.ADLSLocationParts locationParts =
        ADLSLocationUtils.parseLocation(context.getStorageBase());
    ADLSStorageConfig config = adlsConfigurations.get(locationParts.accountName());

    TokenCredential tokenCredential;
    if (config == null) {
      // fallback to creating credential from environment variables (or somewhere on the default
      // chain)
      tokenCredential = new DefaultAzureCredentialBuilder().build();
    } else {
      if (config.isTestMode()) {
        // allow pass-through of a dummy value for integration testing
        return AzureCredential.builder()
            .sasToken(
                format(
                    "%s/%s/%s",
                    config.getTenantId(), config.getClientId(), config.getClientSecret()))
            .expirationTimeInEpochMillis(253370790000000L)
            .build();
      }
      tokenCredential =
          new ClientSecretCredentialBuilder()
              .tenantId(config.getTenantId())
              .clientId(config.getClientId())
              .clientSecret(config.getClientSecret())
              .build();
    }
    DataLakeServiceAsyncClient serviceClient =
        new DataLakeServiceClientBuilder()
            .httpClient(HttpClient.createDefault())
            .endpoint("https://" + locationParts.account())
            .credential(tokenCredential)
            .buildAsyncClient();

    // TODO: possibly make this configurable - defaulted to 1 hour right now
    OffsetDateTime start = OffsetDateTime.now();
    OffsetDateTime expiry = start.plusHours(1);
    UserDelegationKey key = serviceClient.getUserDelegationKey(start, expiry).toFuture().join();

    PathSasPermission perms = resolvePrivileges(context.getPrivileges());
    DataLakeServiceSasSignatureValues sasSignatureValues =
        new DataLakeServiceSasSignatureValues(expiry, perms).setStartTime(start);

    // azure supports only downscoping to a single location for now
    // azure wants only the path
    String path = URI.create(context.getLocations().get(0)).getPath();
    // remove any preceding forward slashes or trailing forward slashes
    // hadoop ABFS strips trailing slash when preforming some operations so we need to vend
    // a cred for path without trailing slash
    path = path.replaceAll("^/+|/*$", "");

    String sasToken =
        new DataLakeSasImplUtil(sasSignatureValues, locationParts.container(), path, true)
            .generateUserDelegationSas(key, locationParts.accountName(), Context.NONE);

    return AzureCredential.builder()
        .sasToken(sasToken)
        .expirationTimeInEpochMillis(expiry.toInstant().toEpochMilli())
        .build();
  }

  private PathSasPermission resolvePrivileges(Set<CredentialContext.Privilege> privileges) {
    PathSasPermission result = new PathSasPermission();
    if (privileges.contains(CredentialContext.Privilege.UPDATE)) {
      result.setWritePermission(true);
      result.setDeletePermission(true);
    }
    if (privileges.contains(CredentialContext.Privilege.SELECT)) {
      result.setReadPermission(true);
      result.setListPermission(true);
    }
    return result;
  }
}
