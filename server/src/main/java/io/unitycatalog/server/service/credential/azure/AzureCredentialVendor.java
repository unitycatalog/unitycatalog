package io.unitycatalog.server.service.credential.azure;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.core.util.Context;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.file.datalake.DataLakeServiceAsyncClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.implementation.util.DataLakeSasImplUtil;
import com.azure.storage.file.datalake.models.UserDelegationKey;
import com.azure.storage.file.datalake.sas.DataLakeServiceSasSignatureValues;
import com.azure.storage.file.datalake.sas.PathSasPermission;
import io.unitycatalog.server.persist.utils.ServerPropertiesUtils;
import io.unitycatalog.server.service.credential.CredentialContext;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;

public class AzureCredentialVendor {

  private final Map<String, ADLSStorageConfig> adlsConfigurations;

  public AzureCredentialVendor() {
    this.adlsConfigurations = ServerPropertiesUtils.getInstance().getAdlsConfigurations();
  }

  public AzureCredential vendAzureCredential(CredentialContext context) {
    ADLSStorageConfig config = adlsConfigurations.get(context.getStorageBasePath());
    ADLSLocationUtils.ADLSLocationParts locationParts =
        ADLSLocationUtils.parseLocation(context.getStorageBasePath());

    // FIXME!! azure sas token expiration hardcoded to an hour
    OffsetDateTime start = OffsetDateTime.now();
    OffsetDateTime expiry = start.plusHours(1);

    TokenCredential tokenCredential =
        new ClientSecretCredentialBuilder()
            .tenantId(config.getTenantId())
            .clientId(config.getClientId())
            .clientSecret(config.getClientSecret())
            .build();
    DataLakeServiceAsyncClient serviceClient =
        new DataLakeServiceClientBuilder()
            .httpClient(HttpClient.createDefault())
            .endpoint("https://" + locationParts.account())
            .credential(tokenCredential)
            .buildAsyncClient();
    UserDelegationKey key = serviceClient.getUserDelegationKey(start, expiry).toFuture().join();

    PathSasPermission perms = resolvePrivileges(context.getPrivileges());
    DataLakeServiceSasSignatureValues sasSignatureValues =
        new DataLakeServiceSasSignatureValues(expiry, perms).setStartTime(start);

    String sasToken =
        new DataLakeSasImplUtil(
                sasSignatureValues,
                locationParts.container(),
                context.getStorageBasePath().substring(1),
                true)
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
