package io.unitycatalog.server.service.credential.azure;

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
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Set;

public interface AzureCredentialGenerator {
  AzureCredential generate(CredentialContext ctx);

  class StaticAzureCredentialGenerator implements AzureCredentialGenerator {
    private final ADLSStorageConfig config;

    public StaticAzureCredentialGenerator(ADLSStorageConfig config) {
      this.config = config;
    }

    @Override
    public AzureCredential generate(CredentialContext ctx) {
      String sasToken =
          String.format(
              "%s/%s/%s", config.getTenantId(), config.getClientId(), config.getClientSecret());
      return AzureCredential.builder()
          .sasToken(sasToken)
          .expirationTimeInEpochMillis(253370790000000L)
          .build();
    }
  }

  class DatalakeCredentialGenerator implements AzureCredentialGenerator {
    private final TokenCredential tokenCredential;

    public DatalakeCredentialGenerator(ADLSStorageConfig config) {
      if (config == null) {
        this.tokenCredential = new DefaultAzureCredentialBuilder().build();
      } else {
        this.tokenCredential =
            new ClientSecretCredentialBuilder()
                .tenantId(config.getTenantId())
                .clientId(config.getClientId())
                .clientSecret(config.getClientSecret())
                .build();
      }
    }

    @Override
    public AzureCredential generate(CredentialContext ctx) {
      ADLSLocationUtils.ADLSLocationParts locationParts =
          ADLSLocationUtils.parseLocation(ctx.getStorageBase());

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

      PathSasPermission perms = resolvePrivileges(ctx.getPrivileges());
      DataLakeServiceSasSignatureValues sasSignatureValues =
          new DataLakeServiceSasSignatureValues(expiry, perms).setStartTime(start);

      // azure supports only downscoping to a single location for now
      // azure wants only the path
      String path = URI.create(ctx.getLocations().get(0)).getPath();
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
}
