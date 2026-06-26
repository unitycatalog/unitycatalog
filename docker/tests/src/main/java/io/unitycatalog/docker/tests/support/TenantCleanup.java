package io.unitycatalog.docker.tests.support;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;

public final class TenantCleanup {

  private TenantCleanup() {}

  public static void deleteTenant(
      String serverUrl,
      String adminToken,
      String catalog,
      String externalLocation,
      String dataExternalLocation,
      String credential)
      throws ApiException {
    CatalogsApi catalogsApi =
        new CatalogsApi(UcClientFactory.catalogClient(serverUrl, adminToken));
    ExternalLocationsApi elApi =
        new ExternalLocationsApi(UcClientFactory.catalogClient(serverUrl, adminToken));
    CredentialsApi credApi =
        new CredentialsApi(UcClientFactory.catalogClient(serverUrl, adminToken));

    try {
      catalogsApi.deleteCatalog(catalog, true);
    } catch (ApiException e) {
      if (e.getCode() != 404) {
        throw e;
      }
    }
    deleteExternalLocation(elApi, externalLocation);
    deleteExternalLocation(elApi, dataExternalLocation);
    if (credential != null) {
      try {
        credApi.deleteCredential(credential, null);
      } catch (ApiException e) {
        if (e.getCode() != 404) {
          throw e;
        }
      }
    }
  }

  private static void deleteExternalLocation(ExternalLocationsApi api, String name)
      throws ApiException {
    if (name == null) {
      return;
    }
    try {
      api.deleteExternalLocation(name, true);
    } catch (ApiException e) {
      if (e.getCode() != 404) {
        throw e;
      }
    }
  }
}
