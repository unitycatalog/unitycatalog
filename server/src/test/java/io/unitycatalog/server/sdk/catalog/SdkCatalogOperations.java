package io.unitycatalog.server.sdk.catalog;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.UpdateCatalog;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import java.util.List;
import java.util.Optional;

public class SdkCatalogOperations implements CatalogOperations {
  private final CatalogsApi catalogsApi;

  public SdkCatalogOperations(ApiClient apiClient) {
    this.catalogsApi = new CatalogsApi(apiClient);
  }

  @Override
  public CatalogInfo createCatalog(CreateCatalog createCatalog) throws ApiException {
    return catalogsApi.createCatalog(createCatalog);
  }

  @Override
  public List<CatalogInfo> listCatalogs(Optional<String> pageToken) throws ApiException {
    return catalogsApi.listCatalogs(pageToken.orElse(null), 100).getCatalogs();
  }

  @Override
  public CatalogInfo getCatalog(String name) throws ApiException {
    return catalogsApi.getCatalog(name);
  }

  @Override
  public CatalogInfo updateCatalog(String name, UpdateCatalog updateCatalog) throws ApiException {
    return catalogsApi.updateCatalog(name, updateCatalog);
  }

  @Override
  public void deleteCatalog(String name, Optional<Boolean> force) throws ApiException {
    catalogsApi.deleteCatalog(name, force.orElse(false));
  }
}
