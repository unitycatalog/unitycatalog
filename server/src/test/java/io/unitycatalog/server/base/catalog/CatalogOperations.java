package io.unitycatalog.server.base.catalog;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.UpdateCatalog;
import java.util.List;
import java.util.Optional;

public interface CatalogOperations {
  CatalogInfo createCatalog(CreateCatalog createCatalog) throws ApiException;

  List<CatalogInfo> listCatalogs(Optional<String> pageToken) throws ApiException;

  CatalogInfo getCatalog(String name) throws ApiException;

  CatalogInfo updateCatalog(String name, UpdateCatalog updateCatalog) throws ApiException;

  void deleteCatalog(String name, Optional<Boolean> force) throws ApiException;
}
