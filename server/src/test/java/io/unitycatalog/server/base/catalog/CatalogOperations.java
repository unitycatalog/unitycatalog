package io.unitycatalog.server.base.catalog;


import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CatalogInfo;

import java.util.List;

public interface CatalogOperations {
    CatalogInfo createCatalog(String name, String comment) throws ApiException;
    List<CatalogInfo> listCatalogs() throws ApiException;
    CatalogInfo getCatalog(String name) throws ApiException;
    CatalogInfo updateCatalog(String name, String comment) throws ApiException;
    CatalogInfo updateCatalog(String name, String newName, String comment) throws ApiException;
    void deleteCatalog(String name) throws ApiException;
}