package io.unitycatalog.cli.catalog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.unitycatalog.cli.BaseCliOperations;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.UpdateCatalog;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CliCatalogOperations extends BaseCliOperations implements CatalogOperations {

  public CliCatalogOperations(ServerConfig config) {
    super("catalog", config);
  }

  @Override
  public CatalogInfo createCatalog(CreateCatalog createCatalog) throws ApiException {
    List<String> argsList = new ArrayList<>(List.of("--name", createCatalog.getName()));
    if (createCatalog.getComment() != null) {
      argsList.add("--comment");
      argsList.add(createCatalog.getComment());
    }
    if (createCatalog.getProperties() != null && !createCatalog.getProperties().isEmpty()) {
      argsList.add("--properties");
      try {
        argsList.add(objectMapper.writeValueAsString(createCatalog.getProperties()));
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed to serialize properties", e);
      }
    }
    if (createCatalog.getStorageRoot() != null) {
      argsList.add("--storage_root");
      argsList.add(createCatalog.getStorageRoot());
    }
    return execute(CatalogInfo.class, "create", argsList);
  }

  @Override
  public List<CatalogInfo> listCatalogs(Optional<String> pageToken) throws ApiException {
    List<String> argsList = new ArrayList<>();
    if (pageToken.isPresent()) {
      argsList.add("--page_token");
      argsList.add(pageToken.get());
    }
    return execute(new TypeReference<>() {}, "list", argsList);
  }

  @Override
  public CatalogInfo getCatalog(String name) throws ApiException {
    return execute(CatalogInfo.class, "get", List.of("--name", name));
  }

  @Override
  public CatalogInfo updateCatalog(String name, UpdateCatalog updateCatalog) throws ApiException {
    List<String> argsList = new ArrayList<>();
    if (updateCatalog.getNewName() != null) {
      argsList.add("--new_name");
      argsList.add(updateCatalog.getNewName());
    }
    if (updateCatalog.getComment() != null) {
      argsList.add("--comment");
      argsList.add(updateCatalog.getComment());
    }
    if (updateCatalog.getProperties() != null && !updateCatalog.getProperties().isEmpty()) {
      argsList.add("--properties");
      try {
        argsList.add(objectMapper.writeValueAsString(updateCatalog.getProperties()));
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed to serialize properties", e);
      }
    }
    return executeUpdate(
        CatalogInfo.class,
        /* catchEmptyUpdateCliException= */ true,
        List.of("--name", name),
        argsList);
  }

  @Override
  public void deleteCatalog(String name, Optional<Boolean> force) throws ApiException {
    List<String> argsList = new ArrayList<>(List.of("--name", name));
    if (force.isPresent()) {
      argsList.add("--force");
      argsList.add(force.get().toString());
    }
    execute(Void.class, "delete", argsList);
  }
}
