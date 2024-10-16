package io.unitycatalog.cli.catalog;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.UpdateCatalog;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CliCatalogOperations implements CatalogOperations {
  private final ServerConfig config;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public CliCatalogOperations(ServerConfig config) {
    this.config = config;
  }

  @Override
  public CatalogInfo createCatalog(CreateCatalog createCatalog) {
    List<String> argsList =
        new ArrayList<>(List.of("catalog", "create", "--name", createCatalog.getName()));
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
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode catalogInfoJson = executeCLICommand(args);
    return objectMapper.convertValue(catalogInfoJson, CatalogInfo.class);
  }

  @Override
  public List<CatalogInfo> listCatalogs(Optional<String> pageToken) {
    List<String> argsList = new ArrayList<>(List.of("catalog", "list"));
    if (pageToken.isPresent()) {
      argsList.add("--page_token");
      argsList.add(pageToken.get());
    }
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode catalogList = executeCLICommand(args);
    return objectMapper.convertValue(catalogList, new TypeReference<List<CatalogInfo>>() {});
  }

  @Override
  public CatalogInfo getCatalog(String name) {
    String[] args = addServerAndAuthParams(List.of("catalog", "get", "--name", name), config);
    JsonNode catalogInfoJson = executeCLICommand(args);
    return objectMapper.convertValue(catalogInfoJson, CatalogInfo.class);
  }

  @Override
  public CatalogInfo updateCatalog(String name, UpdateCatalog updateCatalog) {
    List<String> argsList = new ArrayList<>(List.of("catalog", "update", "--name", name));
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
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode updatedCatalogInfo = executeCLICommand(args);
    return objectMapper.convertValue(updatedCatalogInfo, CatalogInfo.class);
  }

  @Override
  public void deleteCatalog(String name, Optional<Boolean> force) {
    String[] args;
    args =
        force
            .map(
                aBoolean ->
                    addServerAndAuthParams(
                        List.of(
                            "catalog", "delete", "--name", name, "--force", aBoolean.toString()),
                        config))
            .orElseGet(
                () -> addServerAndAuthParams(List.of("catalog", "delete", "--name", name), config));
    executeCLICommand(args);
  }
}
