package io.unitycatalog.cli.externallocation;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.UpdateExternalLocation;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CliExternalLocationOperations implements ExternalLocationOperations {
  private final ServerConfig config;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public CliExternalLocationOperations(ServerConfig config) {
    this.config = config;
  }

  @Override
  public ExternalLocationInfo createExternalLocation(CreateExternalLocation createExternalLocation)
      throws ApiException {
    try {
      List<String> argsList =
          new ArrayList<>(
              List.of(
                  "external_location",
                  "create",
                  "--name",
                  createExternalLocation.getName(),
                  "--url",
                  createExternalLocation.getUrl(),
                  "--credential_name",
                  createExternalLocation.getCredentialName()));
      if (createExternalLocation.getComment() != null) {
        argsList.add("--comment");
        argsList.add(createExternalLocation.getComment());
      }
      String[] args = addServerAndAuthParams(argsList, config);
      JsonNode externalLocationInfoJson = executeCLICommand(args);
      return objectMapper.convertValue(externalLocationInfoJson, ExternalLocationInfo.class);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ApiException) {
        throw (ApiException) e.getCause();
      }
      throw e;
    }
  }

  @Override
  public List<ExternalLocationInfo> listExternalLocations(Optional<String> pageToken)
      throws ApiException {
    try {
      List<String> argsList = new ArrayList<>(List.of("external_location", "list"));
      if (pageToken.isPresent()) {
        argsList.add("--page_token");
        argsList.add(pageToken.get());
      }
      String[] args = addServerAndAuthParams(argsList, config);
      JsonNode externalLocationList = executeCLICommand(args);
      return objectMapper.convertValue(
          externalLocationList, new TypeReference<List<ExternalLocationInfo>>() {});
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ApiException) {
        throw (ApiException) e.getCause();
      }
      throw e;
    }
  }

  @Override
  public ExternalLocationInfo getExternalLocation(String name) throws ApiException {
    try {
      String[] args =
          addServerAndAuthParams(List.of("external_location", "get", "--name", name), config);
      JsonNode externalLocationInfoJson = executeCLICommand(args);
      return objectMapper.convertValue(externalLocationInfoJson, ExternalLocationInfo.class);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ApiException) {
        throw (ApiException) e.getCause();
      }
      throw e;
    }
  }

  @Override
  public ExternalLocationInfo updateExternalLocation(
      String name, UpdateExternalLocation updateExternalLocation) throws ApiException {
    try {
      List<String> argsList =
          new ArrayList<>(List.of("external_location", "update", "--name", name));
      if (updateExternalLocation.getNewName() != null) {
        argsList.add("--new_name");
        argsList.add(updateExternalLocation.getNewName());
      }
      if (updateExternalLocation.getUrl() != null) {
        argsList.add("--url");
        argsList.add(updateExternalLocation.getUrl());
      }
      if (updateExternalLocation.getCredentialName() != null) {
        argsList.add("--credential_name");
        argsList.add(updateExternalLocation.getCredentialName());
      }
      if (updateExternalLocation.getComment() != null) {
        argsList.add("--comment");
        argsList.add(updateExternalLocation.getComment());
      }
      String[] args = addServerAndAuthParams(argsList, config);
      JsonNode updatedExternalLocationInfo = executeCLICommand(args);
      return objectMapper.convertValue(updatedExternalLocationInfo, ExternalLocationInfo.class);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ApiException) {
        throw (ApiException) e.getCause();
      }
      throw e;
    }
  }

  @Override
  public void deleteExternalLocation(String name, Optional<Boolean> force) throws ApiException {
    List<String> argsList = new ArrayList<>(List.of("external_location", "delete", "--name", name));
    if (force.isPresent() && force.get()) {
      argsList.add("--force");
      argsList.add("true");
    }
    String[] args = addServerAndAuthParams(argsList, config);
    try {
      executeCLICommand(args);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ApiException) {
        throw (ApiException) e.getCause();
      }
      throw e;
    }
  }
}
