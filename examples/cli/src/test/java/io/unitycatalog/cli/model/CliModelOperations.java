package io.unitycatalog.cli.model;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.model.ModelOperations;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CliModelOperations implements ModelOperations {

  private final ServerConfig config;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public CliModelOperations(ServerConfig config) {
    this.config = config;
  }

  @Override
  public RegisteredModelInfo createRegisteredModel(CreateRegisteredModel createRegisteredModel) {
    List<String> argsList =
        new ArrayList<>(
            List.of(
                "registered_model",
                "create",
                "--name",
                createRegisteredModel.getName(),
                "--schema",
                createRegisteredModel.getSchemaName(),
                "--catalog",
                createRegisteredModel.getCatalogName()));
    if (createRegisteredModel.getComment() != null) {
      argsList.add("--comment");
      argsList.add(createRegisteredModel.getComment());
    }
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode registeredModelInfoJson = executeCLICommand(args);
    return objectMapper.convertValue(registeredModelInfoJson, RegisteredModelInfo.class);
  }

  @Override
  public List<RegisteredModelInfo> listRegisteredModels(
      Optional<String> catalogName, Optional<String> schemaName, Optional<String> pageToken) {
    List<String> argsList;
    if (catalogName.isEmpty() || schemaName.isEmpty()) {
      argsList = new ArrayList<>(List.of("registered_model", "list"));
    } else {
      argsList =
          new ArrayList<>(
              List.of(
                  "registered_model",
                  "list",
                  "--catalog",
                  catalogName.get(),
                  "--schema",
                  schemaName.get()));
    }
    if (pageToken.isPresent()) {
      argsList.add("--page_token");
      argsList.add(pageToken.get());
    }
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode registeredModelList = executeCLICommand(args);
    return objectMapper.convertValue(
        registeredModelList, new TypeReference<List<RegisteredModelInfo>>() {});
  }

  @Override
  public RegisteredModelInfo getRegisteredModel(String registeredModelFullName) {
    String[] args =
        addServerAndAuthParams(
            List.of("registered_model", "get", "--full_name", registeredModelFullName), config);
    JsonNode registeredModelInfoJson = executeCLICommand(args);
    return objectMapper.convertValue(registeredModelInfoJson, RegisteredModelInfo.class);
  }

  @Override
  public RegisteredModelInfo updateRegisteredModel(
      String registeredModelFullName, UpdateRegisteredModel updateRm) {
    List<String> argsList =
        new ArrayList<>(
            List.of("registered_model", "update", "--full_name", registeredModelFullName));
    if (updateRm.getNewName() != null) {
      argsList.add("--new_name");
      argsList.add(updateRm.getNewName());
    }
    if (updateRm.getComment() != null) {
      argsList.add("--comment");
      argsList.add(updateRm.getComment());
    }
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode updatedRegisteredModelInfo = executeCLICommand(args);
    return objectMapper.convertValue(updatedRegisteredModelInfo, RegisteredModelInfo.class);
  }

  @Override
  public void deleteRegisteredModel(String registeredModelFullName, Optional<Boolean> force) {
    List<String> argsList =
        new ArrayList<>(
            List.of("registered_model", "delete", "--full_name", registeredModelFullName));
    if (force.isPresent() && force.get()) {
      argsList.add("--force");
      argsList.add("true");
    }
    String[] args = addServerAndAuthParams(argsList, config);
    executeCLICommand(args);
  }

  @Override
  public ModelVersionInfo createModelVersion(CreateModelVersion createModelVersion) {
    List<String> argsList =
        new ArrayList<>(
            List.of(
                "model_version",
                "create",
                "--name",
                createModelVersion.getModelName(),
                "--schema",
                createModelVersion.getSchemaName(),
                "--catalog",
                createModelVersion.getCatalogName(),
                "--source",
                createModelVersion.getSource()));
    if (createModelVersion.getComment() != null) {
      argsList.add("--comment");
      argsList.add(createModelVersion.getComment());
    }
    if (createModelVersion.getRunId() != null) {
      argsList.add("--run_id");
      argsList.add(createModelVersion.getRunId());
    }
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode modelVersionInfoJson = executeCLICommand(args);
    return objectMapper.convertValue(modelVersionInfoJson, ModelVersionInfo.class);
  }

  @Override
  public List<ModelVersionInfo> listModelVersions(
      String registeredModelFullName, Optional<String> pageToken) {
    List<String> argsList =
        new ArrayList<>(List.of("model_version", "list", "--full_name", registeredModelFullName));
    if (pageToken.isPresent()) {
      argsList.add("--page_token");
      argsList.add(pageToken.get());
    }
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode modelVersionList = executeCLICommand(args);
    return objectMapper.convertValue(
        modelVersionList, new TypeReference<List<ModelVersionInfo>>() {});
  }

  @Override
  public ModelVersionInfo getModelVersion(String registeredModelFullName, Long version) {
    String[] args =
        addServerAndAuthParams(
            List.of(
                "model_version",
                "get",
                "--full_name",
                registeredModelFullName,
                "--version",
                version.toString()),
            config);
    JsonNode modelVersionInfoJson = executeCLICommand(args);
    return objectMapper.convertValue(modelVersionInfoJson, ModelVersionInfo.class);
  }

  @Override
  public ModelVersionInfo updateModelVersion(
      String fullName, Long version, UpdateModelVersion updateMv) {
    List<String> argsList =
        new ArrayList<>(
            List.of(
                "model_version",
                "update",
                "--full_name",
                fullName,
                "--version",
                version.toString()));
    if (updateMv.getComment() != null) {
      argsList.add("--comment");
      argsList.add(updateMv.getComment());
    }
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode updatedModelVersionInfo = executeCLICommand(args);
    return objectMapper.convertValue(updatedModelVersionInfo, ModelVersionInfo.class);
  }

  @Override
  public void deleteModelVersion(String registeredModelFullName, Long version) {
    List<String> argsList =
        new ArrayList<>(
            List.of(
                "model_version",
                "delete",
                "--full_name",
                registeredModelFullName,
                "--version",
                version.toString()));
    String[] args = addServerAndAuthParams(argsList, config);
    executeCLICommand(args);
  }

  @Override
  public ModelVersionInfo finalizeModelVersion(
      String registeredModelFullName, Long version, FinalizeModelVersion finalizeModelVersion) {
    List<String> argsList =
        new ArrayList<>(
            List.of(
                "model_version",
                "finalize",
                "--full_name",
                registeredModelFullName,
                "--version",
                version.toString()));
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode finalizedModelVersionInfo = executeCLICommand(args);
    return objectMapper.convertValue(finalizedModelVersionInfo, ModelVersionInfo.class);
  }
}
