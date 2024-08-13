package io.unitycatalog.cli.model;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.RegisteredModelInfo;
import io.unitycatalog.client.model.UpdateRegisteredModel;
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
                "model",
                "create",
                "--name",
                createRegisteredModel.getModelName(),
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
  public List<RegisteredModelInfo> listRegisteredModels(String catalogName, String schemaName) {
    String[] args =
        addServerAndAuthParams(
            List.of("model", "list", "--catalog", catalogName, "--schema", schemaName), config);
    JsonNode registeredModelList = executeCLICommand(args);
    return objectMapper.convertValue(
        registeredModelList, new TypeReference<List<RegisteredModelInfo>>() {});
  }

  @Override
  public RegisteredModelInfo getRegisteredModel(String registeredModelFullName) {
    String[] args =
        addServerAndAuthParams(
            List.of("model", "get", "--full_name", registeredModelFullName), config);
    JsonNode registeredModelInfoJson = executeCLICommand(args);
    return objectMapper.convertValue(registeredModelInfoJson, RegisteredModelInfo.class);
  }

  @Override
  public RegisteredModelInfo updateRegisteredModel(
      String registeredModelFullName, UpdateRegisteredModel updateRm) {
    List<String> argsList =
        new ArrayList<>(List.of("model", "update", "--full_name", registeredModelFullName));
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
        new ArrayList<>(List.of("model", "delete", "--full_name", registeredModelFullName));
    if (force.isPresent() && force.get()) {
      argsList.add("--force");
      argsList.add("true");
    }
    String[] args = addServerAndAuthParams(argsList, config);
    executeCLICommand(args);
  }
}
