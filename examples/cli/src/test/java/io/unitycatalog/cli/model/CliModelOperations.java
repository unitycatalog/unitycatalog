package io.unitycatalog.cli.model;

import com.fasterxml.jackson.core.type.TypeReference;
import io.unitycatalog.cli.BaseCliOperations;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateModelVersion;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.FinalizeModelVersion;
import io.unitycatalog.client.model.ModelVersionInfo;
import io.unitycatalog.client.model.RegisteredModelInfo;
import io.unitycatalog.client.model.UpdateModelVersion;
import io.unitycatalog.client.model.UpdateRegisteredModel;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.model.ModelOperations;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CliModelOperations implements ModelOperations {

  private final BaseCliOperations registeredModelOps;
  private final BaseCliOperations modelVersionOps;

  public CliModelOperations(ServerConfig config) {
    this.registeredModelOps = new BaseCliOperations("registered_model", config);
    this.modelVersionOps = new BaseCliOperations("model_version", config);
  }

  @Override
  public RegisteredModelInfo createRegisteredModel(CreateRegisteredModel createRegisteredModel)
      throws ApiException {
    List<String> argsList =
        new ArrayList<>(
            List.of(
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
    return registeredModelOps.execute(RegisteredModelInfo.class, "create", argsList);
  }

  @Override
  public List<RegisteredModelInfo> listRegisteredModels(
      Optional<String> catalogName, Optional<String> schemaName, Optional<String> pageToken)
      throws ApiException {
    List<String> argsList = new ArrayList<>();
    if (catalogName.isPresent() && schemaName.isPresent()) {
      argsList.add("--catalog");
      argsList.add(catalogName.get());
      argsList.add("--schema");
      argsList.add(schemaName.get());
    }
    if (pageToken.isPresent()) {
      argsList.add("--page_token");
      argsList.add(pageToken.get());
    }
    return registeredModelOps.execute(new TypeReference<>() {}, "list", argsList);
  }

  @Override
  public RegisteredModelInfo getRegisteredModel(String registeredModelFullName)
      throws ApiException {
    return registeredModelOps.execute(
        RegisteredModelInfo.class, "get", List.of("--full_name", registeredModelFullName));
  }

  @Override
  public RegisteredModelInfo updateRegisteredModel(
      String registeredModelFullName, UpdateRegisteredModel updateRm) throws ApiException {
    List<String> argsList = new ArrayList<>();
    if (updateRm.getNewName() != null) {
      argsList.add("--new_name");
      argsList.add(updateRm.getNewName());
    }
    if (updateRm.getComment() != null) {
      argsList.add("--comment");
      argsList.add(updateRm.getComment());
    }
    // chEmptyUpdateCliException=false because the test expects the exception.
    return registeredModelOps.executeUpdate(
        RegisteredModelInfo.class,
        /* catchEmptyUpdateCliException= */ false,
        List.of("--full_name", registeredModelFullName),
        argsList);
  }

  @Override
  public void deleteRegisteredModel(String registeredModelFullName, Optional<Boolean> force)
      throws ApiException {
    List<String> argsList = new ArrayList<>(List.of("--full_name", registeredModelFullName));
    if (force.isPresent() && force.get()) {
      argsList.add("--force");
      argsList.add("true");
    }
    registeredModelOps.execute(Void.class, "delete", argsList);
  }

  @Override
  public ModelVersionInfo createModelVersion(CreateModelVersion createModelVersion)
      throws ApiException {
    List<String> argsList =
        new ArrayList<>(
            List.of(
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
    return modelVersionOps.execute(ModelVersionInfo.class, "create", argsList);
  }

  @Override
  public List<ModelVersionInfo> listModelVersions(
      String registeredModelFullName, Optional<String> pageToken) throws ApiException {
    List<String> argsList = new ArrayList<>(List.of("--full_name", registeredModelFullName));
    if (pageToken.isPresent()) {
      argsList.add("--page_token");
      argsList.add(pageToken.get());
    }
    return modelVersionOps.execute(new TypeReference<>() {}, "list", argsList);
  }

  @Override
  public ModelVersionInfo getModelVersion(String registeredModelFullName, Long version)
      throws ApiException {
    return modelVersionOps.execute(
        ModelVersionInfo.class,
        "get",
        List.of("--full_name", registeredModelFullName, "--version", version.toString()));
  }

  @Override
  public ModelVersionInfo updateModelVersion(
      String fullName, Long version, UpdateModelVersion updateMv) throws ApiException {
    List<String> argsList = new ArrayList<>();
    if (updateMv.getComment() != null) {
      argsList.add("--comment");
      argsList.add(updateMv.getComment());
    }
    return modelVersionOps.executeUpdate(
        ModelVersionInfo.class,
        /* catchEmptyUpdateCliException= */ true,
        List.of("--full_name", fullName, "--version", version.toString()),
        argsList);
  }

  @Override
  public void deleteModelVersion(String registeredModelFullName, Long version) throws ApiException {
    modelVersionOps.execute(
        Void.class,
        "delete",
        List.of("--full_name", registeredModelFullName, "--version", version.toString()));
  }

  @Override
  public ModelVersionInfo finalizeModelVersion(
      String registeredModelFullName, Long version, FinalizeModelVersion finalizeModelVersion)
      throws ApiException {
    return modelVersionOps.execute(
        ModelVersionInfo.class,
        "finalize",
        List.of("--full_name", registeredModelFullName, "--version", version.toString()));
  }
}
