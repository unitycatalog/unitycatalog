package io.unitycatalog.server.sdk.models;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.ModelVersionsApi;
import io.unitycatalog.client.api.RegisteredModelsApi;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.model.ModelOperations;
import java.util.List;
import java.util.Optional;

public class SdkModelOperations implements ModelOperations {
  private final RegisteredModelsApi registeredModelsApi;
  private final ModelVersionsApi modelVersionsApi;

  public SdkModelOperations(ApiClient apiClient) {
    this.registeredModelsApi = new RegisteredModelsApi(apiClient);
    this.modelVersionsApi = new ModelVersionsApi(apiClient);
  }

  @Override
  public RegisteredModelInfo createRegisteredModel(CreateRegisteredModel createRegisteredModel)
      throws ApiException {
    return registeredModelsApi.createRegisteredModel(createRegisteredModel);
  }

  @Override
  public List<RegisteredModelInfo> listRegisteredModels(
      Optional<String> catalogName, Optional<String> schemaName, Optional<String> pageToken)
      throws ApiException {
    return registeredModelsApi
        .listRegisteredModels(
            catalogName.orElse(null), schemaName.orElse(null), 100, pageToken.orElse(null))
        .getRegisteredModels();
  }

  @Override
  public RegisteredModelInfo getRegisteredModel(String registeredModelFullName)
      throws ApiException {
    return registeredModelsApi.getRegisteredModel(registeredModelFullName);
  }

  @Override
  public RegisteredModelInfo updateRegisteredModel(
      String fullName, UpdateRegisteredModel updateRegisteredModel) throws ApiException {
    return registeredModelsApi.updateRegisteredModel(fullName, updateRegisteredModel);
  }

  @Override
  public void deleteRegisteredModel(String registeredModelFullName, Optional<Boolean> force)
      throws ApiException {
    registeredModelsApi.deleteRegisteredModel(registeredModelFullName, force.orElse(false));
  }

  @Override
  public ModelVersionInfo createModelVersion(CreateModelVersion createModelVersion)
      throws ApiException {
    return modelVersionsApi.createModelVersion(createModelVersion);
  }

  @Override
  public List<ModelVersionInfo> listModelVersions(
      String registeredModelFullName, Optional<String> pageToken) throws ApiException {
    return modelVersionsApi
        .listModelVersions(registeredModelFullName, 100, pageToken.orElse(null))
        .getModelVersions();
  }

  @Override
  public ModelVersionInfo getModelVersion(String registeredModelFullName, Long version)
      throws ApiException {
    return modelVersionsApi.getModelVersion(registeredModelFullName, version);
  }

  @Override
  public ModelVersionInfo updateModelVersion(
      String fullName, Long version, UpdateModelVersion updateModelVersion) throws ApiException {
    return modelVersionsApi.updateModelVersion(fullName, version, updateModelVersion);
  }

  @Override
  public void deleteModelVersion(String registeredModelFullName, Long version) throws ApiException {
    modelVersionsApi.deleteModelVersion(registeredModelFullName, version);
  }

  @Override
  public ModelVersionInfo finalizeModelVersion(
      String fullName, Long version, FinalizeModelVersion finalizeModelVersion)
      throws ApiException {
    return modelVersionsApi.finalizeModelVersion(fullName, version, finalizeModelVersion);
  }
}
