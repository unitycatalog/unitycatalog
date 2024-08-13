package io.unitycatalog.server.sdk.models;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.RegisteredModelsApi;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.model.ModelOperations;
import java.util.List;
import java.util.Optional;

public class SdkModelOperations implements ModelOperations {
  private final RegisteredModelsApi registeredModelsApi;

  public SdkModelOperations(ApiClient apiClient) {
    this.registeredModelsApi = new RegisteredModelsApi(apiClient);
  }

  @Override
  public RegisteredModelInfo createRegisteredModel(CreateRegisteredModel createRegisteredModel)
      throws ApiException {
    return registeredModelsApi.createRegisteredModel(createRegisteredModel);
  }

  @Override
  public List<RegisteredModelInfo> listRegisteredModels(String catalogName, String schemaName)
      throws ApiException {
    return registeredModelsApi
        .listRegisteredModels(catalogName, schemaName, 100, null)
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
}
