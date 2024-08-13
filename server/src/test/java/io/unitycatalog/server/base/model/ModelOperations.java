package io.unitycatalog.server.base.model;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.RegisteredModelInfo;
import io.unitycatalog.client.model.UpdateRegisteredModel;
import java.util.List;
import java.util.Optional;

public interface ModelOperations {
  RegisteredModelInfo createRegisteredModel(CreateRegisteredModel createModel) throws ApiException;

  List<RegisteredModelInfo> listRegisteredModels(String catalogName, String schemaName)
      throws ApiException;

  RegisteredModelInfo getRegisteredModel(String modelFullName) throws ApiException;

  RegisteredModelInfo updateRegisteredModel(
      String fullName, UpdateRegisteredModel updateRegisteredModel) throws ApiException;

  void deleteRegisteredModel(String modelFullName, Optional<Boolean> force) throws ApiException;
}
