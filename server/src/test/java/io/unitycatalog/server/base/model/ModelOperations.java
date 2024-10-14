package io.unitycatalog.server.base.model;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import java.util.List;
import java.util.Optional;

public interface ModelOperations {
  RegisteredModelInfo createRegisteredModel(CreateRegisteredModel createModel) throws ApiException;

  List<RegisteredModelInfo> listRegisteredModels(
      Optional<String> catalogName, Optional<String> schemaName, Optional<String> pageToken)
      throws ApiException;

  RegisteredModelInfo getRegisteredModel(String modelFullName) throws ApiException;

  RegisteredModelInfo updateRegisteredModel(
      String fullName, UpdateRegisteredModel updateRegisteredModel) throws ApiException;

  void deleteRegisteredModel(String modelFullName, Optional<Boolean> force) throws ApiException;

  ModelVersionInfo createModelVersion(CreateModelVersion createModelVersion) throws ApiException;

  List<ModelVersionInfo> listModelVersions(
      String registeredModelFullName, Optional<String> pageToken) throws ApiException;

  ModelVersionInfo getModelVersion(String modelFullName, Long version) throws ApiException;

  ModelVersionInfo updateModelVersion(
      String fullName, Long version, UpdateModelVersion updateModelVersion) throws ApiException;

  void deleteModelVersion(String modelFullName, Long version) throws ApiException;

  ModelVersionInfo finalizeModelVersion(
      String fullName, Long version, FinalizeModelVersion finalizeModelVersion) throws ApiException;
}
