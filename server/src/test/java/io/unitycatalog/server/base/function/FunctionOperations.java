package io.unitycatalog.server.base.function;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateFunctionRequest;
import io.unitycatalog.client.model.FunctionInfo;
import java.util.List;
import java.util.Optional;

public interface FunctionOperations {
  FunctionInfo createFunction(CreateFunctionRequest createFunctionRequest) throws ApiException;

  List<FunctionInfo> listFunctions(
      String catalogName, String schemaName, Optional<String> pageToken) throws ApiException;

  FunctionInfo getFunction(String functionFullName) throws ApiException;

  void deleteFunction(String functionFullName, boolean force) throws ApiException;
}
