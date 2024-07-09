package io.unitycatalog.server.base.function;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateFunctionRequest;
import io.unitycatalog.client.model.FunctionInfo;
import java.util.List;

public interface FunctionOperations {
  FunctionInfo createFunction(CreateFunctionRequest createFunctionRequest) throws ApiException;

  List<FunctionInfo> listFunctions(String catalogName, String schemaName) throws ApiException;

  FunctionInfo getFunction(String functionFullName) throws ApiException;

  void deleteFunction(String functionFullName, boolean force) throws ApiException;
}
