package io.unitycatalog.cli.function;

import com.fasterxml.jackson.core.type.TypeReference;
import io.unitycatalog.cli.BaseCliOperations;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateFunctionRequest;
import io.unitycatalog.client.model.FunctionInfo;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.function.FunctionOperations;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CliFunctionOperations extends BaseCliOperations implements FunctionOperations {

  public CliFunctionOperations(ServerConfig config) {
    super("function", config);
  }

  @Override
  public FunctionInfo createFunction(CreateFunctionRequest createFunctionRequest)
      throws ApiException {
    String fullName =
        createFunctionRequest.getFunctionInfo().getCatalogName()
            + "."
            + createFunctionRequest.getFunctionInfo().getSchemaName()
            + "."
            + createFunctionRequest.getFunctionInfo().getName();
    String inputParams =
        createFunctionRequest.getFunctionInfo().getInputParams().getParameters().stream()
            .map(p -> p.getName() + " " + p.getTypeName())
            .collect(Collectors.joining(", "));
    List<String> argsList =
        new ArrayList<>(
            List.of(
                "--full_name",
                fullName,
                "--input_params",
                inputParams,
                "--data_type",
                createFunctionRequest.getFunctionInfo().getDataType().name()));
    if (createFunctionRequest.getFunctionInfo().getComment() != null) {
      argsList.add("--comment");
      argsList.add(createFunctionRequest.getFunctionInfo().getComment());
    }
    if (createFunctionRequest.getFunctionInfo().getExternalLanguage() != null) {
      argsList.add("--language");
      argsList.add(createFunctionRequest.getFunctionInfo().getExternalLanguage());
    }
    argsList.add("--def");
    argsList.add(createFunctionRequest.getFunctionInfo().getRoutineDefinition());
    return execute(FunctionInfo.class, "create", argsList);
  }

  @Override
  public List<FunctionInfo> listFunctions(
      String catalogName, String schemaName, Optional<String> pageToken) throws ApiException {
    List<String> argsList =
        new ArrayList<>(List.of("--catalog", catalogName, "--schema", schemaName));
    if (pageToken.isPresent()) {
      argsList.add("--page_token");
      argsList.add(pageToken.get());
    }
    return execute(new TypeReference<>() {}, "list", argsList);
  }

  @Override
  public FunctionInfo getFunction(String functionFullName) throws ApiException {
    return execute(FunctionInfo.class, "get", List.of("--full_name", functionFullName));
  }

  @Override
  public void deleteFunction(String functionFullName, boolean force) throws ApiException {
    execute(Void.class, "delete", List.of("--full_name", functionFullName));
  }
}
