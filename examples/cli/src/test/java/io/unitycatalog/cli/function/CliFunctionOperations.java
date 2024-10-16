package io.unitycatalog.cli.function;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.model.CreateFunctionRequest;
import io.unitycatalog.client.model.FunctionInfo;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.function.FunctionOperations;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CliFunctionOperations implements FunctionOperations {
  private final ServerConfig config;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public CliFunctionOperations(ServerConfig config) {
    this.config = config;
  }

  @Override
  public FunctionInfo createFunction(CreateFunctionRequest createFunctionRequest) {
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
                "function",
                "create",
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
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode jsonNode = executeCLICommand(args);
    return objectMapper.convertValue(jsonNode, FunctionInfo.class);
  }

  @Override
  public List<FunctionInfo> listFunctions(
      String catalogName, String schemaName, Optional<String> pageToken) {
    List<String> argsList =
        new ArrayList<>(
            List.of("function", "list", "--catalog", catalogName, "--schema", schemaName));
    if (pageToken.isPresent()) {
      argsList.add("--page_token");
      argsList.add(pageToken.get());
    }
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode jsonNode = executeCLICommand(args);
    return objectMapper.convertValue(jsonNode, new TypeReference<List<FunctionInfo>>() {});
  }

  @Override
  public FunctionInfo getFunction(String functionFullName) {
    String[] args =
        addServerAndAuthParams(List.of("function", "get", "--full_name", functionFullName), config);
    JsonNode jsonNode = executeCLICommand(args);
    return objectMapper.convertValue(jsonNode, FunctionInfo.class);
  }

  @Override
  public void deleteFunction(String functionFullName, boolean force) {
    String[] args =
        addServerAndAuthParams(
            List.of("function", "delete", "--full_name", functionFullName), config);
    executeCLICommand(args);
  }
}
