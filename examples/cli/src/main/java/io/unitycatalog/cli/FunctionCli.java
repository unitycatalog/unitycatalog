package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.cli.utils.PythonInvoker;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.FunctionsApi;
import io.unitycatalog.client.model.CreateFunction;
import io.unitycatalog.client.model.CreateFunctionRequest;
import io.unitycatalog.client.model.FunctionParameterInfos;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.json.JSONObject;

public class FunctionCli {
  private static final ObjectMapper objectMapper = CliUtils.getObjectMapper();
  private static ObjectWriter objectWriter;

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {
    FunctionsApi functionsApi = new FunctionsApi(apiClient);
    String[] subArgs = cmd.getArgs();
    objectWriter = CliUtils.getObjectWriter(cmd);
    String subCommand = subArgs[1];
    JSONObject json = CliUtils.createJsonFromOptions(cmd);
    String output = EMPTY;
    switch (subCommand) {
      case CREATE:
        output = createFunction(functionsApi, json);
        break;
      case LIST:
        output = listFunctions(functionsApi, json);
        break;
      case GET:
        output = getFunction(functionsApi, json);
        break;
      case DELETE:
        output = deleteFunction(functionsApi, json);
        break;
      case EXECUTE:
        output = executeFunction(functionsApi, json);
        break;
      default:
        printEntityHelp(FUNCTION);
    }
    postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static String createFunction(FunctionsApi functionsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    CliUtils.resolveFullNameToThreeLevelNamespace(json);
    FunctionParameterInfos inputParams = CliUtils.parseInputParams(json);
    CreateFunction createFunction = objectMapper.readValue(json.toString(), CreateFunction.class);
    createFunction.setInputParams(inputParams);
    if (createFunction.getIsDeterministic() == null) {
      createFunction.setIsDeterministic(true);
    }
    if (createFunction.getParameterStyle() == null) {
      createFunction.setParameterStyle(CreateFunction.ParameterStyleEnum.S);
    }
    if (createFunction.getFullDataType() == null) {
      createFunction.setFullDataType(createFunction.getDataType().name());
    }
    if (createFunction.getIsNullCall() == null) {
      createFunction.setIsNullCall(true);
    }
    if (createFunction.getRoutineBody() == null) {
      createFunction.setRoutineBody(CreateFunction.RoutineBodyEnum.EXTERNAL);
    }
    if (createFunction.getRoutineDefinition() == null) {
      createFunction.setRoutineDefinition(EMPTY);
    }
    if (createFunction.getSecurityType() == null) {
      createFunction.setSecurityType(CreateFunction.SecurityTypeEnum.DEFINER);
    }
    if (createFunction.getSpecificName() == null) {
      createFunction.setSpecificName(createFunction.getName());
    }
    if (createFunction.getSqlDataAccess() == null) {
      createFunction.setSqlDataAccess(CreateFunction.SqlDataAccessEnum.NO_SQL);
    }
    if (createFunction.getExternalLanguage() == null) {
      createFunction.setExternalLanguage("python");
    }
    return objectWriter.writeValueAsString(
        functionsApi.createFunction(new CreateFunctionRequest().functionInfo(createFunction)));
  }

  private static String listFunctions(FunctionsApi functionsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String catalogName = json.getString(CliParams.CATALOG_NAME.getServerParam());
    String schemaName = json.getString(CliParams.SCHEMA_NAME.getServerParam());
    int maxResults = 100;
    if (json.has(CliParams.MAX_RESULTS.getServerParam())) {
      maxResults = json.getInt(CliParams.MAX_RESULTS.getServerParam());
    }
    String pageToken = null;
    if (json.has(CliParams.PAGE_TOKEN.getServerParam())) {
      pageToken = json.getString(CliParams.PAGE_TOKEN.getServerParam());
    }
    return objectWriter.writeValueAsString(
        functionsApi.listFunctions(catalogName, schemaName, maxResults, pageToken).getFunctions());
  }

  private static String executeFunction(FunctionsApi functionsApi, JSONObject json)
      throws ApiException {
    String args = json.getString(CliParams.INPUT_PARAMS.getServerParam());
    String functionFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    try {
      return PythonInvoker.invokePython(
          functionsApi.getFunction(functionFullName),
          "etc/data/function/python_engine.py",
          Arrays.stream(args.split(",")).map(String::trim).toArray(String[]::new));
    } catch (Exception e) {
      throw new CliException("Failed to execute function " + functionFullName, e);
    }
  }

  private static String getFunction(FunctionsApi functionsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String functionFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    return objectWriter.writeValueAsString(functionsApi.getFunction(functionFullName));
  }

  private static String deleteFunction(FunctionsApi functionsApi, JSONObject json)
      throws ApiException {
    String functionFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    functionsApi.deleteFunction(functionFullName);
    return EMPTY;
  }
}
