package io.unitycatalog.cli;

import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.UpdateSchema;
import org.apache.commons.cli.CommandLine;
import org.json.JSONObject;

import java.util.Collections;
import java.util.List;

public class SchemaCli {
    private static final ObjectMapper objectMapper = CliUtils.getObjectMapper();
    private static ObjectWriter objectWriter;
    public static void handle(CommandLine cmd, ApiClient apiClient) throws JsonProcessingException, ApiException {
        SchemasApi schemasApi = new SchemasApi(apiClient);
        String[] subArgs = cmd.getArgs();
        String subCommand = subArgs[1];
        objectWriter = CliUtils.getObjectWriter(cmd);
        JSONObject json = CliUtils.createJsonFromOptions(cmd);
        String output = CliUtils.EMPTY;
        switch (subCommand) {
            case CliUtils.CREATE:
                output = createSchema(schemasApi, json);
                break;
            case CliUtils.LIST:
                output = listSchemas(schemasApi, json);
                break;
            case CliUtils.GET:
                output = getSchema(schemasApi, json);
                break;
            case CliUtils.UPDATE:
                output = updateSchema(schemasApi, json);
                break;
            case CliUtils.DELETE:
                output = deleteSchema(schemasApi, json);
                break;
            default:
                CliUtils.printEntityHelp(CliUtils.SCHEMA);
        }
        CliUtils.postProcessAndPrintOutput(cmd, output, subCommand);
    }

    private static String createSchema(SchemasApi schemasApi, JSONObject json) throws JsonProcessingException, ApiException {
        CreateSchema createSchema;
        createSchema = objectMapper.readValue(json.toString(), CreateSchema.class);
        return objectWriter.writeValueAsString(schemasApi.createSchema(createSchema));
    }

    private static String listSchemas(SchemasApi schemasApi, JSONObject json) throws JsonProcessingException, ApiException {
        String catalogName = json.getString(CliParams.CATALOG_NAME.getServerParam());
        return objectWriter.writeValueAsString(schemasApi.listSchemas(catalogName, 100, null).getSchemas());
    }

    private static String getSchema(SchemasApi schemasApi, JSONObject json) throws JsonProcessingException, ApiException {
        String schemaFullName = json.getString(CliParams.FULL_NAME.getServerParam());
        return objectWriter.writeValueAsString(schemasApi.getSchema(schemaFullName));
    }

    private static String updateSchema(SchemasApi schemasApi, JSONObject json) throws JsonProcessingException, ApiException {
        UpdateSchema updateSchema = new UpdateSchema()
                .newName(json.optString(CliParams.NEW_NAME.getServerParam()))
                .comment(json.optString(CliParams.COMMENT.getServerParam()));
        return objectWriter.writeValueAsString(schemasApi
                .updateSchema(json.getString(CliParams.FULL_NAME.getServerParam()), updateSchema));
    }

    private static String deleteSchema(SchemasApi schemasApi, JSONObject json) throws ApiException {
        String schemaFullName = json.getString(CliParams.FULL_NAME.getServerParam());
        schemasApi.deleteSchema(schemaFullName, json.has(CliParams.FORCE.getServerParam()) &&
                Boolean.parseBoolean(json.getString(CliParams.FORCE.getServerParam())));
        return CliUtils.EMPTY;
    }
}
