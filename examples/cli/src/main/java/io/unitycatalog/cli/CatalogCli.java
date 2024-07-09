package io.unitycatalog.cli;

import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.UpdateCatalog;
import org.apache.commons.cli.CommandLine;
import org.json.JSONObject;

import static io.unitycatalog.cli.utils.CliUtils.postProcessAndPrintOutput;

public class CatalogCli {
    private static final ObjectMapper objectMapper = CliUtils.getObjectMapper();
    private static ObjectWriter objectWriter;
    private static final String NAME_PARAM = CliParams.NAME.val();

    public static void handle(CommandLine cmd, ApiClient apiClient) throws JsonProcessingException, ApiException {
        CatalogsApi catalogsApi = new CatalogsApi(apiClient);
        String[] subArgs = cmd.getArgs();
        objectWriter = CliUtils.getObjectWriter(cmd);
        String subCommand = subArgs[1];
        JSONObject json = CliUtils.createJsonFromOptions(cmd);
        String output = CliUtils.EMPTY;
        switch (subCommand) {
            case CliUtils.CREATE:
                output = createCatalog(catalogsApi, json);
                break;
            case CliUtils.LIST:
                output = listCatalogs(catalogsApi);
                break;
            case CliUtils.GET:
                output = getCatalog(catalogsApi, json);
                break;
            case CliUtils.UPDATE:
                output = updateCatalog(catalogsApi, json);
                break;
            case CliUtils.DELETE:
                output = deleteCatalog(catalogsApi, json);
                break;
            default:
                CliUtils.printEntityHelp(CliUtils.CATALOG);
        }
        postProcessAndPrintOutput(cmd, output, subCommand);
    }

    private static String createCatalog(CatalogsApi catalogsApi, JSONObject json) throws JsonProcessingException, ApiException {
        CreateCatalog createCatalog;
        createCatalog = objectMapper.readValue(json.toString(), CreateCatalog.class);
        return objectWriter.writeValueAsString(catalogsApi.createCatalog(createCatalog));
    }

    private static String listCatalogs(CatalogsApi catalogsApi) throws JsonProcessingException, ApiException {
        return objectWriter.writeValueAsString(catalogsApi.listCatalogs(null,100).getCatalogs());
    }

    private static String getCatalog(CatalogsApi catalogsApi, JSONObject json) throws JsonProcessingException, ApiException {
        String catalogName = json.getString(NAME_PARAM);
        return objectWriter.writeValueAsString(catalogsApi.getCatalog(catalogName));
    }

    private static String updateCatalog(CatalogsApi apiClient, JSONObject json) throws JsonProcessingException, ApiException {
        String catalogName = json.getString(NAME_PARAM);
        json.remove(NAME_PARAM);
        UpdateCatalog updateCatalog = objectMapper.readValue(json.toString(), UpdateCatalog.class);
        return objectWriter.writeValueAsString(apiClient.updateCatalog(catalogName, updateCatalog));
    }

    private static String deleteCatalog(CatalogsApi catalogsApi, JSONObject json) throws ApiException {
        String catalogName = json.getString(NAME_PARAM);
        catalogsApi.deleteCatalog(catalogName, json.has(CliParams.FORCE.getServerParam()) &&
                Boolean.parseBoolean(json.getString(CliParams.FORCE.getServerParam())));
        return CliUtils.EMPTY_JSON;
    }

}
