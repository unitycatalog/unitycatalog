package io.unitycatalog.cli.catalog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;

import java.util.ArrayList;
import java.util.List;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;

public class CliCatalogOperations implements CatalogOperations {
    private final ServerConfig config;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public CliCatalogOperations(ServerConfig config) {
        this.config = config;
    }

    @Override
    public CatalogInfo createCatalog(String name, String comment) {
        List<String> argsList = new ArrayList<>(List.of("catalog", "create", "--name", name));
        if (comment != null) {
            argsList.add("--comment");
            argsList.add(comment);
        }
        String[] args = addServerAndAuthParams(argsList, config);
        JsonNode catalogInfoJson = executeCLICommand(args);
        return objectMapper.convertValue(catalogInfoJson, CatalogInfo.class);
    }

    @Override
    public List<CatalogInfo> listCatalogs() {
        String [] args = addServerAndAuthParams(List.of("catalog", "list"), config);
        JsonNode catalogList = executeCLICommand(args);
        return objectMapper.convertValue(catalogList, new TypeReference<List<CatalogInfo>>() {});
    }

    @Override
    public CatalogInfo getCatalog(String name) {
        String[] args = addServerAndAuthParams(List.of("catalog", "get", "--name", name), config);
        JsonNode catalogInfoJson = executeCLICommand(args);
        return objectMapper.convertValue(catalogInfoJson, CatalogInfo.class);
    }

    @Override
    public CatalogInfo updateCatalog(String name, String newName, String comment) {
        List<String> argsList = new ArrayList<>(List.of("catalog", "update", "--name", name));
        if (newName != null) {
            argsList.add("--new_name");
            argsList.add(newName);
        }
        if (comment != null) {
            argsList.add("--comment");
            argsList.add(comment);
        }
        String[] args = addServerAndAuthParams(argsList, config);
        JsonNode updatedCatalogInfo = executeCLICommand(args);
        return objectMapper.convertValue(updatedCatalogInfo, CatalogInfo.class);
    }

    @Override
    public void deleteCatalog(String name) {
        String[] args = addServerAndAuthParams(List.of("catalog", "delete", "--name", name), config);
        executeCLICommand(args);
    }
}
