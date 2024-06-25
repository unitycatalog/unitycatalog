package io.unitycatalog.cli.catalog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.UpdateCatalog;
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
    public CatalogInfo createCatalog(CreateCatalog createCatalog) {
        List<String> argsList = new ArrayList<>(List.of("catalog", "create", "--name", createCatalog.getName()));
        if (createCatalog.getComment() != null) {
            argsList.add("--comment");
            argsList.add(createCatalog.getComment());
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
    public CatalogInfo updateCatalog(String name, UpdateCatalog updateCatalog) {
        List<String> argsList = new ArrayList<>(List.of("catalog", "update", "--name", name,
                "--new_name", updateCatalog.getNewName()));
        if (updateCatalog.getComment() != null) {
            argsList.add("--comment");
            argsList.add(updateCatalog.getComment());
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
