package io.unitycatalog.cli.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.ListSchemasResponse;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.UpdateSchema;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;

import java.util.ArrayList;
import java.util.List;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;

public class CliSchemaOperations implements SchemaOperations {

    private final ServerConfig config;
    private final ObjectMapper objectMapper = new ObjectMapper();
    public CliSchemaOperations(ServerConfig config) {
        this.config = config;
    }

    @Override
    public SchemaInfo createSchema(CreateSchema createSchema) {
        List<String> argsList = new ArrayList<>(List.of(
                "schema", "create"
                , "--name", createSchema.getName()
                , "--catalog", createSchema.getCatalogName()));
        if (createSchema.getComment() != null) {
            argsList.add("--comment");
            argsList.add(createSchema.getComment());
        }
        String[] args = addServerAndAuthParams(argsList, config);
        JsonNode schemaInfoJson =  executeCLICommand(args);
        return objectMapper.convertValue(schemaInfoJson, SchemaInfo.class);
    }

    @Override
    public List<SchemaInfo> listSchemas(String catalogName) {
        String[] args = addServerAndAuthParams(List.of(
                "schema", "list"
                , "--catalog", catalogName)
                , config);
        JsonNode schemaList = executeCLICommand(args);
        return objectMapper.convertValue(schemaList,
                new TypeReference<List<SchemaInfo>>() {});
    }

    @Override
    public SchemaInfo getSchema(String schemaFullName) {
        String[] args = addServerAndAuthParams(List.of(
                "schema", "get"
                , "--full_name", schemaFullName)
                , config);
        JsonNode schemaInfoJson = executeCLICommand(args);
        return objectMapper.convertValue(schemaInfoJson, SchemaInfo.class);
    }

    @Override
    public SchemaInfo updateSchema(String schemaName, UpdateSchema updateSchema) {
        String[] args = addServerAndAuthParams(List.of(
                "schema", "update"
                , "--full_name",schemaName
                , "--new_name", updateSchema.getNewName()
                , "--comment", updateSchema.getComment())
                , config);
        JsonNode updatedSchemaInfo = executeCLICommand(args);
        return objectMapper.convertValue(updatedSchemaInfo, SchemaInfo.class);
    }

    @Override
    public void deleteSchema(String schemaFullName) {
        String[] args = addServerAndAuthParams(List.of(
                "schema", "delete"
                , "--full_name", schemaFullName)
                , config);
        executeCLICommand(args);
    }
}
