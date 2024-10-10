package io.unitycatalog.cli.schema;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.UpdateSchema;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CliSchemaOperations implements SchemaOperations {

  private final ServerConfig config;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public CliSchemaOperations(ServerConfig config) {
    this.config = config;
  }

  @Override
  public SchemaInfo createSchema(CreateSchema createSchema) {
    List<String> argsList =
        new ArrayList<>(
            List.of(
                "schema",
                "create",
                "--name",
                createSchema.getName(),
                "--catalog",
                createSchema.getCatalogName()));
    if (createSchema.getComment() != null) {
      argsList.add("--comment");
      argsList.add(createSchema.getComment());
    }
    if (createSchema.getProperties() != null && !createSchema.getProperties().isEmpty()) {
      argsList.add("--properties");
      try {
        argsList.add(objectMapper.writeValueAsString(createSchema.getProperties()));
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed to serialize properties", e);
      }
    }
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode schemaInfoJson = executeCLICommand(args);
    return objectMapper.convertValue(schemaInfoJson, SchemaInfo.class);
  }

  @Override
  public List<SchemaInfo> listSchemas(String catalogName, Optional<String> pageToken) {
    List<String> argsList = new ArrayList<>(List.of("schema", "list", "--catalog", catalogName));
    if (pageToken.isPresent()) {
      argsList.add("--page_token");
      argsList.add(pageToken.get());
    }
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode schemaList = executeCLICommand(args);
    return objectMapper.convertValue(schemaList, new TypeReference<List<SchemaInfo>>() {});
  }

  @Override
  public SchemaInfo getSchema(String schemaFullName) {
    String[] args =
        addServerAndAuthParams(List.of("schema", "get", "--full_name", schemaFullName), config);
    JsonNode schemaInfoJson = executeCLICommand(args);
    return objectMapper.convertValue(schemaInfoJson, SchemaInfo.class);
  }

  @Override
  public SchemaInfo updateSchema(String schemaFullName, UpdateSchema updateSchema) {
    List<String> argsList =
        new ArrayList<>(List.of("schema", "update", "--full_name", schemaFullName));
    if (updateSchema.getNewName() != null) {
      argsList.add("--new_name");
      argsList.add(updateSchema.getNewName());
    }
    if (updateSchema.getComment() != null) {
      argsList.add("--comment");
      argsList.add(updateSchema.getComment());
    }
    if (updateSchema.getProperties() != null && !updateSchema.getProperties().isEmpty()) {
      argsList.add("--properties");
      try {
        argsList.add(objectMapper.writeValueAsString(updateSchema.getProperties()));
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed to serialize properties", e);
      }
    }
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode updatedSchemaInfo = executeCLICommand(args);
    return objectMapper.convertValue(updatedSchemaInfo, SchemaInfo.class);
  }

  @Override
  public void deleteSchema(String schemaFullName, Optional<Boolean> force) {
    List<String> argsList =
        new ArrayList<>(List.of("schema", "delete", "--full_name", schemaFullName));
    if (force.isPresent() && force.get()) {
      argsList.add("--force");
      argsList.add("true");
    }
    String[] args = addServerAndAuthParams(argsList, config);
    executeCLICommand(args);
  }
}
