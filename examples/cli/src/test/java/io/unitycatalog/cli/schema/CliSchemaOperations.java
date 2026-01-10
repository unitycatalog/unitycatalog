package io.unitycatalog.cli.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.unitycatalog.cli.BaseCliOperations;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.UpdateSchema;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CliSchemaOperations extends BaseCliOperations implements SchemaOperations {

  public CliSchemaOperations(ServerConfig config) {
    super("schema", config);
  }

  @Override
  public SchemaInfo createSchema(CreateSchema createSchema) throws ApiException {
    List<String> argsList =
        new ArrayList<>(
            List.of("--name", createSchema.getName(), "--catalog", createSchema.getCatalogName()));
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
    return execute(SchemaInfo.class, "create", argsList);
  }

  @Override
  public List<SchemaInfo> listSchemas(String catalogName, Optional<String> pageToken)
      throws ApiException {
    List<String> argsList = new ArrayList<>(List.of("--catalog", catalogName));
    if (pageToken.isPresent()) {
      argsList.add("--page_token");
      argsList.add(pageToken.get());
    }
    return execute(new TypeReference<>() {}, "list", argsList);
  }

  @Override
  public SchemaInfo getSchema(String schemaFullName) throws ApiException {
    return execute(SchemaInfo.class, "get", List.of("--full_name", schemaFullName));
  }

  @Override
  public SchemaInfo updateSchema(String schemaFullName, UpdateSchema updateSchema)
      throws ApiException {
    List<String> argsList = new ArrayList<>();
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
    return executeUpdate(SchemaInfo.class, List.of("--full_name", schemaFullName), argsList);
  }

  @Override
  public void deleteSchema(String schemaFullName, Optional<Boolean> force) throws ApiException {
    List<String> argsList = new ArrayList<>(List.of("--full_name", schemaFullName));
    if (force.isPresent() && force.get()) {
      argsList.add("--force");
      argsList.add("true");
    }
    execute(Void.class, "delete", argsList);
  }
}
