package io.unitycatalog.server.base.schema;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.UpdateSchema;
import java.util.List;
import java.util.Optional;

public interface SchemaOperations {
  SchemaInfo createSchema(CreateSchema createSchema) throws ApiException;

  List<SchemaInfo> listSchemas(String catalogName, Optional<String> pageToken) throws ApiException;

  SchemaInfo getSchema(String schemaFullName) throws ApiException;

  SchemaInfo updateSchema(String name, UpdateSchema updateSchema) throws ApiException;

  void deleteSchema(String schemaFullName, Optional<Boolean> force) throws ApiException;
}
