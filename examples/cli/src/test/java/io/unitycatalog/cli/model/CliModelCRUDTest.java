package io.unitycatalog.cli.model;

import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.cli.schema.CliSchemaOperations;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.model.BaseModelCRUDTest;
import io.unitycatalog.server.base.model.ModelOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;

public class CliModelCRUDTest extends BaseModelCRUDTest {

  @Override
  protected ModelOperations createModelOperations(ServerConfig config) {
    return new CliModelOperations(config);
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new CliCatalogOperations(serverConfig);
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new CliSchemaOperations(serverConfig);
  }
}
