package io.unitycatalog.cli.schema;

import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.BaseSchemaCRUDTest;
import io.unitycatalog.server.base.schema.SchemaOperations;

public class CliSchemaCRUDTest extends BaseSchemaCRUDTest {

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new CliSchemaOperations(config);
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new CliCatalogOperations(serverConfig);
  }
}
