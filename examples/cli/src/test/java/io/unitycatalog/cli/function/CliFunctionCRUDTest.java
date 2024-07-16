package io.unitycatalog.cli.function;

import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.cli.schema.CliSchemaOperations;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.function.BaseFunctionCRUDTest;
import io.unitycatalog.server.base.function.FunctionOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;

public class CliFunctionCRUDTest extends BaseFunctionCRUDTest {
  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new CliCatalogOperations(serverConfig);
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new CliSchemaOperations(serverConfig);
  }

  @Override
  protected FunctionOperations createFunctionOperations(ServerConfig serverConfig) {
    return new CliFunctionOperations(serverConfig);
  }
}
