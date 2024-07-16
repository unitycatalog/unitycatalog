package io.unitycatalog.cli.table;

import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.cli.schema.CliSchemaOperations;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTest;
import io.unitycatalog.server.base.table.TableOperations;

public class CliTableCRUDTest extends BaseTableCRUDTest {
  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new CliCatalogOperations(serverConfig);
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new CliSchemaOperations(serverConfig);
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig serverConfig) {
    return new CliTableOperations(serverConfig);
  }
}
