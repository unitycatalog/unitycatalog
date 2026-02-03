package io.unitycatalog.cli.managedlocation;

import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.cli.model.CliModelOperations;
import io.unitycatalog.cli.schema.CliSchemaOperations;
import io.unitycatalog.cli.table.CliTableOperations;
import io.unitycatalog.cli.volume.CliVolumeOperations;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.managedlocation.BaseManagedLocationTest;
import io.unitycatalog.server.base.model.ModelOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.base.volume.VolumeOperations;

public class CliManagedLocationTest extends BaseManagedLocationTest {

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

  @Override
  protected VolumeOperations createVolumeOperations(ServerConfig serverConfig) {
    return new CliVolumeOperations(serverConfig);
  }

  @Override
  protected ModelOperations createModelOperations(ServerConfig serverConfig) {
    return new CliModelOperations(serverConfig);
  }
}
