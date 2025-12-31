package io.unitycatalog.cli.credential;

import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.credential.BaseCredentialCRUDTest;
import io.unitycatalog.server.base.credential.CredentialOperations;

public class CliCredentialCRUDTest extends BaseCredentialCRUDTest {

  @Override
  protected CredentialOperations createCredentialOperations(ServerConfig config) {
    return new CliCredentialOperations(config);
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new CliCatalogOperations(config);
  }
}
