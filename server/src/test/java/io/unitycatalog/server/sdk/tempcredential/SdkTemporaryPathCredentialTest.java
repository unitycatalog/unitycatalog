package io.unitycatalog.server.sdk.tempcredential;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.tempcredential.BaseTemporaryPathCredentialTest;
import io.unitycatalog.server.base.tempcredential.TemporaryCredentialOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.utils.TestUtils;

public class SdkTemporaryPathCredentialTest extends BaseTemporaryPathCredentialTest {
  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected TemporaryCredentialOperations createTemporaryCredentialsOperations(
      ServerConfig serverConfig) {
    return new SdkTemporaryCredentialOperations(TestUtils.createApiClient(serverConfig));
  }
}
