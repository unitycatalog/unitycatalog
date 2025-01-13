package io.unitycatalog.server.sdk.tempcredential;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.tempcredential.BaseTemporaryVolumeCredentialTest;
import io.unitycatalog.server.base.tempcredential.TemporaryCredentialOperations;
import io.unitycatalog.server.base.volume.VolumeOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.volume.SdkVolumeOperations;
import io.unitycatalog.server.utils.TestUtils;

public class SdkTemporaryVolumeCredentialTest extends BaseTemporaryVolumeCredentialTest {
  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected TemporaryCredentialOperations createTemporaryCredentialsOperations(
      ServerConfig serverConfig) {
    return new SdkTemporaryCredentialOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected VolumeOperations createVolumeOperations(ServerConfig serverConfig) {
    return new SdkVolumeOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }
}
