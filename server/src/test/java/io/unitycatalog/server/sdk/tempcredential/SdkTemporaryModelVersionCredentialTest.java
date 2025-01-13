package io.unitycatalog.server.sdk.tempcredential;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.model.ModelOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.tempcredential.BaseTemporaryModelVersionCredentialTest;
import io.unitycatalog.server.base.tempcredential.TemporaryCredentialOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.models.SdkModelOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;

public class SdkTemporaryModelVersionCredentialTest
    extends BaseTemporaryModelVersionCredentialTest {

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new SdkSchemaOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected ModelOperations createModelOperations(ServerConfig config) {
    return new SdkModelOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected TemporaryCredentialOperations createTemporaryCredentialsOperations(
      ServerConfig config) {
    return new SdkTemporaryCredentialOperations(TestUtils.createApiClient(config));
  }
}
