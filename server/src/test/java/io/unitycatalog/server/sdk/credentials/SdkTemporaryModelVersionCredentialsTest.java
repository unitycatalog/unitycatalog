package io.unitycatalog.server.sdk.credentials;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.credentials.BaseTemporaryModelVersionCredentialsTest;
import io.unitycatalog.server.base.credentials.TemporaryModelVersionCredentialsOperations;
import io.unitycatalog.server.base.model.ModelOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.models.SdkModelOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;

public class SdkTemporaryModelVersionCredentialsTest
    extends BaseTemporaryModelVersionCredentialsTest {

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
  protected TemporaryModelVersionCredentialsOperations
      createTemporaruModelVersionCredentialsOperations(ServerConfig config) {
    return new SdkTemporaryModelVersionCredentialsOperations(TestUtils.createApiClient(config));
  }
}
