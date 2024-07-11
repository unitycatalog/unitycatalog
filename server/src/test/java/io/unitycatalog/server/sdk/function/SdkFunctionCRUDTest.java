package io.unitycatalog.server.sdk.function;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.function.BaseFunctionCRUDTest;
import io.unitycatalog.server.base.function.FunctionOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;

public class SdkFunctionCRUDTest extends BaseFunctionCRUDTest {
  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new SdkSchemaOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected FunctionOperations createFunctionOperations(ServerConfig config) {
    return new SdkFunctionOperations(TestUtils.createApiClient(config));
  }
}
