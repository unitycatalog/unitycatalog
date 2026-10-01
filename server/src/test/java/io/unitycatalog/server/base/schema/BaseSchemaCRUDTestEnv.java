package io.unitycatalog.server.base.schema;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;

/** Initializes schema operations without creating catalog or schema resources. */
public abstract class BaseSchemaCRUDTestEnv extends BaseCRUDTest {

  protected SchemaOperations schemaOperations;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = createSchemaOperations(serverConfig);
  }

  /**
   * Creates the standard test catalog and schema, setting the catalog storage root when present.
   */
  protected SchemaInfo createCatalogAndSchema(Optional<String> catalogStorageRoot) {
    CreateCatalog createCatalog =
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT);
    catalogStorageRoot.ifPresent(createCatalog::storageRoot);
    try {
      catalogOperations.createCatalog(createCatalog);
      return schemaOperations.createSchema(
          new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }
}
