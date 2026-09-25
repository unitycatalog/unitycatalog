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

/**
 * Fixture that stands up a catalog and schema for tests of schema-scoped resources (tables,
 * volumes, models). It mirrors the securable hierarchy: it sits below {@link BaseCRUDTest} (catalog
 * ops) and is the common parent of the per-domain {@code *CRUDTestEnv} fixtures. It carries no
 * {@code @Test} methods so subclasses can extend it without inheriting a CRUD suite.
 */
public abstract class BaseSchemaCRUDTestEnv extends BaseCRUDTest {

  protected SchemaOperations schemaOperations;
  protected String schemaId;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  /**
   * Storage root for the created catalog. Empty (the default) leaves the catalog without one, so
   * managed tables fall back to the {@code TABLE_STORAGE_ROOT} server property; domains whose
   * managed storage derives from the catalog (for example volumes) override this.
   */
  protected Optional<String> catalogStorageRoot() {
    return Optional.empty();
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = createSchemaOperations(serverConfig);
    createCommonResources();
  }

  private void createCommonResources() {
    CreateCatalog createCatalog =
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT);
    catalogStorageRoot().ifPresent(createCatalog::storageRoot);
    try {
      catalogOperations.createCatalog(createCatalog);
      SchemaInfo schemaInfo =
          schemaOperations.createSchema(
              new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
      schemaId = schemaInfo.getSchemaId();
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }
}
