package io.unitycatalog.server.base.schema;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;

/** Creates a catalog and schema for tests of resources scoped under a schema. */
public abstract class BaseSchemaScopedTestEnv extends BaseSchemaCRUDTestEnv {

  protected String schemaId;

  /**
   * Storage root for the created catalog. Empty (the default) leaves the catalog without one, so
   * managed tables fall back to the {@code TABLE_STORAGE_ROOT} server property.
   */
  protected Optional<String> catalogStorageRoot() {
    return Optional.empty();
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    schemaId = createCatalogAndSchema(catalogStorageRoot()).getSchemaId();
  }
}
