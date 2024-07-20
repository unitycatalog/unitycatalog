package io.unitycatalog.server.base.schema;

import static org.junit.Assert.*;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Optional;
import org.junit.*;

public abstract class BaseSchemaCRUDTest extends BaseCRUDTest {

  protected SchemaOperations schemaOperations;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig config);

  @Before
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = createSchemaOperations(serverConfig);
  }

  @Test
  public void testSchemaCRUDL() throws ApiException {
    // Create a schema
    System.out.println("Testing create schema..");
    CreateSchema createSchema =
        new CreateSchema()
            .name(TestUtils.SCHEMA_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .properties(TestUtils.PROPERTIES);
    assertThrows(Exception.class, () -> schemaOperations.createSchema(createSchema));

    CreateCatalog createCatalog = new CreateCatalog().name(TestUtils.CATALOG_NAME);
    catalogOperations.createCatalog(createCatalog);
    SchemaInfo schemaInfo = schemaOperations.createSchema(createSchema);
    assertEquals(createSchema.getName(), schemaInfo.getName());
    assertEquals(createSchema.getCatalogName(), schemaInfo.getCatalogName());
    assertEquals(TestUtils.SCHEMA_FULL_NAME, schemaInfo.getFullName());
    // TODO: Assert properties once CLI supports it
    assertNotNull(schemaInfo.getCreatedAt());

    // List schemas
    System.out.println("Testing list schemas..");
    Iterable<SchemaInfo> schemaList = schemaOperations.listSchemas(TestUtils.CATALOG_NAME);
    assertTrue(TestUtils.contains(schemaList, schemaInfo, (schema) -> schema.equals(schemaInfo)));

    // Get schema
    System.out.println("Testing get schema..");
    SchemaInfo retrievedSchemaInfo = schemaOperations.getSchema(TestUtils.SCHEMA_FULL_NAME);
    assertEquals(schemaInfo, retrievedSchemaInfo);

    // Calling update schema with nothing to update should not change anything
    System.out.println("Testing updating schema with nothing to update..");
    UpdateSchema emptyUpdateSchema = new UpdateSchema();
    schemaOperations.updateSchema(TestUtils.SCHEMA_FULL_NAME, emptyUpdateSchema);
    SchemaInfo retrievedSchemaInfo2 = schemaOperations.getSchema(TestUtils.SCHEMA_FULL_NAME);
    Assert.assertEquals(schemaInfo, retrievedSchemaInfo2);

    // Update schema name without updating comment and properties
    System.out.println("Testing update schema: changing name..");
    UpdateSchema updateSchema = new UpdateSchema().newName(TestUtils.SCHEMA_NEW_NAME);
    SchemaInfo updatedSchemaInfo =
        schemaOperations.updateSchema(TestUtils.SCHEMA_FULL_NAME, updateSchema);
    assertEquals(updateSchema.getNewName(), updatedSchemaInfo.getName());
    assertEquals(updateSchema.getComment(), updatedSchemaInfo.getComment());
    Assert.assertEquals(TestUtils.SCHEMA_NEW_FULL_NAME, updatedSchemaInfo.getFullName());
    assertNotNull(updatedSchemaInfo.getUpdatedAt());

    // Update schema comment without updating name and properties
    System.out.println("Testing update schema: changing comment..");
    UpdateSchema updateSchema2 = new UpdateSchema().comment(TestUtils.SCHEMA_COMMENT);
    SchemaInfo updatedSchemaInfo2 =
        schemaOperations.updateSchema(TestUtils.SCHEMA_NEW_FULL_NAME, updateSchema2);
    assertEquals(TestUtils.SCHEMA_NEW_NAME, updatedSchemaInfo2.getName());
    assertEquals(updateSchema2.getComment(), updatedSchemaInfo2.getComment());
    Assert.assertEquals(TestUtils.SCHEMA_NEW_FULL_NAME, updatedSchemaInfo2.getFullName());
    assertNotNull(updatedSchemaInfo2.getUpdatedAt());

    // Update schema properties without updating name and comment
    System.out.println("Testing update schema: changing properties..");
    UpdateSchema updateSchema3 = new UpdateSchema().properties(TestUtils.NEW_PROPERTIES);
    schemaOperations.updateSchema(TestUtils.SCHEMA_NEW_FULL_NAME, updateSchema3);
    SchemaInfo retrievedSchemaInfo3 = schemaOperations.getSchema(TestUtils.SCHEMA_NEW_FULL_NAME);
    assertEquals(TestUtils.SCHEMA_NEW_NAME, retrievedSchemaInfo3.getName());
    assertEquals(TestUtils.SCHEMA_COMMENT, retrievedSchemaInfo3.getComment());
    Assert.assertEquals(TestUtils.SCHEMA_NEW_FULL_NAME, retrievedSchemaInfo3.getFullName());
    assertNotNull(retrievedSchemaInfo3.getUpdatedAt());

    // Now update the parent catalog name
    UpdateCatalog updateCatalog = new UpdateCatalog().newName(TestUtils.CATALOG_NEW_NAME);
    catalogOperations.updateCatalog(TestUtils.CATALOG_NAME, updateCatalog);
    SchemaInfo updatedSchemaInfo3 =
        schemaOperations.getSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NEW_NAME);
    assertEquals(retrievedSchemaInfo.getSchemaId(), updatedSchemaInfo3.getSchemaId());

    // Delete schema
    System.out.println("Testing delete schema..");
    schemaOperations.deleteSchema(
        TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NEW_NAME, Optional.of(false));
    assertFalse(
        TestUtils.contains(
            schemaOperations.listSchemas(TestUtils.CATALOG_NEW_NAME),
            updatedSchemaInfo,
            (schema) -> schema.getName().equals(TestUtils.SCHEMA_NEW_NAME)));

    // Delete parent entity when schema exists
    SchemaInfo schemaInfo2 =
        schemaOperations.createSchema(
            new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NEW_NAME));
    assertThrows(
        Exception.class,
        () -> catalogOperations.deleteCatalog(TestUtils.CATALOG_NEW_NAME, Optional.of(false)));
    catalogOperations.deleteCatalog(TestUtils.CATALOG_NEW_NAME, Optional.of(true));
    assertThrows(
        Exception.class,
        () -> schemaOperations.getSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NAME));

    // Test force delete of parent entity when schema exists

    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NEW_NAME).comment("Common catalog for schemas"));
    SchemaInfo schemaInfo3 =
        schemaOperations.createSchema(
            new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NEW_NAME));
    catalogOperations.deleteCatalog(TestUtils.CATALOG_NEW_NAME, Optional.of(true));
    assertThrows(
        Exception.class,
        () -> schemaOperations.getSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NAME));
  }
}
