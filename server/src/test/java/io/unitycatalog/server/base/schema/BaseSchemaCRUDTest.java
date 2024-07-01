package io.unitycatalog.server.base.schema;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.utils.TestUtils;
import org.junit.*;

import static org.junit.Assert.*;

public abstract class BaseSchemaCRUDTest extends BaseCRUDTest {

    protected SchemaOperations schemaOperations;
    @Before
    public void setUp() {
        super.setUp();
        schemaOperations = createSchemaOperations(serverConfig);
        cleanUp();
    }

    protected abstract SchemaOperations createSchemaOperations(ServerConfig config);
    protected void cleanUp() {
        try {
            if (schemaOperations.getSchema(TestUtils.SCHEMA_FULL_NAME) != null) {
                schemaOperations.deleteSchema(TestUtils.SCHEMA_FULL_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (schemaOperations.getSchema(TestUtils.SCHEMA_NEW_FULL_NAME) != null) {
                schemaOperations.deleteSchema(TestUtils.SCHEMA_NEW_FULL_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (schemaOperations.getSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NEW_NAME) != null) {
                schemaOperations.deleteSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NEW_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (schemaOperations.getSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NAME) != null) {
                schemaOperations.deleteSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NAME);
            }
        } catch (Exception e) {
                // Ignore
        }
        super.cleanUp();
    }

    @Test
    public void testSchemaCRUDL() throws ApiException {
        // Create a schema
        System.out.println("Testing create schema..");
        CreateSchema createSchema = new CreateSchema()
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

        // Update schema
        System.out.println("Testing update schema..");
        UpdateSchema updateSchema = new UpdateSchema()
                .newName(TestUtils.SCHEMA_NEW_NAME)
                .comment(TestUtils.SCHEMA_COMMENT);

        // Set update details
        SchemaInfo updatedSchemaInfo = schemaOperations.updateSchema(TestUtils.SCHEMA_FULL_NAME, updateSchema);
        assertEquals(updateSchema.getNewName(), updatedSchemaInfo.getName());
        assertEquals(updateSchema.getComment(), updatedSchemaInfo.getComment());
        Assert.assertEquals(TestUtils.SCHEMA_NEW_FULL_NAME, updatedSchemaInfo.getFullName());
        assertNotNull(updatedSchemaInfo.getUpdatedAt());

        //Now update the parent catalog name
        UpdateCatalog updateCatalog = new UpdateCatalog().newName(TestUtils.CATALOG_NEW_NAME);
        catalogOperations.updateCatalog(TestUtils.CATALOG_NAME, updateCatalog);
        SchemaInfo updatedSchemaInfo2 = schemaOperations.getSchema(TestUtils.CATALOG_NEW_NAME
                + "." + TestUtils.SCHEMA_NEW_NAME);
        assertEquals(retrievedSchemaInfo.getSchemaId(), updatedSchemaInfo2.getSchemaId());

        // Delete schema
        System.out.println("Testing delete schema..");
        schemaOperations.deleteSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NEW_NAME);
        assertFalse(TestUtils.contains(schemaOperations.listSchemas(TestUtils.CATALOG_NEW_NAME), updatedSchemaInfo, (schema) ->
                schema.getName().equals(TestUtils.SCHEMA_NEW_NAME)));
    }
}
