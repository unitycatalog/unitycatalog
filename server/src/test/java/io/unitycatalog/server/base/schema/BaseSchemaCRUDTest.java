package io.unitycatalog.server.base.schema;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.utils.TestUtils;
import org.junit.*;

import java.util.List;
import java.util.Optional;

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
        super.cleanUp();
    }

    protected void createCommonResources() throws ApiException {
        // Common setup operations such as creating a catalog
        catalogOperations.createCatalog(TestUtils.CATALOG_NAME, "Common catalog for schemas");
    }

    @Test
    public void testSchemaCRUDL() throws ApiException {
        // Create a schema
        System.out.println("Testing create schema..");
        CreateSchema createSchema = new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME);
        assertThrows(Exception.class, () -> schemaOperations.createSchema(createSchema));

        CreateCatalog createCatalog = new CreateCatalog().name(TestUtils.CATALOG_NAME);
        catalogOperations.createCatalog(createCatalog.getName(), null);
        SchemaInfo schemaInfo = schemaOperations.createSchema(createSchema);
        assertEquals(createSchema.getName(), schemaInfo.getName());
        assertEquals(createSchema.getCatalogName(), schemaInfo.getCatalogName());
        Assert.assertEquals(TestUtils.SCHEMA_FULL_NAME, schemaInfo.getFullName());
        assertNotNull(schemaInfo.getCreatedAt());

        // List schemas
        System.out.println("Testing list schemas..");
        Iterable<SchemaInfo> schemaList = schemaOperations.listSchemas(TestUtils.CATALOG_NAME);
        assertTrue(TestUtils.contains(schemaList, schemaInfo, (schema) -> schema.getName().equals(TestUtils.SCHEMA_NAME)));

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
        catalogOperations.updateCatalog(TestUtils.CATALOG_NAME, TestUtils.CATALOG_NEW_NAME, "");
        SchemaInfo updatedSchemaInfo2 = schemaOperations.getSchema(TestUtils.CATALOG_NEW_NAME
                + "." + TestUtils.SCHEMA_NEW_NAME);
        assertEquals(retrievedSchemaInfo.getSchemaId(), updatedSchemaInfo2.getSchemaId());

        // Delete schema
        System.out.println("Testing delete schema..");
        schemaOperations.deleteSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NEW_NAME, Optional.of(false));
        assertFalse(TestUtils.contains(schemaOperations.listSchemas(TestUtils.CATALOG_NEW_NAME), updatedSchemaInfo, (schema) ->
                schema.getName().equals(TestUtils.SCHEMA_NEW_NAME)));

        // Delete parent entity when schema exists
        SchemaInfo schemaInfo2 = schemaOperations.createSchema(new CreateSchema().name(TestUtils.SCHEMA_NAME)
                .catalogName(TestUtils.CATALOG_NEW_NAME));
        assertThrows(Exception.class , () ->
                catalogOperations.deleteCatalog(TestUtils.CATALOG_NEW_NAME, Optional.of(false)));
        catalogOperations.deleteCatalog(TestUtils.CATALOG_NEW_NAME, Optional.of(true));
        assertThrows(Exception.class , () ->
                schemaOperations.getSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NAME));

        // Test force delete of parent entity when schema exists
        catalogOperations.createCatalog(TestUtils.CATALOG_NEW_NAME, "Common catalog for schemas");
        SchemaInfo schemaInfo3 = schemaOperations.createSchema(new CreateSchema().name(TestUtils.SCHEMA_NAME)
                .catalogName(TestUtils.CATALOG_NEW_NAME));
        catalogOperations.deleteCatalog(TestUtils.CATALOG_NEW_NAME, Optional.of(true));
        assertThrows(Exception.class , () ->
                schemaOperations.getSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NAME));

    }
}
