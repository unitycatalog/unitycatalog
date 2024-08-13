package io.unitycatalog.server.base.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseSchemaCRUDTest extends BaseCRUDTest {

  protected SchemaOperations schemaOperations;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig config);

  @BeforeEach
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
            .comment(TestUtils.COMMENT)
            .properties(TestUtils.PROPERTIES);
    assertThatThrownBy(() -> schemaOperations.createSchema(createSchema))
        .isInstanceOf(Exception.class);

    CreateCatalog createCatalog = new CreateCatalog().name(TestUtils.CATALOG_NAME);
    catalogOperations.createCatalog(createCatalog);
    SchemaInfo schemaInfo = schemaOperations.createSchema(createSchema);
    assertThat(schemaInfo.getName()).isEqualTo(createSchema.getName());
    assertThat(schemaInfo.getCatalogName()).isEqualTo(createSchema.getCatalogName());
    assertThat(schemaInfo.getFullName()).isEqualTo(TestUtils.SCHEMA_FULL_NAME);
    assertThat(schemaInfo.getProperties()).isEqualTo(createSchema.getProperties());
    assertThat(schemaInfo.getCreatedAt()).isNotNull();

    // List schemas
    System.out.println("Testing list schemas..");
    Iterable<SchemaInfo> schemaList = schemaOperations.listSchemas(TestUtils.CATALOG_NAME);
    assertThat(schemaList).contains(schemaInfo);

    // Get schema
    System.out.println("Testing get schema..");
    SchemaInfo retrievedSchemaInfo = schemaOperations.getSchema(TestUtils.SCHEMA_FULL_NAME);
    assertThat(retrievedSchemaInfo).isEqualTo(schemaInfo);

    // Calling update schema with nothing to update should not change anything
    System.out.println("Testing updating schema with nothing to update..");
    UpdateSchema emptyUpdateSchema = new UpdateSchema();
    SchemaInfo emptyUpdateSchemaInfo =
        schemaOperations.updateSchema(TestUtils.SCHEMA_FULL_NAME, emptyUpdateSchema);
    SchemaInfo retrievedSchemaInfo2 = schemaOperations.getSchema(TestUtils.SCHEMA_FULL_NAME);
    assertThat(retrievedSchemaInfo2).isEqualTo(schemaInfo);

    // Update schema name without updating comment and properties
    System.out.println("Testing update schema: changing name..");
    UpdateSchema updateSchema = new UpdateSchema().newName(TestUtils.SCHEMA_NEW_NAME);
    SchemaInfo updatedSchemaInfo =
        schemaOperations.updateSchema(TestUtils.SCHEMA_FULL_NAME, updateSchema);
    assertThat(updatedSchemaInfo.getName()).isEqualTo(updateSchema.getNewName());
    assertThat(updatedSchemaInfo.getComment()).isEqualTo(TestUtils.COMMENT);
    assertThat(updatedSchemaInfo.getFullName()).isEqualTo(TestUtils.SCHEMA_NEW_FULL_NAME);
    assertThat(updatedSchemaInfo.getProperties()).isEqualTo(TestUtils.PROPERTIES);
    assertThat(updatedSchemaInfo.getUpdatedAt()).isNotNull();

    // Update schema comment without updating name and properties
    System.out.println("Testing update schema: changing comment..");
    UpdateSchema updateSchema2 = new UpdateSchema().comment(TestUtils.SCHEMA_COMMENT);
    SchemaInfo updatedSchemaInfo2 =
        schemaOperations.updateSchema(TestUtils.SCHEMA_NEW_FULL_NAME, updateSchema2);
    assertThat(updatedSchemaInfo2.getName()).isEqualTo(TestUtils.SCHEMA_NEW_NAME);
    assertThat(updatedSchemaInfo2.getComment()).isEqualTo(updateSchema2.getComment());
    assertThat(updatedSchemaInfo2.getFullName()).isEqualTo(TestUtils.SCHEMA_NEW_FULL_NAME);
    assertThat(updatedSchemaInfo2.getProperties()).isEqualTo(TestUtils.PROPERTIES);
    assertThat(updatedSchemaInfo2.getUpdatedAt()).isNotNull();

    // Update schema properties without updating name and comment
    System.out.println("Testing update schema: changing properties..");
    UpdateSchema updateSchema3 = new UpdateSchema().properties(TestUtils.NEW_PROPERTIES);
    SchemaInfo updatedSchemaInfo3 =
        schemaOperations.updateSchema(TestUtils.SCHEMA_NEW_FULL_NAME, updateSchema3);
    assertThat(updatedSchemaInfo3.getName()).isEqualTo(TestUtils.SCHEMA_NEW_NAME);
    assertThat(updatedSchemaInfo3.getComment()).isEqualTo(TestUtils.SCHEMA_COMMENT);
    assertThat(updatedSchemaInfo3.getFullName()).isEqualTo(TestUtils.SCHEMA_NEW_FULL_NAME);
    assertThat(updatedSchemaInfo3.getProperties()).isEqualTo(updateSchema3.getProperties());
    assertThat(updatedSchemaInfo3.getUpdatedAt()).isNotNull();

    // Now update the parent catalog name
    UpdateCatalog updateCatalog = new UpdateCatalog().newName(TestUtils.CATALOG_NEW_NAME);
    catalogOperations.updateCatalog(TestUtils.CATALOG_NAME, updateCatalog);
    SchemaInfo updatedSchemaInfo4 =
        schemaOperations.getSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NEW_NAME);
    assertThat(updatedSchemaInfo4.getSchemaId()).isEqualTo(retrievedSchemaInfo.getSchemaId());

    // Delete schema
    System.out.println("Testing delete schema..");
    schemaOperations.deleteSchema(
        TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NEW_NAME, Optional.of(false));
    assertThat(schemaOperations.listSchemas(TestUtils.CATALOG_NEW_NAME))
        .as("Schema with schema name '%s' exists", TestUtils.CATALOG_NEW_COMMENT)
        .noneSatisfy(schema -> assertThat(schema.getName()).isEqualTo(TestUtils.SCHEMA_NEW_NAME));

    // Delete parent entity when schema exists
    SchemaInfo schemaInfo2 =
        schemaOperations.createSchema(
            new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NEW_NAME));
    assertThatThrownBy(
            () -> catalogOperations.deleteCatalog(TestUtils.CATALOG_NEW_NAME, Optional.of(false)))
        .isInstanceOf(Exception.class);
    catalogOperations.deleteCatalog(TestUtils.CATALOG_NEW_NAME, Optional.of(true));
    assertThatThrownBy(
            () ->
                schemaOperations.getSchema(
                    TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NAME))
        .isInstanceOf(Exception.class);

    // Test force delete of parent entity when schema exists
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NEW_NAME).comment("Common catalog for schemas"));
    SchemaInfo schemaInfo3 =
        schemaOperations.createSchema(
            new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NEW_NAME));
    catalogOperations.deleteCatalog(TestUtils.CATALOG_NEW_NAME, Optional.of(true));
    assertThatThrownBy(
            () ->
                schemaOperations.getSchema(
                    TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NAME))
        .isInstanceOf(Exception.class);
  }
}
