package io.unitycatalog.server.base.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.UpdateCatalog;
import io.unitycatalog.client.model.UpdateSchema;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public abstract class BaseSchemaCRUDTest extends BaseSchemaCRUDTestEnv {

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

    // Create another schema to test pagination
    CreateSchema createSchema2 =
        new CreateSchema()
            .name(TestUtils.COMMON_ENTITY_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .comment(TestUtils.COMMENT)
            .properties(TestUtils.PROPERTIES);
    SchemaInfo schemaInfo2 = schemaOperations.createSchema(createSchema2);
    assertThat(schemaInfo2.getName()).isEqualTo(createSchema2.getName());
    assertThat(schemaInfo2.getCatalogName()).isEqualTo(createSchema2.getCatalogName());
    assertThat(schemaInfo2.getFullName())
        .isEqualTo(TestUtils.CATALOG_NAME + '.' + TestUtils.COMMON_ENTITY_NAME);
    assertThat(schemaInfo2.getProperties()).isEqualTo(createSchema2.getProperties());
    assertThat(schemaInfo2.getCreatedAt()).isNotNull();

    // List schemas
    System.out.println("Testing list schemas..");
    Iterable<SchemaInfo> schemaList =
        schemaOperations.listSchemas(TestUtils.CATALOG_NAME, Optional.empty());
    assertThat(schemaList).contains(schemaInfo);

    // List schemas with page token
    System.out.println("Testing list schemas with page token..");
    schemaList =
        schemaOperations.listSchemas(TestUtils.CATALOG_NAME, Optional.of(TestUtils.SCHEMA_NAME));
    assertThat(schemaList).doesNotContain(schemaInfo);
    assertThat(schemaList).contains(schemaInfo2);

    // Get schema
    System.out.println("Testing get schema..");
    SchemaInfo retrievedSchemaInfo = schemaOperations.getSchema(TestUtils.SCHEMA_FULL_NAME);
    assertThat(retrievedSchemaInfo).isEqualTo(schemaInfo);

    // Calling update schema with nothing to update should not change anything
    System.out.println("Testing updating schema with nothing to update..");
    UpdateSchema emptyUpdateSchema = new UpdateSchema();
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
    assertThat(updatedSchemaInfo.getUpdatedAt()).isNotNull();

    // Update schema comment without updating name and properties
    System.out.println("Testing update schema: changing comment..");
    UpdateSchema updateSchema2 = new UpdateSchema().comment(TestUtils.SCHEMA_NEW_COMMENT);
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
    assertThat(updatedSchemaInfo3.getComment()).isEqualTo(TestUtils.SCHEMA_NEW_COMMENT);
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
    assertThat(schemaOperations.listSchemas(TestUtils.CATALOG_NEW_NAME, Optional.empty()))
        .as("Schema with schema name '%s' exists", TestUtils.CATALOG_NEW_COMMENT)
        .noneSatisfy(schema -> assertThat(schema.getName()).isEqualTo(TestUtils.SCHEMA_NEW_NAME));

    // Delete parent entity when schema exists

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
    schemaOperations.createSchema(
        new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NEW_NAME));
    catalogOperations.deleteCatalog(TestUtils.CATALOG_NEW_NAME, Optional.of(true));
    assertThatThrownBy(
            () ->
                schemaOperations.getSchema(
                    TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NAME))
        .isInstanceOf(Exception.class);
  }

  /** A rename that waits for a concurrent edit of another column keeps that edit. */
  @Test
  public void renameKeepsAConcurrentCommentEdit() throws Exception {
    UUID id = UUID.fromString(createCatalogAndSchema(Optional.empty()).getSchemaId());

    runBlockedByHeldWrite(
        held -> setColumn(held, SchemaInfoDAO.class, id, "comment", "held comment"),
        () ->
            schemaOperations.updateSchema(
                TestUtils.SCHEMA_FULL_NAME, new UpdateSchema().newName("renamed_schema")));

    SchemaInfo renamed = schemaOperations.getSchema(TestUtils.CATALOG_NAME + ".renamed_schema");
    assertThat(renamed.getSchemaId()).isEqualTo(id.toString());
    assertThat(renamed.getComment()).isEqualTo("held comment");
  }

  /** An update that waits for a concurrent rename of the schema misses it and changes nothing. */
  @Test
  public void updateRacingARenameIsNotFound() throws Exception {
    UUID id = UUID.fromString(createCatalogAndSchema(Optional.empty()).getSchemaId());
    String comment = schemaOperations.getSchema(TestUtils.SCHEMA_FULL_NAME).getComment();

    TestUtils.assertApiException(
        () ->
            runBlockedByHeldWrite(
                held -> setColumn(held, SchemaInfoDAO.class, id, "name", "renamed_schema"),
                () ->
                    schemaOperations.updateSchema(
                        TestUtils.SCHEMA_FULL_NAME, new UpdateSchema().comment("lost"))),
        ErrorCode.SCHEMA_NOT_FOUND,
        "Schema not found");

    SchemaInfo renamed = schemaOperations.getSchema(TestUtils.CATALOG_NAME + ".renamed_schema");
    assertThat(renamed.getSchemaId()).isEqualTo(id.toString());
    assertThat(renamed.getComment()).isEqualTo(comment);
  }

  /**
   * A properties update that waits for a concurrent one replaces the properties that one committed,
   * not the ones it saw before the wait (stale under MySQL's REPEATABLE READ).
   */
  @Test
  public void propertiesUpdateReplacesConcurrentlyCommittedProperties() throws Exception {
    UUID id = UUID.fromString(createCatalogAndSchema(Optional.empty()).getSchemaId());
    schemaOperations.updateSchema(
        TestUtils.SCHEMA_FULL_NAME, new UpdateSchema().properties(Map.of("a", "1")));

    runBlockedByHeldWrite(
        held -> {
          // Like an update in flight: it holds the schema row and swaps property "a" for "b".
          setColumn(held, SchemaInfoDAO.class, id, "updatedBy", "held");
          held.createMutationQuery(
                  "delete from PropertyDAO p where p.entityId = :id and p.key = 'a'")
              .setParameter("id", id)
              .executeUpdate();
          PropertyDAO.from(Map.of("b", "2"), id, Constants.SCHEMA).forEach(held::persist);
        },
        () ->
            schemaOperations.updateSchema(
                TestUtils.SCHEMA_FULL_NAME, new UpdateSchema().properties(Map.of("c", "3"))));

    assertThat(schemaOperations.getSchema(TestUtils.SCHEMA_FULL_NAME).getProperties())
        .isEqualTo(Map.of("c", "3"));
  }
}
