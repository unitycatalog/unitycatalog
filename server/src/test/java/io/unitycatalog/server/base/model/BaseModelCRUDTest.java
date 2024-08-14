package io.unitycatalog.server.base.model;

import static io.unitycatalog.server.utils.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseModelCRUDTest extends BaseCRUDTest {
  protected SchemaOperations schemaOperations;
  protected ModelOperations modelOperations;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  protected abstract ModelOperations createModelOperations(ServerConfig serverConfig);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = createSchemaOperations(serverConfig);
    modelOperations = createModelOperations(serverConfig);
  }

  protected void createCommonResources() throws ApiException {
    CreateCatalog createCatalog = new CreateCatalog().name(CATALOG_NAME).comment(COMMENT);
    catalogOperations.createCatalog(createCatalog);
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
  }

  @Test
  public void testModelCRUD() throws ApiException {
    // Model doesn't exist
    assertThatThrownBy(() -> modelOperations.getRegisteredModel(MODEL_FULL_NAME))
        .isInstanceOf(Exception.class);

    // Model creation fails if missing catalog/schema
    CreateRegisteredModel createRm =
        new CreateRegisteredModel()
            .name(MODEL_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .comment(COMMENT);
    assertThatThrownBy(() -> modelOperations.createRegisteredModel(createRm))
        .isInstanceOf(Exception.class);

    // Create a catalog/schema
    createCommonResources();

    // Create a registered model
    System.out.println("Testing create registered model...");
    RegisteredModelInfo rmInfo = modelOperations.createRegisteredModel(createRm);
    assertThat(rmInfo.getName()).isEqualTo(createRm.getName());
    assertThat(rmInfo.getCatalogName()).isEqualTo(createRm.getCatalogName());
    assertThat(rmInfo.getSchemaName()).isEqualTo(createRm.getSchemaName());
    assertThat(rmInfo.getFullName()).isEqualTo(MODEL_FULL_NAME);
    assertThat(rmInfo.getComment()).isEqualTo(createRm.getComment());
    assertThat(rmInfo.getCreatedAt()).isNotNull();
    assertThat(rmInfo.getUpdatedAt()).isNotNull();
    assertThat(rmInfo.getModelId()).isNotNull();
    assertThat(rmInfo.getStorageLocation()).isNotNull();

    // List registered models
    System.out.println("Testing list registered models..");
    Iterable<RegisteredModelInfo> modelList =
        modelOperations.listRegisteredModels(CATALOG_NAME, SCHEMA_NAME);
    assertThat(modelList).contains(rmInfo);

    // Get registered model
    System.out.println("Testing get registered model..");
    RegisteredModelInfo retrievedRmInfo = modelOperations.getRegisteredModel(MODEL_FULL_NAME);
    assertThat(retrievedRmInfo).isEqualTo(rmInfo);

    // Calling update model with nothing to update should throw an exception
    System.out.println("Testing updating registered model with nothing to update..");
    UpdateRegisteredModel emptyUpdateRegisteredModel = new UpdateRegisteredModel();
    assertThatThrownBy(
            () ->
                modelOperations.updateRegisteredModel(MODEL_FULL_NAME, emptyUpdateRegisteredModel))
        .isInstanceOf(Exception.class);
    RegisteredModelInfo retrievedRegisteredModelInfo2 =
        modelOperations.getRegisteredModel(MODEL_FULL_NAME);
    assertThat(retrievedRegisteredModelInfo2).isEqualTo(rmInfo);

    // Update model name without updating comment
    System.out.println("Testing update model: changing name..");
    UpdateRegisteredModel updateRegisteredModel =
        new UpdateRegisteredModel().newName(MODEL_NEW_NAME).fullName(MODEL_FULL_NAME);
    RegisteredModelInfo updatedRegisteredModelInfo =
        modelOperations.updateRegisteredModel(MODEL_FULL_NAME, updateRegisteredModel);
    assertThat(updatedRegisteredModelInfo.getName()).isEqualTo(updateRegisteredModel.getNewName());
    assertThat(updatedRegisteredModelInfo.getComment()).isEqualTo(COMMENT);
    assertThat(updatedRegisteredModelInfo.getFullName()).isEqualTo(MODEL_NEW_FULL_NAME);
    assertThat(updatedRegisteredModelInfo.getUpdatedAt()).isNotNull();
    long firstUpdatedAt = updatedRegisteredModelInfo.getUpdatedAt();
    assertThat(updatedRegisteredModelInfo.getUpdatedAt())
        .isNotEqualTo(updatedRegisteredModelInfo.getCreatedAt());

    // Update model comment without updating name
    System.out.println("Testing update model: changing comment..");
    UpdateRegisteredModel updateModel2 =
        new UpdateRegisteredModel().comment(MODEL_NEW_COMMENT).fullName(MODEL_NEW_FULL_NAME);
    RegisteredModelInfo updatedRegisteredModelInfo2 =
        modelOperations.updateRegisteredModel(MODEL_NEW_FULL_NAME, updateModel2);
    assertThat(updatedRegisteredModelInfo2.getName()).isEqualTo(MODEL_NEW_NAME);
    assertThat(updatedRegisteredModelInfo2.getComment()).isEqualTo(updateModel2.getComment());
    assertThat(updatedRegisteredModelInfo2.getFullName()).isEqualTo(MODEL_NEW_FULL_NAME);
    assertThat(updatedRegisteredModelInfo2.getUpdatedAt()).isNotNull();
    assertThat(updatedRegisteredModelInfo2.getUpdatedAt()).isNotEqualTo(firstUpdatedAt);

    // Now update the parent catalog name
    UpdateCatalog updateCatalog = new UpdateCatalog().newName(TestUtils.CATALOG_NEW_NAME);
    catalogOperations.updateCatalog(TestUtils.CATALOG_NAME, updateCatalog);
    RegisteredModelInfo updatedRegisteredModelInfo3 =
        modelOperations.getRegisteredModel(
            CATALOG_NEW_NAME + "." + SCHEMA_NAME + "." + MODEL_NEW_NAME);
    assertThat(updatedRegisteredModelInfo3.getCatalogName()).isEqualTo(CATALOG_NEW_NAME);

    // Delete registered model
    System.out.println("Testing delete registerd model..");
    modelOperations.deleteRegisteredModel(
        CATALOG_NEW_NAME + "." + SCHEMA_NAME + "." + MODEL_NEW_NAME, Optional.of(false));
    assertThat(modelOperations.listRegisteredModels(CATALOG_NEW_NAME, SCHEMA_NAME))
        .as("Model with model name '%s' exists", MODEL_NEW_NAME)
        .noneSatisfy(modelInfo -> assertThat(modelInfo.getName()).isEqualTo(MODEL_NEW_NAME));

    // TODO: Delete model with model versions exists

    // Test force delete of parent entity when model exists
    CreateRegisteredModel createRm2 =
        new CreateRegisteredModel()
            .name(MODEL_NAME)
            .catalogName(CATALOG_NEW_NAME)
            .schemaName(SCHEMA_NAME)
            .comment(COMMENT);
    modelOperations.createRegisteredModel(createRm2);
    catalogOperations.deleteCatalog(TestUtils.CATALOG_NEW_NAME, Optional.of(true));
    assertThatThrownBy(
            () ->
                schemaOperations.getSchema(
                    TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NAME))
        .isInstanceOf(Exception.class);
  }
}
