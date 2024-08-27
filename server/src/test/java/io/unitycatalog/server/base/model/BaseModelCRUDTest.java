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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
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
    // MODEL VERSION TESTS

    // Recreate common assets
    // Create a catalog/schema
    createCommonResources();

    // Test create model version
    // Create a registered model
    System.out.println("Testing create model version...");
    RegisteredModelInfo rmInfo2 = modelOperations.createRegisteredModel(createRm);
    assertThat(rmInfo2.getName()).isEqualTo(createRm.getName());
    assertThat(rmInfo2.getCatalogName()).isEqualTo(createRm.getCatalogName());
    assertThat(rmInfo2.getSchemaName()).isEqualTo(createRm.getSchemaName());
    assertThat(rmInfo2.getFullName()).isEqualTo(MODEL_FULL_NAME);
    assertThat(rmInfo2.getComment()).isEqualTo(createRm2.getComment());
    assertThat(rmInfo2.getCreatedAt()).isNotNull();
    assertThat(rmInfo2.getUpdatedAt()).isNotNull();
    assertThat(rmInfo2.getModelId()).isNotNull();
    assertThat(rmInfo2.getStorageLocation()).isNotNull();
    CreateModelVersion createMv =
        new CreateModelVersion()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .modelName(MODEL_NAME)
            .comment(MV_COMMENT)
            .source(MV_SOURCE)
            .runId(MV_RUNID);
    ModelVersionInfo mvInfo = modelOperations.createModelVersion(createMv);
    assertThat(mvInfo.getCatalogName()).isEqualTo(createMv.getCatalogName());
    assertThat(mvInfo.getSchemaName()).isEqualTo(createMv.getSchemaName());
    assertThat(mvInfo.getModelName()).isEqualTo(createMv.getModelName());
    assertThat(mvInfo.getComment()).isEqualTo(createMv.getComment());
    assertThat(mvInfo.getRunId()).isEqualTo(createMv.getRunId());
    assertThat(mvInfo.getSource()).isEqualTo(createMv.getSource());
    assertThat(mvInfo.getStatus()).isEqualTo(ModelVersionStatus.PENDING_REGISTRATION);
    assertThat(mvInfo.getVersion()).isEqualTo(1L);
    assertThat(mvInfo.getCreatedAt()).isNotNull();
    assertThat(mvInfo.getUpdatedAt()).isNotNull();
    assertThat(mvInfo.getModelVersionId()).isNotNull();
    // make another and make sure the version increments;
    System.out.println("Testing version increment...");
    ModelVersionInfo mvInfo2 = modelOperations.createModelVersion(createMv);
    assertThat(mvInfo2.getVersion()).isEqualTo(2L);

    // Test get a model version
    System.out.println("Testing get model version...");
    ModelVersionInfo mvInfo2Again = modelOperations.getModelVersion(MODEL_FULL_NAME, 2L);
    assertThat(mvInfo2).isEqualTo(mvInfo2Again);

    // Test delete a model version
    System.out.println("Testing delete model version...");
    modelOperations.deleteModelVersion(MODEL_FULL_NAME, 2L);
    assertThatThrownBy(() -> modelOperations.getModelVersion(MODEL_FULL_NAME, 2L))
        .isInstanceOf(Exception.class);

    // Test creation picks up at 3
    System.out.println("Testing version increment on creation after deletion...");
    ModelVersionInfo mvInfo3 = modelOperations.createModelVersion(createMv);
    assertThat(mvInfo3.getVersion()).isEqualTo(3L);

    // Test list model version
    System.out.println("Testing list model versions..");
    Iterable<ModelVersionInfo> modelVersionList =
        modelOperations.listModelVersions(MODEL_FULL_NAME);
    List<ModelVersionInfo> materializedList =
        StreamSupport.stream(modelVersionList.spliterator(), false).collect(Collectors.toList());
    assertThat(materializedList).contains(mvInfo3);
    assertThat(materializedList).contains(mvInfo);
    assertThat(materializedList.size()).isEqualTo(2);

    // Update model version comment
    System.out.println("Testing update model version comment...");
    UpdateModelVersion updateModelVersion =
        new UpdateModelVersion().comment(MODEL_NEW_COMMENT).fullName(MODEL_FULL_NAME).version(3L);
    ModelVersionInfo updatedModelVersionInfo =
        modelOperations.updateModelVersion(MODEL_FULL_NAME, 3L, updateModelVersion);
    assertThat(updatedModelVersionInfo.getCatalogName()).isEqualTo(mvInfo3.getCatalogName());
    assertThat(updatedModelVersionInfo.getSchemaName()).isEqualTo(mvInfo3.getSchemaName());
    assertThat(updatedModelVersionInfo.getModelName()).isEqualTo(mvInfo3.getModelName());
    assertThat(updatedModelVersionInfo.getComment()).isEqualTo(MODEL_NEW_COMMENT);
    assertThat(updatedModelVersionInfo.getRunId()).isEqualTo(mvInfo3.getRunId());
    assertThat(updatedModelVersionInfo.getSource()).isEqualTo(mvInfo3.getSource());
    assertThat(updatedModelVersionInfo.getStatus())
        .isEqualTo(ModelVersionStatus.PENDING_REGISTRATION);
    assertThat(updatedModelVersionInfo.getVersion()).isEqualTo(3L);
    assertThat(updatedModelVersionInfo.getCreatedAt()).isEqualTo(mvInfo3.getCreatedAt());
    assertThat(updatedModelVersionInfo.getUpdatedAt()).isNotEqualTo(mvInfo3.getUpdatedAt());
    assertThat(updatedModelVersionInfo.getModelVersionId()).isEqualTo(mvInfo3.getModelVersionId());
    ModelVersionInfo anotherMv3 = modelOperations.getModelVersion(MODEL_FULL_NAME, 3L);
    assertThat(updatedModelVersionInfo).isEqualTo(anotherMv3);

    // Finalize model version 3
    System.out.println("Testing finalize model version comment...");
    FinalizeModelVersion finalizeModelVersion =
        new FinalizeModelVersion().fullName(MODEL_FULL_NAME).version(3L);
    ModelVersionInfo finalizedMv3 =
        modelOperations.finalizeModelVersion(MODEL_FULL_NAME, 3L, finalizeModelVersion);
    assertThat(finalizedMv3.getStatus()).isEqualTo(ModelVersionStatus.READY);
    assertThatThrownBy(
            () -> modelOperations.finalizeModelVersion(MODEL_FULL_NAME, 3L, finalizeModelVersion))
        .isInstanceOf(Exception.class);

    // Verify registered model deletion fails with versions
    System.out.println("Testing rm deletion with model versions fails...");
    assertThatThrownBy(
            () -> modelOperations.deleteRegisteredModel(MODEL_FULL_NAME, Optional.of(false)))
        .isInstanceOf(Exception.class);
    RegisteredModelInfo shouldStillExistRm = modelOperations.getRegisteredModel(MODEL_FULL_NAME);
    assertThat(shouldStillExistRm).isNotNull();

    // Verify force delete of registered model deletes the versions
    System.out.println("Testing force rm deletion with model versions...");
    modelOperations.deleteRegisteredModel(MODEL_FULL_NAME, Optional.of(true));
    assertThatThrownBy(
            () -> modelOperations.deleteRegisteredModel(MODEL_FULL_NAME, Optional.of(true)))
        .isInstanceOf(Exception.class);
    assertThatThrownBy(() -> modelOperations.getModelVersion(MODEL_FULL_NAME, 3L))
        .isInstanceOf(Exception.class);
    assertThatThrownBy(() -> modelOperations.getRegisteredModel(MODEL_FULL_NAME))
        .isInstanceOf(Exception.class);

    System.out.println("Completed model tests.");
  }
}
