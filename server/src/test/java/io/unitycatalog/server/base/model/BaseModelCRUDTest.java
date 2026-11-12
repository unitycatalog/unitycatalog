package io.unitycatalog.server.base.model;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME2;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NEW_NAME;
import static io.unitycatalog.server.utils.TestUtils.COMMENT;
import static io.unitycatalog.server.utils.TestUtils.COMMON_ENTITY_NAME;
import static io.unitycatalog.server.utils.TestUtils.MODEL_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.MODEL_NAME;
import static io.unitycatalog.server.utils.TestUtils.MODEL_NEW_COMMENT;
import static io.unitycatalog.server.utils.TestUtils.MODEL_NEW_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.MODEL_NEW_NAME;
import static io.unitycatalog.server.utils.TestUtils.MV_COMMENT;
import static io.unitycatalog.server.utils.TestUtils.MV_RUNID;
import static io.unitycatalog.server.utils.TestUtils.MV_SOURCE;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME2;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateModelVersion;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.FinalizeModelVersion;
import io.unitycatalog.client.model.ModelVersionInfo;
import io.unitycatalog.client.model.ModelVersionStatus;
import io.unitycatalog.client.model.RegisteredModelInfo;
import io.unitycatalog.client.model.UpdateCatalog;
import io.unitycatalog.client.model.UpdateModelVersion;
import io.unitycatalog.client.model.UpdateRegisteredModel;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.cleanup.StorageCleanupTestSupport;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.ModelVersionInfoDAO;
import io.unitycatalog.server.persist.dao.RegisteredModelInfoDAO;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hibernate.Session;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class BaseModelCRUDTest extends BaseCRUDTest {
  protected SchemaOperations schemaOperations;
  protected ModelOperations modelOperations;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  protected abstract ModelOperations createModelOperations(ServerConfig serverConfig);

  @TempDir Path rootBase;

  @Override
  public void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.MODEL_STORAGE_ROOT.getKey(), rootBase.toString());
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = createSchemaOperations(serverConfig);
    modelOperations = createModelOperations(serverConfig);
  }

  protected void createCommonResources() throws ApiException {
    catalogOperations.createCatalog(new CreateCatalog().name(CATALOG_NAME).comment(COMMENT));
    catalogOperations.createCatalog(new CreateCatalog().name(CATALOG_NAME2).comment(COMMENT));
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME2).catalogName(CATALOG_NAME2));
  }

  protected void assertModel(
      RegisteredModelInfo rmInfo, CreateRegisteredModel createRm, String modelFullName) {
    assertThat(rmInfo.getName()).isEqualTo(createRm.getName());
    assertThat(rmInfo.getCatalogName()).isEqualTo(createRm.getCatalogName());
    assertThat(rmInfo.getSchemaName()).isEqualTo(createRm.getSchemaName());
    assertThat(rmInfo.getFullName()).isEqualTo(modelFullName);
    assertThat(rmInfo.getComment()).isEqualTo(createRm.getComment());
    assertThat(rmInfo.getCreatedAt()).isNotNull();
    assertThat(rmInfo.getUpdatedAt()).isNotNull();
    assertThat(rmInfo.getId()).isNotNull();
    assertThat(rmInfo.getStorageLocation()).isNotNull();
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
    CreateRegisteredModel createRmNewCat =
        new CreateRegisteredModel()
            .name(MODEL_NEW_NAME)
            .catalogName(CATALOG_NAME2)
            .schemaName(SCHEMA_NAME2)
            .comment(COMMENT);
    assertThatThrownBy(() -> modelOperations.createRegisteredModel(createRm))
        .isInstanceOf(Exception.class);

    // Create a catalog/schema
    createCommonResources();

    // Create a registered model
    System.out.println("Testing create registered model...");
    RegisteredModelInfo rmInfo = modelOperations.createRegisteredModel(createRm);
    assertModel(rmInfo, createRm, MODEL_FULL_NAME);

    // Create another registered model to test pagination
    CreateRegisteredModel createRm2 =
        new CreateRegisteredModel()
            .name(COMMON_ENTITY_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .comment(COMMENT);
    RegisteredModelInfo rmInfo2 = modelOperations.createRegisteredModel(createRm2);
    assertModel(rmInfo2, createRm2, CATALOG_NAME + "." + SCHEMA_NAME + "." + COMMON_ENTITY_NAME);

    RegisteredModelInfo rmInfoNewCat = modelOperations.createRegisteredModel(createRmNewCat);
    // List registered models
    System.out.println("Testing list registered models..");
    Iterable<RegisteredModelInfo> modelList =
        modelOperations.listRegisteredModels(
            Optional.of(CATALOG_NAME), Optional.of(SCHEMA_NAME), Optional.empty());
    assertThat(modelList).contains(rmInfo);
    Iterable<RegisteredModelInfo> modelList2 =
        modelOperations.listRegisteredModels(
            Optional.of(CATALOG_NAME2), Optional.of(SCHEMA_NAME2), Optional.empty());
    assertThat(modelList2).contains(rmInfoNewCat);
    Iterable<RegisteredModelInfo> modelList3 =
        modelOperations.listRegisteredModels(Optional.empty(), Optional.empty(), Optional.empty());
    assertThat(modelList3).contains(rmInfo);
    assertThat(modelList3).contains(rmInfoNewCat);

    // List registered models with page token
    System.out.println("Testing list registered models with page token..");
    modelList =
        modelOperations.listRegisteredModels(
            Optional.of(CATALOG_NAME), Optional.of(SCHEMA_NAME), Optional.of(MODEL_NAME));
    assertThat(modelList).doesNotContain(rmInfo);
    assertThat(modelList).contains(rmInfo2);

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
        new UpdateRegisteredModel().newName(MODEL_NEW_NAME);
    RegisteredModelInfo updatedRegisteredModelInfo =
        modelOperations.updateRegisteredModel(MODEL_FULL_NAME, updateRegisteredModel);
    assertThat(updatedRegisteredModelInfo.getId()).isEqualTo(rmInfo.getId());
    assertThat(updatedRegisteredModelInfo.getName()).isEqualTo(updateRegisteredModel.getNewName());
    assertThat(updatedRegisteredModelInfo.getComment()).isEqualTo(COMMENT);
    assertThat(updatedRegisteredModelInfo.getFullName()).isEqualTo(MODEL_NEW_FULL_NAME);
    assertThat(updatedRegisteredModelInfo.getUpdatedAt()).isNotNull();
    long firstUpdatedAt = updatedRegisteredModelInfo.getUpdatedAt();
    assertThat(updatedRegisteredModelInfo.getUpdatedAt())
        .isNotEqualTo(updatedRegisteredModelInfo.getCreatedAt());

    // Update model comment without updating name
    System.out.println("Testing update model: changing comment..");
    UpdateRegisteredModel updateModel2 = new UpdateRegisteredModel().comment(MODEL_NEW_COMMENT);
    RegisteredModelInfo updatedRegisteredModelInfo2 =
        modelOperations.updateRegisteredModel(MODEL_NEW_FULL_NAME, updateModel2);
    assertThat(updatedRegisteredModelInfo2.getId()).isEqualTo(updatedRegisteredModelInfo.getId());
    assertThat(updatedRegisteredModelInfo2.getName()).isEqualTo(MODEL_NEW_NAME);
    assertThat(updatedRegisteredModelInfo2.getComment()).isEqualTo(updateModel2.getComment());
    assertThat(updatedRegisteredModelInfo2.getFullName()).isEqualTo(MODEL_NEW_FULL_NAME);
    assertThat(updatedRegisteredModelInfo2.getUpdatedAt()).isNotNull();
    assertThat(updatedRegisteredModelInfo2.getUpdatedAt()).isNotEqualTo(firstUpdatedAt);

    // Now update the parent catalog name
    UpdateCatalog updateCatalog = new UpdateCatalog().newName(CATALOG_NEW_NAME);
    catalogOperations.updateCatalog(CATALOG_NAME, updateCatalog);
    RegisteredModelInfo updatedRegisteredModelInfo3 =
        modelOperations.getRegisteredModel(
            CATALOG_NEW_NAME + "." + SCHEMA_NAME + "." + MODEL_NEW_NAME);
    assertThat(updatedRegisteredModelInfo3.getCatalogName()).isEqualTo(CATALOG_NEW_NAME);

    // Delete registered model
    System.out.println("Testing delete registered model..");
    modelOperations.deleteRegisteredModel(
        CATALOG_NEW_NAME + "." + SCHEMA_NAME + "." + MODEL_NEW_NAME, Optional.of(false));
    assertThat(
            modelOperations.listRegisteredModels(
                Optional.of(CATALOG_NEW_NAME), Optional.of(SCHEMA_NAME), Optional.empty()))
        .as("Model with model name '%s' exists", MODEL_NEW_NAME)
        .noneSatisfy(modelInfo -> assertThat(modelInfo.getName()).isEqualTo(MODEL_NEW_NAME));

    // Test null force delete
    CreateRegisteredModel createRm1a =
        new CreateRegisteredModel()
            .name(MODEL_NEW_NAME)
            .catalogName(CATALOG_NEW_NAME)
            .schemaName(SCHEMA_NAME)
            .comment(COMMENT);
    modelOperations.createRegisteredModel(createRm1a);
    modelOperations.deleteRegisteredModel(
        CATALOG_NEW_NAME + "." + SCHEMA_NAME + "." + MODEL_NEW_NAME, Optional.empty());
    assertThat(
            modelOperations.listRegisteredModels(
                Optional.of(CATALOG_NEW_NAME), Optional.of(SCHEMA_NAME), Optional.empty()))
        .as("Model with model name '%s' exists", MODEL_NEW_NAME)
        .noneSatisfy(modelInfo -> assertThat(modelInfo.getName()).isEqualTo(MODEL_NEW_NAME));

    // Test force delete of parent entity when model exists
    CreateRegisteredModel createRm3 =
        new CreateRegisteredModel()
            .name(MODEL_NAME)
            .catalogName(CATALOG_NEW_NAME)
            .schemaName(SCHEMA_NAME)
            .comment(COMMENT);
    modelOperations.createRegisteredModel(createRm3);
    catalogOperations.deleteCatalog(CATALOG_NEW_NAME, Optional.of(true));
    catalogOperations.deleteCatalog(CATALOG_NAME2, Optional.of(true));
    assertThatThrownBy(() -> schemaOperations.getSchema(CATALOG_NEW_NAME + "." + SCHEMA_NAME))
        .isInstanceOf(Exception.class);
    // MODEL VERSION TESTS

    // Recreate common assets
    // Create a catalog/schema
    createCommonResources();

    // Test create model version
    // Create a registered model
    System.out.println("Testing create model version...");
    RegisteredModelInfo rmInfo3 = modelOperations.createRegisteredModel(createRm);
    assertModel(rmInfo3, createRm, MODEL_FULL_NAME);

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
    assertThat(mvInfo.getId()).isNotNull();

    // make another and make sure the version increments;
    System.out.println("Testing version increment...");
    ModelVersionInfo mvInfo2 = modelOperations.createModelVersion(createMv);
    assertThat(mvInfo2.getVersion()).isEqualTo(2L);

    // Verify that null source triggers an exception on create
    System.out.println("Testing that null source on create triggers exception...");
    createMv.setSource(null);
    assertThatThrownBy(() -> modelOperations.createModelVersion(createMv))
        .isInstanceOf(Exception.class);
    // replace the source in the createMv
    createMv.setSource(MV_SOURCE);

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
        modelOperations.listModelVersions(MODEL_FULL_NAME, Optional.empty());
    List<ModelVersionInfo> materializedList =
        StreamSupport.stream(modelVersionList.spliterator(), false).collect(Collectors.toList());
    assertThat(materializedList).contains(mvInfo3);
    assertThat(materializedList).contains(mvInfo);
    assertThat(materializedList.size()).isEqualTo(2);

    // Test list model version with page token
    System.out.println("Testing list model versions with page token..");
    modelVersionList = modelOperations.listModelVersions(MODEL_FULL_NAME, Optional.of("1"));
    materializedList =
        StreamSupport.stream(modelVersionList.spliterator(), false).collect(Collectors.toList());
    assertThat(materializedList).contains(mvInfo3);
    assertThat(materializedList).doesNotContain(mvInfo);
    assertThat(materializedList.size()).isEqualTo(1);

    // Update model version comment
    System.out.println("Testing update model version comment...");
    UpdateModelVersion updateModelVersion = new UpdateModelVersion().comment(MODEL_NEW_COMMENT);
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
    assertThat(updatedModelVersionInfo.getStorageLocation())
        .isEqualTo(mvInfo3.getStorageLocation());
    assertThat(updatedModelVersionInfo.getCreatedAt()).isEqualTo(mvInfo3.getCreatedAt());
    assertThat(updatedModelVersionInfo.getUpdatedAt()).isNotEqualTo(mvInfo3.getUpdatedAt());
    assertThat(updatedModelVersionInfo.getId()).isEqualTo(mvInfo3.getId());
    ModelVersionInfo anotherMv3 = modelOperations.getModelVersion(MODEL_FULL_NAME, 3L);
    assertThat(updatedModelVersionInfo).isEqualTo(anotherMv3);

    // Finalize model version 3
    System.out.println("Testing finalize model version comment...");
    FinalizeModelVersion finalizeModelVersion =
        new FinalizeModelVersion().fullName(MODEL_FULL_NAME).version(3L);
    ModelVersionInfo finalizedMv3 =
        modelOperations.finalizeModelVersion(MODEL_FULL_NAME, 3L, finalizeModelVersion);
    assertThat(finalizedMv3.getStatus()).isEqualTo(ModelVersionStatus.READY);
    assertThat(finalizedMv3.getId()).isEqualTo(updatedModelVersionInfo.getId());
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
  }

  /** A drop that waits for a concurrent rename must not delete the renamed model. */
  @Test
  public void dropRacingARenameDoesNotDeleteTheRenamedModel() throws Exception {
    UUID id = UUID.fromString(createModel("drop_race").getId());

    TestUtils.assertApiException(
        () ->
            runBlockedByHeldWrite(
                held ->
                    setColumn(held, RegisteredModelInfoDAO.class, id, "name", "drop_race_renamed"),
                () -> {
                  modelOperations.deleteRegisteredModel(
                      modelFullName("drop_race"), Optional.empty());
                  return null;
                }),
        ErrorCode.NOT_FOUND,
        "Registered model not found");

    assertThat(modelOperations.getRegisteredModel(modelFullName("drop_race_renamed")).getId())
        .isEqualTo(id.toString());
    // A refused drop queues no cleanup of the live model's storage.
    assertThat(StorageCleanupTestSupport.findTask(hibernateConfigurator.getSessionFactory(), id))
        .isNull();
  }

  /** A rename that waits for a concurrent edit of another column keeps that edit. */
  @Test
  public void renameKeepsAConcurrentCommentEdit() throws Exception {
    UUID id = UUID.fromString(createModel("edit_race").getId());

    runBlockedByHeldWrite(
        held -> setColumn(held, RegisteredModelInfoDAO.class, id, "comment", "held comment"),
        () ->
            modelOperations.updateRegisteredModel(
                modelFullName("edit_race"), new UpdateRegisteredModel().newName("edited")));

    RegisteredModelInfo renamed = modelOperations.getRegisteredModel(modelFullName("edited"));
    assertThat(renamed.getId()).isEqualTo(id.toString());
    assertThat(renamed.getComment()).isEqualTo("held comment");
  }

  /**
   * A version created while waiting for a concurrent model write keeps it and is numbered after.
   */
  @Test
  public void createVersionKeepsAConcurrentModelWrite() throws Exception {
    UUID id = UUID.fromString(createModel("create_race").getId());

    ModelVersionInfo version =
        runBlockedByHeldWrite(
            held -> {
              setColumn(held, RegisteredModelInfoDAO.class, id, "comment", "held comment");
              setColumn(held, RegisteredModelInfoDAO.class, id, "maxVersionNumber", 1L);
            },
            () ->
                modelOperations.createModelVersion(
                    new CreateModelVersion()
                        .catalogName(CATALOG_NAME)
                        .schemaName(SCHEMA_NAME)
                        .modelName("create_race")
                        .source(MV_SOURCE)));

    assertThat(version.getVersion()).isEqualTo(2L);
    assertThat(modelOperations.getRegisteredModel(modelFullName("create_race")).getComment())
        .isEqualTo("held comment");
  }

  /** Finalizing a version that a concurrent writer already made READY is rejected. */
  @Test
  public void finalizeRacingAnAlreadyFinalizedVersionIsRejected() throws Exception {
    createModel("finalize_race");
    ModelVersionInfo version =
        modelOperations.createModelVersion(
            new CreateModelVersion()
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .modelName("finalize_race")
                .source(MV_SOURCE));
    UUID id = UUID.fromString(version.getId());
    String fullName = modelFullName("finalize_race");

    TestUtils.assertApiException(
        () ->
            runBlockedByHeldWrite(
                held ->
                    setColumn(
                        held,
                        ModelVersionInfoDAO.class,
                        id,
                        "status",
                        ModelVersionStatus.READY.toString()),
                () ->
                    modelOperations.finalizeModelVersion(
                        fullName,
                        version.getVersion(),
                        new FinalizeModelVersion()
                            .fullName(fullName)
                            .version(version.getVersion()))),
        ErrorCode.INVALID_ARGUMENT,
        "not in a pending registration state");
  }

  private RegisteredModelInfo createModel(String name) throws ApiException {
    catalogOperations.createCatalog(new CreateCatalog().name(CATALOG_NAME).comment(COMMENT));
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
    return modelOperations.createRegisteredModel(
        new CreateRegisteredModel()
            .name(name)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .comment(COMMENT));
  }

  private static String modelFullName(String name) {
    return CATALOG_NAME + "." + SCHEMA_NAME + "." + name;
  }

  /** A comment update that waits for a concurrent finalize keeps the version READY. */
  @Test
  public void updateVersionKeepsAConcurrentFinalize() throws Exception {
    createModel("update_race");
    ModelVersionInfo version =
        modelOperations.createModelVersion(
            new CreateModelVersion()
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .modelName("update_race")
                .source(MV_SOURCE));
    UUID id = UUID.fromString(version.getId());
    String fullName = modelFullName("update_race");

    runBlockedByHeldWrite(
        held ->
            setColumn(
                held, ModelVersionInfoDAO.class, id, "status", ModelVersionStatus.READY.toString()),
        () ->
            modelOperations.updateModelVersion(
                fullName, version.getVersion(), new UpdateModelVersion().comment("updated")));

    ModelVersionInfo updated = modelOperations.getModelVersion(fullName, version.getVersion());
    assertThat(updated.getComment()).isEqualTo("updated");
    assertThat(updated.getStatus()).isEqualTo(ModelVersionStatus.READY);
  }

  /**
   * A version delete that waits for a concurrent delete of the same version finds no row, and
   * queues no cleanup of the version's storage.
   */
  @Test
  public void deleteVersionRacingADeleteIsNotFound() throws Exception {
    createModel("delete_version_race");
    ModelVersionInfo version =
        modelOperations.createModelVersion(
            new CreateModelVersion()
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .modelName("delete_version_race")
                .source(MV_SOURCE));
    UUID id = UUID.fromString(version.getId());

    TestUtils.assertApiException(
        () ->
            runBlockedByHeldWrite(
                held ->
                    held.createMutationQuery("delete from ModelVersionInfoDAO where id = :id")
                        .setParameter("id", id)
                        .executeUpdate(),
                () -> {
                  modelOperations.deleteModelVersion(
                      modelFullName("delete_version_race"), version.getVersion());
                  return null;
                }),
        ErrorCode.NOT_FOUND,
        "Model version not found");

    assertThat(StorageCleanupTestSupport.findTask(hibernateConfigurator.getSessionFactory(), id))
        .isNull();
  }

  /** An update that waits for a concurrent rename of the model misses it and changes nothing. */
  @Test
  public void updateRacingARenameIsNotFound() throws Exception {
    RegisteredModelInfo model = createModel("update_model_race");
    UUID id = UUID.fromString(model.getId());

    TestUtils.assertApiException(
        () ->
            runBlockedByHeldWrite(
                held ->
                    setColumn(
                        held,
                        RegisteredModelInfoDAO.class,
                        id,
                        "name",
                        "update_model_race_renamed"),
                () ->
                    modelOperations.updateRegisteredModel(
                        modelFullName("update_model_race"),
                        new UpdateRegisteredModel().comment("lost"))),
        ErrorCode.NOT_FOUND,
        "Registered model not found");

    RegisteredModelInfo renamed =
        modelOperations.getRegisteredModel(modelFullName("update_model_race_renamed"));
    assertThat(renamed.getId()).isEqualTo(id.toString());
    assertThat(renamed.getComment()).isEqualTo(model.getComment());
  }

  /**
   * A version create that waits for a concurrent rename of its model misses it and adds nothing.
   */
  @Test
  public void createVersionRacingAModelRenameIsNotFound() throws Exception {
    UUID id = UUID.fromString(createModel("create_version_race").getId());

    TestUtils.assertApiException(
        () ->
            runBlockedByHeldWrite(
                held ->
                    setColumn(
                        held,
                        RegisteredModelInfoDAO.class,
                        id,
                        "name",
                        "create_version_race_renamed"),
                () ->
                    modelOperations.createModelVersion(
                        new CreateModelVersion()
                            .catalogName(CATALOG_NAME)
                            .schemaName(SCHEMA_NAME)
                            .modelName("create_version_race")
                            .source(MV_SOURCE))),
        ErrorCode.NOT_FOUND,
        "Registered model not found");

    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      assertThat(
              session
                  .createSelectionQuery(
                      "select count(*) from ModelVersionInfoDAO where registeredModelId = :id",
                      Long.class)
                  .setParameter("id", id)
                  .getSingleResult())
          .isZero();
    }
  }
}
