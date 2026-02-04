package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.ModelVersionsApi;
import io.unitycatalog.client.api.RegisteredModelsApi;
import io.unitycatalog.client.model.CreateModelVersion;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.ModelVersionInfo;
import io.unitycatalog.client.model.RegisteredModelInfo;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdateModelVersion;
import io.unitycatalog.client.model.UpdateRegisteredModel;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

/**
 * SDK-based access control tests for Registered Model and Model Version CRUD operations.
 *
 * <p>This test class verifies:
 *
 * <ul>
 *   <li>Model creation requires CREATE MODEL permission on schema
 *   <li>Model get requires ownership
 *   <li>Model list is filtered based on ownership
 *   <li>Model update requires ownership
 *   <li>Model version creation requires ownership of parent model
 *   <li>Model version get requires ownership
 *   <li>Model version list requires ownership
 *   <li>Model version update requires ownership
 *   <li>Model version delete requires ownership
 *   <li>Model delete requires ownership
 * </ul>
 */
public class SdkModelAccessControlCRUDTest extends SdkAccessControlBaseCRUDTest {

  @Test
  @SneakyThrows
  public void testModelAccess() {
    createCommonTestUsers();
    setupCommonCatalogAndSchema();

    // Create API clients for different users
    ServerConfig principal1Config = createTestUserServerConfig(PRINCIPAL_1);
    ServerConfig principal2Config = createTestUserServerConfig(PRINCIPAL_2);

    RegisteredModelsApi principal1ModelsApi =
        new RegisteredModelsApi(TestUtils.createApiClient(principal1Config));
    RegisteredModelsApi principal2ModelsApi =
        new RegisteredModelsApi(TestUtils.createApiClient(principal2Config));
    ModelVersionsApi principal1VersionsApi =
        new ModelVersionsApi(TestUtils.createApiClient(principal1Config));
    ModelVersionsApi principal2VersionsApi =
        new ModelVersionsApi(TestUtils.createApiClient(principal2Config));

    // Grant USE SCHEMA and CREATE MODEL to principal-1
    grantPermissions(PRINCIPAL_1, SecurableType.SCHEMA, "cat_pr1.sch_pr1", Privileges.USE_SCHEMA);
    grantPermissions(PRINCIPAL_1, SecurableType.SCHEMA, "cat_pr1.sch_pr1", Privileges.CREATE_MODEL);

    // Grant USE SCHEMA to principal-2
    grantPermissions(PRINCIPAL_2, SecurableType.SCHEMA, "cat_pr1.sch_pr1", Privileges.USE_SCHEMA);

    // TEST: Create registered model as principal-1 - should succeed
    CreateRegisteredModel createModel =
        new CreateRegisteredModel().name("mod_pr1").catalogName("cat_pr1").schemaName("sch_pr1");
    RegisteredModelInfo modelInfo = principal1ModelsApi.createRegisteredModel(createModel);
    assertThat(modelInfo).isNotNull();
    assertThat(modelInfo.getName()).isEqualTo("mod_pr1");

    // TEST: Get registered model as principal-1 (owner) - should succeed
    RegisteredModelInfo getModelInfo =
        principal1ModelsApi.getRegisteredModel("cat_pr1.sch_pr1.mod_pr1");
    assertThat(getModelInfo).isNotNull();

    // TEST: Get registered model as principal-2 (not owner) - should fail
    assertPermissionDenied(() -> principal2ModelsApi.getRegisteredModel("cat_pr1.sch_pr1.mod_pr1"));

    // TEST: List registered models as principal-1 - should see owned model
    List<RegisteredModelInfo> principal1Models =
        listAllRegisteredModels(principal1ModelsApi, "cat_pr1", "sch_pr1");
    assertThat(principal1Models).hasSize(1);

    // TEST: List registered models as principal-2 - should see empty list
    List<RegisteredModelInfo> principal2Models =
        listAllRegisteredModels(principal2ModelsApi, "cat_pr1", "sch_pr1");
    assertThat(principal2Models).isEmpty();

    // TEST: Update registered model as principal-1 (owner) - should succeed
    UpdateRegisteredModel updateModel = new UpdateRegisteredModel().comment("hello");
    RegisteredModelInfo updatedModel =
        principal1ModelsApi.updateRegisteredModel("cat_pr1.sch_pr1.mod_pr1", updateModel);
    assertThat(updatedModel.getComment()).isEqualTo("hello");

    // TEST: Update registered model as principal-2 (not owner) - should fail
    UpdateRegisteredModel updateModel2 = new UpdateRegisteredModel().comment("hello2");
    assertPermissionDenied(
        () -> principal2ModelsApi.updateRegisteredModel("cat_pr1.sch_pr1.mod_pr1", updateModel2));

    // TEST: Create model version as principal-1 (owner) - should succeed
    CreateModelVersion createVersion =
        new CreateModelVersion()
            .catalogName("cat_pr1")
            .schemaName("sch_pr1")
            .modelName("mod_pr1")
            .source("model_source");
    ModelVersionInfo versionInfo = principal1VersionsApi.createModelVersion(createVersion);
    assertThat(versionInfo).isNotNull();
    assertThat(versionInfo.getVersion()).isEqualTo(1L);

    // TEST: Create model version as principal-2 (not owner) - should fail
    CreateModelVersion createVersion2 =
        new CreateModelVersion()
            .catalogName("cat_pr1")
            .schemaName("sch_pr1")
            .modelName("mod_pr1")
            .source("model_source");
    assertPermissionDenied(() -> principal2VersionsApi.createModelVersion(createVersion2));

    // TEST: Get model version as principal-1 (owner) - should succeed
    ModelVersionInfo getVersionInfo =
        principal1VersionsApi.getModelVersion("cat_pr1.sch_pr1.mod_pr1", 1L);
    assertThat(getVersionInfo).isNotNull();

    // TEST: Get model version as principal-2 (not owner) - should fail
    assertPermissionDenied(
        () -> principal2VersionsApi.getModelVersion("cat_pr1.sch_pr1.mod_pr1", 1L));

    // TEST: List model versions as principal-1 - should see all versions
    List<ModelVersionInfo> principal1Versions =
        listAllModelVersions(principal1VersionsApi, "cat_pr1.sch_pr1.mod_pr1");
    assertThat(principal1Versions).hasSize(1);

    // TEST: List model versions as principal-2 - should fail
    assertPermissionDenied(
        () -> listAllModelVersions(principal2VersionsApi, "cat_pr1.sch_pr1.mod_pr1"));

    // TEST: Update model version as principal-1 (owner) - should succeed
    UpdateModelVersion updateVersion = new UpdateModelVersion().comment("hello");
    ModelVersionInfo updatedVersion =
        principal1VersionsApi.updateModelVersion("cat_pr1.sch_pr1.mod_pr1", 1L, updateVersion);
    assertThat(updatedVersion.getComment()).isEqualTo("hello");

    // TEST: Update model version as principal-2 (not owner) - should fail
    UpdateModelVersion updateVersion2 = new UpdateModelVersion().comment("hello2");
    assertPermissionDenied(
        () ->
            principal2VersionsApi.updateModelVersion(
                "cat_pr1.sch_pr1.mod_pr1", 1L, updateVersion2));

    // TEST: Delete model version as principal-2 (not owner) - should fail
    assertPermissionDenied(
        () -> principal2VersionsApi.deleteModelVersion("cat_pr1.sch_pr1.mod_pr1", 1L));

    // TEST: Delete model version as principal-1 (owner) - should succeed
    principal1VersionsApi.deleteModelVersion("cat_pr1.sch_pr1.mod_pr1", 1L);

    // Verify deletion
    List<ModelVersionInfo> versionsAfterDelete =
        listAllModelVersions(principal1VersionsApi, "cat_pr1.sch_pr1.mod_pr1");
    assertThat(versionsAfterDelete).isEmpty();

    // TEST: Delete registered model as principal-2 (not owner) - should fail
    assertPermissionDenied(
        () -> principal2ModelsApi.deleteRegisteredModel("cat_pr1.sch_pr1.mod_pr1", false));

    // TEST: Delete registered model as principal-1 (owner) - should succeed
    principal1ModelsApi.deleteRegisteredModel("cat_pr1.sch_pr1.mod_pr1", false);

    // Verify deletion
    List<RegisteredModelInfo> modelsAfterDelete =
        listAllRegisteredModels(principal1ModelsApi, "cat_pr1", "sch_pr1");
    assertThat(modelsAfterDelete).isEmpty();
  }
}
