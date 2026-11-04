package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.client.model.SecurableType.CATALOG;
import static io.unitycatalog.client.model.SecurableType.METASTORE;
import static io.unitycatalog.client.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.client.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.ModelVersionsApi;
import io.unitycatalog.client.api.RegisteredModelsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.model.CreateModelVersion;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.FinalizeModelVersion;
import io.unitycatalog.client.model.ModelVersionInfo;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.RegisteredModelInfo;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdateModelVersion;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.client.model.UpdateRegisteredModel;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.UUID;
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
 *   <li>Model version creation requires model ownership or CREATE MODEL VERSION
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
  public void testModelRenamePermissions() {
    createCommonTestUsers();
    for (boolean useCatalog : new boolean[] {false, true}) {
      for (boolean useSchema : new boolean[] {false, true}) {
        boolean hasUsage = useCatalog && useSchema;
        for (SecurableType createGrantType : new SecurableType[] {null, SCHEMA, CATALOG}) {
          boolean hasCreateModel = createGrantType != null;
          for (Privileges createPrivilege :
              new Privileges[] {Privileges.CREATE_MODEL, Privileges.CREATE_FUNCTION}) {
            assertModelRename(
                REGISTERED_MODEL,
                useCatalog,
                useSchema,
                createGrantType,
                createPrivilege,
                hasUsage,
                hasUsage && hasCreateModel);
            assertModelRename(
                SCHEMA,
                useCatalog,
                useSchema,
                createGrantType,
                createPrivilege,
                useCatalog,
                useCatalog);
            assertModelRename(
                CATALOG,
                useCatalog,
                useSchema,
                createGrantType,
                createPrivilege,
                true,
                useSchema && hasCreateModel);
            assertModelRename(
                METASTORE, useCatalog, useSchema, createGrantType, createPrivilege, false, false);
            assertModelRename(
                null, useCatalog, useSchema, createGrantType, createPrivilege, false, false);
          }
        }
      }
    }
  }

  @SneakyThrows
  private void assertModelRename(
      SecurableType ownerType,
      boolean useCatalog,
      boolean useSchema,
      SecurableType createGrantType,
      Privileges createPrivilege,
      boolean canComment,
      boolean canRename) {
    String catalogName = "cat_rename_" + UUID.randomUUID().toString().replace("-", "");
    String schemaFullName = catalogName + ".sch_rename";
    setupCommonCatalogAndSchema(catalogName, "sch_pr1");
    SchemasApi schemaCreatorApi =
        new SchemasApi(TestUtils.createApiClient(createTestUserServerConfig(PRINCIPAL_2)));
    grantPermissions(
        PRINCIPAL_2,
        SecurableType.CATALOG,
        catalogName,
        Privileges.USE_CATALOG,
        Privileges.CREATE_SCHEMA);
    schemaCreatorApi.createSchema(new CreateSchema().name("sch_rename").catalogName(catalogName));
    RegisteredModelsApi modelOwnerApi =
        new RegisteredModelsApi(TestUtils.createApiClient(createTestUserServerConfig(REGULAR_1)));
    RegisteredModelsApi observerApi = new RegisteredModelsApi(adminApiClient);
    grantPermissions(REGULAR_1, SecurableType.CATALOG, catalogName, Privileges.USE_CATALOG);
    grantPermissions(
        REGULAR_1,
        SecurableType.SCHEMA,
        schemaFullName,
        Privileges.USE_SCHEMA,
        Privileges.CREATE_MODEL);
    RegisteredModelInfo original =
        modelOwnerApi.createRegisteredModel(
            new CreateRegisteredModel()
                .name("model")
                .catalogName(catalogName)
                .schemaName("sch_rename"));
    grantsApi.update(
        SecurableType.CATALOG,
        catalogName,
        new UpdatePermissions()
            .changes(
                List.of(
                    new PermissionsChange()
                        .principal(PRINCIPAL_1)
                        .remove(List.of(Privilege.USE_CATALOG)),
                    new PermissionsChange()
                        .principal(PRINCIPAL_2)
                        .remove(List.of(Privilege.USE_CATALOG, Privilege.CREATE_SCHEMA)),
                    new PermissionsChange()
                        .principal(REGULAR_1)
                        .remove(List.of(Privilege.USE_CATALOG)))));
    grantsApi.update(
        SecurableType.SCHEMA,
        schemaFullName,
        new UpdatePermissions()
            .changes(
                List.of(
                    new PermissionsChange()
                        .principal(REGULAR_1)
                        .remove(List.of(Privilege.USE_SCHEMA, Privilege.CREATE_MODEL)))));
    String fullName = schemaFullName + ".model";
    String principal =
        ownerType == null
            ? REGULAR_2
            : switch (ownerType) {
              case METASTORE -> "admin";
              case CATALOG -> PRINCIPAL_1;
              case SCHEMA -> PRINCIPAL_2;
              default -> REGULAR_1;
            };
    RegisteredModelsApi callerApi =
        ownerType == SecurableType.METASTORE
            ? new RegisteredModelsApi(adminApiClient)
            : new RegisteredModelsApi(
                TestUtils.createApiClient(createTestUserServerConfig(principal)));
    if (useCatalog) {
      grantPermissions(principal, SecurableType.CATALOG, catalogName, Privileges.USE_CATALOG);
    }
    if (useSchema) {
      grantPermissions(principal, SecurableType.SCHEMA, schemaFullName, Privileges.USE_SCHEMA);
    }
    if (createGrantType != null) {
      grantPermissions(
          principal,
          createGrantType,
          createGrantType == SecurableType.CATALOG ? catalogName : schemaFullName,
          createPrivilege);
    }

    UpdateRegisteredModel commentOnly = new UpdateRegisteredModel().comment("comment");
    if (canComment) {
      callerApi.updateRegisteredModel(fullName, commentOnly);
      assertThat(observerApi.getRegisteredModel(fullName).getComment()).isEqualTo("comment");
    } else {
      assertPermissionDenied(() -> callerApi.updateRegisteredModel(fullName, commentOnly));
      assertThat(observerApi.getRegisteredModel(fullName).getComment())
          .isEqualTo(original.getComment());
    }

    UpdateRegisteredModel sameName = new UpdateRegisteredModel().newName("model");
    if (canRename) {
      assertApiException(
          () -> callerApi.updateRegisteredModel(fullName, sameName),
          ErrorCode.ALREADY_EXISTS,
          "Registered model already exists");
    } else {
      assertPermissionDenied(() -> callerApi.updateRegisteredModel(fullName, sameName));
    }

    for (String comment : new String[] {null, "renamed comment"}) {
      UpdateRegisteredModel rename =
          new UpdateRegisteredModel().newName("renamed").comment(comment);
      if (canRename) {
        callerApi.updateRegisteredModel(fullName, rename);
        RegisteredModelInfo renamed = observerApi.getRegisteredModel(schemaFullName + ".renamed");
        assertThat(renamed.getId()).isEqualTo(original.getId());
        assertThat(renamed.getName()).isEqualTo("renamed");
        assertThat(renamed.getComment()).isEqualTo(comment == null ? "comment" : comment);
        callerApi.updateRegisteredModel(
            schemaFullName + ".renamed", new UpdateRegisteredModel().newName("model"));
      } else {
        assertPermissionDenied(() -> callerApi.updateRegisteredModel(fullName, rename));
        RegisteredModelInfo unchanged = observerApi.getRegisteredModel(fullName);
        assertThat(unchanged.getName()).isEqualTo("model");
        assertThat(unchanged.getComment())
            .isEqualTo(canComment ? "comment" : original.getComment());
      }
    }
  }

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

    // TEST: the catalog owner can update a model it does NOT own at the model level, via the owner
    // cascade (matching deleteRegisteredModel). principal-2 owns mod_pr2; principal-1 owns the
    // parent catalog/schema but holds no grant on mod_pr2 itself.
    grantPermissions(PRINCIPAL_2, SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);
    grantPermissions(PRINCIPAL_2, SecurableType.SCHEMA, "cat_pr1.sch_pr1", Privileges.CREATE_MODEL);
    principal2ModelsApi.createRegisteredModel(
        new CreateRegisteredModel().name("mod_pr2").catalogName("cat_pr1").schemaName("sch_pr1"));
    RegisteredModelInfo cascadeUpdated =
        principal1ModelsApi.updateRegisteredModel(
            "cat_pr1.sch_pr1.mod_pr2", new UpdateRegisteredModel().comment("via-catalog-owner"));
    assertThat(cascadeUpdated.getComment()).isEqualTo("via-catalog-owner");
    // Clean up mod_pr2 so the final list assertion (mod_pr1 only) is unaffected.
    principal1ModelsApi.deleteRegisteredModel("cat_pr1.sch_pr1.mod_pr2", false);

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

    // finalizeModelVersion rejects a body whose (full_name, version) differs from the path (a
    // confused-deputy guard: authorization is evaluated on the URL target). Only a direct client
    // can exercise it, since the CLI overwrites the body identity with the path args.
    assertThatThrownBy(
            () ->
                principal1VersionsApi.finalizeModelVersion(
                    "cat_pr1.sch_pr1.mod_pr1",
                    1L,
                    new FinalizeModelVersion().fullName("cat_pr1.sch_pr1.mod_pr1").version(2L)))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("Path and request-body model version must match");

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

    // MODIFY applies to tables, not models. Reject the grant and keep model updates denied.
    grantPermissions(PRINCIPAL_2, SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);
    assertApiException(
        () ->
            grantPermissions(
                PRINCIPAL_2,
                SecurableType.REGISTERED_MODEL,
                "cat_pr1.sch_pr1.mod_pr1",
                Privileges.MODIFY),
        ErrorCode.INVALID_ARGUMENT,
        "Privilege MODIFY cannot be granted on registered_model");
    assertPermissionDenied(
        () ->
            principal2VersionsApi.updateModelVersion(
                "cat_pr1.sch_pr1.mod_pr1", 1L, new UpdateModelVersion().comment("modify-denied")));

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
