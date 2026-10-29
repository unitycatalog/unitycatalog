package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.MODEL_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.MODEL_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.api.ModelVersionsApi;
import io.unitycatalog.client.api.RegisteredModelsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.CreateModelVersion;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.FinalizeModelVersion;
import io.unitycatalog.client.model.GenerateTemporaryModelVersionCredential;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.ModelVersionInfo;
import io.unitycatalog.client.model.ModelVersionOperation;
import io.unitycatalog.client.model.ModelVersionStatus;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdateModelVersion;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.ModelVersionInfoDAO;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/** Tests model publication without transferring model ownership. */
public class SdkModelVersionPublishingTest extends SdkAccessControlBaseCRUDTest {
  private RegisteredModelsApi ownerModels;
  private ModelVersionsApi ownerVersions;
  private ModelVersionsApi publisherVersions;
  private ModelVersionsApi adminVersions;
  private TemporaryCredentialsApi ownerCredentials;
  private TemporaryCredentialsApi publisherCredentials;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(
        Property.MODEL_STORAGE_ROOT.getKey(),
        "s3://test-bucket0" + testDirectoryRoot.resolve("models"));
  }

  @Override
  protected void setUpCredentialOperations(ServerProperties properties) {
    // Capture the authorized scope without accessing cloud storage.
    cloudCredentialVendor = mock(CloudCredentialVendor.class);
    when(cloudCredentialVendor.vendCredential(any()))
        .thenAnswer(invocation -> new io.unitycatalog.server.model.TemporaryCredentials());
  }

  @BeforeEach
  @Override
  @SneakyThrows
  public void setUp() {
    super.setUp();
    createCommonTestUsers();
    ApiClient owner = TestUtils.createApiClient(createTestUserServerConfig(PRINCIPAL_1));
    ApiClient publisher = TestUtils.createApiClient(createTestUserServerConfig(PRINCIPAL_2));
    ownerModels = new RegisteredModelsApi(owner);
    ownerVersions = new ModelVersionsApi(owner);
    publisherVersions = new ModelVersionsApi(publisher);
    adminVersions = new ModelVersionsApi(adminApiClient);
    ownerCredentials = new TemporaryCredentialsApi(owner);
    publisherCredentials = new TemporaryCredentialsApi(publisher);
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
    grantPermissions(
        PRINCIPAL_1,
        SecurableType.SCHEMA,
        SCHEMA_FULL_NAME,
        Privileges.USE_SCHEMA,
        Privileges.CREATE_MODEL);
    ownerModels.createRegisteredModel(
        new CreateRegisteredModel()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name(MODEL_NAME));
  }

  @ParameterizedTest
  @EnumSource(
      value = SecurableType.class,
      names = {"REGISTERED_MODEL", "SCHEMA", "CATALOG"})
  public void publishWithDirectOrInheritedGrant(SecurableType scope) throws Exception {
    grantPermissions(PRINCIPAL_2, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
    grantPermissions(PRINCIPAL_2, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    assertPermissionDenied(() -> publisherVersions.createModelVersion(createVersionRequest()));

    String target =
        switch (scope) {
          case REGISTERED_MODEL -> MODEL_FULL_NAME;
          case SCHEMA -> SCHEMA_FULL_NAME;
          case CATALOG -> CATALOG_NAME;
          default -> throw new IllegalArgumentException("Unexpected grant scope");
        };
    grantPermissions(PRINCIPAL_2, scope, target, Privileges.CREATE_MODEL_VERSION);
    ownerModels.createRegisteredModel(
        new CreateRegisteredModel()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name("future_model"));
    CreateModelVersion futureVersion = createVersionRequest().modelName("future_model");
    if (scope == SecurableType.REGISTERED_MODEL) {
      assertPermissionDenied(() -> publisherVersions.createModelVersion(futureVersion));
    } else {
      assertThat(publisherVersions.createModelVersion(futureVersion).getCreatedBy())
          .isEqualTo(PRINCIPAL_2);
    }
    ModelVersionInfo version = publisherVersions.createModelVersion(createVersionRequest());
    assertThat(version.getCreatedBy()).isEqualTo(PRINCIPAL_2);
    assertThat(version.getStatus()).isEqualTo(ModelVersionStatus.PENDING_REGISTRATION);
    assertThat(adminVersions.getModelVersion(MODEL_FULL_NAME, version.getVersion()))
        .isEqualTo(version);

    // Publication does not supply metadata, inference, or model-management rights.
    assertPermissionDenied(
        () -> publisherVersions.getModelVersion(MODEL_FULL_NAME, version.getVersion()));
    assertPermissionDenied(
        () ->
            publisherVersions.updateModelVersion(
                MODEL_FULL_NAME, version.getVersion(), new UpdateModelVersion().comment("denied")));
    assertPermissionDenied(
        () ->
            publisherCredentials.generateTemporaryModelVersionCredentials(
                credentialRequest(version, ModelVersionOperation.READ_MODEL_VERSION)));
    assertPermissionDenied(
        () ->
            publisherCredentials.generateTemporaryPathCredentials(
                new GenerateTemporaryPathCredential()
                    .url(version.getStorageLocation())
                    .operation(PathOperation.PATH_READ_WRITE)));

    assertThat(
            publisherCredentials
                .generateTemporaryModelVersionCredentials(
                    credentialRequest(version, ModelVersionOperation.READ_WRITE_MODEL_VERSION))
                .getUrl())
        .isEqualTo(version.getStorageLocation());
    ArgumentCaptor<CredentialContext> context = ArgumentCaptor.forClass(CredentialContext.class);
    verify(cloudCredentialVendor).vendCredential(context.capture());
    assertThat(context.getValue().getLocations())
        .containsExactly(NormalizedURL.from(version.getStorageLocation()));
    assertThat(context.getValue().getPrivileges())
        .containsExactlyInAnyOrder(
            CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE);

    FinalizeModelVersion finalize =
        new FinalizeModelVersion().fullName(MODEL_FULL_NAME).version(version.getVersion());
    assertApiException(
        () -> ownerVersions.finalizeModelVersion(MODEL_FULL_NAME, version.getVersion(), finalize),
        ErrorCode.PERMISSION_DENIED,
        "Only model version creator can finalize model version");
    assertApiException(
        () ->
            publisherVersions.finalizeModelVersion(
                MODEL_FULL_NAME,
                version.getVersion(),
                new FinalizeModelVersion()
                    .fullName(MODEL_FULL_NAME + "_other")
                    .version(version.getVersion())),
        ErrorCode.INVALID_ARGUMENT,
        "Path and request-body model version must match");

    revokePermission(PRINCIPAL_2, scope, target, Privilege.CREATE_MODEL_VERSION);
    assertPermissionDenied(() -> publisherVersions.createModelVersion(createVersionRequest()));
    assertPermissionDenied(
        () ->
            publisherCredentials.generateTemporaryModelVersionCredentials(
                credentialRequest(version, ModelVersionOperation.READ_WRITE_MODEL_VERSION)));
    assertPermissionDenied(
        () ->
            publisherVersions.finalizeModelVersion(
                MODEL_FULL_NAME, version.getVersion(), finalize));
    assertThat(adminVersions.getModelVersion(MODEL_FULL_NAME, version.getVersion()))
        .isEqualTo(version);

    grantPermissions(PRINCIPAL_2, scope, target, Privileges.CREATE_MODEL_VERSION);
    ModelVersionInfo ready =
        publisherVersions.finalizeModelVersion(MODEL_FULL_NAME, version.getVersion(), finalize);
    assertThat(ready.getStatus()).isEqualTo(ModelVersionStatus.READY);
    assertThat(ready.getCreatedBy()).isEqualTo(PRINCIPAL_2);
    assertThat(ready.getUpdatedBy()).isEqualTo(PRINCIPAL_2);
    assertThat(ready.getStorageLocation()).isEqualTo(version.getStorageLocation());
    assertThat(adminVersions.getModelVersion(MODEL_FULL_NAME, version.getVersion()))
        .isEqualTo(ready);
    assertApiException(
        () ->
            publisherCredentials.generateTemporaryModelVersionCredentials(
                credentialRequest(ready, ModelVersionOperation.READ_WRITE_MODEL_VERSION)),
        ErrorCode.INVALID_ARGUMENT,
        "model version that has been finalized");
    assertPermissionDenied(
        () ->
            publisherCredentials.generateTemporaryModelVersionCredentials(
                credentialRequest(ready, ModelVersionOperation.READ_MODEL_VERSION)));
    grantPermissions(
        PRINCIPAL_2, SecurableType.REGISTERED_MODEL, MODEL_FULL_NAME, Privileges.EXECUTE);
    clearInvocations(cloudCredentialVendor);
    publisherCredentials.generateTemporaryModelVersionCredentials(
        credentialRequest(ready, ModelVersionOperation.READ_MODEL_VERSION));
    verify(cloudCredentialVendor).vendCredential(context.capture());
    assertThat(context.getValue().getLocations())
        .containsExactly(NormalizedURL.from(ready.getStorageLocation()));
    assertThat(context.getValue().getPrivileges())
        .containsExactly(CredentialContext.Privilege.SELECT);

    ModelVersionInfo pending = ownerVersions.createModelVersion(createVersionRequest());
    assertApiException(
        () ->
            publisherCredentials.generateTemporaryModelVersionCredentials(
                credentialRequest(pending, ModelVersionOperation.READ_MODEL_VERSION)),
        ErrorCode.INVALID_ARGUMENT,
        "model version that is not ready");
    assertThat(
            publisherCredentials
                .generateTemporaryModelVersionCredentials(
                    credentialRequest(pending, ModelVersionOperation.READ_WRITE_MODEL_VERSION))
                .getUrl())
        .isEqualTo(pending.getStorageLocation());
    assertApiException(
        () ->
            publisherVersions.finalizeModelVersion(
                MODEL_FULL_NAME,
                pending.getVersion(),
                new FinalizeModelVersion().fullName(MODEL_FULL_NAME).version(pending.getVersion())),
        ErrorCode.PERMISSION_DENIED,
        "Only model version creator can finalize model version");
    assertThat(adminVersions.getModelVersion(MODEL_FULL_NAME, pending.getVersion()))
        .isEqualTo(pending);
    revokePermission(PRINCIPAL_2, scope, target, Privilege.CREATE_MODEL_VERSION);
    assertPermissionDenied(() -> publisherVersions.createModelVersion(createVersionRequest()));
    assertPermissionDenied(
        () ->
            publisherCredentials.generateTemporaryModelVersionCredentials(
                credentialRequest(pending, ModelVersionOperation.READ_WRITE_MODEL_VERSION)));
    assertPermissionDenied(
        () ->
            publisherVersions.finalizeModelVersion(
                MODEL_FULL_NAME,
                pending.getVersion(),
                new FinalizeModelVersion()
                    .fullName(MODEL_FULL_NAME)
                    .version(pending.getVersion())));
  }

  @Test
  public void authenticatedCallerCannotFinalizeVersionWithoutCreator() throws Exception {
    ModelVersionInfo version = ownerVersions.createModelVersion(createVersionRequest());
    TransactionManager.executeWithTransaction(
        hibernateConfigurator.getSessionFactory(),
        session -> {
          session
              .get(ModelVersionInfoDAO.class, UUID.fromString(version.getId()))
              .setCreatedBy(null);
          return null;
        },
        "Failed to clear test model version creator",
        /* readOnly= */ false);
    ModelVersionInfo anonymous =
        adminVersions.getModelVersion(MODEL_FULL_NAME, version.getVersion());
    assertThat(anonymous.getCreatedBy()).isNull();
    assertThat(anonymous.getStatus()).isEqualTo(ModelVersionStatus.PENDING_REGISTRATION);
    assertApiException(
        () ->
            ownerVersions.finalizeModelVersion(
                MODEL_FULL_NAME,
                version.getVersion(),
                new FinalizeModelVersion().fullName(MODEL_FULL_NAME).version(version.getVersion())),
        ErrorCode.PERMISSION_DENIED,
        "Only model version creator can finalize model version");
    assertThat(adminVersions.getModelVersion(MODEL_FULL_NAME, version.getVersion()))
        .isEqualTo(anonymous);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void publishingRequiresBothUsageGrants(boolean modelOwner) throws Exception {
    grantPermissions(PRINCIPAL_2, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
    grantPermissions(PRINCIPAL_2, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    grantPermissions(
        PRINCIPAL_2,
        SecurableType.REGISTERED_MODEL,
        MODEL_FULL_NAME,
        Privileges.CREATE_MODEL_VERSION);
    String principal = modelOwner ? PRINCIPAL_1 : PRINCIPAL_2;
    ModelVersionsApi versions = modelOwner ? ownerVersions : publisherVersions;
    TemporaryCredentialsApi credentials = modelOwner ? ownerCredentials : publisherCredentials;
    ModelVersionInfo version = versions.createModelVersion(createVersionRequest());
    FinalizeModelVersion finalize =
        new FinalizeModelVersion().fullName(MODEL_FULL_NAME).version(version.getVersion());

    for (Privilege usage : List.of(Privilege.USE_CATALOG, Privilege.USE_SCHEMA)) {
      SecurableType scope =
          usage == Privilege.USE_CATALOG ? SecurableType.CATALOG : SecurableType.SCHEMA;
      String target = usage == Privilege.USE_CATALOG ? CATALOG_NAME : SCHEMA_FULL_NAME;
      revokePermission(principal, scope, target, usage);
      assertPermissionDenied(() -> versions.createModelVersion(createVersionRequest()));
      assertPermissionDenied(
          () ->
              credentials.generateTemporaryModelVersionCredentials(
                  credentialRequest(version, ModelVersionOperation.READ_WRITE_MODEL_VERSION)));
      assertPermissionDenied(
          () -> versions.finalizeModelVersion(MODEL_FULL_NAME, version.getVersion(), finalize));
      assertThat(adminVersions.getModelVersion(MODEL_FULL_NAME, version.getVersion()))
          .isEqualTo(version);
      grantPermissions(principal, scope, target, Privileges.fromValue(usage.getValue()));
    }

    assertThat(
            versions
                .finalizeModelVersion(MODEL_FULL_NAME, version.getVersion(), finalize)
                .getStatus())
        .isEqualTo(ModelVersionStatus.READY);
  }

  @Test
  public void parentOwnersAndAdministratorsNeedPublishingRights() throws Exception {
    setupCommonCatalogAndSchema();
    ApiClient catalogOwner = TestUtils.createApiClient(createTestUserServerConfig(PRINCIPAL_1));
    ApiClient schemaOwner = TestUtils.createApiClient(createTestUserServerConfig(PRINCIPAL_2));
    ApiClient modelOwner = TestUtils.createApiClient(createTestUserServerConfig(REGULAR_1));
    grantPermissions(
        PRINCIPAL_2,
        SecurableType.CATALOG,
        "cat_pr1",
        Privileges.USE_CATALOG,
        Privileges.CREATE_SCHEMA);
    new SchemasApi(schemaOwner)
        .createSchema(new CreateSchema().catalogName("cat_pr1").name("publishing"));
    grantPermissions(REGULAR_1, SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);
    grantPermissions(
        REGULAR_1,
        SecurableType.SCHEMA,
        "cat_pr1.publishing",
        Privileges.USE_SCHEMA,
        Privileges.CREATE_MODEL);
    grantPermissions(
        PRINCIPAL_1, SecurableType.SCHEMA, "cat_pr1.publishing", Privileges.USE_SCHEMA);
    new RegisteredModelsApi(modelOwner)
        .createRegisteredModel(
            new CreateRegisteredModel()
                .catalogName("cat_pr1")
                .schemaName("publishing")
                .name("model"));
    String fullName = "cat_pr1.publishing.model";
    CreateModelVersion create =
        new CreateModelVersion()
            .catalogName("cat_pr1")
            .schemaName("publishing")
            .modelName("model")
            .source("model-source");
    ModelVersionInfo version = new ModelVersionsApi(modelOwner).createModelVersion(create);
    FinalizeModelVersion finalize =
        new FinalizeModelVersion().fullName(fullName).version(version.getVersion());
    for (ApiClient client : List.of(catalogOwner, schemaOwner, adminApiClient)) {
      ModelVersionsApi versions = new ModelVersionsApi(client);
      TemporaryCredentialsApi credentials = new TemporaryCredentialsApi(client);
      assertPermissionDenied(() -> versions.createModelVersion(create));
      assertPermissionDenied(
          () ->
              credentials.generateTemporaryModelVersionCredentials(
                  credentialRequest(version, ModelVersionOperation.READ_WRITE_MODEL_VERSION)));
      assertPermissionDenied(
          () -> versions.finalizeModelVersion(fullName, version.getVersion(), finalize));
    }
    grantPermissions("admin", SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);
    grantPermissions("admin", SecurableType.SCHEMA, "cat_pr1.publishing", Privileges.USE_SCHEMA);
    for (Map.Entry<String, ApiClient> owner :
        Map.of(PRINCIPAL_1, catalogOwner, PRINCIPAL_2, schemaOwner, "admin", adminApiClient)
            .entrySet()) {
      grantPermissions(
          owner.getKey(),
          SecurableType.REGISTERED_MODEL,
          fullName,
          Privileges.CREATE_MODEL_VERSION);
      ModelVersionsApi versions = new ModelVersionsApi(owner.getValue());
      assertApiException(
          () -> versions.finalizeModelVersion(fullName, version.getVersion(), finalize),
          ErrorCode.PERMISSION_DENIED,
          "Only model version creator can finalize model version");
      ModelVersionInfo ownVersion = versions.createModelVersion(create);
      new TemporaryCredentialsApi(owner.getValue())
          .generateTemporaryModelVersionCredentials(
              credentialRequest(ownVersion, ModelVersionOperation.READ_WRITE_MODEL_VERSION));
      assertThat(
              versions
                  .finalizeModelVersion(
                      fullName,
                      ownVersion.getVersion(),
                      new FinalizeModelVersion()
                          .fullName(fullName)
                          .version(ownVersion.getVersion()))
                  .getStatus())
          .isEqualTo(ModelVersionStatus.READY);
    }
    assertThat(adminVersions.getModelVersion(fullName, version.getVersion())).isEqualTo(version);
  }

  @Test
  public void schemaInheritanceStaysWithinTheSchema() throws Exception {
    new SchemasApi(adminApiClient)
        .createSchema(new CreateSchema().catalogName(CATALOG_NAME).name("other_schema"));
    grantPermissions(
        PRINCIPAL_1,
        SecurableType.SCHEMA,
        CATALOG_NAME + ".other_schema",
        Privileges.USE_SCHEMA,
        Privileges.CREATE_MODEL);
    grantPermissions(PRINCIPAL_2, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
    grantPermissions(
        PRINCIPAL_2, SecurableType.SCHEMA, CATALOG_NAME + ".other_schema", Privileges.USE_SCHEMA);
    grantPermissions(
        PRINCIPAL_2, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.CREATE_MODEL_VERSION);
    ownerModels.createRegisteredModel(
        new CreateRegisteredModel()
            .catalogName(CATALOG_NAME)
            .schemaName("other_schema")
            .name(MODEL_NAME));
    CreateModelVersion create = createVersionRequest().schemaName("other_schema");
    assertPermissionDenied(() -> publisherVersions.createModelVersion(create));
    grantPermissions(
        PRINCIPAL_2, SecurableType.CATALOG, CATALOG_NAME, Privileges.CREATE_MODEL_VERSION);
    ModelVersionInfo version = publisherVersions.createModelVersion(create);
    assertThat(version.getCreatedBy()).isEqualTo(PRINCIPAL_2);
    String fullName = CATALOG_NAME + ".other_schema." + MODEL_NAME;
    grantPermissions(
        PRINCIPAL_2, SecurableType.REGISTERED_MODEL, fullName, Privileges.CREATE_MODEL_VERSION);
    revokePermission(
        PRINCIPAL_2, SecurableType.CATALOG, CATALOG_NAME, Privilege.CREATE_MODEL_VERSION);
    publisherCredentials.generateTemporaryModelVersionCredentials(
        credentialRequest(version, ModelVersionOperation.READ_WRITE_MODEL_VERSION));
    assertThat(
            publisherVersions
                .finalizeModelVersion(
                    fullName,
                    version.getVersion(),
                    new FinalizeModelVersion().fullName(fullName).version(version.getVersion()))
                .getStatus())
        .isEqualTo(ModelVersionStatus.READY);
  }

  private CreateModelVersion createVersionRequest() {
    return new CreateModelVersion()
        .catalogName(CATALOG_NAME)
        .schemaName(SCHEMA_NAME)
        .modelName(MODEL_NAME)
        .source("model-source");
  }

  private GenerateTemporaryModelVersionCredential credentialRequest(
      ModelVersionInfo version, ModelVersionOperation operation) {
    return new GenerateTemporaryModelVersionCredential()
        .catalogName(version.getCatalogName())
        .schemaName(version.getSchemaName())
        .modelName(version.getModelName())
        .version(version.getVersion())
        .operation(operation);
  }

  private void revokePermission(
      String principal, SecurableType scope, String target, Privilege privilege) throws Exception {
    grantsApi.update(
        scope,
        target,
        new UpdatePermissions()
            .changes(
                List.of(new PermissionsChange().principal(principal).remove(List.of(privilege)))));
  }
}
