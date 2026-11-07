package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.TABLE_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.TABLE_NAME;
import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static io.unitycatalog.server.utils.TestUtils.assertIcebergApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.MediaType;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.FunctionsApi;
import io.unitycatalog.client.api.GrantsApi;
import io.unitycatalog.client.api.ModelVersionsApi;
import io.unitycatalog.client.api.RegisteredModelsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CreateFunction;
import io.unitycatalog.client.model.CreateFunctionRequest;
import io.unitycatalog.client.model.CreateModelVersion;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.Dependency;
import io.unitycatalog.client.model.DependencyList;
import io.unitycatalog.client.model.FinalizeModelVersion;
import io.unitycatalog.client.model.FunctionParameterInfos;
import io.unitycatalog.client.model.GenerateTemporaryModelVersionCredential;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.GenerateTemporaryVolumeCredential;
import io.unitycatalog.client.model.ModelVersionOperation;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.PermissionsList;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.TableDependency;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.UpdateCatalog;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.client.model.UpdateExternalLocation;
import io.unitycatalog.client.model.UpdateFunction;
import io.unitycatalog.client.model.UpdateModelVersion;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.client.model.UpdateRegisteredModel;
import io.unitycatalog.client.model.UpdateSchema;
import io.unitycatalog.client.model.UpdateTable;
import io.unitycatalog.client.model.UpdateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeOperation;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.types.Types;
import org.hibernate.Session;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SdkManageAccessControlTest extends SdkAccessControlBaseCRUDTest {
  private static final String FUNCTION = SCHEMA_FULL_NAME + ".manage_function";
  private static final String VOLUME = SCHEMA_FULL_NAME + ".manage_volume";
  private static final String MODEL = SCHEMA_FULL_NAME + ".manage_model";
  private static final String CREDENTIAL = "manage_credential";
  private static final String LOCATION = "manage_location";
  private static final Map<String, String> CREDENTIAL_CONFIG =
      Map.of(S3FileIOProperties.ACCESS_KEY_ID, "test-key");

  private ApiClient manager;
  private GrantsApi managerGrants;
  private String managerId;
  private String recipientId;
  private List<Target> targets;

  private record Target(SecurableType type, String name, String id, Privilege delegatedPrivilege) {}

  @Override
  protected FileOperations decorateFileOperations(FileOperations operations) {
    FileOperations result = spy(operations);
    doReturn(CREDENTIAL_CONFIG).when(result).getFileIOConfig(any(), any(), any());
    return result;
  }

  @BeforeEach
  @Override
  @SneakyThrows
  public void setUp() {
    super.setUp();
    managerId = createTestUser(PRINCIPAL_1).getId();
    recipientId = createTestUser(PRINCIPAL_2).getId();
    createTestUser(REGULAR_1);
    createTestUser(REGULAR_2);
    manager = TestUtils.createApiClient(createTestUserServerConfig(PRINCIPAL_1));
    managerGrants = new GrantsApi(manager);
    TableInfo table =
        createExternalTable(
            new TablesApi(adminApiClient),
            CATALOG_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            testDirectoryRoot.resolve("table").toString());
    String functionId =
        new FunctionsApi(adminApiClient)
            .createFunction(newFunction("manage_function"))
            .getFunctionId();
    String volumeId =
        new VolumesApi(adminApiClient)
            .createVolume(
                new CreateVolumeRequestContent()
                    .name("manage_volume")
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .volumeType(VolumeType.EXTERNAL)
                    .storageLocation(testDirectoryRoot.resolve("volume").toUri().toString()))
            .getVolumeId();
    String modelId =
        new RegisteredModelsApi(adminApiClient)
            .createRegisteredModel(
                new CreateRegisteredModel()
                    .name("manage_model")
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME))
            .getId();
    createExternalLocationWithCredential(
        CREDENTIAL, LOCATION, testDirectoryRoot.resolve("location").toUri().toString());
    targets =
        List.of(
            new Target(
                SecurableType.CATALOG,
                CATALOG_NAME,
                new CatalogsApi(adminApiClient).getCatalog(CATALOG_NAME).getId(),
                Privilege.USE_CATALOG),
            new Target(
                SecurableType.SCHEMA,
                SCHEMA_FULL_NAME,
                new SchemasApi(adminApiClient).getSchema(SCHEMA_FULL_NAME).getSchemaId(),
                Privilege.USE_SCHEMA),
            new Target(SecurableType.TABLE, TABLE_FULL_NAME, table.getTableId(), Privilege.SELECT),
            new Target(SecurableType.FUNCTION, FUNCTION, functionId, Privilege.EXECUTE),
            new Target(SecurableType.VOLUME, VOLUME, volumeId, Privilege.READ_VOLUME),
            new Target(SecurableType.REGISTERED_MODEL, MODEL, modelId, Privilege.EXECUTE),
            new Target(
                SecurableType.EXTERNAL_LOCATION,
                LOCATION,
                new ExternalLocationsApi(adminApiClient).getExternalLocation(LOCATION).getId(),
                Privilege.READ_FILES),
            new Target(
                SecurableType.CREDENTIAL,
                CREDENTIAL,
                new CredentialsApi(adminApiClient).getCredential(CREDENTIAL).getId(),
                Privilege.CREATE_EXTERNAL_LOCATION));
  }

  @Test
  @SneakyThrows
  void directManageRequiresUsageAboveItsScope() {
    assertApiException(
        () ->
            grantPermissions(
                PRINCIPAL_1, SecurableType.METASTORE, METASTORE_NAME, Privileges.MANAGE),
        ErrorCode.INVALID_ARGUMENT,
        "Privilege MANAGE cannot be granted on metastore");
    for (Target target : targets) {
      assertPermissionDenied(() -> readMetadata(target.type(), target.name()));
      grantPermissions(
          REGULAR_1,
          target.type(),
          target.name(),
          Privileges.fromValue(target.delegatedPrivilege().getValue()));
      grantPermissions(PRINCIPAL_1, target.type(), target.name(), Privileges.MANAGE);
      try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
        assertThat(
                session
                    .createNativeQuery(
                        "select v2 from casbin_rule where ptype = 'p' and v0 = :user and v1 = :id",
                        String.class)
                    .setParameter("user", managerId)
                    .setParameter("id", target.id())
                    .getResultList())
            .containsExactly("MANAGE");
      }
      boolean hierarchical =
          !List.of(SecurableType.CATALOG, SecurableType.EXTERNAL_LOCATION, SecurableType.CREDENTIAL)
              .contains(target.type());
      if (hierarchical) {
        assertPermissionDenied(() -> readMetadata(target.type(), target.name()));
        assertThat(privilegesFor(managerGrants.get(target.type(), target.name(), null), REGULAR_1))
            .isEmpty();
        assertPermissionDenied(
            () ->
                managerGrants.update(
                    target.type(),
                    target.name(),
                    change(REGULAR_2, List.of(target.delegatedPrivilege()), List.of())));
        grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
        if (target.type() != SecurableType.SCHEMA) {
          assertPermissionDenied(() -> readMetadata(target.type(), target.name()));
          grantPermissions(
              PRINCIPAL_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
        }
      }
      readMetadata(target.type(), target.name());
      if (target.type() == SecurableType.EXTERNAL_LOCATION) {
        assertThat(
                new ExternalLocationsApi(manager)
                    .listExternalLocations(null, null)
                    .getExternalLocations())
            .extracting(location -> location.getName())
            .containsExactly(LOCATION);
      }
      if (target.type() == SecurableType.CREDENTIAL) {
        assertThat(new CredentialsApi(manager).listCredentials(null, null, null).getCredentials())
            .extracting(credential -> credential.getName())
            .containsExactly(CREDENTIAL);
      }
      PermissionsList all = managerGrants.get(target.type(), target.name(), null);
      assertThat(privilegesFor(all, PRINCIPAL_1)).containsExactly(Privilege.MANAGE);
      assertThat(privilegesFor(all, REGULAR_1)).containsExactly(target.delegatedPrivilege());
      managerGrants.update(
          target.type(),
          target.name(),
          change(REGULAR_2, List.of(target.delegatedPrivilege()), List.of()));
      managerGrants.update(
          target.type(),
          target.name(),
          change(REGULAR_1, List.of(), List.of(target.delegatedPrivilege())));
      assertThat(privilegesFor(grantsApi.get(target.type(), target.name(), null), REGULAR_2))
          .containsExactly(target.delegatedPrivilege());
      assertThat(privilegesFor(grantsApi.get(target.type(), target.name(), null), REGULAR_1))
          .isEmpty();
      managerGrants.update(
          target.type(), target.name(), change(PRINCIPAL_1, List.of(), List.of(Privilege.MANAGE)));
      assertPermissionDenied(
          () ->
              managerGrants.update(
                  target.type(),
                  target.name(),
                  change(REGULAR_2, List.of(), List.of(target.delegatedPrivilege()))));
      grantsApi.update(
          SecurableType.CATALOG,
          CATALOG_NAME,
          change(PRINCIPAL_1, List.of(), List.of(Privilege.USE_CATALOG)));
      grantsApi.update(
          SecurableType.SCHEMA,
          SCHEMA_FULL_NAME,
          change(PRINCIPAL_1, List.of(), List.of(Privilege.USE_SCHEMA)));
      assertPermissionDenied(() -> readMetadata(target.type(), target.name()));
    }
  }

  @Test
  @SneakyThrows
  void inheritedManageNeedsUsageOnlyAboveTheGrant() {
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.MANAGE);
    for (Target target : targets.subList(0, 6)) {
      readMetadata(target.type(), target.name());
      managerGrants.update(
          target.type(),
          target.name(),
          change(REGULAR_1, List.of(target.delegatedPrivilege()), List.of()));
      assertThat(privilegesFor(managerGrants.get(target.type(), target.name(), null), REGULAR_1))
          .containsExactly(target.delegatedPrivilege());
    }
    assertThat(new CatalogsApi(manager).listCatalogs(null, null).getCatalogs())
        .extracting(catalog -> catalog.getName())
        .containsExactly(CATALOG_NAME);
    assertThat(new SchemasApi(manager).listSchemas(CATALOG_NAME, null, null).getSchemas())
        .extracting(schema -> schema.getName())
        .containsExactly(SCHEMA_NAME);
    assertThat(listAllFunctions(new FunctionsApi(manager), CATALOG_NAME, SCHEMA_NAME))
        .extracting(function -> function.getName())
        .containsExactly("manage_function");
    assertThat(listAllVolumes(new VolumesApi(manager), CATALOG_NAME, SCHEMA_NAME))
        .extracting(volume -> volume.getName())
        .containsExactly("manage_volume");
    assertThat(listAllRegisteredModels(new RegisteredModelsApi(manager), CATALOG_NAME, SCHEMA_NAME))
        .extracting(model -> model.getName())
        .containsExactly("manage_model");
    createExternalTable(
        new TablesApi(adminApiClient),
        CATALOG_NAME,
        SCHEMA_NAME,
        "future_table",
        testDirectoryRoot.resolve("future").toString());
    assertThat(getTable(new TablesApi(manager), SCHEMA_FULL_NAME + ".future_table").getName())
        .isEqualTo("future_table");
    grantPermissions(PRINCIPAL_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.MANAGE);
    grantPermissions(PRINCIPAL_1, SecurableType.TABLE, TABLE_FULL_NAME, Privileges.MANAGE);
    grantsApi.update(
        SecurableType.CATALOG,
        CATALOG_NAME,
        change(PRINCIPAL_1, List.of(), List.of(Privilege.MANAGE)));
    for (Target target : targets.subList(1, 6)) {
      assertPermissionDenied(() -> readMetadata(target.type(), target.name()));
    }
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
    for (Target target : targets.subList(1, 6)) {
      readMetadata(target.type(), target.name());
    }
    assertThat(listAllTables(new TablesApi(manager), CATALOG_NAME, SCHEMA_NAME))
        .extracting(TableInfo::getName)
        .containsExactlyInAnyOrder(TABLE_NAME, "future_table");
    grantsApi.update(
        SecurableType.SCHEMA,
        SCHEMA_FULL_NAME,
        change(PRINCIPAL_1, List.of(), List.of(Privilege.MANAGE)));
    assertPermissionDenied(() -> getTable(new TablesApi(manager), TABLE_FULL_NAME));
    grantPermissions(PRINCIPAL_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    assertThat(listAllTables(new TablesApi(manager), CATALOG_NAME, SCHEMA_NAME))
        .extracting(TableInfo::getName)
        .containsExactly(TABLE_NAME);
    assertPermissionDenied(
        () -> getTable(new TablesApi(manager), SCHEMA_FULL_NAME + ".future_table"));
    assertThat(listAllFunctions(new FunctionsApi(manager), CATALOG_NAME, SCHEMA_NAME)).isEmpty();
    assertThat(listAllVolumes(new VolumesApi(manager), CATALOG_NAME, SCHEMA_NAME)).isEmpty();
    assertThat(listAllRegisteredModels(new RegisteredModelsApi(manager), CATALOG_NAME, SCHEMA_NAME))
        .isEmpty();
    grantsApi.update(
        SecurableType.TABLE,
        TABLE_FULL_NAME,
        change(PRINCIPAL_1, List.of(), List.of(Privilege.MANAGE)));
    assertPermissionDenied(() -> getTable(new TablesApi(manager), TABLE_FULL_NAME));
  }

  @Test
  @SneakyThrows
  void metadataAndOwnerChangesPersistForEveryTarget() {
    for (Target target : targets.subList(2, targets.size())) {
      grantPermissions(PRINCIPAL_1, target.type(), target.name(), Privileges.MANAGE);
    }
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
    grantPermissions(PRINCIPAL_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    new TablesApi(manager).updateTable(TABLE_FULL_NAME, new UpdateTable().owner(PRINCIPAL_2));
    new FunctionsApi(manager).updateFunction(FUNCTION, new UpdateFunction().owner(PRINCIPAL_1));
    new VolumesApi(manager)
        .updateVolume(
            VOLUME, new UpdateVolumeRequestContent().owner(PRINCIPAL_2).comment("managed volume"));
    new RegisteredModelsApi(manager)
        .updateRegisteredModel(
            MODEL, new UpdateRegisteredModel().owner(PRINCIPAL_1).comment("managed model"));
    new ExternalLocationsApi(manager)
        .updateExternalLocation(
            LOCATION, new UpdateExternalLocation().owner(PRINCIPAL_2).comment("managed location"));
    new CredentialsApi(manager)
        .updateCredential(
            CREDENTIAL,
            new UpdateCredentialRequest().owner(PRINCIPAL_2).comment("managed credential"));
    grantPermissions(PRINCIPAL_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.MANAGE);
    new SchemasApi(manager)
        .updateSchema(
            SCHEMA_FULL_NAME,
            new UpdateSchema()
                .owner(PRINCIPAL_2)
                .comment("managed schema")
                .properties(Map.of("team", "schema")));
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.MANAGE);
    new CatalogsApi(manager)
        .updateCatalog(
            CATALOG_NAME,
            new UpdateCatalog()
                .owner(PRINCIPAL_2)
                .comment("managed catalog")
                .properties(Map.of("team", "catalog")));

    assertThat(new CatalogsApi(adminApiClient).getCatalog(CATALOG_NAME).getProperties())
        .containsEntry("team", "catalog");
    assertThat(new SchemasApi(adminApiClient).getSchema(SCHEMA_FULL_NAME).getProperties())
        .containsEntry("team", "schema");
    assertThat(new VolumesApi(adminApiClient).getVolume(VOLUME).getComment())
        .isEqualTo("managed volume");
    assertThat(new RegisteredModelsApi(adminApiClient).getRegisteredModel(MODEL).getComment())
        .isEqualTo("managed model");
    assertThat(new ExternalLocationsApi(adminApiClient).getExternalLocation(LOCATION).getComment())
        .isEqualTo("managed location");
    assertThat(new CredentialsApi(adminApiClient).getCredential(CREDENTIAL).getComment())
        .isEqualTo("managed credential");
    for (Target target : targets) {
      boolean self =
          List.of(SecurableType.FUNCTION, SecurableType.REGISTERED_MODEL).contains(target.type());
      String owner =
          switch (target.type()) {
            case CATALOG -> new CatalogsApi(adminApiClient).getCatalog(target.name()).getOwner();
            case SCHEMA -> new SchemasApi(adminApiClient).getSchema(target.name()).getOwner();
            case TABLE -> getTable(new TablesApi(adminApiClient), target.name()).getOwner();
            case FUNCTION -> new FunctionsApi(adminApiClient).getFunction(target.name()).getOwner();
            case VOLUME -> new VolumesApi(adminApiClient).getVolume(target.name()).getOwner();
            case REGISTERED_MODEL ->
                new RegisteredModelsApi(adminApiClient)
                    .getRegisteredModel(target.name())
                    .getOwner();
            case EXTERNAL_LOCATION ->
                new ExternalLocationsApi(adminApiClient)
                    .getExternalLocation(target.name())
                    .getOwner();
            case CREDENTIAL ->
                new CredentialsApi(adminApiClient).getCredential(target.name()).getOwner();
            default -> throw new IllegalArgumentException();
          };
      assertThat(owner).isEqualTo(self ? PRINCIPAL_1 : PRINCIPAL_2);
      try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
        assertThat(
                session
                    .createNativeQuery(
                        "select v0 from casbin_rule where ptype = 'p' and v1 = :id and v2 ="
                            + " 'OWNER'",
                        String.class)
                    .setParameter("id", target.id())
                    .getResultList())
            .containsExactly(self ? managerId : recipientId);
      }
    }
    // The new catalog owner can manage its children immediately, without separate usage grants.
    ApiClient recipient = TestUtils.createApiClient(createTestUserServerConfig(PRINCIPAL_2));
    new VolumesApi(recipient)
        .updateVolume(VOLUME, new UpdateVolumeRequestContent().comment("new owner"));
    assertThat(new VolumesApi(adminApiClient).getVolume(VOLUME).getComment())
        .isEqualTo("new owner");
  }

  @Test
  @SneakyThrows
  void viewFunctionAndModelRecipientsRequireConsentOrAdmin() {
    DependencyList dependencies =
        new DependencyList()
            .dependencies(
                List.of(
                    new Dependency().table(new TableDependency().tableFullName(TABLE_FULL_NAME))));
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.MANAGE);
    for (TableType type : List.of(TableType.VIEW, TableType.METRIC_VIEW)) {
      String name = type == TableType.VIEW ? "manage_view" : "manage_metric_view";
      String view = SCHEMA_FULL_NAME + "." + name;
      new TablesApi(adminApiClient)
          .createTable(
              new CreateTable()
                  .name(name)
                  .catalogName(CATALOG_NAME)
                  .schemaName(SCHEMA_NAME)
                  .tableType(type)
                  .columns(TEST_COLUMNS)
                  .viewDefinition("SELECT * FROM " + TABLE_FULL_NAME)
                  .viewDependencies(dependencies)
                  .properties(Map.of("team", "views")));
      assertPermissionDenied(
          () -> new TablesApi(manager).updateTable(view, new UpdateTable().owner(PRINCIPAL_2)));
      TableInfo transferred =
          new TablesApi(manager).updateTable(view, new UpdateTable().owner(PRINCIPAL_1));
      assertThat(transferred.getOwner()).isEqualTo(PRINCIPAL_1);
      assertThat(transferred.getViewDefinition()).isEqualTo("SELECT * FROM " + TABLE_FULL_NAME);
      assertThat(transferred.getViewDependencies()).isEqualTo(dependencies);
      assertThat(transferred.getProperties()).containsEntry("team", "views");
      assertThat(getTable(new TablesApi(adminApiClient), view).getOwner()).isEqualTo(PRINCIPAL_1);
      new TablesApi(adminApiClient).updateTable(view, new UpdateTable().owner(PRINCIPAL_2));
      assertThat(getTable(new TablesApi(adminApiClient), view).getOwner()).isEqualTo(PRINCIPAL_2);
    }
    assertPermissionDenied(
        () ->
            new FunctionsApi(manager)
                .updateFunction(FUNCTION, new UpdateFunction().owner(PRINCIPAL_2)));
    assertPermissionDenied(
        () ->
            new RegisteredModelsApi(manager)
                .updateRegisteredModel(MODEL, new UpdateRegisteredModel().owner(PRINCIPAL_2)));
    new FunctionsApi(adminApiClient)
        .updateFunction(FUNCTION, new UpdateFunction().owner(PRINCIPAL_2));
    new RegisteredModelsApi(adminApiClient)
        .updateRegisteredModel(MODEL, new UpdateRegisteredModel().owner(PRINCIPAL_2));
    assertThat(new FunctionsApi(adminApiClient).getFunction(FUNCTION).getOwner())
        .isEqualTo(PRINCIPAL_2);
    assertThat(new RegisteredModelsApi(adminApiClient).getRegisteredModel(MODEL).getOwner())
        .isEqualTo(PRINCIPAL_2);
  }

  @Test
  @SneakyThrows
  void ownershipAndAdminExceptionsKeepTheirScopes() {
    new CatalogsApi(adminApiClient)
        .updateCatalog(CATALOG_NAME, new UpdateCatalog().owner(PRINCIPAL_2));
    new SchemasApi(adminApiClient)
        .updateSchema(SCHEMA_FULL_NAME, new UpdateSchema().owner(REGULAR_1));
    new TablesApi(adminApiClient).updateTable(TABLE_FULL_NAME, new UpdateTable().owner(REGULAR_2));
    new VolumesApi(adminApiClient)
        .updateVolume(VOLUME, new UpdateVolumeRequestContent().owner(PRINCIPAL_2));
    new RegisteredModelsApi(adminApiClient)
        .updateRegisteredModel(MODEL, new UpdateRegisteredModel().owner(PRINCIPAL_2));
    assertPermissionDenied(
        () ->
            new CatalogsApi(adminApiClient)
                .updateCatalog(
                    CATALOG_NAME, new UpdateCatalog().owner(PRINCIPAL_2).comment("denied")));
    assertPermissionDenied(
        () ->
            new SchemasApi(adminApiClient)
                .updateSchema(
                    SCHEMA_FULL_NAME, new UpdateSchema().owner(REGULAR_1).comment("denied")));
    assertPermissionDenied(
        () ->
            new VolumesApi(adminApiClient)
                .updateVolume(
                    VOLUME, new UpdateVolumeRequestContent().owner(PRINCIPAL_2).comment("denied")));
    assertPermissionDenied(
        () ->
            new RegisteredModelsApi(adminApiClient)
                .updateRegisteredModel(
                    MODEL, new UpdateRegisteredModel().owner(PRINCIPAL_2).comment("denied")));

    ApiClient catalogOwner = TestUtils.createApiClient(createTestUserServerConfig(PRINCIPAL_2));
    assertThat(new FunctionsApi(catalogOwner).getFunction(FUNCTION).getRoutineDefinition())
        .isEqualTo("return 1");
    ApiClient schemaOwner = TestUtils.createApiClient(createTestUserServerConfig(REGULAR_1));
    ApiClient tableOwner = TestUtils.createApiClient(createTestUserServerConfig(REGULAR_2));
    grantPermissions(PRINCIPAL_1, SecurableType.TABLE, TABLE_FULL_NAME, Privileges.SELECT);
    for (ApiClient owner : List.of(catalogOwner, schemaOwner, tableOwner)) {
      assertThat(
              privilegesFor(
                  new GrantsApi(owner).get(SecurableType.TABLE, TABLE_FULL_NAME, null),
                  PRINCIPAL_1))
          .containsExactly(Privilege.SELECT);
    }
    assertThat(
            new CatalogsApi(catalogOwner)
                .updateCatalog(CATALOG_NAME, new UpdateCatalog().newName("owner_catalog"))
                .getName())
        .isEqualTo("owner_catalog");
    new CatalogsApi(catalogOwner)
        .updateCatalog("owner_catalog", new UpdateCatalog().newName(CATALOG_NAME));
    assertPermissionDenied(
        () ->
            new RegisteredModelsApi(catalogOwner)
                .updateRegisteredModel(
                    MODEL, new UpdateRegisteredModel().newName("catalog_owner_model")));
    grantPermissions(
        PRINCIPAL_2,
        SecurableType.SCHEMA,
        SCHEMA_FULL_NAME,
        Privileges.USE_SCHEMA,
        Privileges.CREATE_MODEL);
    assertThat(
            new RegisteredModelsApi(catalogOwner)
                .updateRegisteredModel(
                    MODEL, new UpdateRegisteredModel().newName("catalog_owner_model"))
                .getName())
        .isEqualTo("catalog_owner_model");
    new RegisteredModelsApi(catalogOwner)
        .updateRegisteredModel(
            SCHEMA_FULL_NAME + ".catalog_owner_model",
            new UpdateRegisteredModel().newName("manage_model"));
    assertPermissionDenied(() -> getTable(new TablesApi(schemaOwner), TABLE_FULL_NAME));
    grantPermissions(REGULAR_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
    assertThat(getTable(new TablesApi(schemaOwner), TABLE_FULL_NAME).getName())
        .isEqualTo(TABLE_NAME);
    assertThat(
            new SchemasApi(schemaOwner)
                .updateSchema(SCHEMA_FULL_NAME, new UpdateSchema().newName("owner_schema"))
                .getName())
        .isEqualTo("owner_schema");
    new SchemasApi(schemaOwner)
        .updateSchema(CATALOG_NAME + ".owner_schema", new UpdateSchema().newName(SCHEMA_NAME));
    grantPermissions(REGULAR_2, SecurableType.TABLE, TABLE_FULL_NAME, Privileges.SELECT);
    assertThat(
            privilegesFor(
                new GrantsApi(schemaOwner).get(SecurableType.TABLE, TABLE_FULL_NAME, null),
                REGULAR_2))
        .containsExactly(Privilege.SELECT);

    assertPermissionDenied(() -> getTable(new TablesApi(tableOwner), TABLE_FULL_NAME));
    grantPermissions(REGULAR_2, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
    assertPermissionDenied(() -> getTable(new TablesApi(tableOwner), TABLE_FULL_NAME));
    grantPermissions(REGULAR_2, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    assertThat(getTable(new TablesApi(tableOwner), TABLE_FULL_NAME).getName())
        .isEqualTo(TABLE_NAME);
    new GrantsApi(tableOwner)
        .update(
            SecurableType.TABLE,
            TABLE_FULL_NAME,
            change(PRINCIPAL_1, List.of(Privilege.MANAGE), List.of(Privilege.SELECT)));
    assertThat(
            privilegesFor(grantsApi.get(SecurableType.TABLE, TABLE_FULL_NAME, null), PRINCIPAL_1))
        .containsExactly(Privilege.MANAGE);
    new SchemasApi(adminApiClient)
        .updateSchema(SCHEMA_FULL_NAME, new UpdateSchema().owner(PRINCIPAL_2));
    new VolumesApi(adminApiClient)
        .updateVolume(VOLUME, new UpdateVolumeRequestContent().owner(REGULAR_1));
    new RegisteredModelsApi(adminApiClient)
        .updateRegisteredModel(MODEL, new UpdateRegisteredModel().owner(REGULAR_1));
    grantPermissions(REGULAR_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    assertPermissionDenied(
        () ->
            new RegisteredModelsApi(schemaOwner)
                .updateRegisteredModel(MODEL, new UpdateRegisteredModel().newName("owner_model")));
    grantPermissions(REGULAR_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.CREATE_MODEL);
    assertThat(
            new RegisteredModelsApi(schemaOwner)
                .updateRegisteredModel(MODEL, new UpdateRegisteredModel().newName("owner_model"))
                .getName())
        .isEqualTo("owner_model");
  }

  @Test
  @SneakyThrows
  void renameAndCredentialReplacementKeepTheirCreateChecks() {
    grantPermissions(
        PRINCIPAL_1,
        SecurableType.CATALOG,
        CATALOG_NAME,
        Privileges.MANAGE,
        Privileges.USE_CATALOG);
    grantPermissions(PRINCIPAL_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    assertPermissionDenied(
        () ->
            new CatalogsApi(manager)
                .updateCatalog(CATALOG_NAME, new UpdateCatalog().newName("renamed_catalog")));
    assertPermissionDenied(
        () ->
            new SchemasApi(manager)
                .updateSchema(SCHEMA_FULL_NAME, new UpdateSchema().newName("renamed_schema")));
    assertPermissionDenied(
        () ->
            new VolumesApi(manager)
                .updateVolume(VOLUME, new UpdateVolumeRequestContent().newName("renamed_volume")));
    assertPermissionDenied(
        () ->
            new RegisteredModelsApi(manager)
                .updateRegisteredModel(
                    MODEL, new UpdateRegisteredModel().newName("renamed_model")));
    grantPermissions(
        PRINCIPAL_1,
        SecurableType.SCHEMA,
        SCHEMA_FULL_NAME,
        Privileges.CREATE_VOLUME,
        Privileges.CREATE_FUNCTION);
    new VolumesApi(manager)
        .updateVolume(VOLUME, new UpdateVolumeRequestContent().newName("renamed_volume"));
    new RegisteredModelsApi(manager)
        .updateRegisteredModel(MODEL, new UpdateRegisteredModel().newName("renamed_model"));
    assertThat(
            new RegisteredModelsApi(adminApiClient)
                .getRegisteredModel(SCHEMA_FULL_NAME + ".renamed_model")
                .getName())
        .isEqualTo("renamed_model");
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.CREATE_SCHEMA);
    new SchemasApi(manager)
        .updateSchema(SCHEMA_FULL_NAME, new UpdateSchema().newName("renamed_schema"));
    grantPermissions(
        PRINCIPAL_1, SecurableType.METASTORE, METASTORE_NAME, Privileges.CREATE_CATALOG);
    new CatalogsApi(manager)
        .updateCatalog(CATALOG_NAME, new UpdateCatalog().newName("renamed_catalog"));
    assertThat(new CatalogsApi(adminApiClient).getCatalog("renamed_catalog").getName())
        .isEqualTo("renamed_catalog");

    new CredentialsApi(adminApiClient)
        .createCredential(
            new CreateCredentialRequest()
                .name("replacement_credential")
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn("fake_arn")));
    grantPermissions(PRINCIPAL_1, SecurableType.EXTERNAL_LOCATION, LOCATION, Privileges.MANAGE);
    grantPermissions(
        PRINCIPAL_1, SecurableType.CREDENTIAL, "replacement_credential", Privileges.MANAGE);
    assertPermissionDenied(
        () ->
            new ExternalLocationsApi(manager)
                .updateExternalLocation(
                    LOCATION,
                    new UpdateExternalLocation().credentialName("replacement_credential")));
    grantPermissions(
        PRINCIPAL_1,
        SecurableType.CREDENTIAL,
        "replacement_credential",
        Privileges.CREATE_EXTERNAL_LOCATION);
    new ExternalLocationsApi(manager)
        .updateExternalLocation(
            LOCATION, new UpdateExternalLocation().credentialName("replacement_credential"));
    assertThat(
            new ExternalLocationsApi(adminApiClient)
                .getExternalLocation(LOCATION)
                .getCredentialName())
        .isEqualTo("replacement_credential");
  }

  @Test
  @SneakyThrows
  void manageDoesNotGrantDataCreateOrPublication() {
    for (Target target : targets) {
      grantPermissions(PRINCIPAL_1, target.type(), target.name(), Privileges.MANAGE);
    }
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
    grantPermissions(PRINCIPAL_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    TemporaryCredentialsApi credentials = new TemporaryCredentialsApi(manager);
    for (TableOperation operation : List.of(TableOperation.READ, TableOperation.READ_WRITE)) {
      assertPermissionDenied(
          () ->
              credentials.generateTemporaryTableCredentials(
                  new GenerateTemporaryTableCredential()
                      .tableId(targets.get(2).id())
                      .operation(operation)));
    }
    for (VolumeOperation operation :
        List.of(VolumeOperation.READ_VOLUME, VolumeOperation.WRITE_VOLUME)) {
      assertPermissionDenied(
          () ->
              credentials.generateTemporaryVolumeCredentials(
                  new GenerateTemporaryVolumeCredential()
                      .volumeId(targets.get(4).id())
                      .operation(operation)));
    }
    new ModelVersionsApi(adminApiClient)
        .createModelVersion(
            new CreateModelVersion()
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .modelName("manage_model")
                .source("model_source"));
    for (ModelVersionOperation operation :
        List.of(
            ModelVersionOperation.READ_MODEL_VERSION,
            ModelVersionOperation.READ_WRITE_MODEL_VERSION)) {
      assertPermissionDenied(
          () ->
              credentials.generateTemporaryModelVersionCredentials(
                  new GenerateTemporaryModelVersionCredential()
                      .catalogName(CATALOG_NAME)
                      .schemaName(SCHEMA_NAME)
                      .modelName("manage_model")
                      .version(1L)
                      .operation(operation)));
    }
    for (PathOperation operation :
        List.of(
            PathOperation.PATH_READ,
            PathOperation.PATH_READ_WRITE,
            PathOperation.PATH_CREATE_TABLE)) {
      assertPermissionDenied(
          () ->
              credentials.generateTemporaryPathCredentials(
                  new GenerateTemporaryPathCredential()
                      .url(testDirectoryRoot.resolve("location/data").toUri().toString())
                      .operation(operation)));
    }
    assertPermissionDenied(
        () ->
            new CatalogsApi(manager)
                .createCatalog(new CreateCatalog().name("manage_cannot_create")));
    assertPermissionDenied(
        () ->
            new SchemasApi(manager)
                .createSchema(
                    new CreateSchema().catalogName(CATALOG_NAME).name("manage_cannot_create")));
    assertPermissionDenied(
        () ->
            createExternalTable(
                new TablesApi(manager),
                CATALOG_NAME,
                SCHEMA_NAME,
                "manage_cannot_create",
                testDirectoryRoot.resolve("denied").toString()));
    assertPermissionDenied(
        () -> new FunctionsApi(manager).createFunction(newFunction("manage_cannot_create")));
    assertPermissionDenied(
        () ->
            new RegisteredModelsApi(manager)
                .createRegisteredModel(
                    new CreateRegisteredModel()
                        .catalogName(CATALOG_NAME)
                        .schemaName(SCHEMA_NAME)
                        .name("manage_cannot_create")));
    assertPermissionDenied(
        () ->
            new VolumesApi(manager)
                .createVolume(
                    new CreateVolumeRequestContent()
                        .catalogName(CATALOG_NAME)
                        .schemaName(SCHEMA_NAME)
                        .name("manage_cannot_create")
                        .volumeType(VolumeType.EXTERNAL)
                        .storageLocation(testDirectoryRoot.resolve("denied_volume").toString())));
    assertPermissionDenied(
        () ->
            new ExternalLocationsApi(manager)
                .createExternalLocation(
                    new CreateExternalLocation()
                        .name("manage_cannot_create")
                        .credentialName(CREDENTIAL)
                        .url(testDirectoryRoot.resolve("denied_location").toUri().toString())));
    assertPermissionDenied(
        () ->
            new CredentialsApi(manager)
                .createCredential(
                    new CreateCredentialRequest()
                        .name("manage_cannot_create")
                        .purpose(CredentialPurpose.STORAGE)
                        .awsIamRole(new AwsIamRoleRequest().roleArn("fake_arn"))));
    ModelVersionsApi versions = new ModelVersionsApi(manager);
    assertPermissionDenied(
        () ->
            versions.createModelVersion(
                new CreateModelVersion()
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .modelName("manage_model")
                    .source("model_source")));
    assertPermissionDenied(
        () ->
            versions.finalizeModelVersion(
                MODEL, 1L, new FinalizeModelVersion().fullName(MODEL).version(1L)));
    assertThat(versions.getModelVersion(MODEL, 1L).getSource()).isEqualTo("model_source");
    assertThat(
            versions
                .updateModelVersion(MODEL, 1L, new UpdateModelVersion().comment("managed version"))
                .getComment())
        .isEqualTo("managed version");
    assertApiException(
        () -> new RegisteredModelsApi(manager).deleteRegisteredModel(MODEL, false),
        ErrorCode.ABORTED,
        "Unable to delete a registered model with existing model versions");
    versions.deleteModelVersion(MODEL, 1L);
    assertThat(versions.listModelVersions(MODEL, null, null).getModelVersions()).isEmpty();
  }

  @Test
  @SneakyThrows
  void protocolMetadataNeverAddsCredentialsOrWriteAuthority() {
    IcebergRestClient admin = new IcebergRestClient(adminConfig);
    IcebergRestClient client = new IcebergRestClient(createTestUserServerConfig(PRINCIPAL_1));
    admin.createTable(
        CATALOG_NAME,
        SCHEMA_NAME,
        CreateTableRequest.builder()
            .withName("manage_iceberg")
            .withSchema(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
            .build());
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.MANAGE);
    assertThat(client.loadTable(CATALOG_NAME, SCHEMA_NAME, "manage_iceberg").config()).isEmpty();
    assertThat(client.loadTable(CATALOG_NAME, SCHEMA_NAME, "manage_iceberg").tableMetadata())
        .isNotNull();
    assertIcebergApiException(
        () -> client.loadCredentials(CATALOG_NAME, SCHEMA_NAME, "manage_iceberg"),
        403,
        "Access denied");
    UpdateTableRequest update =
        new UpdateTableRequest(
            List.of(), List.of(new MetadataUpdate.SetProperties(Map.of("managed", "denied"))));
    assertIcebergApiException(
        () -> client.updateTable(CATALOG_NAME, SCHEMA_NAME, "manage_iceberg", update),
        403,
        "Access denied");
    String path =
        "/api/2.1/unity-catalog/delta/v1/catalogs/"
            + CATALOG_NAME
            + "/schemas/"
            + SCHEMA_NAME
            + "/tables/"
            + TABLE_NAME;
    WebClient delta =
        WebClient.builder(serverConfig.getServerUrl())
            .addHeader(HttpHeaderNames.AUTHORIZATION, "Bearer " + createTestJwtToken(PRINCIPAL_1))
            .build();
    assertThat(delta.get(path).aggregate().join().status().code()).isEqualTo(200);
    assertThat(
            delta
                .prepare()
                .post(path)
                .content(MediaType.JSON_UTF_8, "{}")
                .execute()
                .aggregate()
                .join()
                .status()
                .code())
        .isEqualTo(403);
    assertThat(delta.get(path + "/credentials?operation=READ").aggregate().join().status().code())
        .isEqualTo(403);
    // Adding data grants permits only the requested data scope.
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
    grantPermissions(PRINCIPAL_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    grantPermissions(
        PRINCIPAL_1, SecurableType.TABLE, SCHEMA_FULL_NAME + ".manage_iceberg", Privileges.SELECT);
    grantsApi.update(
        SecurableType.CATALOG,
        CATALOG_NAME,
        change(PRINCIPAL_1, List.of(), List.of(Privilege.MANAGE)));
    for (boolean addManage : List.of(false, true)) {
      if (addManage) {
        grantPermissions(
            PRINCIPAL_1,
            SecurableType.TABLE,
            SCHEMA_FULL_NAME + ".manage_iceberg",
            Privileges.MANAGE);
      }
      assertThat(client.loadTable(CATALOG_NAME, SCHEMA_NAME, "manage_iceberg").config())
          .isEqualTo(CREDENTIAL_CONFIG);
      assertThat(client.loadCredentials(CATALOG_NAME, SCHEMA_NAME, "manage_iceberg").credentials())
          .hasSize(1);
    }
  }

  @Test
  @SneakyThrows
  void deletesKeepDependencyChecksAndClearPolicy() {
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.MANAGE);
    grantPermissions(PRINCIPAL_1, SecurableType.EXTERNAL_LOCATION, LOCATION, Privileges.MANAGE);
    grantPermissions(PRINCIPAL_1, SecurableType.CREDENTIAL, CREDENTIAL, Privileges.MANAGE);
    assertApiException(
        () -> new CatalogsApi(manager).deleteCatalog(CATALOG_NAME, false),
        ErrorCode.FAILED_PRECONDITION,
        "Cannot delete catalog with schemas");
    assertApiException(
        () -> new SchemasApi(manager).deleteSchema(SCHEMA_FULL_NAME, false),
        ErrorCode.FAILED_PRECONDITION,
        "Cannot delete schema with tables");
    assertApiException(
        () -> new CredentialsApi(manager).deleteCredential(CREDENTIAL, false),
        ErrorCode.INVALID_ARGUMENT,
        "used by external location");
    createExternalTable(
        new TablesApi(adminApiClient),
        CATALOG_NAME,
        SCHEMA_NAME,
        "location_table",
        testDirectoryRoot.resolve("location/table").toString());
    assertApiException(
        () -> new ExternalLocationsApi(manager).deleteExternalLocation(LOCATION, false),
        ErrorCode.INVALID_ARGUMENT,
        "External location still used by");
    new TablesApi(manager).deleteTable(SCHEMA_FULL_NAME + ".location_table");
    new TablesApi(manager).deleteTable(TABLE_FULL_NAME);
    new FunctionsApi(manager).deleteFunction(FUNCTION);
    new VolumesApi(manager).deleteVolume(VOLUME);
    new RegisteredModelsApi(manager).deleteRegisteredModel(MODEL, false);
    new SchemasApi(manager).deleteSchema(SCHEMA_FULL_NAME, false);
    new CatalogsApi(manager).deleteCatalog(CATALOG_NAME, false);
    new ExternalLocationsApi(manager).deleteExternalLocation(LOCATION, false);
    new CredentialsApi(manager).deleteCredential(CREDENTIAL, false);
    for (Target target : targets) {
      assertThat(countCasbinRulesReferencing(target.id())).isZero();
    }
  }

  private void readMetadata(SecurableType type, String name) throws Exception {
    switch (type) {
      case CATALOG -> assertThat(new CatalogsApi(manager).getCatalog(name).getOwner()).isNotBlank();
      case SCHEMA -> assertThat(new SchemasApi(manager).getSchema(name).getOwner()).isNotBlank();
      case TABLE ->
          assertThat(getTable(new TablesApi(manager), name).getColumns())
              .hasSize(TEST_COLUMNS.size());
      case FUNCTION ->
          assertThat(new FunctionsApi(manager).getFunction(name).getRoutineDefinition())
              .isEqualTo("return 1");
      case VOLUME ->
          assertThat(new VolumesApi(manager).getVolume(name).getStorageLocation()).isNotBlank();
      case REGISTERED_MODEL ->
          assertThat(new RegisteredModelsApi(manager).getRegisteredModel(name).getStorageLocation())
              .isNotBlank();
      case EXTERNAL_LOCATION ->
          assertThat(
                  new ExternalLocationsApi(manager).getExternalLocation(name).getCredentialName())
              .isEqualTo(CREDENTIAL);
      case CREDENTIAL ->
          assertThat(new CredentialsApi(manager).getCredential(name).getAwsIamRole().getRoleArn())
              .isEqualTo("fake_arn");
      default -> throw new IllegalArgumentException("Unsupported test target: " + type);
    }
  }

  private static List<Privilege> privilegesFor(PermissionsList permissions, String principal) {
    return permissions.getPrivilegeAssignments().stream()
        .filter(p -> principal.equals(p.getPrincipal()))
        .flatMap(p -> p.getPrivileges().stream())
        .toList();
  }

  private static UpdatePermissions change(
      String principal, List<Privilege> add, List<Privilege> remove) {
    return new UpdatePermissions()
        .changes(List.of(new PermissionsChange().principal(principal).add(add).remove(remove)));
  }

  private static CreateFunctionRequest newFunction(String name) {
    return new CreateFunctionRequest()
        .functionInfo(
            new CreateFunction()
                .name(name)
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .parameterStyle(CreateFunction.ParameterStyleEnum.S)
                .isDeterministic(true)
                .isNullCall(false)
                .securityType(CreateFunction.SecurityTypeEnum.DEFINER)
                .specificName(name)
                .dataType(ColumnTypeName.INT)
                .fullDataType("INT")
                .inputParams(new FunctionParameterInfos().parameters(List.of()))
                .externalLanguage("python")
                .routineBody(CreateFunction.RoutineBodyEnum.EXTERNAL)
                .routineDefinition("return 1"));
  }
}
