package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.assertDeltaPermissionDenied;
import static io.unitycatalog.server.utils.TestUtils.assertIcebergApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

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
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateFunction;
import io.unitycatalog.client.model.CreateFunctionRequest;
import io.unitycatalog.client.model.CreateModelVersion;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.FunctionParameterInfos;
import io.unitycatalog.client.model.GenerateTemporaryModelVersionCredential;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.GenerateTemporaryVolumeCredential;
import io.unitycatalog.client.model.ModelVersionInfo;
import io.unitycatalog.client.model.ModelVersionOperation;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.client.model.UpdateExternalLocation;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.client.model.UpdateRegisteredModel;
import io.unitycatalog.client.model.UpdateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeOperation;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class SdkReadMetadataAccessControlTest extends SdkAccessControlBaseCRUDTest {

  private static final String TABLE = "metadata_table";
  private static final String VIEW = "metadata_view";
  private static final String VOLUME = "metadata_volume";
  private static final String FUNCTION = "metadata_function";
  private static final String MODEL = "metadata_model";
  private static final String CREDENTIAL = "metadata_credential";
  private static final String LOCATION = "metadata_location";
  private static final Map<String, String> CREDENTIAL_CONFIG =
      Map.of(S3FileIOProperties.ACCESS_KEY_ID, "test-key");

  private ApiClient reader;
  private GrantsApi readerGrants;
  private List<MetadataRead> objects;
  private List<MetadataRead> roots;
  private TableInfo table;
  private VolumeInfo volume;
  private ModelVersionInfo version;

  private record MetadataRead(
      SecurableType type,
      String name,
      Object expected,
      Callable<?> get,
      Callable<? extends List<?>> list) {}

  @Override
  protected FileOperations decorateFileOperations(FileOperations fileOperations) {
    FileOperations operations = spy(fileOperations);
    // A credential leak must be visible even though the metadata files stay local.
    doReturn(CREDENTIAL_CONFIG).when(operations).getFileIOConfig(any(), any(), any());
    return operations;
  }

  private void createMetadataFixtures() throws Exception {
    createTestUser(REGULAR_1);
    createTestUser(REGULAR_2);
    reader = TestUtils.createApiClient(createTestUserServerConfig(REGULAR_1));
    readerGrants = new GrantsApi(reader);
    table =
        createExternalTable(
            new TablesApi(adminApiClient),
            CATALOG_NAME,
            SCHEMA_NAME,
            TABLE,
            testDirectoryRoot.resolve(TABLE).toString());
    new TablesApi(adminApiClient)
        .createTable(
            new CreateTable()
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .name(VIEW)
                .tableType(TableType.VIEW)
                .columns(TEST_COLUMNS)
                .viewDefinition("SELECT 1 AS id"));
    volume =
        new VolumesApi(adminApiClient)
            .createVolume(
                new CreateVolumeRequestContent()
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .name(VOLUME)
                    .comment("Volume metadata")
                    .volumeType(VolumeType.EXTERNAL)
                    .storageLocation(testDirectoryRoot.resolve(VOLUME).toString()));
    new FunctionsApi(adminApiClient)
        .createFunction(
            new CreateFunctionRequest()
                .functionInfo(
                    new CreateFunction()
                        .catalogName(CATALOG_NAME)
                        .schemaName(SCHEMA_NAME)
                        .name(FUNCTION)
                        .specificName(FUNCTION)
                        .parameterStyle(CreateFunction.ParameterStyleEnum.S)
                        .securityType(CreateFunction.SecurityTypeEnum.DEFINER)
                        .isDeterministic(true)
                        .isNullCall(false)
                        .dataType(ColumnTypeName.INT)
                        .fullDataType("INT")
                        .inputParams(new FunctionParameterInfos().parameters(List.of()))
                        .routineBody(CreateFunction.RoutineBodyEnum.EXTERNAL)
                        .externalLanguage("python")
                        .routineDefinition("return 1")));
    new RegisteredModelsApi(adminApiClient)
        .createRegisteredModel(
            new CreateRegisteredModel()
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .name(MODEL)
                .comment("Model metadata"));
    version =
        new ModelVersionsApi(adminApiClient)
            .createModelVersion(
                new CreateModelVersion()
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .modelName(MODEL)
                    .source(testDirectoryRoot.resolve("source").toString()));
    objects =
        List.of(
            new MetadataRead(
                SecurableType.TABLE,
                SCHEMA_FULL_NAME + "." + TABLE,
                getTable(new TablesApi(adminApiClient), SCHEMA_FULL_NAME + "." + TABLE),
                () -> getTable(new TablesApi(reader), SCHEMA_FULL_NAME + "." + TABLE),
                () -> listAllTables(new TablesApi(reader), CATALOG_NAME, SCHEMA_NAME)),
            new MetadataRead(
                SecurableType.TABLE,
                SCHEMA_FULL_NAME + "." + VIEW,
                getTable(new TablesApi(adminApiClient), SCHEMA_FULL_NAME + "." + VIEW),
                () -> getTable(new TablesApi(reader), SCHEMA_FULL_NAME + "." + VIEW),
                () -> listAllTables(new TablesApi(reader), CATALOG_NAME, SCHEMA_NAME)),
            new MetadataRead(
                SecurableType.VOLUME,
                SCHEMA_FULL_NAME + "." + VOLUME,
                new VolumesApi(adminApiClient).getVolume(SCHEMA_FULL_NAME + "." + VOLUME),
                () -> new VolumesApi(reader).getVolume(SCHEMA_FULL_NAME + "." + VOLUME),
                () -> listAllVolumes(new VolumesApi(reader), CATALOG_NAME, SCHEMA_NAME)),
            new MetadataRead(
                SecurableType.FUNCTION,
                SCHEMA_FULL_NAME + "." + FUNCTION,
                new FunctionsApi(adminApiClient).getFunction(SCHEMA_FULL_NAME + "." + FUNCTION),
                () -> new FunctionsApi(reader).getFunction(SCHEMA_FULL_NAME + "." + FUNCTION),
                () -> listAllFunctions(new FunctionsApi(reader), CATALOG_NAME, SCHEMA_NAME)),
            new MetadataRead(
                SecurableType.REGISTERED_MODEL,
                SCHEMA_FULL_NAME + "." + MODEL,
                new RegisteredModelsApi(adminApiClient)
                    .getRegisteredModel(SCHEMA_FULL_NAME + "." + MODEL),
                () ->
                    new RegisteredModelsApi(reader)
                        .getRegisteredModel(SCHEMA_FULL_NAME + "." + MODEL),
                () -> listAllRegisteredModels(new RegisteredModelsApi(reader), null, null)));
    createExternalLocationWithCredential(
        CREDENTIAL, LOCATION, testDirectoryRoot.resolve("external").toUri().toString());
    roots =
        List.of(
            new MetadataRead(
                SecurableType.CATALOG,
                CATALOG_NAME,
                new CatalogsApi(adminApiClient).getCatalog(CATALOG_NAME),
                () -> new CatalogsApi(reader).getCatalog(CATALOG_NAME),
                () -> new CatalogsApi(reader).listCatalogs(null, null).getCatalogs()),
            new MetadataRead(
                SecurableType.SCHEMA,
                SCHEMA_FULL_NAME,
                new SchemasApi(adminApiClient).getSchema(SCHEMA_FULL_NAME),
                () -> new SchemasApi(reader).getSchema(SCHEMA_FULL_NAME),
                () -> new SchemasApi(reader).listSchemas(CATALOG_NAME, null, null).getSchemas()),
            new MetadataRead(
                SecurableType.CREDENTIAL,
                CREDENTIAL,
                new CredentialsApi(adminApiClient).getCredential(CREDENTIAL),
                () -> new CredentialsApi(reader).getCredential(CREDENTIAL),
                () ->
                    new CredentialsApi(reader).listCredentials(null, null, null).getCredentials()),
            new MetadataRead(
                SecurableType.EXTERNAL_LOCATION,
                LOCATION,
                new ExternalLocationsApi(adminApiClient).getExternalLocation(LOCATION),
                () -> new ExternalLocationsApi(reader).getExternalLocation(LOCATION),
                () ->
                    new ExternalLocationsApi(reader)
                        .listExternalLocations(null, null)
                        .getExternalLocations()));
    for (MetadataRead read : objects) {
      grantPermissions(REGULAR_2, read.type(), read.name(), Privileges.READ_METADATA);
    }
    for (MetadataRead read : roots) {
      grantPermissions(REGULAR_2, read.type(), read.name(), Privileges.READ_METADATA);
    }
  }

  @Test
  public void metadataAndGrantVisibilityFollowGrantScope() throws Exception {
    createMetadataFixtures();
    assertMetadataAccess(objects, false);
    for (SecurableType scope :
        List.of(SecurableType.METASTORE, SecurableType.CATALOG, SecurableType.SCHEMA)) {
      String name = scopeName(scope);
      grantPermissions(REGULAR_1, scope, name, Privileges.READ_METADATA);
      if (scope == SecurableType.SCHEMA) {
        assertMetadataAccess(objects, false);
        grantPermissions(REGULAR_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
      }
      assertMetadataAccess(objects, true);
      for (MetadataRead read : roots) {
        boolean visible =
            scope == SecurableType.METASTORE
                || read.type() == SecurableType.SCHEMA
                || (scope == SecurableType.CATALOG && read.type() == SecurableType.CATALOG);
        if (scope == SecurableType.SCHEMA && read.type() == SecurableType.CATALOG) {
          // USE CATALOG exposes catalog metadata but does not expose other users' grants.
          assertThat(read.get().call()).isEqualTo(read.expected());
          assertThat(readerGrants.get(read.type(), read.name(), null).getPrivilegeAssignments())
              .extracting(assignment -> assignment.getPrincipal())
              .doesNotContain(REGULAR_2);
        } else {
          assertMetadataAccess(List.of(read), visible);
        }
      }
      revoke(scope, name, Privilege.READ_METADATA);
      assertMetadataAccess(objects, false);
      if (scope == SecurableType.SCHEMA) {
        revoke(SecurableType.CATALOG, CATALOG_NAME, Privilege.USE_CATALOG);
      }
    }

    for (MetadataRead read : objects) {
      grantPermissions(REGULAR_1, read.type(), read.name(), Privileges.READ_METADATA);
      assertMetadataAccess(List.of(read), false);
      grantPermissions(REGULAR_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
      assertMetadataAccess(List.of(read), false);
      grantPermissions(REGULAR_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
      assertMetadataAccess(List.of(read), true);
      assertThat(read.list().call()).hasSize(1);
      revoke(SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privilege.USE_SCHEMA);
      assertMetadataAccess(List.of(read), false);
      revoke(read.type(), read.name(), Privilege.READ_METADATA);
      revoke(SecurableType.CATALOG, CATALOG_NAME, Privilege.USE_CATALOG);
    }
  }

  @Test
  public void rootMetadataGrantsExposeGrantsWithoutAllowingChanges() throws Exception {
    createMetadataFixtures();
    assertMetadataAccess(roots, false);
    for (MetadataRead read : roots) {
      grantPermissions(REGULAR_1, read.type(), read.name(), Privileges.READ_METADATA);
      if (read.type() == SecurableType.SCHEMA) {
        assertMetadataAccess(List.of(read), false);
        grantPermissions(REGULAR_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
      }
      assertMetadataAccess(List.of(read), true);
      revoke(read.type(), read.name(), Privilege.READ_METADATA);
      if (read.type() == SecurableType.SCHEMA) {
        revoke(SecurableType.CATALOG, CATALOG_NAME, Privilege.USE_CATALOG);
      }
      assertMetadataAccess(List.of(read), false);
    }
    grantPermissions(REGULAR_2, SecurableType.METASTORE, METASTORE_NAME, Privileges.CREATE_CATALOG);
    grantPermissions(REGULAR_1, SecurableType.METASTORE, METASTORE_NAME, Privileges.READ_METADATA);
    assertMetadataAccess(roots, true);
    assertThat(readerGrants.get(SecurableType.METASTORE, METASTORE_NAME, null))
        .isEqualTo(grantsApi.get(SecurableType.METASTORE, METASTORE_NAME, null));
    assertPermissionDenied(
        () -> readerGrants.update(SecurableType.METASTORE, METASTORE_NAME, removeMetadataGrant()));
    assertPermissionDenied(
        () ->
            new CredentialsApi(reader)
                .updateCredential(CREDENTIAL, new UpdateCredentialRequest().comment("changed")));
    assertPermissionDenied(() -> new CredentialsApi(reader).deleteCredential(CREDENTIAL, true));
    assertPermissionDenied(
        () ->
            new ExternalLocationsApi(reader)
                .updateExternalLocation(LOCATION, new UpdateExternalLocation().comment("changed")));
    assertPermissionDenied(
        () -> new ExternalLocationsApi(reader).deleteExternalLocation(LOCATION, true));
    assertMetadataAccess(roots, true);
    revoke(SecurableType.METASTORE, METASTORE_NAME, Privilege.READ_METADATA);
    assertMetadataAccess(roots, false);
  }

  @Test
  public void protocolsReturnMetadataWithoutDataCredentialsOrWrites() throws Exception {
    createMetadataFixtures();
    String icebergTable = "iceberg_table";
    IcebergRestClient adminIceberg = new IcebergRestClient(adminConfig);
    Schema icebergSchema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    adminIceberg.createTable(
        CATALOG_NAME,
        SCHEMA_NAME,
        CreateTableRequest.builder().withName(icebergTable).withSchema(icebergSchema).build());
    String hiddenTable = "hidden_iceberg_table";
    adminIceberg.createTable(
        CATALOG_NAME,
        SCHEMA_NAME,
        CreateTableRequest.builder().withName(hiddenTable).withSchema(icebergSchema).build());
    IcebergRestClient iceberg = new IcebergRestClient(createTestUserServerConfig(REGULAR_1));
    DeltaTablesApi delta = new DeltaTablesApi(reader);
    TemporaryCredentialsApi credentials = new TemporaryCredentialsApi(reader);
    DeltaTemporaryCredentialsApi deltaCredentials = new DeltaTemporaryCredentialsApi(reader);

    for (SecurableType scope :
        List.of(
            SecurableType.METASTORE,
            SecurableType.CATALOG,
            SecurableType.SCHEMA,
            SecurableType.TABLE)) {
      String name = scopeName(scope);
      grantPermissions(REGULAR_1, scope, name, Privileges.READ_METADATA);
      if (scope == SecurableType.TABLE) {
        grantPermissions(
            REGULAR_1,
            SecurableType.TABLE,
            SCHEMA_FULL_NAME + "." + icebergTable,
            Privileges.READ_METADATA);
      }
      if (scope == SecurableType.SCHEMA || scope == SecurableType.TABLE) {
        assertDeltaPermissionDenied(() -> delta.loadTable(CATALOG_NAME, SCHEMA_NAME, TABLE));
        assertIcebergApiException(
            () -> iceberg.loadTable(CATALOG_NAME, SCHEMA_NAME, icebergTable), 403, "Access denied");
        grantPermissions(REGULAR_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
      }
      if (scope == SecurableType.TABLE) {
        assertDeltaPermissionDenied(() -> delta.loadTable(CATALOG_NAME, SCHEMA_NAME, TABLE));
        assertIcebergApiException(
            () -> iceberg.loadTable(CATALOG_NAME, SCHEMA_NAME, icebergTable), 403, "Access denied");
        grantPermissions(REGULAR_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
      }
      assertThat(iceberg.config(CATALOG_NAME)).isNotNull();
      assertThat(iceberg.loadNamespace(CATALOG_NAME, SCHEMA_NAME).namespace().toString())
          .isEqualTo(SCHEMA_NAME);
      assertThat(iceberg.namespaceExists(CATALOG_NAME, SCHEMA_NAME)).isTrue();
      assertThat(iceberg.listNamespaces(CATALOG_NAME).namespaces())
          .extracting(namespace -> namespace.toString())
          .containsExactly(SCHEMA_NAME);
      assertThat(getTable(new TablesApi(reader), SCHEMA_FULL_NAME + "." + TABLE)).isEqualTo(table);
      assertThat(delta.loadTable(CATALOG_NAME, SCHEMA_NAME, TABLE).getMetadata())
          .isEqualTo(
              new DeltaTablesApi(adminApiClient)
                  .loadTable(CATALOG_NAME, SCHEMA_NAME, TABLE)
                  .getMetadata());
      assertThat(delta.tableExistsWithHttpInfo(CATALOG_NAME, SCHEMA_NAME, TABLE).getStatusCode())
          .isEqualTo(204);
      assertThat(
              iceberg
                  .loadTable(CATALOG_NAME, SCHEMA_NAME, icebergTable)
                  .tableMetadata()
                  .schema()
                  .asStruct())
          .isEqualTo(icebergSchema.asStruct());
      assertThat(iceberg.loadTable(CATALOG_NAME, SCHEMA_NAME, icebergTable).config()).isEmpty();
      assertThat(iceberg.tableExists(CATALOG_NAME, SCHEMA_NAME, icebergTable)).isTrue();
      assertThat(iceberg.listTables(CATALOG_NAME, SCHEMA_NAME).identifiers())
          .extracting(identifier -> identifier.name())
          .containsExactlyInAnyOrderElementsOf(
              scope == SecurableType.TABLE
                  ? List.of(icebergTable)
                  : List.of(icebergTable, hiddenTable));
      if (scope == SecurableType.TABLE) {
        assertIcebergApiException(
            () -> iceberg.loadView(CATALOG_NAME, SCHEMA_NAME, VIEW), 403, "Access denied");
        assertIcebergApiException(
            () -> iceberg.loadTable(CATALOG_NAME, SCHEMA_NAME, hiddenTable), 403, "Access denied");
      } else {
        assertIcebergApiException(
            () -> iceberg.loadView(CATALOG_NAME, SCHEMA_NAME, VIEW), 404, "View does not exist");
      }
      grantPermissions(REGULAR_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
      grantPermissions(REGULAR_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
      assertIcebergApiException(
          () -> iceberg.loadCredentials(CATALOG_NAME, SCHEMA_NAME, icebergTable),
          403,
          "Access denied");
      for (TableOperation operation : List.of(TableOperation.READ, TableOperation.READ_WRITE)) {
        assertPermissionDenied(
            () ->
                credentials.generateTemporaryTableCredentials(
                    new GenerateTemporaryTableCredential()
                        .tableId(table.getTableId())
                        .operation(operation)));
      }
      for (DeltaCredentialOperation operation :
          List.of(DeltaCredentialOperation.READ, DeltaCredentialOperation.READ_WRITE)) {
        assertDeltaPermissionDenied(
            () ->
                deltaCredentials.getTableCredentials(operation, CATALOG_NAME, SCHEMA_NAME, TABLE));
      }
      assertIcebergApiException(
          () ->
              iceberg.updateTable(
                  CATALOG_NAME,
                  SCHEMA_NAME,
                  icebergTable,
                  new UpdateTableRequest(
                      List.of(),
                      List.of(new MetadataUpdate.SetProperties(Map.of("changed", "true"))))),
          403,
          "Access denied");
      assertThat(
              adminIceberg
                  .loadTable(CATALOG_NAME, SCHEMA_NAME, icebergTable)
                  .tableMetadata()
                  .properties())
          .doesNotContainKey("changed");
      revoke(scope, name, Privilege.READ_METADATA);
      if (scope == SecurableType.TABLE) {
        revoke(SecurableType.TABLE, SCHEMA_FULL_NAME + "." + icebergTable, Privilege.READ_METADATA);
      }
      assertPermissionDenied(() -> getTable(new TablesApi(reader), SCHEMA_FULL_NAME + "." + TABLE));
      assertDeltaPermissionDenied(() -> delta.loadTable(CATALOG_NAME, SCHEMA_NAME, TABLE));
      assertIcebergApiException(
          () -> iceberg.loadTable(CATALOG_NAME, SCHEMA_NAME, icebergTable), 403, "Access denied");
      revoke(SecurableType.CATALOG, CATALOG_NAME, Privilege.USE_CATALOG);
      revoke(SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privilege.USE_SCHEMA);
    }

    grantPermissions(REGULAR_1, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
    grantPermissions(REGULAR_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    grantPermissions(REGULAR_1, SecurableType.METASTORE, METASTORE_NAME, Privileges.READ_METADATA);
    for (VolumeOperation operation :
        List.of(VolumeOperation.READ_VOLUME, VolumeOperation.WRITE_VOLUME)) {
      assertPermissionDenied(
          () ->
              credentials.generateTemporaryVolumeCredentials(
                  new GenerateTemporaryVolumeCredential()
                      .volumeId(volume.getVolumeId())
                      .operation(operation)));
    }
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
                      .modelName(MODEL)
                      .version(version.getVersion())
                      .operation(operation)));
    }
    for (String path :
        List.of(
            table.getStorageLocation(),
            volume.getStorageLocation(),
            version.getStorageLocation(),
            testDirectoryRoot.resolve("external").toUri().toString())) {
      for (PathOperation operation :
          List.of(PathOperation.PATH_READ, PathOperation.PATH_READ_WRITE)) {
        assertPermissionDenied(
            () ->
                credentials.generateTemporaryPathCredentials(
                    new GenerateTemporaryPathCredential().url(path).operation(operation)));
      }
    }
    assertPermissionDenied(() -> new TablesApi(reader).deleteTable(SCHEMA_FULL_NAME + "." + TABLE));
    assertPermissionDenied(
        () -> new FunctionsApi(reader).deleteFunction(SCHEMA_FULL_NAME + "." + FUNCTION));
    assertPermissionDenied(
        () ->
            new VolumesApi(reader)
                .updateVolume(
                    SCHEMA_FULL_NAME + "." + VOLUME,
                    new UpdateVolumeRequestContent().comment("changed")));
    assertPermissionDenied(
        () ->
            new RegisteredModelsApi(reader)
                .updateRegisteredModel(
                    SCHEMA_FULL_NAME + "." + MODEL,
                    new UpdateRegisteredModel().comment("changed")));
    assertMetadataAccess(objects, true);
    grantPermissions(
        REGULAR_1, SecurableType.TABLE, SCHEMA_FULL_NAME + "." + icebergTable, Privileges.SELECT);
    assertThat(iceberg.loadTable(CATALOG_NAME, SCHEMA_NAME, icebergTable).config())
        .containsAllEntriesOf(CREDENTIAL_CONFIG);
    assertThat(iceberg.loadCredentials(CATALOG_NAME, SCHEMA_NAME, icebergTable).credentials())
        .singleElement()
        .satisfies(
            credential -> assertThat(credential.config()).containsAllEntriesOf(CREDENTIAL_CONFIG));
  }

  private void assertMetadataAccess(List<MetadataRead> reads, boolean allowed) throws Exception {
    for (MetadataRead read : reads) {
      if (allowed) {
        assertThat(read.get().call()).isEqualTo(read.expected());
        assertThat(read.list().call()).anyMatch(read.expected()::equals);
        assertThat(readerGrants.get(read.type(), read.name(), null).getPrivilegeAssignments())
            .containsExactlyInAnyOrderElementsOf(
                grantsApi.get(read.type(), read.name(), null).getPrivilegeAssignments());
        assertPermissionDenied(
            () -> readerGrants.update(read.type(), read.name(), removeMetadataGrant()));
      } else {
        assertPermissionDenied(() -> read.get().call());
        assertThat(read.list().call()).isEmpty();
        assertThat(readerGrants.get(read.type(), read.name(), null).getPrivilegeAssignments())
            .extracting(assignment -> assignment.getPrincipal())
            .doesNotContain(REGULAR_2);
      }
      if (read.type() == SecurableType.REGISTERED_MODEL) {
        ModelVersionsApi versions = new ModelVersionsApi(reader);
        if (allowed) {
          assertThat(versions.getModelVersion(read.name(), version.getVersion()))
              .isEqualTo(
                  new ModelVersionsApi(adminApiClient)
                      .getModelVersion(read.name(), version.getVersion()));
          assertThat(listAllModelVersions(versions, read.name())).hasSize(1);
        } else {
          assertPermissionDenied(() -> versions.getModelVersion(read.name(), version.getVersion()));
          assertPermissionDenied(() -> versions.listModelVersions(read.name(), null, null));
        }
      }
    }
  }

  private static UpdatePermissions removeMetadataGrant() {
    return new UpdatePermissions()
        .changes(
            List.of(
                new PermissionsChange()
                    .principal(REGULAR_2)
                    .remove(List.of(Privilege.READ_METADATA))));
  }

  private void revoke(SecurableType type, String name, Privilege privilege) throws Exception {
    grantsApi.update(
        type,
        name,
        new UpdatePermissions()
            .changes(
                List.of(new PermissionsChange().principal(REGULAR_1).remove(List.of(privilege)))));
  }

  private static String scopeName(SecurableType scope) {
    return switch (scope) {
      case METASTORE -> METASTORE_NAME;
      case CATALOG -> CATALOG_NAME;
      case SCHEMA -> SCHEMA_FULL_NAME;
      case TABLE -> SCHEMA_FULL_NAME + "." + TABLE;
      default -> throw new IllegalArgumentException("Unexpected scope: " + scope);
    };
  }
}
