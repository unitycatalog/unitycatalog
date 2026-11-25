package io.unitycatalog.server.sdk.managedlocation;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME2;
import static io.unitycatalog.server.utils.TestUtils.TABLE_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.TABLE_NAME;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateModelVersion;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.ModelVersionInfo;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.managedlocation.BaseManagedLocationTest;
import io.unitycatalog.server.base.model.ModelOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.base.volume.VolumeOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.models.SdkModelOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.sdk.volume.SdkVolumeOperations;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class SdkManagedLocationTest extends BaseManagedLocationTest {

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig serverConfig) {
    return new SdkTableOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected VolumeOperations createVolumeOperations(ServerConfig serverConfig) {
    return new SdkVolumeOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected ModelOperations createModelOperations(ServerConfig serverConfig) {
    return new SdkModelOperations(TestUtils.createApiClient(serverConfig));
  }

  @SneakyThrows
  @Test
  public void testCreateVolumeOverOrphanStagingTable() {
    // Create a staging table using managed storage of schema
    createCatalog(false);
    createSchema(true);
    TablesApi tablesApi = new TablesApi(TestUtils.createApiClient(serverConfig));
    StagingTableInfo stagingTableInfo =
        tablesApi.createStagingTable(
            new CreateStagingTable()
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .name(TABLE_NAME));

    // Create an external location using storageRootDir should fail because the first schema has
    // managed storage under it.
    schemaOperations.createSchema(new CreateSchema().catalogName(CATALOG_NAME).name(SCHEMA_NAME2));
    CreateVolumeRequestContent createExternalVolume =
        new CreateVolumeRequestContent()
            .name(EXTERNAL_VOLUME_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME2)
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation(storageRootDir.toString());
    TestUtils.assertApiException(
        () -> volumeOperations.createVolume(createExternalVolume),
        ErrorCode.INVALID_ARGUMENT,
        "overlaps with managed storage");

    // Delete the first schema so that no schema managed storage exists. However, the staging
    // table still exists so external volume creation still fails
    schemaOperations.deleteSchema(SCHEMA_FULL_NAME, Optional.empty());
    TestUtils.assertApiException(
        () -> volumeOperations.createVolume(createExternalVolume),
        ErrorCode.INVALID_ARGUMENT,
        "overlaps with staging table");

    // To remove the staging table, promote it to a real table then delete it. Then the external
    // volume can be created.
    createSchema(false);
    tablesApi.createTable(
        new CreateTable()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name(TABLE_NAME)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .tableType(TableType.MANAGED)
            .storageLocation(stagingTableInfo.getStagingLocation())
            .properties(Map.of(TableProperties.UC_TABLE_ID, stagingTableInfo.getId())));
    tablesApi.deleteTable(TABLE_FULL_NAME);

    VolumeInfo externalVolumeInfo = volumeOperations.createVolume(createExternalVolume);
    assertThat(externalVolumeInfo.getStorageLocation())
        .isEqualTo(NormalizedURL.normalize(storageRootDir.toString()));
  }

  @SneakyThrows
  private void assertManagedVolumeUnder(String schemaName, String parentLocation) {
    VolumeInfo managed =
        volumeOperations.createVolume(
            new CreateVolumeRequestContent()
                .name(MANAGED_VOLUME_NAME1)
                .catalogName(CATALOG_NAME)
                .schemaName(schemaName)
                .volumeType(VolumeType.MANAGED));
    assertThat(managed.getStorageLocation()).startsWith(parentLocation + "/volumes/");
    assertThat(managed.getStorageLocation()).hasSize(2048);
    assertThat(volumeOperations.getVolume(managed.getFullName()).getStorageLocation())
        .isEqualTo(managed.getStorageLocation());
  }

  @SneakyThrows
  private void assertManagedStagingTableUnder(String schemaName, String parentLocation) {
    StagingTableInfo staging =
        new TablesApi(TestUtils.createApiClient(serverConfig))
            .createStagingTable(
                new CreateStagingTable()
                    .catalogName(CATALOG_NAME)
                    .schemaName(schemaName)
                    .name(TABLE_NAME));
    assertThat(staging.getStagingLocation()).startsWith(parentLocation + "/tables/");
    assertThat(staging.getStagingLocation()).hasSize(2047);
  }

  @SneakyThrows
  @Test
  public void testStoragePathsUpToTheColumnLimit() {
    // The longest roots allowed. A managed volume under each is exactly 2048 characters.
    String catalogRoot = "s3://catalog-bucket/" + "c".repeat(1922);
    String schemaRoot = "s3://schema-bucket/" + "s".repeat(1924);
    TestUtils.assertApiException(
        () ->
            catalogOperations.createCatalog(
                new CreateCatalog().name(CATALOG_NAME).storageRoot(catalogRoot + "c")),
        ErrorCode.INVALID_ARGUMENT,
        "Storage root is 1943 bytes long in UTF-8, but it can be at most 1942 bytes");
    createCatalog(Optional.of(catalogRoot));
    TestUtils.assertApiException(
        () ->
            schemaOperations.createSchema(
                new CreateSchema()
                    .catalogName(CATALOG_NAME)
                    .name(SCHEMA_NAME)
                    .storageRoot(schemaRoot + "s")),
        ErrorCode.INVALID_ARGUMENT,
        "Storage root is 1944 bytes long in UTF-8, but it can be at most 1943 bytes");
    createSchema(Optional.of(schemaRoot));
    assertThat(catalogOperations.getCatalog(CATALOG_NAME).getStorageRoot()).isEqualTo(catalogRoot);
    assertThat(catalogOperations.getCatalog(CATALOG_NAME).getStorageLocation())
        .isEqualTo(catalogInfo.getStorageLocation());
    assertThat(schemaOperations.getSchema(SCHEMA_FULL_NAME).getStorageRoot()).isEqualTo(schemaRoot);
    assertThat(schemaOperations.getSchema(SCHEMA_FULL_NAME).getStorageLocation())
        .isEqualTo(schemaInfo.getStorageLocation());

    schemaOperations.createSchema(new CreateSchema().catalogName(CATALOG_NAME).name(SCHEMA_NAME2));
    assertManagedVolumeUnder(SCHEMA_NAME, schemaInfo.getStorageLocation());
    assertManagedVolumeUnder(SCHEMA_NAME2, catalogInfo.getStorageLocation());
    assertManagedStagingTableUnder(SCHEMA_NAME, schemaInfo.getStorageLocation());
    assertManagedStagingTableUnder(SCHEMA_NAME2, catalogInfo.getStorageLocation());

    String externalVolumeLocation = "s3://volume-bucket/" + "v".repeat(2029);
    assertThat(externalVolumeLocation).hasSize(2048);
    TestUtils.assertApiException(
        () ->
            volumeOperations.createVolume(
                new CreateVolumeRequestContent()
                    .name(EXTERNAL_VOLUME_NAME)
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .volumeType(VolumeType.EXTERNAL)
                    .storageLocation(externalVolumeLocation + "v")),
        ErrorCode.INVALID_ARGUMENT,
        "Storage location is too long: it is 2049 characters, and the limit is 2048.");
    volumeOperations.createVolume(
        new CreateVolumeRequestContent()
            .name(EXTERNAL_VOLUME_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation(externalVolumeLocation));
    assertThat(
            volumeOperations
                .getVolume(SCHEMA_FULL_NAME + "." + EXTERNAL_VOLUME_NAME)
                .getStorageLocation())
        .isEqualTo(externalVolumeLocation);

    String externalTableLocation = "s3://table-bucket/" + "t".repeat(2031);
    assertThat(externalTableLocation).hasSize(2049);
    TestUtils.assertApiException(
        () ->
            tableOperations.createTable(
                new CreateTable()
                    .name(EXTERNAL_TABLE_NAME)
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .columns(COLUMNS)
                    .tableType(TableType.EXTERNAL)
                    .dataSourceFormat(DataSourceFormat.DELTA)
                    .storageLocation(externalTableLocation)),
        ErrorCode.INVALID_ARGUMENT,
        "Storage location is too long: it is 2049 characters, and the limit is 2048.");

    String modelVersionSource = "s3://model-bucket/" + "m".repeat(2030);
    assertThat(modelVersionSource).hasSize(2048);
    modelOperations.createRegisteredModel(
        new CreateRegisteredModel()
            .name(MANAGED_MODEL_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME));
    TestUtils.assertApiException(
        () ->
            modelOperations.createModelVersion(
                new CreateModelVersion()
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .modelName(MANAGED_MODEL_NAME)
                    .source(modelVersionSource + "m")),
        ErrorCode.INVALID_ARGUMENT,
        "Model version source is too long: it is 2049 characters, and the limit is 2048.");
    ModelVersionInfo version =
        modelOperations.createModelVersion(
            new CreateModelVersion()
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .modelName(MANAGED_MODEL_NAME)
                .source(modelVersionSource));
    assertThat(
            modelOperations
                .getModelVersion(SCHEMA_FULL_NAME + "." + MANAGED_MODEL_NAME, version.getVersion())
                .getSource())
        .isEqualTo(modelVersionSource);
  }

  @SneakyThrows
  @Test
  public void testDirectoryLocationsLeaveRoomForAFile() {
    createCatalog(Optional.empty());
    createSchema(Optional.empty());
    String container = "abfss://container@account.dfs.core.windows.net/";
    // A blob name has at most 1,024 characters, and a file under a directory adds "/x".
    String directory = container + "d".repeat(1022);
    String noRoomForAFile = "Azure blob names under it have at least 1025 characters";

    TestUtils.assertApiException(
        () -> createExternalVolume(EXTERNAL_VOLUME_NAME, directory + "d"),
        ErrorCode.INVALID_ARGUMENT,
        noRoomForAFile);
    assertThat(createExternalVolume(EXTERNAL_VOLUME_NAME, directory).getStorageLocation())
        .isEqualTo(directory);
    // An external Delta table has to hold its first commit, _delta_log/<20 digit version>.json.
    String delta = container + "l".repeat(1024 - "/_delta_log/00000000000000000000.json".length());
    TestUtils.assertApiException(
        () ->
            createExternalTable(
                EXTERNAL_TABLE_NAME + "_delta", DataSourceFormat.DELTA, delta + "l"),
        ErrorCode.INVALID_ARGUMENT,
        "the Azure blob name of its first Delta commit file has 1025 characters");
    createExternalTable(EXTERNAL_TABLE_NAME + "_delta", DataSourceFormat.DELTA, delta);

    // A CSV table may be a single file, so only its own name has to fit.
    String file = container + "f".repeat(1024);
    createExternalTable(EXTERNAL_TABLE_NAME + "_csv", DataSourceFormat.CSV, file);
    TestUtils.assertApiException(
        () -> createExternalTable(EXTERNAL_TABLE_NAME + "_csv2", DataSourceFormat.CSV, file + "f"),
        ErrorCode.INVALID_ARGUMENT,
        "its Azure blob name has 1025 characters");
  }

  @SneakyThrows
  private VolumeInfo createExternalVolume(String name, String location) {
    return volumeOperations.createVolume(
        new CreateVolumeRequestContent()
            .name(name)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation(location));
  }

  @SneakyThrows
  private void createExternalTable(String name, DataSourceFormat format, String location) {
    tableOperations.createTable(
        new CreateTable()
            .name(name)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .columns(COLUMNS)
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(format)
            .storageLocation(location));
  }

  @SneakyThrows
  @Test
  public void testMalformedCloudStorageRootsAreRejected() {
    List<String> malformedRoots = List.of("s3:///", "gs:///", "s3://", "gs://", "s3:a/b", "gs:a/b");
    for (String root : malformedRoots) {
      TestUtils.assertApiException(
          () ->
              catalogOperations.createCatalog(
                  new CreateCatalog().name(CATALOG_NAME).storageRoot(root)),
          ErrorCode.INVALID_ARGUMENT,
          "");
    }
    createCatalog(Optional.of("s3://bucket-root"));
    for (String root : malformedRoots) {
      TestUtils.assertApiException(
          () ->
              schemaOperations.createSchema(
                  new CreateSchema().catalogName(CATALOG_NAME).name(SCHEMA_NAME).storageRoot(root)),
          ErrorCode.INVALID_ARGUMENT,
          "");
    }
    createSchema(Optional.of("gs://bucket-root"));
  }

  @SneakyThrows
  @Test
  public void testLocationsUnderAStoredLocationAreChecked() {
    String container = "abfss://container@account.dfs.core.windows.net/";
    createCatalog(Optional.of(container + "catalog"));
    createSchema(Optional.empty());
    modelOperations.createRegisteredModel(
        new CreateRegisteredModel()
            .name(MANAGED_MODEL_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME));
    // Stored before these checks existed, and too long for an Azure blob under it.
    String storedLocation = container + "s".repeat(1100);
    setStored("CatalogInfoDAO", "storageLocation", CATALOG_NAME, storedLocation);
    setStored("RegisteredModelInfoDAO", "url", MANAGED_MODEL_NAME, storedLocation);
    String rejection = "Azure blob names under it have at least";

    TestUtils.assertApiException(
        () ->
            volumeOperations.createVolume(
                new CreateVolumeRequestContent()
                    .name(MANAGED_VOLUME_NAME1)
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .volumeType(VolumeType.MANAGED)),
        ErrorCode.INVALID_ARGUMENT,
        rejection);
    TestUtils.assertApiException(
        () ->
            new TablesApi(TestUtils.createApiClient(serverConfig))
                .createStagingTable(
                    new CreateStagingTable()
                        .catalogName(CATALOG_NAME)
                        .schemaName(SCHEMA_NAME)
                        .name(TABLE_NAME)),
        ErrorCode.INVALID_ARGUMENT,
        "the Azure blob names of its temporary Delta checkpoint sidecar files have");
    TestUtils.assertApiException(
        () ->
            modelOperations.createRegisteredModel(
                new CreateRegisteredModel()
                    .name(MANAGED_MODEL_NAME + "2")
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)),
        ErrorCode.INVALID_ARGUMENT,
        rejection);
    TestUtils.assertApiException(
        () ->
            modelOperations.createModelVersion(
                new CreateModelVersion()
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .modelName(MANAGED_MODEL_NAME)
                    .source("s3://model-bucket/source")),
        ErrorCode.INVALID_ARGUMENT,
        rejection);
  }

  private void setStored(String entity, String field, String name, String location) {
    TransactionManager.executeWithTransaction(
        hibernateConfigurator.getSessionFactory(),
        session ->
            session
                .createMutationQuery(
                    "UPDATE " + entity + " SET " + field + " = :location WHERE name = :name")
                .setParameter("location", location)
                .setParameter("name", name)
                .executeUpdate(),
        "Failed to store a location",
        /* readOnly= */ false);
  }
}
