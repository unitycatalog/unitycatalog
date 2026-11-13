package io.unitycatalog.server.sdk.managedlocation;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME2;
import static io.unitycatalog.server.utils.TestUtils.TABLE_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.TABLE_NAME;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.unitycatalog.client.api.TablesApi;
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
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.models.SdkModelOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.sdk.volume.SdkVolumeOperations;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.TestUtils;
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
    assertThat(volumeOperations.getVolume(managed.getFullName()).getStorageLocation())
        .isEqualTo(managed.getStorageLocation());
  }

  @SneakyThrows
  @Test
  public void testStoragePathsLongerThan255Characters() {
    String catalogRoot = "s3://catalog-bucket/" + "c".repeat(1000);
    String schemaRoot = "s3://schema-bucket/" + "s".repeat(1200);
    createCatalog(Optional.of(catalogRoot));
    createSchema(Optional.of(schemaRoot));
    assertThat(catalogOperations.getCatalog(CATALOG_NAME).getStorageLocation())
        .isEqualTo(catalogInfo.getStorageLocation());
    assertThat(schemaOperations.getSchema(SCHEMA_FULL_NAME).getStorageLocation())
        .isEqualTo(schemaInfo.getStorageLocation());

    schemaOperations.createSchema(new CreateSchema().catalogName(CATALOG_NAME).name(SCHEMA_NAME2));
    assertManagedVolumeUnder(SCHEMA_NAME, schemaInfo.getStorageLocation());
    assertManagedVolumeUnder(SCHEMA_NAME2, catalogInfo.getStorageLocation());

    String externalVolumeLocation = "s3://volume-bucket/" + "v".repeat(2029);
    assertThat(externalVolumeLocation).hasSize(2048);
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

    String modelVersionSource = "s3://model-bucket/" + "m".repeat(2030);
    assertThat(modelVersionSource).hasSize(2048);
    modelOperations.createRegisteredModel(
        new CreateRegisteredModel()
            .name(MANAGED_MODEL_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME));
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
}
