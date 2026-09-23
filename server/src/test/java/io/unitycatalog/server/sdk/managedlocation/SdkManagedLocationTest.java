package io.unitycatalog.server.sdk.managedlocation;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME2;
import static io.unitycatalog.server.utils.TestUtils.TABLE_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.TABLE_NAME;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.DataSourceFormat;
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
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.models.SdkModelOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.sdk.volume.SdkVolumeOperations;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.TestUtils;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.SneakyThrows;
import org.hibernate.Session;
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
  @Test
  public void testManagedVolumeDropQueuesCleanupTask() {
    createCatalog(true);
    createSchema(false);
    String volumeFullName = CATALOG_NAME + "." + SCHEMA_NAME + "." + MANAGED_VOLUME_NAME1;
    VolumeInfo volume =
        volumeOperations.createVolume(
            new CreateVolumeRequestContent()
                .name(MANAGED_VOLUME_NAME1)
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .volumeType(VolumeType.MANAGED));
    Path volumeDirectory = Path.of(URI.create(volume.getStorageLocation()));
    Path marker = volumeDirectory.resolve("data/marker.txt");
    Files.createDirectories(marker.getParent());
    Files.writeString(marker, "data");

    volumeOperations.deleteVolume(volumeFullName);

    // The drop removes the volume and queues a cleanup task; storage is reclaimed later by the
    // background worker (default initial delay), so the files are still present right after the
    // drop rather than deleted synchronously.
    assertThatThrownBy(() -> volumeOperations.getVolume(volumeFullName))
        .isInstanceOf(ApiException.class);
    assertThat(findCleanupTask(UUID.fromString(volume.getVolumeId()))).isNotNull();
    assertThat(Files.exists(marker)).isTrue();
  }

  private StorageCleanupTaskDAO findCleanupTask(UUID resourceId) {
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      return session.get(StorageCleanupTaskDAO.class, resourceId);
    }
  }
}
