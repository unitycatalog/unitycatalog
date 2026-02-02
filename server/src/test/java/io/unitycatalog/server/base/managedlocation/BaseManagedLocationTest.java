package io.unitycatalog.server.base.managedlocation;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.COMMENT;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateModelVersion;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.ModelVersionInfo;
import io.unitycatalog.client.model.RegisteredModelInfo;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.model.ModelOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.base.volume.VolumeOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class BaseManagedLocationTest extends BaseCRUDTest {

  protected static final String MANAGED_VOLUME_NAME1 = "uc_managedvolume1";
  protected static final String MANAGED_VOLUME_NAME2 = "uc_managedvolume2";
  protected static final String EXTERNAL_VOLUME_NAME = "uc_externalvolume2";
  protected static final String MANAGED_TABLE_NAME = "uc_managedtable1";
  protected static final String EXTERNAL_TABLE_NAME = "uc_externaltable";
  protected static final String MANAGED_MODEL_NAME = "uc_managedmodel1";
  protected static final List<ColumnInfo> COLUMNS =
      List.of(
          new ColumnInfo()
              .name("as_int")
              .typeText("INTEGER")
              .typeJson("{\"type\": \"integer\"}")
              .typeName(ColumnTypeName.INT)
              .position(0)
              .comment("Integer column")
              .nullable(true));

  protected SchemaOperations schemaOperations;
  protected TableOperations tableOperations;
  protected VolumeOperations volumeOperations;
  protected ModelOperations modelOperations;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  protected abstract TableOperations createTableOperations(ServerConfig serverConfig);

  protected abstract VolumeOperations createVolumeOperations(ServerConfig serverConfig);

  protected abstract ModelOperations createModelOperations(ServerConfig serverConfig);

  @TempDir protected Path storageRootDir;
  protected CatalogInfo catalogInfo;
  protected SchemaInfo schemaInfo;

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();

    schemaOperations = createSchemaOperations(serverConfig);
    tableOperations = createTableOperations(serverConfig);
    volumeOperations = createVolumeOperations(serverConfig);
    modelOperations = createModelOperations(serverConfig);
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    // Remove TABLE_STORAGE_ROOT set by super.setUpProperties() so managed table creation relies
    // solely on catalog/schema managed location
    serverProperties.remove(Property.TABLE_STORAGE_ROOT.getKey());
  }

  @SneakyThrows
  protected void createCatalog(boolean withManagedStorage) {
    if (withManagedStorage) {
      createCatalog(Optional.of(NormalizedURL.normalize(storageRootDir.toString())));
    } else {
      createCatalog(Optional.empty());
    }
  }

  @SneakyThrows
  protected void createCatalog(Optional<String> storageRoot) {
    CreateCatalog createCatalog =
        new CreateCatalog()
            .name(CATALOG_NAME)
            .comment(COMMENT)
            .storageRoot(storageRoot.orElse(null));
    catalogInfo = catalogOperations.createCatalog(createCatalog);

    assertThat(catalogInfo.getName()).isEqualTo(CATALOG_NAME);
    // verify the full url
    if (storageRoot.isEmpty()) {
      assertThat(catalogInfo.getStorageRoot()).isNull();
      assertThat(catalogInfo.getStorageLocation()).isNull();
    } else {
      assertThat(catalogInfo.getStorageRoot()).isEqualTo(storageRoot.get());
      assertThat(catalogInfo.getStorageLocation())
          .isEqualTo(
              storageRoot.get()
                  + "/"
                  + Constants.MANAGED_STORAGE_CATALOG_PREFIX
                  + "/"
                  + catalogInfo.getId());
    }
  }

  @SneakyThrows
  protected void createSchema(boolean withManagedStorage) {
    if (withManagedStorage) {
      createSchema(Optional.of(NormalizedURL.normalize(storageRootDir.toString())));
    } else {
      createSchema(Optional.empty());
    }
  }

  @SneakyThrows
  protected void createSchema(Optional<String> storageRoot) {
    CreateSchema createSchema =
        new CreateSchema()
            .catalogName(CATALOG_NAME)
            .name(SCHEMA_NAME)
            .comment(COMMENT)
            .storageRoot(storageRoot.orElse(null));
    schemaInfo = schemaOperations.createSchema(createSchema);

    assertThat(schemaInfo.getName()).isEqualTo(SCHEMA_NAME);
    // verify the full url
    if (storageRoot.isEmpty()) {
      assertThat(schemaInfo.getStorageRoot()).isNull();
      assertThat(schemaInfo.getStorageLocation()).isNull();
    } else {
      assertThat(schemaInfo.getStorageRoot()).isEqualTo(storageRoot.get());
      assertThat(schemaInfo.getStorageLocation())
          .isEqualTo(
              storageRoot.get()
                  + "/"
                  + Constants.MANAGED_STORAGE_SCHEMA_PREFIX
                  + "/"
                  + schemaInfo.getSchemaId());
    }
  }

  protected String expectedStorageRoot(
      boolean managedLocationOnCatalog, boolean managedLocationOnSchema) {
    if (managedLocationOnSchema) {
      return schemaInfo.getStorageLocation();
    } else if (managedLocationOnCatalog) {
      return catalogInfo.getStorageLocation();
    } else {
      throw new RuntimeException("Does not have managed storage created.");
    }
  }

  protected static Stream<Arguments> allManagedLocationCombinations() {
    return Stream.of(
        Arguments.of(false, false),
        Arguments.of(false, true),
        Arguments.of(true, false),
        Arguments.of(true, true));
  }

  @Test
  public void testCatalogAndSchemaCannotUseManagedStoragePrefix() {
    String storageRoot =
        NormalizedURL.normalize(storageRootDir + "/" + Constants.MANAGED_STORAGE_PREFIX);
    TestUtils.assertApiException(
        () -> createCatalog(Optional.of(storageRoot)),
        ErrorCode.INVALID_ARGUMENT,
        Constants.MANAGED_STORAGE_PREFIX);

    createCatalog(false);
    TestUtils.assertApiException(
        () -> createSchema(Optional.of(storageRoot)),
        ErrorCode.INVALID_ARGUMENT,
        Constants.MANAGED_STORAGE_PREFIX);
  }

  @SneakyThrows
  @ParameterizedTest
  @MethodSource("allManagedLocationCombinations")
  public void testCreateVolume(boolean managedLocationOnCatalog, boolean managedLocationOnSchema) {
    createCatalog(managedLocationOnCatalog);
    createSchema(managedLocationOnSchema);
    // Create a managed volume - should use catalog or schema's storage location
    CreateVolumeRequestContent createManagedVolume =
        new CreateVolumeRequestContent()
            .name(MANAGED_VOLUME_NAME1)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .volumeType(VolumeType.MANAGED);

    if (managedLocationOnCatalog || managedLocationOnSchema) {
      VolumeInfo volumeInfo = volumeOperations.createVolume(createManagedVolume);
      assertThat(volumeInfo.getName()).isEqualTo(MANAGED_VOLUME_NAME1);
      assertThat(volumeInfo.getVolumeType()).isEqualTo(VolumeType.MANAGED);
      // Volume should be under the catalog's storage location
      assertThat(volumeInfo.getStorageLocation())
          .startsWith(
              expectedStorageRoot(managedLocationOnCatalog, managedLocationOnSchema)
                  + "/volumes/"
                  + volumeInfo.getVolumeId());
    } else {
      // Creating a managed volume should fail because there's no storage location configured
      TestUtils.assertApiException(
          () -> volumeOperations.createVolume(createManagedVolume),
          ErrorCode.FAILED_PRECONDITION,
          "Neither catalog nor schema has managed location configured");
    }

    // Managed volume must not specify a location
    createManagedVolume.name(MANAGED_VOLUME_NAME2).storageLocation("/tmp/some_location");
    TestUtils.assertApiException(
        () -> volumeOperations.createVolume(createManagedVolume),
        ErrorCode.INVALID_ARGUMENT,
        "Storage location should not be specified for managed volume");

    // Testing failure cases of external volume creation

    CreateVolumeRequestContent createExternalVolume =
        new CreateVolumeRequestContent()
            .name(EXTERNAL_VOLUME_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .volumeType(VolumeType.EXTERNAL);

    // Creating an external volume with managed storage prefix should fail
    createExternalVolume.setStorageLocation(
        "/tmp/" + Constants.MANAGED_STORAGE_PREFIX + "/some_location");
    TestUtils.assertApiException(
        () -> volumeOperations.createVolume(createExternalVolume),
        ErrorCode.INVALID_ARGUMENT,
        Constants.MANAGED_STORAGE_PREFIX);

    // Creating an external volume above managed storage should fail
    if (managedLocationOnCatalog || managedLocationOnSchema) {
      createExternalVolume.setStorageLocation(storageRootDir.toString());
      TestUtils.assertApiException(
          () -> volumeOperations.createVolume(createExternalVolume),
          ErrorCode.INVALID_ARGUMENT,
          "overlaps with managed storage");
    }
  }

  @SneakyThrows
  @ParameterizedTest
  @MethodSource("allManagedLocationCombinations")
  public void testCreateTable(boolean managedLocationOnCatalog, boolean managedLocationOnSchema) {
    createCatalog(managedLocationOnCatalog);
    createSchema(managedLocationOnSchema);

    // Create a managed table - should use catalog or schema's storage location
    CreateTable createManagedTable =
        new CreateTable()
            .name(MANAGED_TABLE_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .columns(COLUMNS)
            .tableType(TableType.MANAGED)
            .dataSourceFormat(DataSourceFormat.DELTA);

    if (managedLocationOnCatalog || managedLocationOnSchema) {
      TableInfo tableInfo = tableOperations.createTable(createManagedTable);
      assertThat(tableInfo.getName()).isEqualTo(MANAGED_TABLE_NAME);
      assertThat(tableInfo.getTableType()).isEqualTo(TableType.MANAGED);
      // Table should be under the catalog/schema's storage location
      assertThat(tableInfo.getStorageLocation())
          .startsWith(
              expectedStorageRoot(managedLocationOnCatalog, managedLocationOnSchema)
                  + "/tables/"
                  + tableInfo.getTableId());
    } else {
      // Creating a managed table should fail because there's no storage location configured
      TestUtils.assertApiException(
          () -> tableOperations.createTable(createManagedTable),
          ErrorCode.FAILED_PRECONDITION,
          "Neither catalog nor schema has managed location configured");
    }

    // Testing failure cases of external table creation

    CreateTable createExternalTable =
        new CreateTable()
            .name(EXTERNAL_TABLE_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .columns(COLUMNS)
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA);

    // Creating an external table with managed storage prefix should fail
    createExternalTable.setStorageLocation(
        "/tmp/" + Constants.MANAGED_STORAGE_PREFIX + "/some_location");
    TestUtils.assertApiException(
        () -> tableOperations.createTable(createExternalTable),
        ErrorCode.INVALID_ARGUMENT,
        Constants.MANAGED_STORAGE_PREFIX);

    // Creating an external table above managed storage should fail
    if (managedLocationOnCatalog || managedLocationOnSchema) {
      createExternalTable.setStorageLocation(storageRootDir.toString());
      TestUtils.assertApiException(
          () -> tableOperations.createTable(createExternalTable),
          ErrorCode.INVALID_ARGUMENT,
          "overlaps with managed storage");
    }
  }

  @SneakyThrows
  @ParameterizedTest
  @MethodSource("allManagedLocationCombinations")
  public void testCreateModel(boolean managedLocationOnCatalog, boolean managedLocationOnSchema) {
    createCatalog(managedLocationOnCatalog);
    createSchema(managedLocationOnSchema);

    // Create a registered model - should use catalog or schema's storage location
    CreateRegisteredModel createModel =
        new CreateRegisteredModel()
            .name(MANAGED_MODEL_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .comment("Test managed model");

    if (managedLocationOnCatalog || managedLocationOnSchema) {
      RegisteredModelInfo modelInfo = modelOperations.createRegisteredModel(createModel);
      assertThat(modelInfo.getName()).isEqualTo(MANAGED_MODEL_NAME);
      assertThat(modelInfo.getStorageLocation()).isNotNull();
      // Model should be under the catalog/schema's storage location
      assertThat(modelInfo.getStorageLocation())
          .startsWith(
              expectedStorageRoot(managedLocationOnCatalog, managedLocationOnSchema)
                  + "/models/"
                  + modelInfo.getId());

      // Create a model version and verify its storage location
      CreateModelVersion createModelVersion =
          new CreateModelVersion()
              .catalogName(CATALOG_NAME)
              .schemaName(SCHEMA_NAME)
              .modelName(MANAGED_MODEL_NAME)
              .comment("Test model version")
              .source("test/source/path");

      ModelVersionInfo modelVersionInfo = modelOperations.createModelVersion(createModelVersion);
      assertThat(modelVersionInfo.getVersion()).isEqualTo(1L);
      assertThat(modelVersionInfo.getStorageLocation()).isNotNull();
      // Model version should be under the model's storage location
      assertThat(modelVersionInfo.getStorageLocation())
          .startsWith(modelInfo.getStorageLocation() + "/versions/" + modelVersionInfo.getId());
    } else {
      // Creating a model should fail because there's no storage location configured
      TestUtils.assertApiException(
          () -> modelOperations.createRegisteredModel(createModel),
          ErrorCode.FAILED_PRECONDITION,
          "Neither catalog nor schema has managed location configured");
    }
  }
}
