package io.unitycatalog.server.sdk.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.ApiResponse;
import io.unitycatalog.client.delta.model.DeltaErrorType;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.client.delta.model.DeltaRenameTableRequest;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.delta.DeltaBaseTableCRUDTestEnv;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.TestUtils;
import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Schema;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * E2E tests for the Delta REST Catalog {@code POST .../rename} endpoint (204 on success, 404 when
 * the source table is missing, 409 when the target name already exists, 400 when the source is not
 * a Delta table).
 */
public class SdkRenameTableTest extends DeltaBaseTableCRUDTestEnv {

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
    // Needed to plant non-Delta rows (parquet, Iceberg, view) through the UC REST API.
    return new SdkTableOperations(TestUtils.createApiClient(serverConfig));
  }

  @Test
  public void testRenameTableEndpoint() throws Exception {
    String source = "renameSource";
    String existingTarget = "existingTarget";
    String missingSource = "missingSource";
    String unusedTarget = "unusedTarget";

    // Missing or blank target names return 400 before source lookup.
    for (String newName : new String[] {null, "", " ", "\t"}) {
      assertThatThrownBy(
              () ->
                  deltaTablesApi.renameTable(
                      TestUtils.CATALOG_NAME,
                      TestUtils.SCHEMA_NAME,
                      source,
                      new DeltaRenameTableRequest().newName(newName)))
          .isInstanceOfSatisfying(
              ApiException.class,
              e -> {
                assertThat(e.getCode()).isEqualTo(400);
                assertThat(e.getMessage()).contains("New table name is required");
              });
    }

    // A missing source returns 404 whether the target is unused, unchanged, or already exists.
    createDeltaManaged(existingTarget, Map.of());
    for (String newName : new String[] {unusedTarget, missingSource, existingTarget}) {
      TestUtils.assertDeltaApiException(
          () ->
              deltaTablesApi.renameTable(
                  TestUtils.CATALOG_NAME,
                  TestUtils.SCHEMA_NAME,
                  missingSource,
                  new DeltaRenameTableRequest().newName(newName)),
          DeltaErrorType.NO_SUCH_TABLE_EXCEPTION,
          "Table not found");
    }

    createDeltaManaged(source, Map.of());
    DeltaLoadTableResponse originalTable =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, source);

    // Renaming to an existing target returns 409.
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.renameTable(
                TestUtils.CATALOG_NAME,
                TestUtils.SCHEMA_NAME,
                source,
                new DeltaRenameTableRequest().newName(existingTarget)),
        DeltaErrorType.ALREADY_EXISTS_EXCEPTION,
        "already exists");

    // Renaming a table to its current name is a no-op.
    ApiResponse<Void> noOpResponse =
        deltaTablesApi.renameTableWithHttpInfo(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            source,
            new DeltaRenameTableRequest().newName(source));
    assertThat(noOpResponse.getStatusCode()).isEqualTo(204);

    DeltaLoadTableResponse afterNoOp =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, source);
    assertTableStateUnchanged(originalTable, afterNoOp);
    assertThat(afterNoOp.getMetadata().getUpdatedTime())
        .isEqualTo(originalTable.getMetadata().getUpdatedTime());
    assertThat(afterNoOp.getMetadata().getEtag()).isEqualTo(originalTable.getMetadata().getEtag());

    // A successful rename preserves table state and removes the old name.
    ApiResponse<Void> renameResponse =
        deltaTablesApi.renameTableWithHttpInfo(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            source,
            new DeltaRenameTableRequest().newName(unusedTarget));
    assertThat(renameResponse.getStatusCode()).isEqualTo(204);

    DeltaLoadTableResponse renamedTable =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, unusedTarget);
    assertTableStateUnchanged(afterNoOp, renamedTable);

    TestUtils.assertDeltaApiException(
        () -> deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, source),
        DeltaErrorType.NO_SUCH_TABLE_EXCEPTION,
        "Table not found");
  }

  @Test
  public void testRenameExternalDeltaTable() throws Exception {
    Handle external = createDeltaExternal("renameExternalSource");
    DeltaLoadTableResponse original =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, external.name());

    deltaTablesApi.renameTable(
        TestUtils.CATALOG_NAME,
        TestUtils.SCHEMA_NAME,
        external.name(),
        new DeltaRenameTableRequest().newName("renameExternalTarget"));

    assertTableStateUnchanged(
        original,
        deltaTablesApi.loadTable(
            TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, "renameExternalTarget"));
  }

  @Test
  public void testRenameRejectsNonDeltaSource() throws Exception {
    TableInfo parquet =
        createTestingTable(
            "renameParquet",
            TableType.EXTERNAL,
            Optional.of(Files.createTempDirectory(testDirectoryRoot, "parquet").toString()),
            DataSourceFormat.PARQUET,
            tableOperations);
    // UC REST refuses native Iceberg creates; an Iceberg row comes from the Iceberg REST catalog.
    new IcebergRestClient(serverConfig)
        .createTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            CreateTableRequest.builder()
                .withName("renameIceberg")
                .withSchema(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
                .withLocation(
                    Files.createTempDirectory(testDirectoryRoot, "iceberg").toUri().toString())
                .build());
    TableInfo iceberg =
        tableOperations.getTable(
            TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + ".renameIceberg");
    assertThat(iceberg.getDataSourceFormat()).isEqualTo(DataSourceFormat.ICEBERG);
    TableInfo view =
        tableOperations.createTable(
            new CreateTable()
                .name("renameView")
                .catalogName(TestUtils.CATALOG_NAME)
                .schemaName(TestUtils.SCHEMA_NAME)
                .columns(COLUMNS)
                .tableType(TableType.VIEW)
                .viewDefinition("SELECT 1 AS id"));
    // UC REST accepts a view that claims the DELTA format; the type check must still reject it.
    TableInfo deltaFormatView =
        tableOperations.createTable(
            new CreateTable()
                .name("renameDeltaFormatView")
                .catalogName(TestUtils.CATALOG_NAME)
                .schemaName(TestUtils.SCHEMA_NAME)
                .columns(COLUMNS)
                .tableType(TableType.VIEW)
                .dataSourceFormat(DataSourceFormat.DELTA)
                .viewDefinition("SELECT 1 AS id"));
    assertThat(deltaFormatView.getDataSourceFormat()).isEqualTo(DataSourceFormat.DELTA);
    // A Delta table occupying the target name: the format check must win over the collision check.
    createDeltaManaged("renameExistingDelta", Map.of());

    for (TableInfo source : new TableInfo[] {parquet, iceberg, view, deltaFormatView}) {
      String expectedMessage =
          source == deltaFormatView
              ? "Only MANAGED and EXTERNAL tables can be renamed"
              : "Table is not a Delta table";
      for (String newName : new String[] {"renameUnusedTarget", source.getName()}) {
        TestUtils.assertDeltaApiException(
            () ->
                deltaTablesApi.renameTable(
                    TestUtils.CATALOG_NAME,
                    TestUtils.SCHEMA_NAME,
                    source.getName(),
                    new DeltaRenameTableRequest().newName(newName)),
            DeltaErrorType.UNSUPPORTED_TABLE_FORMAT_EXCEPTION,
            expectedMessage);
      }
      TestUtils.assertDeltaApiException(
          () ->
              deltaTablesApi.renameTable(
                  TestUtils.CATALOG_NAME,
                  TestUtils.SCHEMA_NAME,
                  source.getName(),
                  new DeltaRenameTableRequest().newName("renameExistingDelta")),
          DeltaErrorType.UNSUPPORTED_TABLE_FORMAT_EXCEPTION,
          expectedMessage);
      // The rejected row keeps its name.
      String fullName =
          TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + source.getName();
      assertThat(tableOperations.getTable(fullName).getName()).isEqualTo(source.getName());
    }
  }

  private static void assertTableStateUnchanged(
      DeltaLoadTableResponse originalTable, DeltaLoadTableResponse currentTable) {
    assertThat(currentTable.getMetadata().getTableUuid())
        .isEqualTo(originalTable.getMetadata().getTableUuid());
    assertThat(currentTable.getMetadata().getLocation())
        .isEqualTo(originalTable.getMetadata().getLocation());
    assertThat(currentTable.getMetadata().getCreatedTime())
        .isEqualTo(originalTable.getMetadata().getCreatedTime());
    assertThat(currentTable.getMetadata().getProperties())
        .isEqualTo(originalTable.getMetadata().getProperties());
    assertThat(currentTable.getCommits()).isEqualTo(originalTable.getCommits());
    assertThat(currentTable.getLatestTableVersion())
        .isEqualTo(originalTable.getLatestTableVersion());
  }
}
