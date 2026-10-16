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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

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

  private void rename(String table, String newName) throws ApiException {
    deltaTablesApi.renameTable(
        TestUtils.CATALOG_NAME,
        TestUtils.SCHEMA_NAME,
        table,
        new DeltaRenameTableRequest().newName(newName));
  }

  private ApiResponse<Void> renameWithHttpInfo(String table, String newName) throws ApiException {
    return deltaTablesApi.renameTableWithHttpInfo(
        TestUtils.CATALOG_NAME,
        TestUtils.SCHEMA_NAME,
        table,
        new DeltaRenameTableRequest().newName(newName));
  }

  private static String fullName(String table) {
    return TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + table;
  }

  /** Missing or blank target names return 400 before the source lookup. */
  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t"})
  public void testRenameRejectsBlankNewName(String newName) {
    assertThatThrownBy(() -> rename("renameSource", newName))
        .isInstanceOfSatisfying(
            ApiException.class,
            e -> {
              assertThat(e.getCode()).isEqualTo(400);
              assertThat(e.getMessage()).contains("New table name is required");
            });
  }

  @Test
  public void testRenameTableEndpoint() throws Exception {
    String source = "renameSource";
    String existingTarget = "existingTarget";
    String missingSource = "missingSource";
    String unusedTarget = "unusedTarget";

    // A missing source returns 404 whether the target is unused, unchanged, or already exists.
    createDeltaManaged(existingTarget, Map.of());
    for (String newName : new String[] {unusedTarget, missingSource, existingTarget}) {
      TestUtils.assertDeltaApiException(
          () -> rename(missingSource, newName),
          DeltaErrorType.NO_SUCH_TABLE_EXCEPTION,
          "Table not found");
    }

    createDeltaManaged(source, Map.of());
    DeltaLoadTableResponse originalTable =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, source);

    // Renaming to an existing target returns 409.
    TestUtils.assertDeltaApiException(
        () -> rename(source, existingTarget),
        DeltaErrorType.ALREADY_EXISTS_EXCEPTION,
        "already exists");

    // Renaming a table to its current name is a no-op.
    ApiResponse<Void> noOpResponse = renameWithHttpInfo(source, source);
    assertThat(noOpResponse.getStatusCode()).isEqualTo(204);

    DeltaLoadTableResponse afterNoOp =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, source);
    assertTableStateUnchanged(originalTable, afterNoOp);
    assertThat(afterNoOp.getMetadata().getUpdatedTime())
        .isEqualTo(originalTable.getMetadata().getUpdatedTime());
    assertThat(afterNoOp.getMetadata().getEtag()).isEqualTo(originalTable.getMetadata().getEtag());

    // A successful rename preserves table state and removes the old name.
    ApiResponse<Void> renameResponse = renameWithHttpInfo(source, unusedTarget);
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

    rename(external.name(), "renameExternalTarget");

    assertTableStateUnchanged(
        original,
        deltaTablesApi.loadTable(
            TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, "renameExternalTarget"));
  }

  /** Kinds of non-Delta source the Delta rename endpoint must reject. */
  enum NonDeltaSource {
    PARQUET("Table is not a Delta table"),
    ICEBERG("Table is not a Delta table"),
    VIEW("Table is not a Delta table"),
    // UC REST accepts a view that claims the DELTA format; the type check must still reject it.
    DELTA_FORMAT_VIEW("Only MANAGED and EXTERNAL tables can be renamed");

    final String expectedMessage;

    NonDeltaSource(String expectedMessage) {
      this.expectedMessage = expectedMessage;
    }
  }

  private String createNonDeltaSource(NonDeltaSource kind) throws Exception {
    String name = "rename_" + kind.name().toLowerCase();
    switch (kind) {
      case PARQUET ->
          createTestingTable(
              name,
              TableType.EXTERNAL,
              Optional.of(Files.createTempDirectory(testDirectoryRoot, "parquet").toString()),
              DataSourceFormat.PARQUET,
              tableOperations);
      case ICEBERG -> {
        // UC REST refuses native Iceberg creates; an Iceberg row comes from Iceberg REST.
        new IcebergRestClient(serverConfig)
            .createTable(
                TestUtils.CATALOG_NAME,
                TestUtils.SCHEMA_NAME,
                CreateTableRequest.builder()
                    .withName(name)
                    .withSchema(
                        new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
                    .withLocation(
                        Files.createTempDirectory(testDirectoryRoot, "iceberg").toUri().toString())
                    .build());
        assertThat(tableOperations.getTable(fullName(name)).getDataSourceFormat())
            .isEqualTo(DataSourceFormat.ICEBERG);
      }
      case VIEW -> createView(name, null);
      case DELTA_FORMAT_VIEW ->
          assertThat(createView(name, DataSourceFormat.DELTA).getDataSourceFormat())
              .isEqualTo(DataSourceFormat.DELTA);
    }
    return name;
  }

  private TableInfo createView(String name, DataSourceFormat format) throws Exception {
    return tableOperations.createTable(
        new CreateTable()
            .name(name)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(COLUMNS)
            .tableType(TableType.VIEW)
            .dataSourceFormat(format)
            .viewDefinition("SELECT 1 AS id"));
  }

  @ParameterizedTest
  @EnumSource(NonDeltaSource.class)
  public void testRenameRejectsNonDeltaSource(NonDeltaSource kind) throws Exception {
    String source = createNonDeltaSource(kind);
    // A Delta table occupying a target name: the format check must win over the collision check.
    createDeltaManaged("renameExistingDelta", Map.of());

    for (String newName : new String[] {"renameUnusedTarget", source, "renameExistingDelta"}) {
      TestUtils.assertDeltaApiException(
          () -> rename(source, newName),
          DeltaErrorType.UNSUPPORTED_TABLE_FORMAT_EXCEPTION,
          kind.expectedMessage);
    }
    // The rejected row keeps its name.
    assertThat(tableOperations.getTable(fullName(source)).getName()).isEqualTo(source);
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
