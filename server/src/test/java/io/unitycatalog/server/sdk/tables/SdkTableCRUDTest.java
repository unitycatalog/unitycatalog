package io.unitycatalog.server.sdk.tables;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTest;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SdkTableCRUDTest extends BaseTableCRUDTest {

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new SdkSchemaOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig config) {
    localTablesApi = new TablesApi(TestUtils.createApiClient(config));
    return new SdkTableOperations(TestUtils.createApiClient(config));
  }

  /**
   * The SDK tests elide the `Response` object and directly return the model object. However,
   * clients can interact with the REST API directly, so its signature needs to be tested as well.
   *
   * <p>The tests below use a direct `TableAPI` client to inspect the response object.
   */
  private TablesApi localTablesApi;

  @Test
  public void testListTablesWithNoNextPageTokenShouldReturnNull() throws Exception {
    createCommonResources();
    TableInfo testingTable =
        createTestingTable(TestUtils.TABLE_NAME, TestUtils.STORAGE_LOCATION, tableOperations);
    ListTablesResponse resp =
        localTablesApi.listTables(
            testingTable.getCatalogName(), testingTable.getSchemaName(), 100, null);
    assertThat(resp.getNextPageToken()).isNull();
    assertThat(resp.getTables())
        .hasSize(1)
        .first()
        .usingRecursiveComparison()
        .ignoringFields("columns", "storageLocation")
        .isEqualTo(testingTable);
  }

  @Test
  public void testListTablesWithNextPageTokenShouldReturnNextPageToken() throws Exception {
    createCommonResources();
    List<TableInfo> testingTables = createMultipleTestingTables(11);
    ListTablesResponse resp =
        localTablesApi.listTables(
            testingTables.get(0).getCatalogName(), testingTables.get(0).getSchemaName(), 10, null);
    assertThat(resp.getNextPageToken()).isNotNull();
    assertThat(resp.getTables()).hasSize(10);
    // Check the next page has the last table
    ListTablesResponse nextPageResp =
        localTablesApi.listTables(
            testingTables.get(0).getCatalogName(),
            testingTables.get(0).getSchemaName(),
            10,
            resp.getNextPageToken());
    assertThat(nextPageResp.getNextPageToken()).isNull();
    assertThat(nextPageResp.getTables()).hasSize(1);
  }
}
