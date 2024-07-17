package io.unitycatalog.server.sdk.tables;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTest;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

public class SdkTableCRUDTest extends BaseTableCRUDTest {

    @BeforeAll
    public static void setUpClass() {
        // Any static setup specific to this test class
    }

    @AfterAll
    public static void tearDownClass() {
        // Any static teardown specific to this test class
    }

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
     * The SDK tests elide the `Response` object and directly return the model object.
     * However, clients can interact with the REST API directly, so its signature needs to be tested as well.
     * <p>
     * The tests below use a direct `TableAPI` client to inspect the response object.
     */
    private TablesApi localTablesApi;

    @Test
    public void testListTablesWithNoNextPageTokenShouldReturnNull() throws Exception {
        createCommonResources();
        TableInfo testingTable = createDefaultTestingTable();
        ListTablesResponse resp = localTablesApi.listTables(testingTable.getCatalogName(), testingTable.getSchemaName(), 100, null);
        assert resp.getNextPageToken() == null;
        assert Objects.equals(resp.getTables(), List.of(testingTable));
    }
}
