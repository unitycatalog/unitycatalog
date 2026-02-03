package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * This test suite runs all tests in ExternalTableReadWriteTest plus extra test. Tests that are only
 * for Delta external tables should live here.
 */
public class DeltaExternalTableReadWriteTest extends ExternalTableReadWriteTest {

  @Test
  public void testDeltaPathTable() throws IOException {
    // We must replace the `spark_catalog` in order to support Delta path tables.
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    int tableCounter = 0;
    for (boolean ctas : List.of(true, false)) {
      TableSetupOptions options = new TableSetupOptions();
      if (ctas) {
        options.setAsSelect(1, "a");
      }
      String path = new File(dataDir, "test_delta_path" + tableCounter).getCanonicalPath();
      String tableName = String.format("delta.`%s`", path);
      tableCounter++;
      sql(options.createDeltaPathTableSql(path));
      if (ctas) {
        testTableReadWriteCreatedAsSelect(tableName, Pair.of(1, "a"));
      } else {
        testTableReadWrite(tableName);
      }
    }
  }

  @Override
  protected String tableFormat() {
    return "DELTA";
  }

  /** Creates a SparkSession with SSP (server-side planning) optionally enabled for the catalog. */
  private SparkSession createSparkSessionWithSsp(boolean sspEnabled) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("test-ssp")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog");

    // Configure the UC catalog
    String catalogConf = "spark.sql.catalog." + CATALOG_NAME;
    builder =
        builder
            .config(catalogConf, UCSingleCatalog.class.getName())
            .config(catalogConf + "." + OptionsUtil.URI, serverConfig.getServerUrl())
            .config(catalogConf + "." + OptionsUtil.TOKEN, serverConfig.getAuthToken())
            .config(catalogConf + "." + OptionsUtil.WAREHOUSE, CATALOG_NAME);
    if (sspEnabled) {
      builder =
          builder.config(catalogConf + "." + OptionsUtil.SERVER_SIDE_PLANNING_ENABLED, "true");
    }

    // Use fake file system for cloud storage so that we can test credentials.
    builder.config("fs.s3.impl", S3CredentialTestFileSystem.class.getName());
    builder.config("fs.gs.impl", GCSCredentialTestFileSystem.class.getName());
    builder.config("fs.abfs.impl", AzureCredentialTestFileSystem.class.getName());
    return builder.getOrCreate();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testServerSidePlanningCredentialFallback(boolean sspEnabled) throws Exception {
    // Create an EXTERNAL table via SDK API (bypasses Spark connector's credential check).
    // The table points to a bucket with no credentials configured on server.
    // This allows table metadata lookup to succeed, but credential vending will fail.
    // Pattern from SdkTemporaryTableCredentialTest.
    String tableName = "test_ssp_fallback_" + sspEnabled;
    String noCredsLocation = "s3://" + NO_CREDS_BUCKET + "/" + tableName;

    TableOperations tableOperations = new SdkTableOperations(createApiClient(serverConfig));
    io.unitycatalog.server.base.table.BaseTableCRUDTest.createTestingTable(
        tableName, TableType.EXTERNAL, Optional.of(noCredsLocation), tableOperations);

    // Close existing session and create one with SSP setting
    if (session != null) {
      session.close();
      session = null;
    }
    session = createSparkSessionWithSsp(sspEnabled);

    // Load table directly via catalog API (bypasses Spark analysis/Delta data access)
    CatalogPlugin catalogPlugin = session.sessionState().catalogManager().catalog(CATALOG_NAME);
    TableCatalog tableCatalog = (TableCatalog) catalogPlugin;
    Identifier tableId = Identifier.of(new String[] {SCHEMA_NAME}, tableName);

    // Capture any exception thrown when loading the table
    Exception caughtException = null;
    Table loadedTable = null;
    try {
      loadedTable = tableCatalog.loadTable(tableId);
    } catch (Exception e) {
      caughtException = e;
    }

    if (!sspEnabled) {
      // SSP disabled (default): loadTable() throws ApiException because credential API fails
      assertThat(caughtException).isInstanceOf(ApiException.class);
    } else {
      // SSP enabled: loadTable() succeeds with empty credentials (no ApiException)
      // The SSP fallback logs: "Server-side planning enabled... Proceeding with empty credentials"
      assertThat(caughtException).isNull();
      assertThat(loadedTable).isNotNull();
    }
  }
}
