package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.provider.Arguments;

public abstract class ExternalTableReadWriteTest extends BaseTableReadWriteTest {
  @TempDir protected File dataDir;

  // CredentialTestFileSystem provides two test buckets test-bucket0 and test-bucket1.
  // Tests in this class wants to use them alternating. So this variable will be changing between
  // 0 and 1 to construct the bucket name.
  private int bucketIndex = 0;

  private TableOperations tableOperations;

  /**
   * This function provides a set of test parameters that cloud-aware tests should run for this
   * class.
   *
   * @return A stream of Arguments.of(String scheme, boolean renewCredEnabled)
   */
  protected static Stream<Arguments> cloudParameters() {
    return Stream.of(
        Arguments.of("file", false),
        Arguments.of("s3", false),
        Arguments.of("s3", true),
        Arguments.of("gs", false),
        Arguments.of("abfs", false),
        Arguments.of("abfs", true));
  }

  @Test
  @DisabledIf("testingDelta")
  public void testNoDeltaCatalog() {
    UCSingleCatalog.LOAD_DELTA_CATALOG().set(false);
    UCSingleCatalog.DELTA_CATALOG_LOADED().set(false);
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    String fullTableName = setupTable(SPARK_CATALOG, TEST_TABLE);
    testTableReadWrite(fullTableName);
    assertThat(UCSingleCatalog.DELTA_CATALOG_LOADED().get()).isEqualTo(false);
    // LOAD_DELTA_CATALOG is reset in cleanUp() just in case this test fails it will still be
    // reset.
  }

  @Test
  public void testCreateExternalTable() throws ApiException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    int tableNameCounter = 0;
    for (boolean withExistingTable : List.of(true, false)) {
      if (withExistingTable && !testingDelta()) {
        // This is only doable with Delta
        continue;
      }
      for (boolean withPartitionColumns : List.of(true, false)) {
        for (boolean withCtas : List.of(true, false)) {
          for (String catalogName : List.of(SPARK_CATALOG, CATALOG_NAME)) {
            String tableName = TEST_TABLE + tableNameCounter;
            tableNameCounter++;
            SetupTableOptions options =
                new SetupTableOptions().setCatalogName(catalogName).setTableName(tableName);
            if (withPartitionColumns) {
              options.setPartitionColumn("s");
            }
            if (withCtas) {
              options.setAsSelect(1, "a");
            }

            String fullTableName;
            if (withExistingTable) {
              fullTableName = setupWithPathTable(options);
            } else {
              fullTableName = setupTable(options);
            }
            if (!withCtas) {
              validateTableEmpty(fullTableName);
              sql("INSERT INTO %s VALUES (1, 'a')", fullTableName);
            }
            validateRows(sql("SELECT * FROM %s", fullTableName), Pair.of(1, "a"));

            assertThat(session.catalog().tableExists(fullTableName)).isTrue();
            TableInfo tableInfo1 = tableOperations.getTable(fullTableName);
            if (testingDelta()) {
              // By default, Delta tables do not store schema in the catalog.
              assertThat(tableInfo1.getColumns()).isEmpty();
            } else {
              List<ColumnInfo> columns = tableInfo1.getColumns();
              assertThat(columns).hasSize(2);
              assertThat(columns.get(0).getName()).isEqualTo("i");
              assertThat(columns.get(0).getTypeName()).isEqualTo(ColumnTypeName.INT);
              assertThat(columns.get(1).getName()).isEqualTo("s");
              assertThat(columns.get(1).getTypeName()).isEqualTo(ColumnTypeName.STRING);
            }
            validateTableSchema(
                session.table(fullTableName).schema(),
                Pair.of("i", DataTypes.IntegerType),
                Pair.of("s", DataTypes.StringType));
          }
        }
      }
    }
  }

  @Test
  public void testCreateExternalTableWithoutLocation() throws IOException {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);

    String fullTableName = CATALOG_NAME + "." + SCHEMA_NAME + "." + TEST_TABLE;
    assertThatThrownBy(
            () ->
                sql("CREATE EXTERNAL TABLE %s(name STRING) USING %s", fullTableName, tableFormat()))
        .hasMessageContaining("Cannot create EXTERNAL TABLE without location");
  }

  /**
   * Returns an emulated cloud bucket to be used for creating external table. Alternating between
   * two configured test buckets
   */
  private String testBucket(String scheme) {
    bucketIndex++;
    bucketIndex %= 2;
    return String.format("%s://test-bucket%d", scheme, bucketIndex);
  }

  @SneakyThrows
  protected String getLocation(SetupTableOptions options) {
    String cloudPrefix =
        options.getCloudScheme().equals("file") ? "" : testBucket(options.getCloudScheme());
    String rawCatalogName = options.getCatalogName().replace("`", "");
    String rawTableName = options.getTableName().replace("`", "");
    String path = new File(new File(dataDir, rawCatalogName), rawTableName).getCanonicalPath();
    return cloudPrefix + path;
  }

  @Override
  protected String setupTable(SetupTableOptions options) {
    String location = getLocation(options);
    sql(options.createExternalTableSql(location));
    return options.fullTableName();
  }

  protected String setupWithPathTable(SetupTableOptions options) {
    String location = getLocation(options);
    sql(options.createDeltaPathTableSql(location));

    // The CTAS is already done in existing path table. Do not do it twice.
    SetupTableOptions optionsWithoutAsSelect = options.withAsSelect(Optional.empty());
    sql(optionsWithoutAsSelect.createExternalTableSql(location));
    return options.fullTableName();
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    tableOperations = new SdkTableOperations(createApiClient(serverConfig));
  }

  @AfterEach
  @Override
  public void cleanUp() {
    UCSingleCatalog.LOAD_DELTA_CATALOG().set(true);
    super.cleanUp();
  }
}
