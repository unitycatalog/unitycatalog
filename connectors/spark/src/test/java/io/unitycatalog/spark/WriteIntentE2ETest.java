package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs.TableOperation;
import io.unitycatalog.hadoop.internal.CredPropsUtil;
import io.unitycatalog.hadoop.internal.CredPropsUtil.GenericCredentialFetcherFactory;
import io.unitycatalog.hadoop.internal.auth.CredentialCache;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import io.unitycatalog.hadoop.internal.id.TableCredId;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end coverage for write-intent credential selection: real Spark SQL through {@code
 * UCSingleCatalog} -> DeltaCatalog -> {@code UCProxy} against a live UC server, with table
 * credential fetches recorded (and selectively denied) at the {@link
 * CredPropsUtil#genericCredFetcherFactory} seam. Complements {@code WriteIntentCredentialSuite},
 * which unit-tests the vend-site logic in isolation.
 */
public class WriteIntentE2ETest extends BaseSparkIntegrationTest {

  // The test filesystem maps s3://test-bucket0/<path> to file:///<path>, so locations must
  // embed an absolute local directory.
  @TempDir private File dataDir;

  /** One table-credential fetch: tableId + requested operation + write-intent flag at fetch. */
  private static final class Fetch {
    final String tableId;
    final String operation;
    final boolean writeIntent;

    Fetch(String tableId, String operation, boolean writeIntent) {
      this.tableId = tableId;
      this.operation = operation;
      this.writeIntent = writeIntent;
    }
  }

  private final List<Fetch> fetches = new CopyOnWriteArrayList<>();
  private final GenericCredentialFetcherFactory realFactory =
      CredPropsUtil.genericCredFetcherFactory;

  @AfterEach
  public void resetSeam() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
  }

  // With the UC Delta REST API enabled (Delta >= 4.3), table loads vend credentials through
  // delta-io's own client (DeltaTableCredId), not UCProxy.loadV1Table; intent selection there is
  // upstream work. These tests pin the path this connector owns by disabling the Delta REST API.
  private void disableDeltaRestApi() {
    session
        .conf()
        .set("spark.sql.catalog." + CATALOG_NAME + "." + OptionsUtil.DELTA_API_ENABLED, "false");
  }

  @Test
  public void sqlStatementsRequestCredentialsMatchingTheirIntent() {
    session = createSparkSessionWithCatalogs(false, false, SPARK_CATALOG, CATALOG_NAME);
    disableDeltaRestApi();
    installRecorder(/* denyReadWriteForTableIds= */ Set.of());

    String target = CATALOG_NAME + "." + SCHEMA_NAME + ".wi_target";
    String source = CATALOG_NAME + "." + SCHEMA_NAME + ".wi_source";
    sql(
        "CREATE TABLE %s (i INT, s STRING) USING delta LOCATION 's3://test-bucket0%s/wi_target'",
        target, dataDir);
    sql(
        "CREATE TABLE %s (i INT, s STRING) USING delta LOCATION 's3://test-bucket0%s/wi_source'",
        source, dataDir);
    sql("INSERT INTO %s VALUES (1, 'a')", source);

    assertStatementIntent(true, "INSERT INTO %s VALUES (1, 'a'), (2, 'b')", target);
    assertStatementIntent(false, "SELECT * FROM %s", target);
    assertStatementIntent(true, "UPDATE %s SET s = 'c' WHERE i = 1", target);
    assertStatementIntent(true, "DELETE FROM %s WHERE i = 2", target);

    // MERGE resolves the target with intent and the source without, in the same statement.
    fetches.clear();
    clearCredentialCache();
    sql(
        "MERGE INTO %s USING %s ON %s.i = %s.i WHEN NOT MATCHED THEN INSERT *",
        target, source, target, source);
    assertThat(intentsFor(tableIdOf(target))).contains(true);
    assertThat(intentsFor(tableIdOf(source))).containsOnly(false);
    assertThat(currentWriteIntent()).isFalse();
  }

  @Test
  public void readOnlyPrincipalReadsViaFallbackAndWritesFailFast() {
    session = createSparkSessionWithCatalogs(false, false, SPARK_CATALOG, CATALOG_NAME);
    disableDeltaRestApi();
    installRecorder(Set.of());

    String table = CATALOG_NAME + "." + SCHEMA_NAME + ".wi_readonly";
    sql(
        "CREATE TABLE %s (i INT, s STRING) USING delta LOCATION 's3://test-bucket0%s/wi_ro'",
        table, dataDir);
    sql("INSERT INTO %s VALUES (1, 'a')", table);
    String tableId = tableIdOf(table);

    // From here on the "server" denies READ_WRITE for this table, as it would for a principal
    // holding only SELECT.
    installRecorder(Set.of(tableId));

    // SELECT: denied READ_WRITE, then READ succeeds and the query returns data.
    fetches.clear();
    clearCredentialCache();
    List<Row> rows = sql("SELECT * FROM %s", table);
    assertThat(rows).hasSize(1);
    assertThat(operationsFor(tableId))
        .containsExactly(TableOperation.READ_WRITE.value(), TableOperation.READ.value());

    // A write on the denied table fails at analysis time, without a READ fallback fetch.
    fetches.clear();
    clearCredentialCache();
    assertThatThrownBy(() -> sql("INSERT INTO %s VALUES (2, 'b')", table))
        .hasMessageContaining("SIMULATED_DENIAL");
    assertThat(operationsFor(tableId)).containsOnly(TableOperation.READ_WRITE.value());
    assertThat(currentWriteIntent()).isFalse();

    // Once the "grant" returns, the declared write succeeds.
    installRecorder(Set.of());
    fetches.clear();
    clearCredentialCache();
    sql("INSERT INTO %s VALUES (3, 'c')", table);
    assertThat(operationsFor(tableId)).containsOnly(TableOperation.READ_WRITE.value());
  }

  @Test
  public void parquetV1WritePathDeclaresWriteIntent() {
    session = createSparkSessionWithCatalogs(false, false, SPARK_CATALOG, CATALOG_NAME);
    installRecorder(Set.of());

    String table = CATALOG_NAME + "." + SCHEMA_NAME + ".wi_parquet";
    sql(
        "CREATE TABLE %s (i INT, s STRING) USING parquet LOCATION 's3://test-bucket0%s/wi_pq'",
        table, dataDir);

    assertStatementIntent(true, "INSERT INTO %s VALUES (1, 'a')", table);
    assertStatementIntent(false, "SELECT * FROM %s", table);
  }

  /** Runs the statement and asserts the write-intent flag observed at every credential fetch. */
  private void assertStatementIntent(boolean expectedIntent, String statement, Object... args) {
    fetches.clear();
    clearCredentialCache();
    sql(statement, args);
    assertThat(fetches).isNotEmpty();
    assertThat(fetches.stream().map(f -> f.writeIntent).collect(Collectors.toSet()))
        .containsExactly(expectedIntent);
    assertThat(currentWriteIntent()).isFalse();
  }

  /**
   * Records every table-credential fetch and denies READ_WRITE for the given table ids; all other
   * fetches are served by the real factory against the live server.
   */
  private void installRecorder(Set<String> denyReadWriteForTableIds) {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          if (!(credId instanceof TableCredId)) {
            return realFactory.create(apiClient, credId);
          }
          TableCredId tableCredId = (TableCredId) credId;
          fetches.add(
              new Fetch(tableCredId.tableId(), tableCredId.tableOperation(), currentWriteIntent()));
          return () -> {
            if (denyReadWriteForTableIds.contains(tableCredId.tableId())
                && TableOperation.READ_WRITE.value().equals(tableCredId.tableOperation())) {
              throw new ApiException(403, "SIMULATED_DENIAL: READ_WRITE not permitted");
            }
            return realFactory.create(apiClient, credId).createCredentials();
          };
        };
  }

  private static boolean currentWriteIntent() {
    return (Boolean) UCSingleCatalog$.MODULE$.WRITE_INTENT().get();
  }

  /** Clears the JVM-global credential cache so the next load reaches the recording seam. */
  private static void clearCredentialCache() {
    try {
      java.lang.reflect.Field cacheField = CredPropsUtil.class.getDeclaredField("initialCredCache");
      cacheField.setAccessible(true);
      ((CredentialCache<?, ?>) cacheField.get(null)).clear();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private String tableIdOf(String fullName) {
    try {
      TableOperations tableOperations =
          new SdkTableOperations(TestUtils.createApiClient(serverConfig));
      return tableOperations.getTable(fullName).getTableId();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<Boolean> intentsFor(String tableId) {
    return fetches.stream()
        .filter(f -> f.tableId.equals(tableId))
        .map(f -> f.writeIntent)
        .collect(Collectors.toList());
  }

  private List<String> operationsFor(String tableId) {
    return fetches.stream()
        .filter(f -> f.tableId.equals(tableId))
        .map(f -> f.operation)
        .collect(Collectors.toList());
  }
}
