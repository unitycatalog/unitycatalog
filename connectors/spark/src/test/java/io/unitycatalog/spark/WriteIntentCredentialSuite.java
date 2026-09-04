package io.unitycatalog.spark;

import static io.unitycatalog.spark.UCProxyTestFixture.CATALOG_NAME;
import static io.unitycatalog.spark.UCProxyTestFixture.NAMESPACE;
import static io.unitycatalog.spark.UCProxyTestFixture.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs.TableOperation;
import io.unitycatalog.hadoop.internal.CredPropsUtil;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import io.unitycatalog.hadoop.internal.id.TableCredId;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the credential-operation choice in {@code UCProxy.loadV1Table}, with the UC
 * credential endpoint stubbed at the {@link CredPropsUtil#genericCredFetcherFactory} seam. The
 * end-to-end path (Spark SQL -> {@code UCSingleCatalog} -> DeltaCatalog -> vend site) is covered by
 * {@code WriteIntentE2ETest}.
 */
public class WriteIntentCredentialSuite {

  private static final String LOCATION = "s3://test-bucket/tables/t";

  private UCProxyTestFixture fixture;
  private TableCatalog proxy;
  private final AtomicInteger readWriteAttempts = new AtomicInteger();
  private final AtomicInteger readAttempts = new AtomicInteger();

  @BeforeEach
  public void setUp() throws Exception {
    fixture = new UCProxyTestFixture().build();
    proxy = fixture.proxy;
    readWriteAttempts.set(0);
    readAttempts.set(0);
  }

  @AfterEach
  public void reset() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
    UCSingleCatalog$.MODULE$.WRITE_INTENT().remove();
  }

  @Test
  public void intentLessLoadRemembersWriteDenialAndSkipsRetry() throws Exception {
    String tableId = stubTable("t_denied");
    denyReadWriteGrantRead();

    proxy.loadTable(Identifier.of(NAMESPACE, "t_denied"));
    assertThat(readWriteAttempts.get()).isEqualTo(1);
    assertThat(readAttempts.get()).isEqualTo(1);
    assertThat(writeDeniedTables()).containsOnlyKeys(tableId);

    // Subsequent loads must not retry the denied READ_WRITE.
    proxy.loadTable(Identifier.of(NAMESPACE, "t_denied"));
    assertThat(readWriteAttempts.get()).isEqualTo(1);
  }

  @Test
  public void declaredWriteFailsFastOnDenialWithoutReadFallback() throws Exception {
    stubTable("t_write_denied");
    denyReadWriteGrantRead();

    UCSingleCatalog$.MODULE$.WRITE_INTENT().set(true);
    assertThatThrownBy(() -> proxy.loadTable(Identifier.of(NAMESPACE, "t_write_denied")))
        .isInstanceOf(ApiException.class);
    assertThat(readWriteAttempts.get()).isEqualTo(1);
    assertThat(readAttempts.get()).isEqualTo(0);
  }

  @Test
  public void successfulDeclaredWriteClearsDenialMemory() throws Exception {
    String tableId = stubTable("t_regranted");
    denyReadWriteGrantRead();

    proxy.loadTable(Identifier.of(NAMESPACE, "t_regranted"));
    assertThat(writeDeniedTables()).containsOnlyKeys(tableId);

    grantAllOperations();
    UCSingleCatalog$.MODULE$.WRITE_INTENT().set(true);
    proxy.loadTable(Identifier.of(NAMESPACE, "t_regranted"));
    assertThat(writeDeniedTables()).isEmpty();
  }

  @Test
  public void transientReadWriteFailureDoesNotPoisonDenialMemory() throws Exception {
    stubTable("t_transient");
    AtomicInteger calls = new AtomicInteger();
    // First READ_WRITE attempt fails with a 503; everything afterwards is granted.
    stubFetcher(() -> calls.getAndIncrement() == 0 ? 503 : 0);

    proxy.loadTable(Identifier.of(NAMESPACE, "t_transient"));
    assertThat(writeDeniedTables()).isEmpty();

    // The next load must retry READ_WRITE instead of trusting the transient failure.
    proxy.loadTable(Identifier.of(NAMESPACE, "t_transient"));
    assertThat(readWriteAttempts.get()).isEqualTo(2);
  }

  @Test
  public void declaredWriteDenialSeedsMemoryForLaterReads() throws Exception {
    String tableId = stubTable("t_write_seeds");
    denyReadWriteGrantRead();

    UCSingleCatalog$.MODULE$.WRITE_INTENT().set(true);
    assertThatThrownBy(() -> proxy.loadTable(Identifier.of(NAMESPACE, "t_write_seeds")))
        .isInstanceOf(ApiException.class);
    UCSingleCatalog$.MODULE$.WRITE_INTENT().remove();
    assertThat(writeDeniedTables()).containsOnlyKeys(tableId);

    // The failed write already proved the denial; reads skip the doomed round-trip.
    proxy.loadTable(Identifier.of(NAMESPACE, "t_write_seeds"));
    assertThat(readWriteAttempts.get()).isEqualTo(1);
  }

  @Test
  public void expiredDenialEntryRestoresReadWriteFirstProbing() throws Exception {
    String tableId = stubTable("t_expired");
    denyReadWriteGrantRead();

    proxy.loadTable(Identifier.of(NAMESPACE, "t_expired"));
    assertThat(readWriteAttempts.get()).isEqualTo(1);

    // Simulate the TTL elapsing (e.g. a grant added later): the next load probes again.
    writeDeniedTables().put(tableId, 0L);
    proxy.loadTable(Identifier.of(NAMESPACE, "t_expired"));
    assertThat(readWriteAttempts.get()).isEqualTo(2);
  }

  /** Table ids are unique per test: the hadoop credential cache is JVM-global. */
  private String stubTable(String name) throws Exception {
    String tableId = "table-id-" + name;
    TableInfo ucTable =
        new TableInfo()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name(name)
            .tableId(tableId)
            .tableType(TableType.EXTERNAL)
            .storageLocation(LOCATION)
            .dataSourceFormat(DataSourceFormat.PARQUET)
            .columns(
                List.of(
                    new ColumnInfo()
                        .name("id")
                        .typeName(ColumnTypeName.INT)
                        .typeText("int")
                        .typeJson(
                            "{\"name\":\"id\",\"type\":\"integer\","
                                + "\"nullable\":true,\"metadata\":{}}")
                        .nullable(true)
                        .position(0)));
    when(fixture.mockTablesApi.getTable(
            eq(CATALOG_NAME + "." + SCHEMA_NAME + "." + name), eq(true), eq(true)))
        .thenReturn(ucTable);
    return tableId;
  }

  private void denyReadWriteGrantRead() {
    stubFetcher(() -> HttpURLConnection.HTTP_FORBIDDEN);
  }

  private void grantAllOperations() {
    stubFetcher(() -> 0);
  }

  /** Stubs the fetcher; a non-zero status from {@code denyRwStatus} fails READ_WRITE requests. */
  private void stubFetcher(IntSupplier denyRwStatus) {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            () -> {
              String operation = ((TableCredId) credId).tableOperation();
              if (TableOperation.READ_WRITE.value().equals(operation)) {
                readWriteAttempts.incrementAndGet();
                int status = denyRwStatus.getAsInt();
                if (status != 0) {
                  throw new ApiException(status, "cannot vend READ_WRITE credential");
                }
              } else {
                readAttempts.incrementAndGet();
              }
              return List.of(
                  new AwsCredential("access-key", "secret-key", "session-token", null, LOCATION));
            };
  }

  @SuppressWarnings("unchecked")
  private java.util.Map<String, Long> writeDeniedTables() throws Exception {
    return (java.util.Map<String, Long>)
        fixture.proxyObj.getClass().getMethod("writeDeniedTables").invoke(fixture.proxyObj);
  }
}
