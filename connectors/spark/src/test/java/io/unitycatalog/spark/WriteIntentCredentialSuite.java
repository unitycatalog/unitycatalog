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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the credential-operation choice in {@code UCProxy.loadV1Table}: a load with
 * declared write intent (Spark's {@code loadTable(ident, writePrivileges)}, recorded in {@code
 * UCSingleCatalog.WRITE_INTENT}) requests READ_WRITE and surfaces a denial immediately, while
 * intent-less loads remember a READ_WRITE denial per table and go straight to READ afterwards
 * instead of re-paying the denied round-trip on every query.
 *
 * <p>The UC credential endpoint is stubbed at the hadoop connector's {@link
 * CredPropsUtil#genericCredFetcherFactory} seam, which sees the requested {@link TableOperation}
 * via {@link TableCredId} and can grant or deny per operation.
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

    // First intent-less load: READ_WRITE is attempted once, denied, and remembered; the load
    // still succeeds on READ credentials.
    proxy.loadTable(Identifier.of(NAMESPACE, "t_denied"));
    assertThat(readWriteAttempts.get()).isEqualTo(1);
    assertThat(readAttempts.get()).isEqualTo(1);
    assertThat(writeDeniedTables()).containsExactly(tableId);

    // Subsequent loads skip the guaranteed READ_WRITE denial entirely.
    proxy.loadTable(Identifier.of(NAMESPACE, "t_denied"));
    assertThat(readWriteAttempts.get()).isEqualTo(1);
  }

  @Test
  public void declaredWriteFailsFastOnDenialWithoutReadFallback() throws Exception {
    stubTable("t_write_denied");
    denyReadWriteGrantRead();

    UCSingleCatalog$.MODULE$.WRITE_INTENT().set(true);
    // Vending READ credentials to a declared write would only defer the same failure to the
    // storage layer mid-job, so the denial must surface here and READ must not be attempted.
    assertThatThrownBy(() -> proxy.loadTable(Identifier.of(NAMESPACE, "t_write_denied")))
        .isInstanceOf(ApiException.class);
    assertThat(readWriteAttempts.get()).isEqualTo(1);
    assertThat(readAttempts.get()).isEqualTo(0);
  }

  @Test
  public void successfulDeclaredWriteClearsDenialMemory() throws Exception {
    String tableId = stubTable("t_regranted");
    denyReadWriteGrantRead();

    // Prime the denial memory through an intent-less load.
    proxy.loadTable(Identifier.of(NAMESPACE, "t_regranted"));
    assertThat(writeDeniedTables()).containsExactly(tableId);

    // The principal is granted write access; a declared write still requests READ_WRITE (the
    // denial memory never gates declared writes) and, once granted, clears the memory so
    // intent-less loads return to the READ_WRITE-first path.
    grantAllOperations();
    UCSingleCatalog$.MODULE$.WRITE_INTENT().set(true);
    proxy.loadTable(Identifier.of(NAMESPACE, "t_regranted"));
    assertThat(writeDeniedTables()).isEmpty();
  }

  /** Registers a mock UC table with an s3 location and returns its unique table id. */
  private String stubTable(String name) throws Exception {
    // Unique per test method: the hadoop connector's credential cache is JVM-global and keyed by
    // (context, tableId, operation), so reusing ids would leak cached grants across tests.
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
    stubFetcher(/* denyReadWrite= */ true);
  }

  private void grantAllOperations() {
    stubFetcher(/* denyReadWrite= */ false);
  }

  private void stubFetcher(boolean denyReadWrite) {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            () -> {
              String operation = ((TableCredId) credId).tableOperation();
              if (TableOperation.READ_WRITE.value().equals(operation)) {
                readWriteAttempts.incrementAndGet();
                if (denyReadWrite) {
                  throw new ApiException("PERMISSION_DENIED: cannot vend READ_WRITE credential");
                }
              } else {
                readAttempts.incrementAndGet();
              }
              return List.of(
                  new AwsCredential("access-key", "secret-key", "session-token", null, LOCATION));
            };
  }

  @SuppressWarnings("unchecked")
  private java.util.Set<String> writeDeniedTables() throws Exception {
    // UCProxy is package-private Scala; read the tracked set through its generated accessor.
    return (java.util.Set<String>)
        fixture.proxyObj.getClass().getMethod("writeDeniedTables").invoke(fixture.proxyObj);
  }
}
