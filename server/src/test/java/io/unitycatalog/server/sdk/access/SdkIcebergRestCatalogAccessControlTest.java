package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertApiExceptionStatusOnly;
import static io.unitycatalog.server.utils.TestUtils.assertIcebergApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Exhaustive access-control tests for the Iceberg REST catalog endpoints (all except {@code
 * updateTable}, still gated on metastore-owner), driving each through {@link IcebergRestClient}
 * (one per test user) to exercise every branch of its authorization expression. Resources are
 * created in create-then-use order; in-body section comments document each expression and case.
 */
public class SdkIcebergRestCatalogAccessControlTest extends SdkAccessControlBaseCRUDTest {

  private static final String CAT = TestUtils.CATALOG_NAME2;
  private static final String SCHEMA = TestUtils.SCHEMA_NAME2; // created by setup, owned by p1
  private static final String SCHEMA_P2 = "sch_p2"; // created via createNamespace, owned by p2
  private static final String SCHEMA_R2 = "sch_r2"; // created via createNamespace, owned by r2
  private static final String CREATOR_ONLY = "creator-only@localhost";

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    // Native Iceberg REST writes are opt-in in production; enable so the write endpoints work.
    serverProperties.setProperty(Property.ICEBERG_TABLE_ENABLED.getKey(), "true");
  }

  @Test
  @SneakyThrows
  public void testIcebergRestCatalogAccessControl() {
    createCommonTestUsers();
    createTestUser(CREATOR_ONLY);
    setupCommonCatalogAndSchema(CAT, SCHEMA); // principal-1 owns CAT + SCHEMA

    IcebergRestClient admin = new IcebergRestClient(adminConfig);
    IcebergRestClient p1 = icebergClientFor(PRINCIPAL_1);
    IcebergRestClient p2 = icebergClientFor(PRINCIPAL_2);
    IcebergRestClient r1 = icebergClientFor(REGULAR_1);
    IcebergRestClient r2 = icebergClientFor(REGULAR_2);
    IcebergRestClient creatorOnly = icebergClientFor(CREATOR_ONLY);

    // ===== config (GET_CATALOG) =====
    assertDenied(() -> r1.config(CAT)); // no access -> deny
    admin.config(CAT); // metastore OWNER
    p1.config(CAT); // catalog OWNER
    grantPermissions(REGULAR_1, SecurableType.CATALOG, CAT, Privileges.USE_CATALOG);
    r1.config(CAT); // catalog USE_CATALOG

    // ===== createNamespace (CREATE_SCHEMA) -- creates the schemas reused below =====
    p1.createNamespace(CAT, "sch_p1b"); // catalog OWNER
    assertDenied(() -> r2.createNamespace(CAT, SCHEMA_R2)); // no USE_CATALOG / CREATE_SCHEMA
    grantPermissions(REGULAR_2, SecurableType.CATALOG, CAT, Privileges.USE_CATALOG);
    assertDenied(() -> r2.createNamespace(CAT, SCHEMA_R2)); // USE_CATALOG only, no CREATE_SCHEMA
    grantPermissions(REGULAR_2, SecurableType.CATALOG, CAT, Privileges.CREATE_SCHEMA);
    r2.createNamespace(CAT, SCHEMA_R2); // USE_CATALOG + CREATE_SCHEMA -> r2 owns SCHEMA_R2
    grantPermissions(CREATOR_ONLY, SecurableType.CATALOG, CAT, Privileges.CREATE_SCHEMA);
    assertDenied(() -> creatorOnly.createNamespace(CAT, "sch_co")); // CREATE_SCHEMA, no USE_CATALOG
    grantPermissions(PRINCIPAL_2, SecurableType.CATALOG, CAT, Privileges.USE_CATALOG);
    grantPermissions(PRINCIPAL_2, SecurableType.CATALOG, CAT, Privileges.CREATE_SCHEMA);
    p2.createNamespace(CAT, SCHEMA_P2); // USE_CATALOG + CREATE_SCHEMA -> p2 owns SCHEMA_P2

    // ===== loadNamespace (GET_SCHEMA) =====
    admin.loadNamespace(CAT, SCHEMA); // metastore OWNER
    p1.loadNamespace(CAT, SCHEMA); // catalog OWNER
    p2.loadNamespace(CAT, SCHEMA_P2); // schema OWNER + catalog USE_CATALOG
    assertDenied(() -> r1.loadNamespace(CAT, SCHEMA)); // USE_CATALOG only -> deny
    grantPermissions(REGULAR_1, SecurableType.SCHEMA, CAT + "." + SCHEMA, Privileges.USE_SCHEMA);
    r1.loadNamespace(CAT, SCHEMA); // schema USE_SCHEMA + catalog USE_CATALOG
    // USE_SCHEMA without USE_CATALOG -> deny (creator-only never receives USE_CATALOG).
    grantPermissions(CREATOR_ONLY, SecurableType.SCHEMA, CAT + "." + SCHEMA, Privileges.USE_SCHEMA);
    assertDenied(() -> creatorOnly.loadNamespace(CAT, SCHEMA));

    // ===== listNamespaces filter (GET_SCHEMA) =====
    assertThat(namespaces(admin)).contains(SCHEMA, SCHEMA_P2, SCHEMA_R2); // metastore sees all
    assertThat(namespaces(p1)).contains(SCHEMA, SCHEMA_P2, SCHEMA_R2); // catalog owner sees all
    assertThat(namespaces(p2)).contains(SCHEMA_P2).doesNotContain(SCHEMA); // owns only SCHEMA_P2
    assertThat(namespaces(r1)).contains(SCHEMA).doesNotContain(SCHEMA_P2); // USE_SCHEMA on SCHEMA
    assertThat(namespaces(r2)).contains(SCHEMA_R2).doesNotContain(SCHEMA); // owns only SCHEMA_R2

    // ===== createTable (CREATE_ICEBERG_TABLE) =====
    // catalog tier deny: creator-only has CREATE_SCHEMA + USE_SCHEMA but no USE_CATALOG/OWNER.
    assertDenied(() -> createTable(creatorOnly, SCHEMA, "ct_deny_cat"));
    // schema tier deny: regular-2 has USE_CATALOG but no schema OWNER / USE_SCHEMA+CREATE_TABLE.
    assertDenied(() -> createTable(r2, SCHEMA, "ct_deny_schema"));
    // catalog + schema OWNER (principal-1 owns CAT and SCHEMA) -> allowed.
    createTable(p1, SCHEMA, "ct_owner");
    // schema OWNER with catalog USE_CATALOG (principal-2): a managed create (no location, so
    // #location == null takes the managed branch).
    createTable(p2, SCHEMA_P2, "tbl_p2");
    // USE_SCHEMA + CREATE_TABLE lets regular-1 create (and own what it creates). External-table
    // tier is a 2x2: {path under a registered external location?} x {holds CREATE_EXTERNAL_TABLE?}.
    grantPermissions(REGULAR_1, SecurableType.SCHEMA, CAT + "." + SCHEMA, Privileges.CREATE_TABLE);
    Path externalLocation = testDirectoryRoot.resolve("ac_external_location");
    createExternalLocationWithCredential("ac_cred", "ac_el", externalLocation.toUri().toString());
    String registeredPathA = externalLocation.resolve("t1").toUri().toString();
    String registeredPathB = externalLocation.resolve("t2").toUri().toString();
    // unregistered path, no CREATE_EXTERNAL_TABLE -> allowed (also the table-OWNER fixture)
    createTable(r1, SCHEMA, "tbl_r1own", tmpLocation("tbl_r1own"));
    // registered path, no CREATE_EXTERNAL_TABLE -> denied
    assertDenied(() -> createTable(r1, SCHEMA, "ct_reg_deny", registeredPathA));
    grantPermissions(
        REGULAR_1, SecurableType.EXTERNAL_LOCATION, "ac_el", Privileges.CREATE_EXTERNAL_TABLE);
    // registered path, with CREATE_EXTERNAL_TABLE -> allowed
    createTable(r1, SCHEMA, "ct_reg_ok", registeredPathB);
    // unregistered path, with CREATE_EXTERNAL_TABLE -> still allowed (#external_location == null)
    createTable(r1, SCHEMA, "ct_unreg_ok", tmpLocation("ct_unreg_ok"));

    // ===== loadTable / tableExists (GET_TABLE) =====
    // Each owner reads a table it does NOT own (OWNER is exact-match, no cascade), so each read
    // maps to a single GET_TABLE disjunct.

    // tbl_none in SCHEMA (owned by principal-1): the metastore owner reads it (no cascade needed);
    // regular-1 (USE_CATALOG + USE_SCHEMA, no table privilege) is denied on both loadTable and
    // the HEAD tableExists.
    createTable(p1, SCHEMA, "tbl_none");
    admin.loadTable(CAT, SCHEMA, "tbl_none"); // metastore OWNER
    assertDenied(() -> r1.loadTable(CAT, SCHEMA, "tbl_none")); // no table privilege
    // tableExists is a HEAD request, so its denial is body-less (no Iceberg ErrorResponse).
    assertApiExceptionStatusOnly(() -> r1.tableExists(CAT, SCHEMA, "tbl_none"), 403);

    // tbl_p2 in SCHEMA_P2 (owned by principal-2): principal-1 owns CAT but neither SCHEMA_P2 nor
    // tbl_p2, so catalog OWNER is the only matching disjunct.
    p1.loadTable(CAT, SCHEMA_P2, "tbl_p2"); // catalog OWNER
    assertThat(p1.tableExists(CAT, SCHEMA_P2, "tbl_p2")).isTrue();

    // tbl_p2_r1 in SCHEMA_P2, created (hence owned) by regular-1: principal-2 owns SCHEMA_P2 but
    // not this table, so schema OWNER + catalog USE_CATALOG is the only match.
    grantPermissions(REGULAR_1, SecurableType.SCHEMA, CAT + "." + SCHEMA_P2, Privileges.USE_SCHEMA);
    grantPermissions(
        REGULAR_1, SecurableType.SCHEMA, CAT + "." + SCHEMA_P2, Privileges.CREATE_TABLE);
    createTable(r1, SCHEMA_P2, "tbl_p2_r1");
    p2.loadTable(CAT, SCHEMA_P2, "tbl_p2_r1"); // schema OWNER + catalog USE_CATALOG

    // tbl_sel / tbl_mod in SCHEMA (owned by principal-1): regular-1 reads each via its granted
    // table privilege (with USE_CATALOG + USE_SCHEMA).
    createTable(p1, SCHEMA, "tbl_sel");
    grantPermissions(
        REGULAR_1, SecurableType.TABLE, CAT + "." + SCHEMA + ".tbl_sel", Privileges.SELECT);
    r1.loadTable(CAT, SCHEMA, "tbl_sel"); // table SELECT
    assertThat(r1.tableExists(CAT, SCHEMA, "tbl_sel")).isTrue();
    createTable(p1, SCHEMA, "tbl_mod");
    grantPermissions(
        REGULAR_1, SecurableType.TABLE, CAT + "." + SCHEMA + ".tbl_mod", Privileges.MODIFY);
    r1.loadTable(CAT, SCHEMA, "tbl_mod"); // table MODIFY

    // tbl_r1own in SCHEMA, created (hence owned) by regular-1: read via table OWNER.
    r1.loadTable(CAT, SCHEMA, "tbl_r1own"); // table OWNER (creator)

    // ===== listTables filter (GET_TABLE) =====
    // regular-1 sees only tables it has a table privilege on; tbl_none is filtered out
    // (USE_CATALOG + USE_SCHEMA don't satisfy GET_TABLE per table).
    assertThat(tables(r1, SCHEMA))
        .contains("tbl_sel", "tbl_mod", "tbl_r1own")
        .doesNotContain("tbl_none");
    // catalog owner passes GET_TABLE for every table, so nothing is filtered out.
    assertThat(tables(p1, SCHEMA)).contains("tbl_sel", "tbl_mod", "tbl_none", "tbl_r1own");

    // ===== loadView gate (GET_TABLE); the endpoint is a stub that 404s once authorized =====
    assertDenied(() -> r1.loadView(CAT, SCHEMA, "tbl_none")); // no table privilege -> 403
    assertIcebergApiException(
        () -> p1.loadView(CAT, SCHEMA, "tbl_none"), 404); // authz passes -> 404

    // ===== reportMetrics gate (GET_TABLE) =====
    ReportMetricsRequest report = scanReport("tbl_none");
    assertDenied(() -> r1.reportMetrics(CAT, SCHEMA, "tbl_none", report)); // no privilege -> 403
    p1.reportMetrics(CAT, SCHEMA, "tbl_none", report); // catalog OWNER -> report accepted

    // ===== dropTable (DELETE_TABLE) =====
    // As with reads, each owner drops a table it does NOT own; metastore-owner alone cannot delete.
    createTable(p1, SCHEMA, "tbl_drop_deny"); // owned by principal-1
    assertDenied(() -> admin.dropTable(CAT, SCHEMA, "tbl_drop_deny")); // metastore owner -> deny
    // regular-1 has USE_CATALOG + USE_SCHEMA but no OWNER on the table -> deny.
    assertDenied(() -> r1.dropTable(CAT, SCHEMA, "tbl_drop_deny"));
    p1.dropTable(CAT, SCHEMA_P2, "tbl_p2"); // catalog OWNER (not schema/table owner)
    p2.dropTable(CAT, SCHEMA_P2, "tbl_p2_r1"); // schema OWNER + USE_CATALOG (not table owner)
    r1.dropTable(CAT, SCHEMA, "tbl_r1own"); // table OWNER (creator) + USE_CATALOG + USE_SCHEMA
  }

  // --- helpers -------------------------------------------------------------------------------

  private IcebergRestClient icebergClientFor(String userEmail) {
    return new IcebergRestClient(createTestUserServerConfig(userEmail));
  }

  /**
   * Asserts an authorization denial: 403 whose Iceberg error carries the "Access denied" reason.
   */
  private static void assertDenied(Executable call) {
    assertIcebergApiException(call, 403, "Access denied");
  }

  /**
   * Creates a native managed Iceberg table as {@code owner} (no location, so {@code #location} is
   * null -- the managed branch). The common fixture case: managed vs external is irrelevant to the
   * read/drop authorization the fixtures exercise.
   */
  @SneakyThrows
  private void createTable(IcebergRestClient owner, String schema, String name) {
    createTable(owner, schema, name, null);
  }

  /**
   * Creates a native Iceberg table (as {@code owner}) at {@code location}: {@code null} = managed
   * table; a path = external table there (use {@link #tmpLocation} for a fresh unregistered path).
   */
  @SneakyThrows
  private void createTable(IcebergRestClient owner, String schema, String name, String location) {
    owner.createTable(CAT, schema, tableRequest(name, location));
  }

  @SneakyThrows
  private static List<String> namespaces(IcebergRestClient client) {
    return client.listNamespaces(CAT).namespaces().stream()
        .map(namespace -> namespace.level(0))
        .collect(Collectors.toList());
  }

  @SneakyThrows
  private static List<String> tables(IcebergRestClient client, String schema) {
    return client.listTables(CAT, schema).identifiers().stream()
        .map(TableIdentifier::name)
        .collect(Collectors.toList());
  }

  /** A minimal valid scan-metrics report for the given table (its single field is {@code id}). */
  private static ReportMetricsRequest scanReport(String tableName) {
    return ReportMetricsRequest.of(
        ImmutableScanReport.builder()
            .tableName(tableName)
            .schemaId(0)
            .addProjectedFieldIds(1)
            .addProjectedFieldNames("id")
            .snapshotId(1L)
            .filter(Expressions.alwaysTrue())
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
            .build());
  }

  private static CreateTableRequest tableRequest(String name, String location) {
    CreateTableRequest.Builder builder =
        CreateTableRequest.builder()
            .withName(name)
            .withSchema(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
    if (location != null) {
      builder.withLocation(location);
    }
    return builder.build();
  }

  private String tmpLocation(String name) {
    return testDirectoryRoot
        .resolve("ac_tables")
        .resolve(name + "_" + UUID.randomUUID())
        .toUri()
        .toString();
  }
}
