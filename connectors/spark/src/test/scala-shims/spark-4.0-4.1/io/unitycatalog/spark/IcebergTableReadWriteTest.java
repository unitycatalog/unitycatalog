package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;

import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Shared base for the Spark + Iceberg end-to-end suites. It drives Iceberg tables through a real
 * Spark + Iceberg runtime against Unity Catalog's Iceberg REST catalog ({@code
 * /api/2.1/unity-catalog/iceberg}), rather than through the UC Spark connector (which has no
 * Iceberg support). Spark is configured with Iceberg's own {@code SparkCatalog} (REST), wired by
 * {@code BaseSparkIntegrationTest} for every catalog its {@code isIcebergCatalog} marks; the {@code
 * /v1/config} handshake returns a {@code prefix} so the standard Iceberg client targets UC's
 * catalog-scoped paths, exercising the server's config / namespace / create / commit / load / drop
 * endpoints exactly as an external Iceberg client would.
 *
 * <p>Storage is a local {@code file://} warehouse: the server writes the first metadata file
 * through its native local FileIO and Spark's Iceberg {@code HadoopFileIO} reads and writes data
 * files and later metadata to the same directory, so the full create / write / read / commit path
 * round-trips without cloud credentials. Cloud credential-vending is covered separately by the
 * server-side Iceberg REST catalog tests.
 *
 * <p>Concrete subclasses pick managed ({@link IcebergManagedTableReadWriteTest}) or external
 * ({@link IcebergExternalTableReadWriteTest}) tables, mirroring the Delta {@code
 * DeltaManagedTableReadWriteTest} / {@code DeltaExternalTableReadWriteTest} split. The class reuses
 * {@link BaseTableReadWriteTest}'s create/read/write matrix and helpers; the {@code
 * ExternalTableReadWriteTest} base is UC-connector-specific, so it is not reused here.
 *
 * <p>Lives under {@code src/test/scala-shims/spark-4.0-4.1} so it is compiled only for the Spark
 * versions that have an Iceberg Spark runtime ({@code supportIceberg} in the cross-Spark build).
 */
public abstract class IcebergTableReadWriteTest extends BaseTableReadWriteTest {

  @Override
  protected boolean isIcebergCatalog(String catalog) {
    return true;
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    // Advertise and accept the Iceberg write endpoints (createTable / updateTable / dropTable);
    // otherwise the REST client refuses them as unsupported and the server rejects them.
    serverProperties.setProperty(ServerProperties.Property.ICEBERG_TABLE_ENABLED.getKey(), "true");
  }

  @Override
  protected String tableFormat() {
    return "ICEBERG";
  }

  // These list only the named catalog; the Iceberg suites leave spark_catalog as Spark's built-in
  // session catalog (the UC-connector tests also drive spark_catalog).
  @Override
  protected List<String> supportedCatalogNames() {
    return List.of(CATALOG_NAME);
  }

  @Override
  protected List<String> sessionCatalogNames() {
    return List.of(CATALOG_NAME);
  }

  // Iceberg supports partitioned creates and CTAS, so every create in the base matrix is expected
  // to succeed (the base defaults non-Delta CTAS / non-plain-CREATE to an expected failure).
  @Override
  protected List<String> expectedCreateFailureMessages(TableSetupOptions options) {
    return null;
  }

  // The Iceberg REST catalog vends no cloud credentials and these suites use a local warehouse, so
  // testTableOperations (and the other cloud-parameterized base tests) run once on the file scheme.
  protected static Stream<Arguments> cloudParameters() {
    return Stream.of(Arguments.of("file", false, false));
  }
}
