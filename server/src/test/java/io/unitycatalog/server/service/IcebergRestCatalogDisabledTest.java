package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.base.BaseCRUDTestWithMockCredentials;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Iceberg REST catalog behavior when native Iceberg table writes are disabled (the production
 * default: {@code server.iceberg-table.enabled=false}). This class deliberately does not enable the
 * flag, unlike {@link IcebergRestCatalogTest}. The write endpoints all call {@code
 * checkIcebergTableEnabled()} before touching any resource, so they are rejected regardless of
 * whether the target catalog/schema/table exists.
 */
public class IcebergRestCatalogDisabledTest extends BaseCRUDTestWithMockCredentials {

  private IcebergRestClient icebergClient;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    icebergClient = new IcebergRestClient(serverConfig);
  }

  @Test
  public void testConfigAdvertisesOnlyReadEndpoints() throws Exception {
    ConfigResponse resp = icebergClient.config(TestUtils.CATALOG_NAME);
    assertThat(resp.overrides()).containsEntry("prefix", "catalogs/" + TestUtils.CATALOG_NAME);
    assertThat(resp.endpoints())
        .containsExactlyInAnyOrder(
            Endpoint.V1_LIST_NAMESPACES,
            Endpoint.V1_LOAD_NAMESPACE,
            Endpoint.V1_TABLE_EXISTS,
            Endpoint.V1_LOAD_TABLE,
            Endpoint.V1_LOAD_VIEW,
            Endpoint.V1_REPORT_METRICS,
            Endpoint.V1_LIST_TABLES);
  }

  @Test
  public void testWriteEndpointsRejectedWhenDisabled() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    // createNamespace
    TestUtils.assertIcebergApiException(
        () -> icebergClient.createNamespace(TestUtils.CATALOG_NAME, "iceberg_disabled_ns"),
        400,
        "currently disabled");

    // createTable
    TestUtils.assertIcebergApiException(
        () ->
            icebergClient.createTable(
                TestUtils.CATALOG_NAME,
                TestUtils.SCHEMA_NAME,
                CreateTableRequest.builder()
                    .withName(TestUtils.TABLE_NAME)
                    .withSchema(schema)
                    .build()),
        400,
        "currently disabled");

    // updateTable
    TestUtils.assertIcebergApiException(
        () ->
            icebergClient.updateTable(
                TestUtils.CATALOG_NAME,
                TestUtils.SCHEMA_NAME,
                TestUtils.TABLE_NAME,
                new UpdateTableRequest(
                    List.of(), List.of(new MetadataUpdate.SetProperties(Map.of("a", "b"))))),
        400,
        "currently disabled");

    // dropTable
    TestUtils.assertIcebergApiException(
        () ->
            icebergClient.dropTable(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME),
        400,
        "currently disabled");
  }
}
