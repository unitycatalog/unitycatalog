package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.auth.AuthToken;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.RESTObjectMapper;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DeltaV1ServiceTest extends BaseServerTest {

  private static final String BASE_PREFIX =
      "/v1/catalogs/" + TestUtils.CATALOG_NAME + "/schemas/" + TestUtils.SCHEMA_NAME;

  private CatalogOperations catalogOperations;
  private SchemaOperations schemaOperations;
  private WebClient client;

  @BeforeEach
  public void setUp() {
    super.setUp();
    String uri = serverConfig.getServerUrl() + "/api/2.1/unity-catalog/delta";
    String token = serverConfig.getAuthToken();
    catalogOperations = new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
    schemaOperations = new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
    client = WebClient.builder(uri).auth(AuthToken.ofOAuth2(token)).build();
    cleanUp();
  }

  private void cleanUp() {
    try {
      if (catalogOperations.getCatalog(TestUtils.CATALOG_NAME) != null) {
        catalogOperations.deleteCatalog(TestUtils.CATALOG_NAME, Optional.of(true));
      }
    } catch (Exception e) {
      // ignore cleanup failures
    }
  }

  private void createCatalogAndSchema() throws ApiException {
    CatalogInfo catalogInfo =
        catalogOperations.createCatalog(
            new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    assertThat(catalogInfo.getName()).isEqualTo(TestUtils.CATALOG_NAME);
    SchemaInfo schemaInfo =
        schemaOperations.createSchema(
            new CreateSchema().catalogName(TestUtils.CATALOG_NAME).name(TestUtils.SCHEMA_NAME));
    assertThat(schemaInfo.getFullName()).isEqualTo(TestUtils.SCHEMA_FULL_NAME);
  }

  private AggregatedHttpResponse postJson(String path, String body) {
    RequestHeaders headers =
        RequestHeaders.builder()
            .method(HttpMethod.POST)
            .path(path)
            .contentType(MediaType.JSON_UTF_8)
            .build();
    return client.execute(headers, HttpData.ofUtf8(body)).aggregate().join();
  }

  private JsonNode requireField(JsonNode node, String... fieldNames) {
    for (String fieldName : fieldNames) {
      JsonNode value = node.get(fieldName);
      if (value != null) {
        return value;
      }
    }
    throw new IllegalStateException("Missing field in response: " + String.join(" / ", fieldNames));
  }

  @Test
  public void testConfig() throws IOException {
    AggregatedHttpResponse resp = client.get("/v1/config").aggregate().join();
    assertThat(resp.status()).isEqualTo(HttpStatus.OK);

    JsonNode body = RESTObjectMapper.mapper().readTree(resp.contentUtf8());
    assertThat(
            requireField(body, "protocolVersion", "protocol_version", "protocol-version")
                .asInt())
        .isEqualTo(1);
    assertThat(requireField(body, "endpoints").toString())
        .contains("GET /config")
        .contains("POST /catalogs/{catalog}/schemas/{schema}/staging-tables")
        .contains("POST /catalogs/{catalog}/schemas/{schema}/tables/{table}");
  }

  @Test
  public void testManagedTableCreateLoadUpdateFlow() throws Exception {
    createCatalogAndSchema();

    AggregatedHttpResponse stagingResp =
        postJson(BASE_PREFIX + "/staging-tables", "{\"name\":\"delta_v1_demo\"}");
    assertThat(stagingResp.status()).isEqualTo(HttpStatus.OK);
    JsonNode staging = RESTObjectMapper.mapper().readTree(stagingResp.contentUtf8());
    String tableId = requireField(staging, "tableId", "table_id", "table-id").asText();
    String location = requireField(staging, "location").asText();
    assertThat(tableId).isNotBlank();
    assertThat(location).isNotBlank();
    assertThat(
            requireField(
                    staging,
                    "requiredProperties",
                    "required_properties",
                    "required-properties")
                .get("delta.feature.catalogManaged")
                .asText())
        .isEqualTo("supported");

    AggregatedHttpResponse stagingCredsResp =
        client
            .get(BASE_PREFIX + "/staging-tables/" + tableId + "/credentials?operation=READ_WRITE")
            .aggregate()
            .join();
    assertThat(stagingCredsResp.status()).isEqualTo(HttpStatus.OK);

    String createBody =
        """
        {
          "table_id": "%s",
          "name": "delta_v1_demo",
          "storage_location": "%s",
          "comment": "demo table",
          "properties": {
            "io.unitycatalog.tableId": "%s",
            "delta.feature.catalogManaged": "supported",
            "delta.feature.vacuumProtocolCheck": "supported",
            "demo.flag": "off"
          },
          "columns": [
            {
              "name": "id",
              "type_text": "integer",
              "type_json": "{\\"type\\":\\"integer\\"}",
              "type_name": "INT",
              "position": 0,
              "nullable": false
            },
            {
              "name": "value",
              "type_text": "string",
              "type_json": "{\\"type\\":\\"string\\"}",
              "type_name": "STRING",
              "position": 1,
              "nullable": true
            }
          ]
        }
        """
            .formatted(tableId, location, tableId);

    AggregatedHttpResponse createResp = postJson(BASE_PREFIX + "/tables", createBody);
    assertThat(createResp.status()).isEqualTo(HttpStatus.OK);
    JsonNode created = RESTObjectMapper.mapper().readTree(createResp.contentUtf8());
    JsonNode createdTableInfo = requireField(created, "tableInfo", "table_info", "table-info");
    assertThat(createdTableInfo.toString()).contains("delta_v1_demo");
    assertThat(
            requireField(
                    created,
                    "latestTableVersion",
                    "latest_table_version",
                    "latest-table-version")
                .asLong())
        .isEqualTo(0L);
    assertThat(requireField(created, "etag").asText()).isNotBlank();

    AggregatedHttpResponse loadResp =
        client.get(BASE_PREFIX + "/tables/delta_v1_demo").aggregate().join();
    assertThat(loadResp.status()).isEqualTo(HttpStatus.OK);
    JsonNode loaded = RESTObjectMapper.mapper().readTree(loadResp.contentUtf8());
    assertThat(loaded.toString()).contains(location).contains("demo.flag").contains("off");

    String updateBody =
        """
        {
          "assert_table_id": "%s",
          "assert_etag": "%s",
          "set_properties": {
            "demo.flag": "on"
          },
          "commit_info": {
            "version": 1,
            "timestamp": 123456789,
            "file_name": "00000000000000000001.uuid.json",
            "file_size": 128,
            "file_modification_timestamp": 123456789
          },
          "latest_backfilled_version": 0
        }
        """
            .formatted(tableId, loaded.get("etag").asText());

    AggregatedHttpResponse updateResp =
        postJson(BASE_PREFIX + "/tables/delta_v1_demo", updateBody);
    assertThat(updateResp.status()).isEqualTo(HttpStatus.OK);
    JsonNode updated = RESTObjectMapper.mapper().readTree(updateResp.contentUtf8());
    assertThat(
            requireField(
                    updated,
                    "latestTableVersion",
                    "latest_table_version",
                    "latest-table-version")
                .asLong())
        .isEqualTo(1L);
    assertThat(updated.toString()).contains("demo.flag").contains("on");
    assertThat(updated.toString()).contains("delta.feature.catalogManaged").contains("supported");
    assertThat(requireField(updated, "commits")).hasSize(1);
    assertThat(
            requireField(
                    updated,
                    "lastKnownBackfilledVersion",
                    "last_known_backfilled_version",
                    "last-known-backfilled-version")
                .asLong())
        .isEqualTo(0L);

    AggregatedHttpResponse tableCredsResp =
        client
            .get(BASE_PREFIX + "/tables/delta_v1_demo/credentials?operation=READ")
            .aggregate()
            .join();
    assertThat(tableCredsResp.status()).isEqualTo(HttpStatus.OK);
  }
}
