package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.auth.AuthToken;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DeltaRestCatalogTest extends BaseServerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String TEST_BASE =
      "/v1/catalogs/" + TestUtils.CATALOG_NAME + "/schemas/" + TestUtils.SCHEMA_NAME;

  protected CatalogOperations catalogOperations;
  protected SchemaOperations schemaOperations;
  protected TableOperations tableOperations;
  private WebClient client;

  @BeforeEach
  public void setUp() {
    super.setUp();
    String uri = serverConfig.getServerUrl() + "/api/2.1/unity-catalog/delta";
    String token = serverConfig.getAuthToken();
    catalogOperations = new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
    schemaOperations = new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
    tableOperations = new SdkTableOperations(TestUtils.createApiClient(serverConfig));
    client = WebClient.builder(uri).auth(AuthToken.ofOAuth2(token)).build();
    cleanUp();
  }

  protected void cleanUp() {
    try {
      if (catalogOperations.getCatalog(TestUtils.CATALOG_NAME) != null) {
        catalogOperations.deleteCatalog(TestUtils.CATALOG_NAME, Optional.of(true));
      }
    } catch (Exception e) {
      // Ignore
    }
  }

  @Test
  public void testConfig() throws Exception {
    AggregatedHttpResponse resp =
        client.get("/v1/config?catalog=" + TestUtils.CATALOG_NAME + "&protocol-versions=1.0")
            .aggregate()
            .join();

    assertThat(resp.status().code()).isEqualTo(200);
    JsonNode response = MAPPER.readTree(resp.contentUtf8());
    assertThat(response.path("protocol-version").asDouble()).isEqualTo(1.0d);
    assertThat(response.path("endpoints").isArray()).isTrue();
    assertThat(response.path("endpoints").toString())
        .contains("/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}")
        .contains("/v1/catalogs/{catalog}/schemas/{schema}/staging-tables");
  }

  @Test
  public void testListTablesSupportsPagination() throws Exception {
    createNamespace();
    createExternalDeltaTable("table_a");
    createExternalDeltaTable("table_b");
    createExternalDeltaTable("table_c");

    AggregatedHttpResponse resp =
        client.get(TEST_BASE + "/tables?maxResults=2").aggregate().join();

    assertThat(resp.status().code()).isEqualTo(200);
    JsonNode response = MAPPER.readTree(resp.contentUtf8());
    assertThat(response.path("identifiers").isArray()).isTrue();
    assertThat(response.path("identifiers")).hasSize(2);
    assertThat(response.has("next-page-token")).isTrue();
  }

  @Test
  public void testLoadTableDoesNotReturnInlineCredentials() throws Exception {
    createNamespace();
    createExternalDeltaTable(TestUtils.TABLE_NAME);

    AggregatedHttpResponse resp =
        client
            .get(TEST_BASE + "/tables/" + TestUtils.TABLE_NAME)
            .aggregate()
            .join();

    assertThat(resp.status().code()).isEqualTo(200);
    JsonNode response = MAPPER.readTree(resp.contentUtf8());
    assertThat(response.has("storage-credentials")).isFalse();
    assertThat(response.path("metadata").path("table-uuid").asText()).isNotBlank();
  }

  @Test
  public void testCreateTableUsesStructTypeSchemaShape() throws Exception {
    createNamespace();

    String body =
        """
        {
          "name": "delta_struct_table",
          "location": "%s",
          "table-type": "EXTERNAL",
          "data-source-format": "DELTA",
          "columns": {
            "type": "struct",
            "fields": [
              {
                "name": "id",
                "type": "long",
                "nullable": false,
                "metadata": {}
              }
            ]
          },
          "partition-columns": ["id"],
          "protocol": {
            "min-reader-version": 1,
            "min-writer-version": 2
          },
          "properties": {
            "delta.appendOnly": "true"
          }
        }
        """
            .formatted(
                getManagedStorageCloudPath(testDirectoryRoot.resolve("delta_struct_table")));

    AggregatedHttpResponse createResp =
        client.execute(jsonPost(TEST_BASE + "/tables"), HttpData.ofUtf8(body)).aggregate().join();

    assertThat(createResp.status().code()).isEqualTo(200);
    JsonNode createResponse = MAPPER.readTree(createResp.contentUtf8());
    assertThat(createResponse.path("metadata").path("columns").path("type").asText())
        .isEqualTo("struct");
    assertThat(createResponse.path("metadata").path("columns").path("fields")).hasSize(1);
    assertThat(
            createResponse
                .path("metadata")
                .path("columns")
                .path("fields")
                .get(0)
                .path("name")
                .asText())
        .isEqualTo("id");

    AggregatedHttpResponse loadResp =
        client.get(TEST_BASE + "/tables/delta_struct_table").aggregate().join();

    assertThat(loadResp.status().code()).isEqualTo(200);
    JsonNode loadResponse = MAPPER.readTree(loadResp.contentUtf8());
    assertThat(loadResponse.path("metadata").path("columns").path("type").asText())
        .isEqualTo("struct");
    assertThat(loadResponse.path("metadata").path("partition-columns").isArray()).isTrue();
    assertThat(loadResponse.path("metadata").path("partition-columns")).hasSize(1);
    assertThat(loadResponse.path("metadata").path("partition-columns").get(0).asText())
        .isEqualTo("id");
  }

  @Test
  public void testUpdateTableRejectsDerivedPropertyWrites() throws Exception {
    createNamespace();
    createExternalDeltaTable(TestUtils.TABLE_NAME);

    String body =
        """
        {
          "requirements": [],
          "updates": [
            {
              "action": "set-properties",
              "updates": {
                "delta.minReaderVersion": "3"
              }
            }
          ]
        }
        """;

    AggregatedHttpResponse resp =
        client
            .execute(jsonPost(TEST_BASE + "/tables/" + TestUtils.TABLE_NAME), HttpData.ofUtf8(body))
            .aggregate()
            .join();

    assertThat(resp.status().code()).isEqualTo(400);
    JsonNode response = MAPPER.readTree(resp.contentUtf8());
    assertThat(response.path("error").path("message").asText()).contains("Derived properties");
  }

  @Test
  public void testUpdateTableRemovePropertiesOnlyDeletesNamedKeys() throws Exception {
    createNamespace();

    String createBody =
        """
        {
          "name": "property_delta_table",
          "location": "%s",
          "table-type": "EXTERNAL",
          "data-source-format": "DELTA",
          "columns": {
            "type": "struct",
            "fields": [
              {
                "name": "id",
                "type": "long",
                "nullable": false,
                "metadata": {}
              }
            ]
          },
          "protocol": {
            "min-reader-version": 1,
            "min-writer-version": 2
          },
          "properties": {
            "keep": "value",
            "drop": "value"
          }
        }
        """
            .formatted(
                getManagedStorageCloudPath(testDirectoryRoot.resolve("property_delta_table")));

    AggregatedHttpResponse createResp =
        client
            .execute(jsonPost(TEST_BASE + "/tables"), HttpData.ofUtf8(createBody))
            .aggregate()
            .join();
    assertThat(createResp.status().code()).isEqualTo(200);

    String updateBody =
        """
        {
          "requirements": [],
          "updates": [
            {
              "action": "remove-properties",
              "removals": ["drop"]
            }
          ]
        }
        """;

    AggregatedHttpResponse updateResp =
        client
            .execute(
                jsonPost(TEST_BASE + "/tables/property_delta_table"), HttpData.ofUtf8(updateBody))
            .aggregate()
            .join();

    assertThat(updateResp.status().code()).isEqualTo(200);
    JsonNode updateResponse = MAPPER.readTree(updateResp.contentUtf8());
    assertThat(updateResponse.path("metadata").path("properties").path("keep").asText())
        .isEqualTo("value");
    assertThat(updateResponse.path("metadata").path("properties").has("drop")).isFalse();
  }

  @Test
  public void testUpdateTableAcceptsMetadataSnapshotVersionAction() throws Exception {
    createNamespace();
    createExternalDeltaTable(TestUtils.TABLE_NAME);

    String body =
        """
        {
          "updates": [
            {
              "action": "update-metadata-snapshot-version",
              "last-commit-version": 7,
              "last-commit-timestamp-ms": 1700000000123
            }
          ]
        }
        """;

    AggregatedHttpResponse resp =
        client
            .execute(jsonPost(TEST_BASE + "/tables/" + TestUtils.TABLE_NAME), HttpData.ofUtf8(body))
            .aggregate()
            .join();

    assertThat(resp.status().code()).isEqualTo(200);
    JsonNode response = MAPPER.readTree(resp.contentUtf8());
    assertThat(response.path("metadata").path("last-commit-version").asLong()).isEqualTo(7L);
    assertThat(response.path("metadata").path("last-commit-timestamp-ms").asLong())
        .isEqualTo(1700000000123L);
  }

  @Test
  public void testUpdateTableSetProtocol() throws Exception {
    createNamespace();
    createDeltaRestTable("proto_table", "{\"delta.appendOnly\": \"true\"}");

    String body =
        """
        {
          "updates": [
            {
              "action": "set-protocol",
              "protocol": {
                "min-reader-version": 3,
                "min-writer-version": 7,
                "reader-features": ["deletionVectors", "vacuumProtocolCheck"],
                "writer-features": ["deletionVectors", "inCommitTimestamp", "v2Checkpoint", "vacuumProtocolCheck"]
              }
            }
          ]
        }
        """;

    AggregatedHttpResponse resp =
        client
            .execute(jsonPost(TEST_BASE + "/tables/proto_table"), HttpData.ofUtf8(body))
            .aggregate()
            .join();

    assertThat(resp.status().code()).isEqualTo(200);
    JsonNode response = MAPPER.readTree(resp.contentUtf8());
    JsonNode protocol = response.path("metadata").path("protocol");
    assertThat(protocol.path("min-reader-version").asInt()).isEqualTo(3);
    assertThat(protocol.path("min-writer-version").asInt()).isEqualTo(7);
    assertThat(protocol.path("reader-features").toString()).contains("deletionVectors");
    assertThat(protocol.path("writer-features").toString()).contains("inCommitTimestamp");

    // Verify persistence via load
    AggregatedHttpResponse loadResp =
        client.get(TEST_BASE + "/tables/proto_table").aggregate().join();
    assertThat(loadResp.status().code()).isEqualTo(200);
    JsonNode loadResponse = MAPPER.readTree(loadResp.contentUtf8());
    JsonNode loadProtocol = loadResponse.path("metadata").path("protocol");
    assertThat(loadProtocol.path("min-reader-version").asInt()).isEqualTo(3);
    assertThat(loadProtocol.path("min-writer-version").asInt()).isEqualTo(7);
  }

  @Test
  public void testUpdateTableSetColumns() throws Exception {
    createNamespace();
    createDeltaRestTable("col_table", "{}");

    String body =
        """
        {
          "updates": [
            {
              "action": "set-columns",
              "columns": {
                "type": "struct",
                "fields": [
                  {"name": "id", "type": "long", "nullable": false, "metadata": {}},
                  {"name": "name", "type": "string", "nullable": true, "metadata": {}},
                  {"name": "score", "type": "double", "nullable": true, "metadata": {}}
                ]
              }
            }
          ]
        }
        """;

    AggregatedHttpResponse resp =
        client
            .execute(jsonPost(TEST_BASE + "/tables/col_table"), HttpData.ofUtf8(body))
            .aggregate()
            .join();

    assertThat(resp.status().code()).isEqualTo(200);
    JsonNode columns = MAPPER.readTree(resp.contentUtf8()).path("metadata").path("columns");
    assertThat(columns.path("type").asText()).isEqualTo("struct");
    assertThat(columns.path("fields")).hasSize(3);
    assertThat(columns.path("fields").get(1).path("name").asText()).isEqualTo("name");
    assertThat(columns.path("fields").get(2).path("type").asText()).isEqualTo("double");

    // Verify persistence via load
    AggregatedHttpResponse loadResp =
        client.get(TEST_BASE + "/tables/col_table").aggregate().join();
    JsonNode loadColumns =
        MAPPER.readTree(loadResp.contentUtf8()).path("metadata").path("columns");
    assertThat(loadColumns.path("fields")).hasSize(3);
  }

  @Test
  public void testUpdateTableSetDomainMetadata() throws Exception {
    createNamespace();
    createDeltaRestTable("domain_table", "{}");

    String setBody =
        """
        {
          "updates": [
            {
              "action": "set-domain-metadata",
              "updates": {
                "delta.clustering": {
                  "clusteringColumns": [["user_id"], ["event_date"]]
                }
              }
            }
          ]
        }
        """;

    AggregatedHttpResponse setResp =
        client
            .execute(jsonPost(TEST_BASE + "/tables/domain_table"), HttpData.ofUtf8(setBody))
            .aggregate()
            .join();

    assertThat(setResp.status().code()).isEqualTo(200);
    JsonNode setResponse = MAPPER.readTree(setResp.contentUtf8());
    assertThat(
            setResponse.path("metadata").path("properties").path("clusteringColumns").asText())
        .contains("user_id");

    // Now remove the domain metadata
    String removeBody =
        """
        {
          "updates": [
            {
              "action": "remove-domain-metadata",
              "domains": ["delta.clustering"]
            }
          ]
        }
        """;

    AggregatedHttpResponse removeResp =
        client
            .execute(jsonPost(TEST_BASE + "/tables/domain_table"), HttpData.ofUtf8(removeBody))
            .aggregate()
            .join();

    assertThat(removeResp.status().code()).isEqualTo(200);
    JsonNode removeResponse = MAPPER.readTree(removeResp.contentUtf8());
    assertThat(removeResponse.path("metadata").path("properties").has("clusteringColumns"))
        .isFalse();
  }

  @Test
  public void testUpdateTableSetTableComment() throws Exception {
    createNamespace();
    createDeltaRestTable("comment_table", "{}");

    String body =
        """
        {
          "updates": [
            {
              "action": "set-table-comment",
              "comment": "this is a test comment"
            }
          ]
        }
        """;

    AggregatedHttpResponse resp =
        client
            .execute(jsonPost(TEST_BASE + "/tables/comment_table"), HttpData.ofUtf8(body))
            .aggregate()
            .join();

    assertThat(resp.status().code()).isEqualTo(200);
  }

  @Test
  public void testUpdateTableSetPartitionColumns() throws Exception {
    createNamespace();
    createDeltaRestTable("part_table", "{}");

    // First add more columns so we have something to partition by
    String colBody =
        """
        {
          "updates": [
            {
              "action": "set-columns",
              "columns": {
                "type": "struct",
                "fields": [
                  {"name": "id", "type": "long", "nullable": false, "metadata": {}},
                  {"name": "region", "type": "string", "nullable": true, "metadata": {}}
                ]
              }
            },
            {
              "action": "set-partition-columns",
              "partition-columns": ["region"]
            }
          ]
        }
        """;

    AggregatedHttpResponse resp =
        client
            .execute(jsonPost(TEST_BASE + "/tables/part_table"), HttpData.ofUtf8(colBody))
            .aggregate()
            .join();

    assertThat(resp.status().code()).isEqualTo(200);
    JsonNode partitions =
        MAPPER.readTree(resp.contentUtf8()).path("metadata").path("partition-columns");
    assertThat(partitions.isArray()).isTrue();
    assertThat(partitions).hasSize(1);
    assertThat(partitions.get(0).asText()).isEqualTo("region");
  }

  @Test
  public void testDeleteTable() throws Exception {
    createNamespace();
    createExternalDeltaTable("delete_me");

    AggregatedHttpResponse deleteResp =
        client
            .execute(
                RequestHeaders.builder()
                    .method(HttpMethod.DELETE)
                    .path(TEST_BASE + "/tables/delete_me")
                    .build())
            .aggregate()
            .join();

    assertThat(deleteResp.status().code()).isEqualTo(204);

    // Verify it's gone
    AggregatedHttpResponse loadResp =
        client.get(TEST_BASE + "/tables/delete_me").aggregate().join();
    assertThat(loadResp.status().code()).isEqualTo(404);
  }

  @Test
  public void testTableExists() throws Exception {
    createNamespace();
    createExternalDeltaTable("exists_table");

    AggregatedHttpResponse existsResp =
        client
            .execute(
                RequestHeaders.builder()
                    .method(HttpMethod.HEAD)
                    .path(TEST_BASE + "/tables/exists_table")
                    .build())
            .aggregate()
            .join();
    assertThat(existsResp.status().code()).isEqualTo(204);

    AggregatedHttpResponse notExistsResp =
        client
            .execute(
                RequestHeaders.builder()
                    .method(HttpMethod.HEAD)
                    .path(TEST_BASE + "/tables/no_such_table")
                    .build())
            .aggregate()
            .join();
    assertThat(notExistsResp.status().code()).isEqualTo(404);
  }

  @Test
  public void testRenameTable() throws Exception {
    createNamespace();
    createExternalDeltaTable("old_name");

    String body = """
        {"new-name": "new_name"}
        """;

    AggregatedHttpResponse renameResp =
        client
            .execute(jsonPost(TEST_BASE + "/tables/old_name/rename"), HttpData.ofUtf8(body))
            .aggregate()
            .join();

    assertThat(renameResp.status().code()).isEqualTo(204);

    // Old name should be gone
    AggregatedHttpResponse oldResp =
        client.get(TEST_BASE + "/tables/old_name").aggregate().join();
    assertThat(oldResp.status().code()).isEqualTo(404);

    // New name should work
    AggregatedHttpResponse newResp =
        client.get(TEST_BASE + "/tables/new_name").aggregate().join();
    assertThat(newResp.status().code()).isEqualTo(200);
  }

  @Test
  public void testGetTableCredentials() throws Exception {
    createNamespace();
    createExternalDeltaTable("cred_table");

    AggregatedHttpResponse resp =
        client
            .get(TEST_BASE + "/tables/cred_table/credentials?operation=READ")
            .aggregate()
            .join();

    assertThat(resp.status().code()).isEqualTo(200);
    JsonNode response = MAPPER.readTree(resp.contentUtf8());
    assertThat(response.has("storage-credentials")).isTrue();
    assertThat(response.path("storage-credentials").isArray()).isTrue();
  }

  @Test
  public void testGetTemporaryPathCredentials() throws Exception {
    createNamespace();
    String location = getManagedStorageCloudPath(testDirectoryRoot.resolve("temp_path"));

    AggregatedHttpResponse resp =
        client
            .get("/v1/temporary-path-credentials?location=" + location + "&operation=READ_WRITE")
            .aggregate()
            .join();

    assertThat(resp.status().code()).isEqualTo(200);
    JsonNode response = MAPPER.readTree(resp.contentUtf8());
    assertThat(response.has("storage-credentials")).isTrue();
  }

  @Test
  public void testStagingTableCreate() throws Exception {
    createNamespace();

    String body = """
        {"name": "managed_staging"}
        """;

    AggregatedHttpResponse resp =
        client
            .execute(jsonPost(TEST_BASE + "/staging-tables"), HttpData.ofUtf8(body))
            .aggregate()
            .join();

    assertThat(resp.status().code()).isEqualTo(200);
    JsonNode response = MAPPER.readTree(resp.contentUtf8());
    assertThat(response.path("table-id").asText()).isNotBlank();
    assertThat(response.path("table-type").asText()).isEqualTo("MANAGED");
    assertThat(response.path("location").asText()).isNotBlank();
    assertThat(response.has("storage-credentials")).isTrue();
    assertThat(response.has("required-protocol")).isTrue();
    assertThat(response.has("suggested-protocol")).isTrue();
    assertThat(response.has("required-properties")).isTrue();

    // Verify required-protocol shape
    JsonNode reqProto = response.path("required-protocol");
    assertThat(reqProto.path("min-reader-version").asInt()).isGreaterThanOrEqualTo(3);
    assertThat(reqProto.path("min-writer-version").asInt()).isGreaterThanOrEqualTo(7);
    assertThat(reqProto.path("writer-features").toString()).contains("catalogManaged");
  }

  @Test
  public void testReportMetrics() throws Exception {
    createNamespace();
    createExternalDeltaTable("metrics_table");

    String body = """
        {"report": "test"}
        """;

    AggregatedHttpResponse resp =
        client
            .execute(
                jsonPost(TEST_BASE + "/tables/metrics_table/metrics"), HttpData.ofUtf8(body))
            .aggregate()
            .join();

    assertThat(resp.status().code()).isEqualTo(204);
  }

  @Test
  public void testUpdateTableMultipleActionsInOneRequest() throws Exception {
    createNamespace();
    createDeltaRestTable("multi_table", "{\"keep\": \"original\"}");

    String body =
        """
        {
          "updates": [
            {
              "action": "set-properties",
              "updates": {"added": "new_value"}
            },
            {
              "action": "set-columns",
              "columns": {
                "type": "struct",
                "fields": [
                  {"name": "id", "type": "long", "nullable": false, "metadata": {}},
                  {"name": "ts", "type": "timestamp", "nullable": true, "metadata": {}}
                ]
              }
            },
            {
              "action": "set-table-comment",
              "comment": "multi-update comment"
            }
          ]
        }
        """;

    AggregatedHttpResponse resp =
        client
            .execute(jsonPost(TEST_BASE + "/tables/multi_table"), HttpData.ofUtf8(body))
            .aggregate()
            .join();

    assertThat(resp.status().code()).isEqualTo(200);
    JsonNode response = MAPPER.readTree(resp.contentUtf8());
    JsonNode metadata = response.path("metadata");
    assertThat(metadata.path("properties").path("added").asText()).isEqualTo("new_value");
    assertThat(metadata.path("properties").path("keep").asText()).isEqualTo("original");
    assertThat(metadata.path("columns").path("fields")).hasSize(2);
    assertThat(metadata.path("columns").path("fields").get(1).path("name").asText())
        .isEqualTo("ts");
  }

  private RequestHeaders jsonPost(String path) {
    return RequestHeaders.builder()
        .method(HttpMethod.POST)
        .path(path)
        .contentType(MediaType.JSON)
        .build();
  }

  private void createNamespace() throws Exception {
    CreateCatalog createCatalog =
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT);
    CatalogInfo catalogInfo = catalogOperations.createCatalog(createCatalog);
    assertThat(catalogInfo.getName()).isEqualTo(createCatalog.getName());

    CreateSchema createSchema =
        new CreateSchema().catalogName(TestUtils.CATALOG_NAME).name(TestUtils.SCHEMA_NAME);
    SchemaInfo schemaInfo = schemaOperations.createSchema(createSchema);
    assertThat(schemaInfo.getFullName()).isEqualTo(TestUtils.SCHEMA_FULL_NAME);
  }

  /**
   * Create a table via the Delta REST API with StructType columns, so tests exercise the
   * contract-native path end-to-end.
   */
  private void createDeltaRestTable(String tableName, String propertiesJson) throws Exception {
    String body =
        """
        {
          "name": "%s",
          "location": "%s",
          "table-type": "EXTERNAL",
          "data-source-format": "DELTA",
          "columns": {
            "type": "struct",
            "fields": [
              {"name": "id", "type": "long", "nullable": false, "metadata": {}}
            ]
          },
          "protocol": {
            "min-reader-version": 1,
            "min-writer-version": 2
          },
          "properties": %s
        }
        """
            .formatted(
                tableName,
                getManagedStorageCloudPath(testDirectoryRoot.resolve(tableName)),
                propertiesJson);

    AggregatedHttpResponse resp =
        client.execute(jsonPost(TEST_BASE + "/tables"), HttpData.ofUtf8(body)).aggregate().join();
    assertThat(resp.status().code()).isEqualTo(200);
  }

  private void createExternalDeltaTable(String tableName) throws Exception {
    ColumnInfo columnInfo =
        new ColumnInfo()
            .name("id")
            .typeText("INTEGER")
            .typeJson("{\"type\": \"integer\"}")
            .typeName(ColumnTypeName.INT)
            .typePrecision(10)
            .typeScale(0)
            .position(0)
            .nullable(false);
    CreateTable createTable =
        new CreateTable()
            .name(tableName)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(List.of(columnInfo))
            .storageLocation(getManagedStorageCloudPath(testDirectoryRoot.resolve(tableName)))
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA);
    TableInfo tableInfo = tableOperations.createTable(createTable);
    assertThat(tableInfo.getTableId()).isNotBlank();
  }
}
