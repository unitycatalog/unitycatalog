package io.unitycatalog.server.sdk.delta;

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
import io.unitycatalog.client.delta.api.TablesApi;
import io.unitycatalog.client.delta.model.CreateStagingTableRequest;
import io.unitycatalog.client.delta.model.ErrorType;
import io.unitycatalog.client.delta.model.StagingTableResponse;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Raw-HTTP integration tests for the Delta REST Catalog {@code POST /v1/.../tables} endpoint.
 *
 * <p>The auto-generated {@code unitycatalog-client} can't construct certain malformed payloads --
 * its DTOs reject them at compile time -- but a non-SDK client (Rust, Kernel) is not bound by the
 * SDK's typed shape. This test posts hand-crafted JSON straight at the endpoint to pin server-
 * side validation that the SDK tests cannot reach. Sister to {@link SdkCreateTableTest}, which
 * covers everything reachable through the typed client.
 */
public class RawCreateTableTest extends BaseCRUDTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String CREATE_TABLE_PATH =
      "/api/2.1/unity-catalog/delta/v1/catalogs/"
          + TestUtils.CATALOG_NAME
          + "/schemas/"
          + TestUtils.SCHEMA_NAME
          + "/tables";

  private TablesApi deltaTablesApi;
  private WebClient client;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @BeforeEach
  @Override
  @SneakyThrows
  public void setUp() {
    super.setUp();
    deltaTablesApi = new TablesApi(TestUtils.createApiClient(serverConfig));
    SdkSchemaOperations schemaOperations =
        new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
    catalogOperations.createCatalog(new CreateCatalog().name(TestUtils.CATALOG_NAME));
    schemaOperations.createSchema(
        new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
    client =
        WebClient.builder(serverConfig.getServerUrl())
            .auth(AuthToken.ofOAuth2(serverConfig.getAuthToken()))
            .build();
  }

  @Test
  public void rawHttpRejectsMalformedColumnRequests() throws Exception {
    StagingTableResponse staging =
        deltaTablesApi.createStagingTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new CreateStagingTableRequest().name("tbl_raw"));

    // -------- StructField missing 'name' --------
    // Caught by ColumnUtils.validateStructType, called from DeltaCreateTableMapper before the
    // per-column conversion runs. The path locates the offending field.
    assertRejected(
        staging,
        """
        {"type": "long", "nullable": false, "metadata": {}}""",
        "columns.fields[0].name is required.");

    // -------- StructField missing 'type', second column in a multi-column request --------
    // Pins that the validator iterates every column (not just the first) and that the path index
    // reflects the offending column's position. A valid first column should pass and the
    // malformed second column should be the one that surfaces in the error.
    assertRejected(
        staging,
        """
        {"name": "id", "type": "long", "nullable": false, "metadata": {}},
        {"name": "amount", "nullable": true, "metadata": {}}""",
        "columns.fields[1].type is required.");

    // -------- a field element that is the bare string instead of a StructField object --------
    // Pins the bug shape from PR #1524: a client serializes the value of `type` (the bare
    // string "long") in the field position rather than the full StructField wrapper. Jackson
    // can't coerce the string to a StructField bean, so the request must be rejected at the
    // deserialization boundary before any validator runs. Substring match because the Jackson +
    // Armeria envelope adds source-location and reference-chain noise we don't want to pin.
    assertRejectedMessageContains(
        staging,
        "\"long\"",
        "Cannot construct instance of"
            + " `io.unitycatalog.server.delta.model.StructField`"
            + " (although at least one Creator exists):"
            + " no String-argument constructor/factory method"
            + " to deserialize from String value ('long')");

    // -------- column with an unsupported primitive type --------
    // Pins that primitive type names go through validatePrimitiveType -> resolveColumnTypeName
    // at the API boundary. "void" is Spark's NullType wire form, which Delta rejects on read;
    // PR #1524 hardened the closely-related Spark-connector type_json path, so we mirror that
    // rejection on the DRC create-table path with a raw-HTTP test.
    assertRejected(
        staging,
        """
        {"name": "x", "type": "void", "nullable": false, "metadata": {}}""",
        "Unsupported Delta primitive type: void");

    // -------- StructField missing 'nullable' --------
    // Per Delta PROTOCOL.md every StructField must carry nullable.
    assertRejected(
        staging,
        """
        {"name": "id", "type": "long", "metadata": {}}""",
        "columns.fields[0].nullable is required.");

    // -------- StructField missing 'metadata' --------
    // Enforceable now that delta.yaml types StructField.metadata as a typed wrapper class
    // (StructFieldMetadata) instead of a raw Map -- the generator no longer auto-fills the
    // field with new HashMap<>(), so a JSON omitting "metadata" leaves the field at null.
    assertRejected(
        staging,
        """
        {"name": "id", "type": "long", "nullable": false}""",
        "columns.fields[0].metadata is required.");

    // -------- ArrayType missing 'contains-null' --------
    // Validator descends into the StructField's nested type. Path picks up ".type" as it crosses
    // into the array. Enforceable now that delta.yaml dropped `default: true` for this field.
    assertRejected(
        staging,
        """
        {
          "name": "tags",
          "type": {"type": "array", "element-type": "string"},
          "nullable": true,
          "metadata": {}
        }""",
        "columns.fields[0].type.contains-null is required.");

    // -------- MapType missing 'value-contains-null' --------
    assertRejected(
        staging,
        """
        {
          "name": "props",
          "type": {"type": "map", "key-type": "string", "value-type": "string"},
          "nullable": true,
          "metadata": {}
        }""",
        "columns.fields[0].type.value-contains-null is required.");

    // -------- nested StructField inside an array of structs --------
    // Pins that recursion descends into array elementType -> struct fields. The malformed inner
    // field has no name; the path captures the full descent.
    assertRejected(
        staging,
        """
        {
          "name": "items",
          "type": {
            "type": "array",
            "element-type": {
              "type": "struct",
              "fields": [{"type": "long", "nullable": false, "metadata": {}}]
            },
            "contains-null": true
          },
          "nullable": true,
          "metadata": {}
        }""",
        "columns.fields[0].type.element-type.fields[0].name is required.");

    // -------- columns.fields is an empty list --------
    assertRejected(staging, "", "Table must have at least one column.");
  }

  // ---------- helpers ----------

  /**
   * Post a createTable request whose {@code columns.fields} contains the supplied {@code
   * columnJson} (or no fields at all when {@code columnJson} is empty), and assert the server
   * rejects it with HTTP 400 and the exact {@code expectedMessage} in {@code error.message}. The
   * surrounding request body is a full UC-managed shape (location/table-type/data-source-format/
   * protocol/properties) bound to the given staging response, so cases that pass the column
   * gates land on a request the rest of the server would normally accept.
   */
  @SneakyThrows
  private void assertRejected(
      StagingTableResponse staging, String columnJson, String expectedMessage) {
    JsonNode error = postAndAssertRejected(staging, columnJson);
    assertThat(error.get("message").asText()).isEqualTo(expectedMessage);
  }

  /**
   * Like {@link #assertRejected} but matches a substring of {@code error.message} -- used when
   * the response message comes from Jackson + Armeria framework wrapping (e.g. a deserialization
   * mismatch) and contains source-location / reference-chain noise we don't want to pin.
   */
  @SneakyThrows
  private void assertRejectedMessageContains(
      StagingTableResponse staging, String columnJson, String messageSubstring) {
    JsonNode error = postAndAssertRejected(staging, columnJson);
    assertThat(error.get("message").asText()).contains(messageSubstring);
  }

  /**
   * Posts a createTable request whose {@code columns.fields} contains {@code columnJson} and
   * asserts the envelope (HTTP 400, {@code error.code} 400, {@code error.type}
   * "InvalidParameterValueException"). Returns {@code error} so the caller can additionally
   * assert on {@code message}.
   */
  @SneakyThrows
  private JsonNode postAndAssertRejected(StagingTableResponse staging, String columnJson) {
    String body =
        String.format(
            """
            {
              "name": "tbl_raw",
              "location": "%s",
              "table-type": "MANAGED",
              "data-source-format": "DELTA",
              "columns": {
                "type": "struct",
                "fields": [%s]
              },
              "protocol": {
                "min-reader-version": 3,
                "min-writer-version": 7,
                "reader-features": [
                  "catalogManaged", "deletionVectors", "v2Checkpoint", "vacuumProtocolCheck"
                ],
                "writer-features": [
                  "catalogManaged", "deletionVectors", "inCommitTimestamp",
                  "v2Checkpoint", "vacuumProtocolCheck"
                ]
              },
              "properties": {
                "delta.checkpointPolicy": "v2",
                "delta.enableDeletionVectors": "true",
                "delta.enableInCommitTimestamps": "true",
                "io.unitycatalog.tableId": "%s",
                "delta.inCommitTimestampEnablementVersion": "0",
                "delta.inCommitTimestampEnablementTimestamp": "1700000000000"
              }
            }
            """,
            staging.getLocation(), columnJson, staging.getTableId().toString());

    RequestHeaders headers =
        RequestHeaders.builder()
            .method(HttpMethod.POST)
            .path(CREATE_TABLE_PATH)
            .contentType(MediaType.JSON)
            .build();
    AggregatedHttpResponse resp =
        client.execute(headers, HttpData.ofUtf8(body)).aggregate().join();

    String content = resp.contentUtf8();
    assertThat(resp.status().code()).as("body: %s", content).isEqualTo(400);
    JsonNode error = MAPPER.readTree(content).get("error");
    assertThat(error).as("body: %s", content).isNotNull();
    assertThat(error.get("code").asInt()).isEqualTo(400);
    assertThat(error.get("type").asText())
        .isEqualTo(ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION.getValue());
    return error;
  }
}
