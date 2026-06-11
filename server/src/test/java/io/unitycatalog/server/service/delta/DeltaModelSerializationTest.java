package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.delta.model.DeltaAddCommitUpdate;
import io.unitycatalog.server.delta.model.DeltaArrayType;
import io.unitycatalog.server.delta.model.DeltaAssertEtag;
import io.unitycatalog.server.delta.model.DeltaAssertTableUUID;
import io.unitycatalog.server.delta.model.DeltaClusteringDomainMetadata;
import io.unitycatalog.server.delta.model.DeltaCommit;
import io.unitycatalog.server.delta.model.DeltaCreateTableRequest;
import io.unitycatalog.server.delta.model.DeltaDecimalType;
import io.unitycatalog.server.delta.model.DeltaDomainMetadataUpdates;
import io.unitycatalog.server.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.server.delta.model.DeltaMapType;
import io.unitycatalog.server.delta.model.DeltaPrimitiveType;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DeltaRemoveDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.DeltaRemovePropertiesUpdate;
import io.unitycatalog.server.delta.model.DeltaSetDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.DeltaSetLatestBackfilledVersionUpdate;
import io.unitycatalog.server.delta.model.DeltaSetPartitionColumnsUpdate;
import io.unitycatalog.server.delta.model.DeltaSetPropertiesUpdate;
import io.unitycatalog.server.delta.model.DeltaSetProtocolUpdate;
import io.unitycatalog.server.delta.model.DeltaSetSchemaUpdate;
import io.unitycatalog.server.delta.model.DeltaSetTableCommentUpdate;
import io.unitycatalog.server.delta.model.DeltaStructField;
import io.unitycatalog.server.delta.model.DeltaStructFieldMetadata;
import io.unitycatalog.server.delta.model.DeltaStructType;
import io.unitycatalog.server.delta.model.DeltaTableType;
import io.unitycatalog.server.delta.model.DeltaTableUpdate;
import io.unitycatalog.server.delta.model.DeltaUniformMetadata;
import io.unitycatalog.server.delta.model.DeltaUniformMetadataIceberg;
import io.unitycatalog.server.delta.model.DeltaUpdateSnapshotVersionUpdate;
import io.unitycatalog.server.delta.model.DeltaUpdateTableRequest;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests JSON deserialization and serialization of UC Delta API model types. The deserialization
 * test loads a JSON fixture and verifies all fields including typed DeltaDataType subtypes. The
 * serialization test constructs objects from scratch and compares against the same fixture.
 */
public class DeltaModelSerializationTest {

  private static final ObjectMapper MAPPER = DeltaApiMappers.MAPPER;

  private static String fixtureJson;

  @BeforeAll
  static void loadFixture() throws Exception {
    try (InputStream is =
        DeltaModelSerializationTest.class.getResourceAsStream(
            "/delta-model-test/update-table-request.json")) {
      fixtureJson = new String(is.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  // ==================== Deserialization ====================

  @Test
  public void testDeserialization() throws Exception {
    DeltaUpdateTableRequest request = MAPPER.readValue(fixtureJson, DeltaUpdateTableRequest.class);

    // Requirements
    assertThat(request.getRequirements()).hasSize(2);
    DeltaAssertTableUUID uuidReq = (DeltaAssertTableUUID) request.getRequirements().get(0);
    assertThat(uuidReq.getType()).isEqualTo("assert-table-uuid");
    assertThat(uuidReq.getUuid()).hasToString("550e8400-e29b-41d4-a716-446655440000");
    DeltaAssertEtag etagReq = (DeltaAssertEtag) request.getRequirements().get(1);
    assertThat(etagReq.getType()).isEqualTo("assert-etag");
    assertThat(etagReq.getEtag()).isEqualTo("etagabcdef");

    // Updates (11 total)
    List<DeltaTableUpdate> updates = request.getUpdates();
    assertThat(updates).hasSize(11);

    // set-properties
    DeltaSetPropertiesUpdate setProps = (DeltaSetPropertiesUpdate) updates.get(0);
    assertThat(setProps.getUpdates())
        .containsEntry("delta.columnMapping.mode", "name")
        .containsEntry("delta.enableDeletionVectors", "true");

    // remove-properties
    assertThat(((DeltaRemovePropertiesUpdate) updates.get(1)).getRemovals())
        .containsExactly("delta.logRetentionDuration");

    // set-columns: DeltaStructType with 4 fields, typed via DeltaDataTypeModule
    DeltaStructType schema = ((DeltaSetSchemaUpdate) updates.get(2)).getColumns();
    List<DeltaStructField> fields = schema.getFields();
    assertThat(fields).hasSize(4);

    // id: DeltaPrimitiveType("long")
    assertThat(fields.get(0).getType()).isInstanceOf(DeltaPrimitiveType.class);
    assertThat(fields.get(0).getType().getType()).isEqualTo("long");
    assertThat(fields.get(0).getNullable()).isFalse();

    // price: DeltaDecimalType(10, 2)
    assertThat(fields.get(1).getType()).isInstanceOf(DeltaDecimalType.class);
    DeltaDecimalType dt = (DeltaDecimalType) fields.get(1).getType();
    assertThat(dt.getPrecision()).isEqualTo(10);
    assertThat(dt.getScale()).isEqualTo(2);

    // tags: DeltaArrayType(elementType=DeltaPrimitiveType("string"))
    assertThat(fields.get(2).getType()).isInstanceOf(DeltaArrayType.class);
    DeltaArrayType arrType = (DeltaArrayType) fields.get(2).getType();
    assertThat(arrType.getElementType()).isInstanceOf(DeltaPrimitiveType.class);
    assertThat(arrType.getElementType().getType()).isEqualTo("string");
    assertThat(arrType.getContainsNull()).isTrue();

    // scores: DeltaMapType(key=string, value=DeltaStructType([value:double, timestamp:long]))
    assertThat(fields.get(3).getType()).isInstanceOf(DeltaMapType.class);
    DeltaMapType mapType = (DeltaMapType) fields.get(3).getType();
    assertThat(mapType.getKeyType()).isInstanceOf(DeltaPrimitiveType.class);
    assertThat(mapType.getKeyType().getType()).isEqualTo("string");
    assertThat(mapType.getValueType()).isInstanceOf(DeltaStructType.class);
    DeltaStructType innerStruct = (DeltaStructType) mapType.getValueType();
    assertThat(innerStruct.getFields()).hasSize(2);
    assertThat(innerStruct.getFields().get(0).getName()).isEqualTo("value");
    assertThat(innerStruct.getFields().get(0).getType()).isInstanceOf(DeltaPrimitiveType.class);
    assertThat(innerStruct.getFields().get(0).getType().getType()).isEqualTo("double");
    assertThat(innerStruct.getFields().get(1).getName()).isEqualTo("timestamp");
    assertThat(innerStruct.getFields().get(1).getType()).isInstanceOf(DeltaPrimitiveType.class);

    // set-table-comment
    assertThat(((DeltaSetTableCommentUpdate) updates.get(3)).getComment())
        .isEqualTo("updated table");

    // add-commit with uniform
    DeltaCommit commit = ((DeltaAddCommitUpdate) updates.get(4)).getCommit();
    assertThat(commit.getVersion()).isEqualTo(5);
    assertThat(commit.getTimestamp()).isEqualTo(1700000000000L);
    assertThat(commit.getFileName()).isEqualTo("00000005.json");
    assertThat(commit.getFileSize()).isEqualTo(2048);
    DeltaUniformMetadataIceberg iceberg =
        ((DeltaAddCommitUpdate) updates.get(4)).getUniform().getIceberg();
    assertThat(iceberg.getMetadataLocation())
        .isEqualTo("s3://bucket/table/metadata/v5.metadata.json");
    assertThat(iceberg.getConvertedDeltaVersion()).isEqualTo(5);
    assertThat(iceberg.getConvertedDeltaTimestamp()).isEqualTo(1700000000000L);

    // set-latest-backfilled-version
    assertThat(((DeltaSetLatestBackfilledVersionUpdate) updates.get(5)).getLatestPublishedVersion())
        .isEqualTo(4);

    // set-protocol
    DeltaProtocol protocol = ((DeltaSetProtocolUpdate) updates.get(6)).getProtocol();
    assertThat(protocol.getMinReaderVersion()).isEqualTo(3);
    assertThat(protocol.getMinWriterVersion()).isEqualTo(7);
    assertThat(protocol.getWriterFeatures())
        .containsExactly("catalogManaged", "deletionVectors", "inCommitTimestamp");

    // set-domain-metadata
    DeltaSetDomainMetadataUpdate setDomain = (DeltaSetDomainMetadataUpdate) updates.get(7);
    assertThat(setDomain.getUpdates().getDeltaClustering().getClusteringColumns())
        .isEqualTo(List.of(List.of("region"), List.of("event_time")));

    // remove-domain-metadata
    assertThat(((DeltaRemoveDomainMetadataUpdate) updates.get(8)).getDomains())
        .containsExactly("delta.rowTracking");

    // set-partition-columns
    assertThat(((DeltaSetPartitionColumnsUpdate) updates.get(9)).getPartitionColumns())
        .containsExactly("region", "date");

    // update-metadata-snapshot-version
    DeltaUpdateSnapshotVersionUpdate snapUpdate =
        (DeltaUpdateSnapshotVersionUpdate) updates.get(10);
    assertThat(snapUpdate.getLastCommitVersion()).isEqualTo(42);
    assertThat(snapUpdate.getLastCommitTimestampMs()).isEqualTo(1700000000000L);
  }

  @Test
  public void testDeserNestedComplex() throws Exception {
    // map<string, array<struct<v:double>>> -- deeper nesting than the fixture covers
    String json =
        """
        {
          "name": "data",
          "type": {
            "type": "map",
            "key-type": "string",
            "value-type": {
              "type": "array",
              "element-type": {
                "type": "struct",
                "fields": [{
                  "name": "v",
                  "type": "double",
                  "nullable": false,
                  "metadata": {}
                }]
              },
              "contains-null": true
            },
            "value-contains-null": true
          },
          "nullable": true,
          "metadata": {}
        }
        """;
    DeltaStructField col = MAPPER.readValue(json, DeltaStructField.class);
    assertThat(col.getType()).isInstanceOf(DeltaMapType.class);
    DeltaMapType mt = (DeltaMapType) col.getType();
    assertThat(mt.getValueType()).isInstanceOf(DeltaArrayType.class);
    DeltaArrayType at = (DeltaArrayType) mt.getValueType();
    assertThat(at.getElementType()).isInstanceOf(DeltaStructType.class);
    DeltaStructType st = (DeltaStructType) at.getElementType();
    assertThat(st.getFields().get(0).getType()).isInstanceOf(DeltaPrimitiveType.class);
    assertThat(st.getFields().get(0).getType().getType()).isEqualTo("double");
  }

  // ==================== Edge cases ====================

  @Test
  public void testDeserPrimitiveAndDecimalEdgeCases() throws Exception {
    // All primitive types deserialize as DeltaPrimitiveType
    for (String type :
        List.of(
            "long",
            "string",
            "boolean",
            "binary",
            "date",
            "timestamp",
            "integer",
            "short",
            "byte",
            "float",
            "double")) {
      String json =
          "{\"name\":\"col\",\"type\":\"" + type + "\",\"nullable\":true,\"metadata\":{}}";
      DeltaStructField col = MAPPER.readValue(json, DeltaStructField.class);
      assertThat(col.getType()).isInstanceOf(DeltaPrimitiveType.class);
      assertThat(col.getType().getType()).isEqualTo(type);

      // Round-trip: serializes back to bare string
      String serialized = MAPPER.writeValueAsString(col);
      JsonNode node = MAPPER.readTree(serialized);
      assertThat(node.get("type").isTextual()).isTrue();
      assertThat(node.get("type").asText()).isEqualTo(type);
    }

    // Decimal with various precision/scale
    for (String decStr : List.of("decimal(10,2)", "decimal(38,0)", "decimal(1,1)")) {
      String json =
          "{\"name\":\"col\",\"type\":\"" + decStr + "\",\"nullable\":true,\"metadata\":{}}";
      DeltaStructField col = MAPPER.readValue(json, DeltaStructField.class);
      assertThat(col.getType()).isInstanceOf(DeltaDecimalType.class);

      // Round-trip
      String serialized = MAPPER.writeValueAsString(col);
      JsonNode node = MAPPER.readTree(serialized);
      assertThat(node.get("type").asText()).isEqualTo(decStr);
    }

    // Bare "decimal" (no parens) is treated as a primitive, not DeltaDecimalType
    String json = "{\"name\":\"col\",\"type\":\"decimal\",\"nullable\":true,\"metadata\":{}}";
    DeltaStructField col = MAPPER.readValue(json, DeltaStructField.class);
    assertThat(col.getType()).isInstanceOf(DeltaPrimitiveType.class);
    assertThat(col.getType().getType()).isEqualTo("decimal");
  }

  @Test
  public void testDeserDecimalAsJsonObject() throws Exception {
    // DeltaDecimalType can also appear as a JSON object (not just bare string)
    String json =
        """
        {
          "name": "price",
          "type": {"type": "decimal", "precision": 10, "scale": 2},
          "nullable": true,
          "metadata": {}
        }
        """;
    DeltaStructField col = MAPPER.readValue(json, DeltaStructField.class);
    assertThat(col.getType()).isInstanceOf(DeltaDecimalType.class);
    DeltaDecimalType dt = (DeltaDecimalType) col.getType();
    assertThat(dt.getPrecision()).isEqualTo(10);
    assertThat(dt.getScale()).isEqualTo(2);

    // Round-trip: serializes back to string form "decimal(10,2)"
    String serialized = MAPPER.writeValueAsString(col);
    JsonNode node = MAPPER.readTree(serialized);
    assertThat(node.get("type").isTextual()).isTrue();
    assertThat(node.get("type").asText()).isEqualTo("decimal(10,2)");
  }

  @Test
  public void testDeserMalformedDecimalString() throws Exception {
    // Malformed decimal string doesn't match decimal(N,N) regex,
    // so it falls through to DeltaPrimitiveType (same as any unrecognized type string)
    String json =
        "{\"name\":\"col\",\"type\":\"decimal(abc)\",\"nullable\":true,\"metadata\":{}}";
    DeltaStructField col = MAPPER.readValue(json, DeltaStructField.class);
    assertThat(col.getType()).isInstanceOf(DeltaPrimitiveType.class);
    assertThat(col.getType().getType()).isEqualTo("decimal(abc)");

    // Missing scale: "decimal(10)" -- also not matched, becomes DeltaPrimitiveType
    json = "{\"name\":\"col\",\"type\":\"decimal(10)\",\"nullable\":true,\"metadata\":{}}";
    col = MAPPER.readValue(json, DeltaStructField.class);
    assertThat(col.getType()).isInstanceOf(DeltaPrimitiveType.class);
  }

  @Test
  public void testDeserOmittedContainsNullLeavesNull() throws Exception {
    // delta.yaml dropped `default: true` on contains-null / value-contains-null so the generated
    // POJOs no longer auto-fill those fields; an omitted JSON property leaves the POJO field at
    // null. The mapper's per-column validator (ColumnUtils.validateStructType) then rejects with
    // a 400. Pinned here so a future regen that re-introduces a default is caught.
    String arrayJson =
        """
        {
          "name": "tags",
          "type": {"type": "array", "element-type": "string"},
          "nullable": true,
          "metadata": {}
        }
        """;
    DeltaStructField arrCol = MAPPER.readValue(arrayJson, DeltaStructField.class);
    assertThat(((DeltaArrayType) arrCol.getType()).getContainsNull()).isNull();

    String mapJson =
        """
        {
          "name": "kv",
          "type": {
            "type": "map",
            "key-type": "string",
            "value-type": "integer"
          },
          "nullable": true,
          "metadata": {}
        }
        """;
    DeltaStructField mapCol = MAPPER.readValue(mapJson, DeltaStructField.class);
    assertThat(((DeltaMapType) mapCol.getType()).getValueContainsNull()).isNull();
  }

  // ==================== Serialization ====================

  /**
   * Constructs a full DeltaUpdateTableRequest and serializes it to JSON, then compares against the
   * fixture. See client-side DeltaModelSerializationTest for the full list of tested types.
   */
  @Test
  public void testSerialization() throws Exception {
    DeltaUpdateTableRequest request =
        new DeltaUpdateTableRequest()
            .requirements(
                List.of(
                    new DeltaAssertTableUUID()
                        .type("assert-table-uuid")
                        .uuid(UUID.fromString("550e8400-e29b-41d4-a716-446655440000")),
                    new DeltaAssertEtag().type("assert-etag").etag("etagabcdef")));

    DeltaSetPropertiesUpdate setProps =
        new DeltaSetPropertiesUpdate()
            .action("set-properties")
            .updates(
                Map.of(
                    "delta.columnMapping.mode", "name",
                    "delta.enableDeletionVectors", "true"));

    DeltaRemovePropertiesUpdate removeProps =
        new DeltaRemovePropertiesUpdate()
            .action("remove-properties")
            .removals(List.of("delta.logRetentionDuration"));

    // set-columns with typed DeltaStructType
    DeltaStructField colId =
        new DeltaStructField()
            .name("id")
            .type(new DeltaPrimitiveType().type("long"))
            .nullable(false)
            .metadata(
                meta(
                    null,
                    Map.of(
                        "delta.columnMapping.id", 1,
                        "delta.columnMapping.physicalName", "col-1")));
    DeltaStructField colPrice =
        new DeltaStructField()
            .name("price")
            .type(new DeltaDecimalType().precision(10).scale(2))
            .nullable(true)
            .metadata(new DeltaStructFieldMetadata());
    DeltaStructField colTags =
        new DeltaStructField()
            .name("tags")
            .type(
                new DeltaArrayType()
                    .elementType(new DeltaPrimitiveType().type("string"))
                    .containsNull(true))
            .nullable(true)
            .metadata(new DeltaStructFieldMetadata());
    DeltaStructField colScores =
        new DeltaStructField()
            .name("scores")
            .type(
                new DeltaMapType()
                    .keyType(new DeltaPrimitiveType().type("string"))
                    .valueType(
                        new DeltaStructType()
                            .fields(
                                List.of(
                                    new DeltaStructField()
                                        .name("value")
                                        .type(new DeltaPrimitiveType().type("double"))
                                        .nullable(false)
                                        .metadata(
                                            meta(
                                                "score value",
                                                Map.of(
                                                    "delta.columnMapping.id", 10,
                                                    "delta.columnMapping.physicalName", "col-10"))),
                                    new DeltaStructField()
                                        .name("timestamp")
                                        .type(new DeltaPrimitiveType().type("long"))
                                        .nullable(true)
                                        .metadata(
                                            meta(null, Map.of("delta.columnMapping.id", 11))))))
                    .valueContainsNull(true))
            .nullable(true)
            .metadata(new DeltaStructFieldMetadata());
    DeltaSetSchemaUpdate setSchema =
        new DeltaSetSchemaUpdate()
            .action("set-columns")
            .columns(new DeltaStructType().fields(List.of(colId, colPrice, colTags, colScores)));

    DeltaSetTableCommentUpdate setComment =
        new DeltaSetTableCommentUpdate().action("set-table-comment").comment("updated table");

    DeltaAddCommitUpdate addCommit =
        new DeltaAddCommitUpdate()
            .action("add-commit")
            .commit(
                new DeltaCommit()
                    .version(5L)
                    .timestamp(1700000000000L)
                    .fileName("00000005.json")
                    .fileSize(2048L)
                    .fileModificationTimestamp(1700000001000L))
            .uniform(
                new DeltaUniformMetadata()
                    .iceberg(
                        new DeltaUniformMetadataIceberg()
                            .metadataLocation("s3://bucket/table/metadata/v5.metadata.json")
                            .convertedDeltaVersion(5L)
                            .convertedDeltaTimestamp(1700000000000L)));

    DeltaSetLatestBackfilledVersionUpdate setBackfill =
        new DeltaSetLatestBackfilledVersionUpdate()
            .action("set-latest-backfilled-version")
            .latestPublishedVersion(4L);

    DeltaSetProtocolUpdate setProtocol =
        new DeltaSetProtocolUpdate()
            .action("set-protocol")
            .protocol(
                new DeltaProtocol()
                    .minReaderVersion(3)
                    .minWriterVersion(7)
                    .readerFeatures(List.of("deletionVectors", "vacuumProtocolCheck"))
                    .writerFeatures(
                        List.of("catalogManaged", "deletionVectors", "inCommitTimestamp")));

    DeltaSetDomainMetadataUpdate setDomain =
        new DeltaSetDomainMetadataUpdate()
            .action("set-domain-metadata")
            .updates(
                new DeltaDomainMetadataUpdates()
                    .deltaClustering(
                        new DeltaClusteringDomainMetadata()
                            .clusteringColumns(List.of(List.of("region"), List.of("event_time")))));

    DeltaRemoveDomainMetadataUpdate removeDomain =
        new DeltaRemoveDomainMetadataUpdate()
            .action("remove-domain-metadata")
            .domains(List.of("delta.rowTracking"));

    DeltaSetPartitionColumnsUpdate setPartition =
        new DeltaSetPartitionColumnsUpdate()
            .action("set-partition-columns")
            .partitionColumns(List.of("region", "date"));

    DeltaUpdateSnapshotVersionUpdate snapUpdate =
        new DeltaUpdateSnapshotVersionUpdate()
            .action("update-metadata-snapshot-version")
            .lastCommitVersion(42L)
            .lastCommitTimestampMs(1700000000000L);

    request.setUpdates(
        List.of(
            setProps,
            removeProps,
            setSchema,
            setComment,
            addCommit,
            setBackfill,
            setProtocol,
            setDomain,
            removeDomain,
            setPartition,
            snapUpdate));

    // Serialize and compare with fixture (tree comparison is order-independent).
    // Pretty-print in the assertion message so failures show readable diffs.
    JsonNode expected = MAPPER.readTree(fixtureJson);
    JsonNode actual = MAPPER.readTree(MAPPER.writeValueAsString(request));
    assertThat(actual)
        .as(
            "Expected:\n%s\n\nActual:\n%s",
            MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(expected),
            MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(actual))
        .isEqualTo(expected);
  }

  // ==================== DeltaCreateTableRequest round-trip ====================

  @Test
  public void testCreateTableRequestRoundTrip() throws Exception {
    String json = readFixture("/delta-model-test/create-table-request.json");
    DeltaCreateTableRequest req =
        MAPPER.readValue(json, DeltaCreateTableRequest.class);

    // Verify DeltaDataType serde works on nested DeltaStructType fields
    DeltaStructField idField = req.getColumns().getFields().get(0);
    assertThat(idField.getType()).isInstanceOf(DeltaPrimitiveType.class);
    assertThat(idField.getType().getType()).isEqualTo("long");

    DeltaStructField amountField = req.getColumns().getFields().get(1);
    assertThat(amountField.getType()).isInstanceOf(DeltaDecimalType.class);
    assertThat(((DeltaDecimalType) amountField.getType()).getPrecision()).isEqualTo(10);

    DeltaStructField tagsField = req.getColumns().getFields().get(2);
    assertThat(tagsField.getType()).isInstanceOf(DeltaArrayType.class);

    // Shallow-clone fields: enum wire value and kebab-case base-table-id key
    assertThat(req.getTableType()).isEqualTo(DeltaTableType.MANAGED_SHALLOW_CLONE);
    assertThat(req.getBaseTableId())
        .isEqualTo(UUID.fromString("770e8400-e29b-41d4-a716-446655440222"));

    // Round-trip: re-serialize, decimal-as-object becomes bare "decimal(10,2)" string
    JsonNode actual = MAPPER.readTree(MAPPER.writeValueAsString(req));
    assertThat(actual.path("columns").path("fields").get(0).path("type").asText())
        .isEqualTo("long");
    assertThat(actual.path("columns").path("fields").get(1).path("type").asText())
        .isEqualTo("decimal(10,2)");
    assertThat(actual.path("columns").path("fields").get(2).path("type").path("type").asText())
        .isEqualTo("array");
    assertThat(actual.path("table-type").asText()).isEqualTo("MANAGED_SHALLOW_CLONE");
    assertThat(actual.path("base-table-id").asText())
        .isEqualTo("770e8400-e29b-41d4-a716-446655440222");
  }

  // ==================== DeltaLoadTableResponse round-trip ====================

  @Test
  public void testLoadTableResponseRoundTrip() throws Exception {
    String json = readFixture("/delta-model-test/load-table-response.json");
    DeltaLoadTableResponse resp =
        MAPPER.readValue(json, DeltaLoadTableResponse.class);

    // Verify DeltaDataType serde works in DeltaTableMetadata.columns
    DeltaStructField priceField = resp.getMetadata().getColumns().getFields().get(1);
    assertThat(priceField.getType()).isInstanceOf(DeltaDecimalType.class);
    assertThat(((DeltaDecimalType) priceField.getType()).getPrecision()).isEqualTo(10);

    DeltaStructField scoresField = resp.getMetadata().getColumns().getFields().get(2);
    assertThat(scoresField.getType()).isInstanceOf(DeltaMapType.class);

    // Round-trip matches fixture
    JsonNode expected = MAPPER.readTree(json);
    JsonNode actual = MAPPER.readTree(MAPPER.writeValueAsString(resp));
    assertThat(actual)
        .as(
            "Expected:\n%s\n\nActual:\n%s",
            MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(expected),
            MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(actual))
        .isEqualTo(expected);
  }

  private static String readFixture(String resourcePath) throws Exception {
    try (InputStream is = DeltaModelSerializationTest.class.getResourceAsStream(resourcePath)) {
      return new String(is.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  /**
   * Build a {@link DeltaStructFieldMetadata} with the optional {@code comment} plus a bag of
   * additional properties for the spec's dotted Delta keys (e.g. {@code delta.columnMapping.id}).
   * The schema models {@code DeltaStructFieldMetadata} as a bare additionalProperties wrapper, so
   * the generated class extends {@code HashMap<String, Object>}; all keys (including
   * {@code comment}) go through the map view.
   */
  private static DeltaStructFieldMetadata meta(String comment, Map<String, Object> additional) {
    DeltaStructFieldMetadata m = new DeltaStructFieldMetadata();
    if (comment != null) {
      m.put("comment", comment);
    }
    m.putAll(additional);
    return m;
  }
}
