package io.unitycatalog.client.delta;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.unitycatalog.client.delta.model.AddCommitUpdate;
import io.unitycatalog.client.delta.model.ArrayType;
import io.unitycatalog.client.delta.model.AssertEtag;
import io.unitycatalog.client.delta.model.AssertTableUUID;
import io.unitycatalog.client.delta.model.ClusteringDomainMetadata;
import io.unitycatalog.client.delta.model.DecimalType;
import io.unitycatalog.client.delta.model.DeltaCommit;
import io.unitycatalog.client.delta.model.DeltaProtocol;
import io.unitycatalog.client.delta.model.DomainMetadataUpdates;
import io.unitycatalog.client.delta.model.MapType;
import io.unitycatalog.client.delta.model.PrimitiveType;
import io.unitycatalog.client.delta.model.RemoveDomainMetadataUpdate;
import io.unitycatalog.client.delta.model.RemovePropertiesUpdate;
import io.unitycatalog.client.delta.model.SetDomainMetadataUpdate;
import io.unitycatalog.client.delta.model.SetLatestBackfilledVersionUpdate;
import io.unitycatalog.client.delta.model.SetPartitionColumnsUpdate;
import io.unitycatalog.client.delta.model.SetPropertiesUpdate;
import io.unitycatalog.client.delta.model.SetProtocolUpdate;
import io.unitycatalog.client.delta.model.SetSchemaUpdate;
import io.unitycatalog.client.delta.model.SetTableCommentUpdate;
import io.unitycatalog.client.delta.model.StructField;
import io.unitycatalog.client.delta.model.StructType;
import io.unitycatalog.client.delta.model.TableUpdate;
import io.unitycatalog.client.delta.model.UniformMetadata;
import io.unitycatalog.client.delta.model.UniformMetadataIceberg;
import io.unitycatalog.client.delta.model.UpdateSnapshotVersionUpdate;
import io.unitycatalog.client.delta.model.UpdateTableRequest;
import io.unitycatalog.client.delta.serde.DeltaTypeModule;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests JSON deserialization and serialization of Delta REST API model types. The deserialization
 * test loads a JSON fixture and verifies all fields including typed DeltaType subtypes. The
 * serialization test constructs objects from scratch and compares against the same fixture.
 */
public class DeltaModelSerializationTest {

  private static final ObjectMapper MAPPER =
      JsonMapper.builder().serializationInclusion(JsonInclude.Include.NON_NULL).build();

  static {
    MAPPER.registerModule(new DeltaTypeModule());
  }

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
    UpdateTableRequest request = MAPPER.readValue(fixtureJson, UpdateTableRequest.class);

    // Requirements
    assertThat(request.getRequirements()).hasSize(2);
    AssertTableUUID uuidReq = (AssertTableUUID) request.getRequirements().get(0);
    assertThat(uuidReq.getType()).isEqualTo("assert-table-uuid");
    assertThat(uuidReq.getUuid()).hasToString("550e8400-e29b-41d4-a716-446655440000");
    AssertEtag etagReq = (AssertEtag) request.getRequirements().get(1);
    assertThat(etagReq.getType()).isEqualTo("assert-etag");
    assertThat(etagReq.getEtag()).isEqualTo("etagabcdef");

    // Updates (11 total)
    List<TableUpdate> updates = request.getUpdates();
    assertThat(updates).hasSize(11);

    // set-properties
    SetPropertiesUpdate setProps = (SetPropertiesUpdate) updates.get(0);
    assertThat(setProps.getUpdates())
        .containsEntry("delta.columnMapping.mode", "name")
        .containsEntry("delta.enableDeletionVectors", "true");

    // remove-properties
    assertThat(((RemovePropertiesUpdate) updates.get(1)).getRemovals())
        .containsExactly("delta.logRetentionDuration");

    // set-columns: StructType with 4 fields, typed via DeltaTypeModule
    StructType schema = ((SetSchemaUpdate) updates.get(2)).getColumns();
    List<StructField> fields = schema.getFields();
    assertThat(fields).hasSize(4);

    // id: PrimitiveType("long")
    assertThat(fields.get(0).getType()).isInstanceOf(PrimitiveType.class);
    assertThat(fields.get(0).getType().getType()).isEqualTo("long");
    assertThat(fields.get(0).getNullable()).isFalse();

    // price: DecimalType(10, 2)
    assertThat(fields.get(1).getType()).isInstanceOf(DecimalType.class);
    DecimalType dt = (DecimalType) fields.get(1).getType();
    assertThat(dt.getPrecision()).isEqualTo(10);
    assertThat(dt.getScale()).isEqualTo(2);

    // tags: ArrayType(elementType=PrimitiveType("string"))
    assertThat(fields.get(2).getType()).isInstanceOf(ArrayType.class);
    ArrayType arrType = (ArrayType) fields.get(2).getType();
    assertThat(arrType.getElementType()).isInstanceOf(PrimitiveType.class);
    assertThat(arrType.getElementType().getType()).isEqualTo("string");
    assertThat(arrType.getContainsNull()).isTrue();

    // scores: MapType(key=string, value=StructType([value:double, timestamp:long]))
    assertThat(fields.get(3).getType()).isInstanceOf(MapType.class);
    MapType mapType = (MapType) fields.get(3).getType();
    assertThat(mapType.getKeyType()).isInstanceOf(PrimitiveType.class);
    assertThat(mapType.getKeyType().getType()).isEqualTo("string");
    assertThat(mapType.getValueType()).isInstanceOf(StructType.class);
    StructType innerStruct = (StructType) mapType.getValueType();
    assertThat(innerStruct.getFields()).hasSize(2);
    assertThat(innerStruct.getFields().get(0).getName()).isEqualTo("value");
    assertThat(innerStruct.getFields().get(0).getType()).isInstanceOf(PrimitiveType.class);
    assertThat(innerStruct.getFields().get(0).getType().getType()).isEqualTo("double");
    assertThat(innerStruct.getFields().get(1).getName()).isEqualTo("timestamp");
    assertThat(innerStruct.getFields().get(1).getType()).isInstanceOf(PrimitiveType.class);

    // set-table-comment
    assertThat(((SetTableCommentUpdate) updates.get(3)).getComment()).isEqualTo("updated table");

    // add-commit with uniform
    DeltaCommit commit = ((AddCommitUpdate) updates.get(4)).getCommit();
    assertThat(commit.getVersion()).isEqualTo(5);
    assertThat(commit.getTimestamp()).isEqualTo(1700000000000L);
    assertThat(commit.getFileName()).isEqualTo("00000005.json");
    assertThat(commit.getFileSize()).isEqualTo(2048);
    UniformMetadataIceberg iceberg = ((AddCommitUpdate) updates.get(4)).getUniform().getIceberg();
    assertThat(iceberg.getMetadataLocation())
        .isEqualTo("s3://bucket/table/metadata/v5.metadata.json");
    assertThat(iceberg.getConvertedDeltaVersion()).isEqualTo(5);
    assertThat(iceberg.getConvertedDeltaTimestamp()).isEqualTo(1700000000000L);

    // set-latest-backfilled-version
    assertThat(((SetLatestBackfilledVersionUpdate) updates.get(5)).getLatestPublishedVersion())
        .isEqualTo(4);

    // set-protocol
    DeltaProtocol protocol = ((SetProtocolUpdate) updates.get(6)).getProtocol();
    assertThat(protocol.getMinReaderVersion()).isEqualTo(3);
    assertThat(protocol.getMinWriterVersion()).isEqualTo(7);
    assertThat(protocol.getWriterFeatures())
        .containsExactly("catalogManaged", "deletionVectors", "inCommitTimestamp");

    // set-domain-metadata
    SetDomainMetadataUpdate setDomain = (SetDomainMetadataUpdate) updates.get(7);
    assertThat(setDomain.getUpdates().getDeltaClustering().getClusteringColumns())
        .isEqualTo(List.of(List.of("region"), List.of("event_time")));

    // remove-domain-metadata
    assertThat(((RemoveDomainMetadataUpdate) updates.get(8)).getDomains())
        .containsExactly("delta.rowTracking");

    // set-partition-columns
    assertThat(((SetPartitionColumnsUpdate) updates.get(9)).getPartitionColumns())
        .containsExactly("region", "date");

    // update-metadata-snapshot-version
    UpdateSnapshotVersionUpdate snapUpdate = (UpdateSnapshotVersionUpdate) updates.get(10);
    assertThat(snapUpdate.getLastCommitVersion()).isEqualTo(42);
    assertThat(snapUpdate.getLastCommitTimestampMs()).isEqualTo(1700000000000L);
  }

  @Test
  public void testDeserNestedComplex() throws Exception {
    // map<string, array<struct<v:double>>>
    String json =
        "{"
            + " \"name\": \"data\","
            + " \"type\": {"
            + "   \"type\": \"map\","
            + "   \"key-type\": \"string\","
            + "   \"value-type\": {"
            + "     \"type\": \"array\","
            + "     \"element-type\": {"
            + "       \"type\": \"struct\","
            + "       \"fields\": [{"
            + "         \"name\": \"v\","
            + "         \"type\": \"double\","
            + "         \"nullable\": false,"
            + "         \"metadata\": {}"
            + "       }]"
            + "     },"
            + "     \"contains-null\": true"
            + "   },"
            + "   \"value-contains-null\": true"
            + " },"
            + " \"nullable\": true,"
            + " \"metadata\": {}"
            + "}";
    StructField col = MAPPER.readValue(json, StructField.class);
    assertThat(col.getType()).isInstanceOf(MapType.class);
    MapType mt = (MapType) col.getType();
    assertThat(mt.getValueType()).isInstanceOf(ArrayType.class);
    ArrayType at = (ArrayType) mt.getValueType();
    assertThat(at.getElementType()).isInstanceOf(StructType.class);
    StructType st = (StructType) at.getElementType();
    assertThat(st.getFields().get(0).getType()).isInstanceOf(PrimitiveType.class);
    assertThat(st.getFields().get(0).getType().getType()).isEqualTo("double");
  }

  // ==================== Edge cases ====================

  @Test
  public void testDeserPrimitiveAndDecimalEdgeCases() throws Exception {
    // All primitive types deserialize as PrimitiveType
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
      StructField col = MAPPER.readValue(json, StructField.class);
      assertThat(col.getType()).isInstanceOf(PrimitiveType.class);
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
      StructField col = MAPPER.readValue(json, StructField.class);
      assertThat(col.getType()).isInstanceOf(DecimalType.class);

      // Round-trip
      String serialized = MAPPER.writeValueAsString(col);
      JsonNode node = MAPPER.readTree(serialized);
      assertThat(node.get("type").asText()).isEqualTo(decStr);
    }

    // Bare "decimal" (no parens) is treated as a primitive, not DecimalType
    String json = "{\"name\":\"col\",\"type\":\"decimal\",\"nullable\":true,\"metadata\":{}}";
    StructField col = MAPPER.readValue(json, StructField.class);
    assertThat(col.getType()).isInstanceOf(PrimitiveType.class);
    assertThat(col.getType().getType()).isEqualTo("decimal");
  }

  @Test
  public void testDeserDecimalAsJsonObject() throws Exception {
    // DecimalType can also appear as a JSON object (not just bare string)
    String json =
        "{"
            + " \"name\": \"price\","
            + " \"type\": {\"type\": \"decimal\", \"precision\": 10, \"scale\": 2},"
            + " \"nullable\": true,"
            + " \"metadata\": {}"
            + "}";
    StructField col = MAPPER.readValue(json, StructField.class);
    assertThat(col.getType()).isInstanceOf(DecimalType.class);
    DecimalType dt = (DecimalType) col.getType();
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
    // so it falls through to PrimitiveType (same as any unrecognized type string)
    String json = "{\"name\":\"col\",\"type\":\"decimal(abc)\",\"nullable\":true,\"metadata\":{}}";
    StructField col = MAPPER.readValue(json, StructField.class);
    assertThat(col.getType()).isInstanceOf(PrimitiveType.class);
    assertThat(col.getType().getType()).isEqualTo("decimal(abc)");

    // Missing scale: "decimal(10)" -- also not matched, becomes PrimitiveType
    json = "{\"name\":\"col\",\"type\":\"decimal(10)\",\"nullable\":true,\"metadata\":{}}";
    col = MAPPER.readValue(json, StructField.class);
    assertThat(col.getType()).isInstanceOf(PrimitiveType.class);
  }

  @Test
  public void testDeserOmittedContainsNull() throws Exception {
    // When contains-null / value-contains-null are omitted, defaults apply
    String arrayJson =
        "{"
            + " \"name\": \"tags\","
            + " \"type\": {\"type\": \"array\", \"element-type\": \"string\"},"
            + " \"nullable\": true,"
            + " \"metadata\": {}"
            + "}";
    StructField arrCol = MAPPER.readValue(arrayJson, StructField.class);
    ArrayType at = (ArrayType) arrCol.getType();
    // Default: containsNull = true
    assertThat(at.getContainsNull()).isTrue();

    // Round-trip preserves the default
    String serialized = MAPPER.writeValueAsString(arrCol);
    StructField roundTrip = MAPPER.readValue(serialized, StructField.class);
    assertThat(((ArrayType) roundTrip.getType()).getContainsNull()).isTrue();

    String mapJson =
        "{"
            + " \"name\": \"kv\","
            + " \"type\": {"
            + "   \"type\": \"map\","
            + "   \"key-type\": \"string\","
            + "   \"value-type\": \"integer\""
            + " },"
            + " \"nullable\": true,"
            + " \"metadata\": {}"
            + "}";
    StructField mapCol = MAPPER.readValue(mapJson, StructField.class);
    MapType mt = (MapType) mapCol.getType();
    // Default: valueContainsNull = true
    assertThat(mt.getValueContainsNull()).isTrue();

    // Round-trip preserves the default
    String mapSerialized = MAPPER.writeValueAsString(mapCol);
    StructField mapRoundTrip = MAPPER.readValue(mapSerialized, StructField.class);
    assertThat(((MapType) mapRoundTrip.getType()).getValueContainsNull()).isTrue();
  }

  // ==================== Serialization ====================

  /**
   * Constructs a full UpdateTableRequest and serializes it to JSON, then compares against the
   * fixture. Tested types and actions:
   *
   * <ul>
   *   <li>Requirements: assert-table-uuid, assert-etag
   *   <li>set-properties, remove-properties
   *   <li>set-columns: PrimitiveType("long"), DecimalType(10,2), ArrayType(string), MapType(string,
   *       StructType([double, long]) with column metadata)
   *   <li>set-table-comment
   *   <li>add-commit with UniformMetadata (Iceberg)
   *   <li>set-latest-backfilled-version
   *   <li>set-protocol (reader/writer features)
   *   <li>set-domain-metadata (clustering)
   *   <li>remove-domain-metadata
   *   <li>set-partition-columns
   *   <li>update-metadata-snapshot-version
   * </ul>
   */
  @Test
  public void testSerialization() throws Exception {
    UpdateTableRequest request =
        new UpdateTableRequest()
            .requirements(
                List.of(
                    new AssertTableUUID()
                        .type("assert-table-uuid")
                        .uuid(UUID.fromString("550e8400-e29b-41d4-a716-446655440000")),
                    new AssertEtag().type("assert-etag").etag("etagabcdef")));

    SetPropertiesUpdate setProps =
        new SetPropertiesUpdate()
            .action("set-properties")
            .updates(
                Map.of(
                    "delta.columnMapping.mode", "name",
                    "delta.enableDeletionVectors", "true"));

    RemovePropertiesUpdate removeProps =
        new RemovePropertiesUpdate()
            .action("remove-properties")
            .removals(List.of("delta.logRetentionDuration"));

    StructField colId =
        new StructField()
            .name("id")
            .type(new PrimitiveType().type("long"))
            .nullable(false)
            .metadata(
                Map.of("delta.columnMapping.id", 1, "delta.columnMapping.physicalName", "col-1"));
    StructField colPrice =
        new StructField()
            .name("price")
            .type(new DecimalType().precision(10).scale(2))
            .nullable(true)
            .metadata(Map.of());
    StructField colTags =
        new StructField()
            .name("tags")
            .type(
                new ArrayType().elementType(new PrimitiveType().type("string")).containsNull(true))
            .nullable(true)
            .metadata(Map.of());
    StructField colScores =
        new StructField()
            .name("scores")
            .type(
                new MapType()
                    .keyType(new PrimitiveType().type("string"))
                    .valueType(
                        new StructType()
                            .fields(
                                List.of(
                                    new StructField()
                                        .name("value")
                                        .type(new PrimitiveType().type("double"))
                                        .nullable(false)
                                        .metadata(
                                            Map.of(
                                                "delta.columnMapping.id", 10,
                                                "delta.columnMapping.physicalName", "col-10",
                                                "comment", "score value")),
                                    new StructField()
                                        .name("timestamp")
                                        .type(new PrimitiveType().type("long"))
                                        .nullable(true)
                                        .metadata(Map.of("delta.columnMapping.id", 11)))))
                    .valueContainsNull(true))
            .nullable(true)
            .metadata(Map.of());
    SetSchemaUpdate setSchema =
        new SetSchemaUpdate()
            .action("set-columns")
            .columns(new StructType().fields(List.of(colId, colPrice, colTags, colScores)));

    SetTableCommentUpdate setComment =
        new SetTableCommentUpdate().action("set-table-comment").comment("updated table");

    AddCommitUpdate addCommit =
        new AddCommitUpdate()
            .action("add-commit")
            .commit(
                new DeltaCommit()
                    .version(5L)
                    .timestamp(1700000000000L)
                    .fileName("00000005.json")
                    .fileSize(2048L)
                    .fileModificationTimestamp(1700000001000L))
            .uniform(
                new UniformMetadata()
                    .iceberg(
                        new UniformMetadataIceberg()
                            .metadataLocation("s3://bucket/table/metadata/v5.metadata.json")
                            .convertedDeltaVersion(5L)
                            .convertedDeltaTimestamp(1700000000000L)));

    SetLatestBackfilledVersionUpdate setBackfill =
        new SetLatestBackfilledVersionUpdate()
            .action("set-latest-backfilled-version")
            .latestPublishedVersion(4L);

    SetProtocolUpdate setProtocol =
        new SetProtocolUpdate()
            .action("set-protocol")
            .protocol(
                new DeltaProtocol()
                    .minReaderVersion(3)
                    .minWriterVersion(7)
                    .readerFeatures(List.of("deletionVectors", "vacuumProtocolCheck"))
                    .writerFeatures(
                        List.of("catalogManaged", "deletionVectors", "inCommitTimestamp")));

    SetDomainMetadataUpdate setDomain =
        new SetDomainMetadataUpdate()
            .action("set-domain-metadata")
            .updates(
                new DomainMetadataUpdates()
                    .deltaClustering(
                        new ClusteringDomainMetadata()
                            .clusteringColumns(List.of(List.of("region"), List.of("event_time")))));

    RemoveDomainMetadataUpdate removeDomain =
        new RemoveDomainMetadataUpdate()
            .action("remove-domain-metadata")
            .domains(List.of("delta.rowTracking"));

    SetPartitionColumnsUpdate setPartition =
        new SetPartitionColumnsUpdate()
            .action("set-partition-columns")
            .partitionColumns(List.of("region", "date"));

    UpdateSnapshotVersionUpdate snapUpdate =
        new UpdateSnapshotVersionUpdate()
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

  // ==================== CreateTableRequest round-trip ====================

  @Test
  public void testCreateTableRequestRoundTrip() throws Exception {
    String json = readFixture("/delta-model-test/create-table-request.json");
    io.unitycatalog.client.delta.model.CreateTableRequest req =
        MAPPER.readValue(json, io.unitycatalog.client.delta.model.CreateTableRequest.class);

    // Verify DeltaType serde works on nested StructType fields
    StructField idField = req.getColumns().getFields().get(0);
    assertThat(idField.getType()).isInstanceOf(PrimitiveType.class);
    assertThat(idField.getType().getType()).isEqualTo("long");

    StructField amountField = req.getColumns().getFields().get(1);
    assertThat(amountField.getType()).isInstanceOf(DecimalType.class);
    assertThat(((DecimalType) amountField.getType()).getPrecision()).isEqualTo(10);

    StructField tagsField = req.getColumns().getFields().get(2);
    assertThat(tagsField.getType()).isInstanceOf(ArrayType.class);

    // Round-trip: re-serialize, decimal-as-object becomes bare "decimal(10,2)" string
    JsonNode actual = MAPPER.readTree(MAPPER.writeValueAsString(req));
    assertThat(actual.path("columns").path("fields").get(0).path("type").asText())
        .isEqualTo("long");
    assertThat(actual.path("columns").path("fields").get(1).path("type").asText())
        .isEqualTo("decimal(10,2)");
    assertThat(actual.path("columns").path("fields").get(2).path("type").path("type").asText())
        .isEqualTo("array");
  }

  // ==================== LoadTableResponse round-trip ====================

  @Test
  public void testLoadTableResponseRoundTrip() throws Exception {
    String json = readFixture("/delta-model-test/load-table-response.json");
    io.unitycatalog.client.delta.model.LoadTableResponse resp =
        MAPPER.readValue(json, io.unitycatalog.client.delta.model.LoadTableResponse.class);

    // Verify DeltaType serde works in TableMetadata.columns
    StructField priceField = resp.getMetadata().getColumns().getFields().get(1);
    assertThat(priceField.getType()).isInstanceOf(DecimalType.class);
    assertThat(((DecimalType) priceField.getType()).getPrecision()).isEqualTo(10);

    StructField scoresField = resp.getMetadata().getColumns().getFields().get(2);
    assertThat(scoresField.getType()).isInstanceOf(MapType.class);

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
}
