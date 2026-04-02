package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.delta.model.AddCommitUpdate;
import io.unitycatalog.server.delta.model.AssertEtag;
import io.unitycatalog.server.delta.model.AssertTableUUID;
import io.unitycatalog.server.delta.model.ClusteringDomainMetadata;
import io.unitycatalog.server.delta.model.DeltaColumn;
import io.unitycatalog.server.delta.model.DeltaCommit;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DomainMetadataUpdates;
import io.unitycatalog.server.delta.model.RemoveDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.RemovePropertiesUpdate;
import io.unitycatalog.server.delta.model.SetDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.SetLatestBackfilledVersionUpdate;
import io.unitycatalog.server.delta.model.SetPartitionColumnsUpdate;
import io.unitycatalog.server.delta.model.SetPropertiesUpdate;
import io.unitycatalog.server.delta.model.SetProtocolUpdate;
import io.unitycatalog.server.delta.model.SetSchemaUpdate;
import io.unitycatalog.server.delta.model.SetTableCommentUpdate;
import io.unitycatalog.server.delta.model.TableUpdate;
import io.unitycatalog.server.delta.model.UniformMetadata;
import io.unitycatalog.server.delta.model.UniformMetadataIceberg;
import io.unitycatalog.server.delta.model.UpdateSnapshotVersionUpdate;
import io.unitycatalog.server.delta.model.UpdateTableRequest;
import io.unitycatalog.server.delta.type.ArrayDataType;
import io.unitycatalog.server.delta.type.DataTypes;
import io.unitycatalog.server.delta.type.DecimalDataType;
import io.unitycatalog.server.delta.type.MapDataType;
import io.unitycatalog.server.delta.type.PrimitiveDataType;
import io.unitycatalog.server.delta.type.StructDataType;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests JSON deserialization and serialization of Delta REST API model types. The deserialization
 * test loads a JSON fixture and verifies all fields. The serialization test constructs objects from
 * scratch and compares the output against the same JSON fixture.
 */
public class DeltaModelSerializationTest {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

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
  @SuppressWarnings("unchecked")
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
    assertThat(setProps.getUpdates()).hasSize(2);
    assertThat(setProps.getUpdates())
        .containsEntry("delta.columnMapping.mode", "name")
        .containsEntry("delta.enableDeletionVectors", "true");

    // remove-properties
    assertThat(((RemovePropertiesUpdate) updates.get(1)).getRemovals())
        .containsExactly("delta.logRetentionDuration");

    // set-columns (4 columns: primitive, decimal, array, map<string,struct>)
    List<DeltaColumn> columns = ((SetSchemaUpdate) updates.get(2)).getColumns();
    assertThat(columns).hasSize(4);
    assertThat(columns.get(0).getType()).isEqualTo("long");
    assertThat(columns.get(1).getType()).isEqualTo("decimal(10,2)");
    // DataTypes.resolve: String/Map -> typed DataType
    assertThat(DataTypes.resolve(columns.get(0))).isEqualTo(new PrimitiveDataType("long"));
    assertThat(DataTypes.resolve(columns.get(1))).isEqualTo(new DecimalDataType(10, 2));

    ArrayDataType arrDt = (ArrayDataType) DataTypes.resolve(columns.get(2));
    assertThat(arrDt.getElementType()).isEqualTo(new PrimitiveDataType("string"));

    MapDataType mapDt = (MapDataType) DataTypes.resolve(columns.get(3));
    assertThat(mapDt.getKeyType()).isEqualTo(new PrimitiveDataType("string"));

    StructDataType structDt = (StructDataType) mapDt.getValueType();
    assertThat(structDt.getFields()).hasSize(2);
    DeltaColumn valueField = structDt.getFields().get(0);
    assertThat(valueField.getType()).isEqualTo(new PrimitiveDataType("double"));
    Map<String, Object> valueMeta = (Map<String, Object>) valueField.getMetadata();
    assertThat(valueMeta)
        .containsEntry("delta.columnMapping.id", 10)
        .containsEntry("delta.columnMapping.physicalName", "col-10")
        .containsEntry("comment", "score value");
    DeltaColumn tsField = structDt.getFields().get(1);
    assertThat(tsField.getType()).isEqualTo(new PrimitiveDataType("long"));
    Map<String, Object> tsMeta = (Map<String, Object>) tsField.getMetadata();
    assertThat(tsMeta).containsEntry("delta.columnMapping.id", 11);

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

  // ==================== Serialization ====================

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

    // set-columns
    DeltaColumn colId =
        new DeltaColumn()
            .name("id")
            .type(new PrimitiveDataType("long"))
            .nullable(false)
            .metadata(
                Map.of("delta.columnMapping.id", 1, "delta.columnMapping.physicalName", "col-1"));
    DeltaColumn colPrice =
        new DeltaColumn()
            .name("price")
            .type(new DecimalDataType(10, 2))
            .nullable(true)
            .metadata(Map.of());
    DeltaColumn colTags =
        new DeltaColumn()
            .name("tags")
            .type(new ArrayDataType().elementType("string").containsNull(true))
            .nullable(true)
            .metadata(Map.of());
    DeltaColumn colScores =
        new DeltaColumn()
            .name("scores")
            .type(
                new MapDataType()
                    .keyType("string")
                    .valueType(
                        new StructDataType()
                            .fields(
                                List.of(
                                    new DeltaColumn()
                                        .name("value")
                                        .type("double")
                                        .nullable(false)
                                        .metadata(
                                            Map.of(
                                                "delta.columnMapping.id", 10,
                                                "delta.columnMapping.physicalName", "col-10",
                                                "comment", "score value")),
                                    new DeltaColumn()
                                        .name("timestamp")
                                        .type("long")
                                        .nullable(true)
                                        .metadata(Map.of("delta.columnMapping.id", 11)))))
                    .valueContainsNull(true))
            .nullable(true)
            .metadata(Map.of());
    SetSchemaUpdate setSchema =
        new SetSchemaUpdate()
            .action("set-columns")
            .columns(List.of(colId, colPrice, colTags, colScores));

    // set-table-comment
    SetTableCommentUpdate setComment =
        new SetTableCommentUpdate().action("set-table-comment").comment("updated table");

    // add-commit with uniform
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

    // set-latest-backfilled-version
    SetLatestBackfilledVersionUpdate setBackfill =
        new SetLatestBackfilledVersionUpdate()
            .action("set-latest-backfilled-version")
            .latestPublishedVersion(4L);

    // set-protocol
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

    // set-domain-metadata
    SetDomainMetadataUpdate setDomain =
        new SetDomainMetadataUpdate()
            .action("set-domain-metadata")
            .updates(
                new DomainMetadataUpdates()
                    .deltaClustering(
                        new ClusteringDomainMetadata()
                            .clusteringColumns(List.of(List.of("region"), List.of("event_time")))));

    // remove-domain-metadata
    RemoveDomainMetadataUpdate removeDomain =
        new RemoveDomainMetadataUpdate()
            .action("remove-domain-metadata")
            .domains(List.of("delta.rowTracking"));

    // set-partition-columns
    SetPartitionColumnsUpdate setPartition =
        new SetPartitionColumnsUpdate()
            .action("set-partition-columns")
            .partitionColumns(List.of("region", "date"));

    // update-metadata-snapshot-version
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

    // Serialize and compare with fixture (JSON tree comparison, order-independent)
    JsonNode expected = MAPPER.readTree(fixtureJson);
    JsonNode actual = MAPPER.readTree(MAPPER.writeValueAsString(request));
    assertThat(actual).isEqualTo(expected);
  }
}
