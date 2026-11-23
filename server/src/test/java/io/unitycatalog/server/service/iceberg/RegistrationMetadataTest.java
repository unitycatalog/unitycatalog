package io.unitycatalog.server.service.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.SimpleLocalFileIO;
import io.unitycatalog.server.utils.IcebergTestUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RegistrationMetadataTest {
  @TempDir Path directory;
  private BaseTable table;
  private MetadataService service;

  @BeforeEach
  void setUp() {
    table =
        IcebergTestUtils.createTable(
            directory.resolve("table"),
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
    FileOperations operations = mock();
    when(operations.getFileIO(any()))
        .thenAnswer(
            invocation ->
                new SimpleLocalFileIO(
                    Path.of(((NormalizedURL) invocation.getArgument(0)).toUri())));
    service = new MetadataService(operations);
  }

  @AfterEach
  void tearDown() {
    table.io().close();
  }

  @Test
  void rejectsMetadataWithoutTableUuid() throws Exception {
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            table.schema(),
            table.spec(),
            table.location(),
            Map.of(TableProperties.FORMAT_VERSION, "1"));
    ObjectNode json =
        (ObjectNode) new ObjectMapper().readTree(TableMetadataParser.toJson(metadata));
    json.remove("table-uuid");
    assertRejected(json, "must contain table-uuid");
  }

  @ParameterizedTest
  @ValueSource(strings = {"data", "historical-data", "delete", "manifest", "manifest-list"})
  void rejectsFileReferencesOutsideTable(String kind) throws Exception {
    String outside = NormalizedURL.from(directory.resolve("other.avro").toUri()).toString();
    if (kind.equals("data") || kind.equals("historical-data")) {
      var file =
          DataFiles.builder(table.spec())
              .withPath(outside)
              .withFormat("avro")
              .withFileSizeInBytes(1)
              .withRecordCount(1)
              .build();
      table.newFastAppend().appendFile(file).commit();
      if (kind.equals("historical-data")) {
        table.newDelete().deleteFile(file).commit();
        assertThat(table.operations().current().snapshots()).hasSize(2);
      }
    } else if (kind.equals("delete")) {
      table
          .newRowDelta()
          .addDeletes(
              FileMetadata.deleteFileBuilder(table.spec())
                  .ofPositionDeletes()
                  .withPath(outside)
                  .withFormat("avro")
                  .withFileSizeInBytes(1)
                  .withRecordCount(1)
                  .build())
          .commit();
    } else {
      appendRecord();
    }
    ObjectNode json = metadataJson();
    ObjectNode snapshot = (ObjectNode) json.withArray("snapshots").get(0);
    if (kind.equals("manifest-list")) {
      snapshot.put("manifest-list", outside);
    } else if (kind.equals("manifest")) {
      // Version 1 also permits manifest paths embedded directly in the snapshot.
      var manifest = table.currentSnapshot().allManifests(table.io()).get(0);
      Files.copy(
          Path.of(NormalizedURL.from(manifest.path()).toUri()),
          Path.of(NormalizedURL.from(outside).toUri()));
      json.put("format-version", 1);
      snapshot.remove("sequence-number");
      snapshot.remove("manifest-list");
      snapshot.putArray("manifests").add(outside);
    }
    assertRejected(json, "must be strictly below");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "write.data.path",
        "write.folder-storage.path",
        "write.object-storage.path",
        "metadata-log",
        "statistics",
        "partition-statistics"
      })
  void rejectsAuxiliaryLocationsOutsideTable(String field) throws Exception {
    ObjectNode json = metadataJson();
    String outside = directory.resolve("other").toUri().toString();
    if (field.startsWith("write.")) {
      ((ObjectNode) json.get("properties")).put(field, outside);
    } else {
      ObjectNode entry = json.putArray(field).addObject();
      if (field.equals("metadata-log")) {
        entry.put("timestamp-ms", 1).put("metadata-file", outside);
      } else {
        entry.put("snapshot-id", 1).put("statistics-path", outside).put("file-size-in-bytes", 1);
        if (field.equals("statistics")) {
          entry.put("file-footer-size-in-bytes", 1).putArray("blob-metadata");
        }
      }
    }
    assertRejected(
        json, field.startsWith("write.") ? "must be at or below" : "must be strictly below");
  }

  @Test
  void acceptsDataAndDeleteManifestsAndRejectsUnreadableManifest() throws Exception {
    appendRecord();
    table
        .newRowDelta()
        .addDeletes(
            FileMetadata.deleteFileBuilder(table.spec())
                .ofPositionDeletes()
                .withPath(table.location() + "/data/delete.avro")
                .withFormat("avro")
                .withFileSizeInBytes(1)
                .withRecordCount(1)
                .build())
        .commit();
    table
        .updateProperties()
        .set(TableProperties.WRITE_DATA_LOCATION, table.location())
        .set(TableProperties.WRITE_FOLDER_STORAGE_LOCATION, table.location() + "/data")
        .set(TableProperties.OBJECT_STORE_PATH, table.location() + "/data")
        .commit();
    var metadata = table.operations().current();
    assertThat(
            service
                .readRegistrationMetadata(
                    NormalizedURL.from(metadata.metadataFileLocation()),
                    NormalizedURL.from(directory.toUri()))
                .snapshots())
        .hasSize(2);
    Path manifest =
        Path.of(NormalizedURL.from(table.currentSnapshot().manifestListLocation()).toUri());
    Files.delete(manifest);
    assertRejected(metadataJson(), "Cannot read Iceberg manifests");
  }

  private void appendRecord() throws Exception {
    GenericRecord record = GenericRecord.create(table.schema());
    record.setField("id", 42L);
    table
        .newFastAppend()
        .appendFile(IcebergTestUtils.writeDataFile(table, "record", record))
        .commit();
  }

  private ObjectNode metadataJson() throws Exception {
    return (ObjectNode)
        new ObjectMapper().readTree(TableMetadataParser.toJson(table.operations().current()));
  }

  private void assertRejected(ObjectNode json, String message) throws Exception {
    Path metadata =
        Path.of(NormalizedURL.from(table.location()).toUri())
            .resolve("metadata/import.metadata.json");
    Files.writeString(metadata, json.toString());
    assertThatThrownBy(
            () ->
                service.readRegistrationMetadata(
                    NormalizedURL.from(metadata.toUri()), NormalizedURL.from(directory.toUri())))
        .isInstanceOfSatisfying(
            BaseException.class,
            failure -> {
              assertThat(failure.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
              assertThat(failure).hasMessageContaining(message);
            });
  }
}
