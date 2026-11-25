package io.unitycatalog.server.service.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.SimpleLocalFileIO;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.IcebergTestUtils;
import io.unitycatalog.server.utils.LocalMappingFileIO;
import io.unitycatalog.server.utils.NormalizedURL;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.GenericAvroWriter;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.rest.responses.LoadTableResponse;
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
  @ValueSource(strings = {"abfs", "abfss"})
  void acceptsAzureStorageSchemeAliases(String storageScheme) throws Exception {
    String tableScheme = storageScheme.equals("abfs") ? "abfss" : "abfs";
    String storagePath = "://container@account.dfs.core.windows.net/warehouse";
    String tableLocation = tableScheme + storagePath + "/orders";
    String metadataLocation = tableLocation + "/metadata/v1.metadata.json";
    Path metadataFile = directory.resolve("azure.metadata.json");
    Files.writeString(metadataFile, metadataJson().put("location", tableLocation).toString());
    InputFile input = mock();
    when(input.location()).thenReturn(metadataLocation);
    when(input.newStream())
        .thenAnswer(
            ignored -> org.apache.iceberg.Files.localInput(metadataFile.toFile()).newStream());
    FileIO fileIO = mock(FileIO.class, CALLS_REAL_METHODS);
    when(fileIO.newInputFile(metadataLocation)).thenReturn(input);
    FileOperations operations = mock();
    when(operations.getFileIO(any())).thenReturn(fileIO);

    TableMetadata registered =
        new MetadataService(operations)
            .readRegistrationMetadata(
                NormalizedURL.from(metadataLocation),
                NormalizedURL.from(storageScheme + storagePath));

    assertThat(registered.uuid()).isEqualTo(table.operations().current().uuid());
    assertThat(registered.location()).isEqualTo(tableLocation);
    assertThat(registered.metadataFileLocation()).isEqualTo(metadataLocation);
  }

  @ParameterizedTest
  @ValueSource(strings = {"abfs", "abfss"})
  void acceptsAzureAliasesInPropertiesAndRetainedManifests(String scheme) throws Exception {
    String root = scheme + "://container@account.dfs.core.windows.net/warehouse/orders";
    String alias =
        (scheme.equals("abfs") ? "abfss" : "abfs")
            + "://container@account.dfs.core.windows.net/warehouse/orders";
    appendRecord();
    Path localManifest = directory.resolve("table/metadata/alias.avro");
    try (var writer =
        ManifestFiles.write(
            2, table.spec(), org.apache.iceberg.Files.localOutput(localManifest.toFile()), 1L)) {
      writer.add(
          DataFiles.builder(table.spec())
              .withPath(alias + "/data/file.avro")
              .withFormat("avro")
              .withFileSizeInBytes(1)
              .withRecordCount(1)
              .build());
    }
    ObjectNode json = metadataJson();
    json.put("format-version", 1);
    json.put("location", root);
    ObjectNode properties = (ObjectNode) json.get("properties");
    properties.put(TableProperties.WRITE_METADATA_LOCATION, alias + "/metadata");
    properties.put(TableProperties.WRITE_DATA_LOCATION, alias);
    ObjectNode snapshot = (ObjectNode) json.withArray("snapshots").get(0);
    snapshot.remove("sequence-number");
    snapshot.remove("manifest-list");
    snapshot.putArray("manifests").add(alias + "/metadata/alias.avro");
    json.putArray("metadata-log");
    Path localMetadata = directory.resolve("table/metadata/azure.metadata.json");
    Files.writeString(localMetadata, json.toString());
    FileIO fileIO = mock(FileIO.class, CALLS_REAL_METHODS);
    when(fileIO.newInputFile(any(String.class)))
        .thenAnswer(
            invocation -> {
              String path = invocation.getArgument(0);
              String prefix = path.startsWith(root + "/") ? root : alias;
              Path local = directory.resolve("table").resolve(path.substring(prefix.length() + 1));
              InputFile input = mock();
              when(input.location()).thenReturn(path);
              when(input.getLength()).thenAnswer(ignored -> Files.size(local));
              when(input.newStream())
                  .thenAnswer(
                      ignored -> org.apache.iceberg.Files.localInput(local.toFile()).newStream());
              return input;
            });
    FileOperations operations = mock();
    when(operations.getFileIO(any())).thenReturn(fileIO);
    TableMetadata metadata =
        new MetadataService(operations)
            .readRegistrationMetadata(
                NormalizedURL.from(root + "/metadata/azure.metadata.json"),
                NormalizedURL.from(root.substring(0, root.lastIndexOf('/'))));
    assertThat(metadata.properties()).containsEntry(TableProperties.WRITE_DATA_LOCATION, alias);
    assertThat(metadata.snapshots()).hasSize(1);
    assertThat(
            IcebergObjectMapper.mapper()
                .writeValueAsString(
                    LoadTableResponse.builder().withTableMetadata(metadata).build()))
        .contains(alias + "/metadata/alias.avro");
  }

  @ParameterizedTest
  @ValueSource(strings = {"abfs", "abfss"})
  void rejectsAzureMetadataLocationsOutsideStorageRoot(String storageScheme) {
    String aliasScheme = storageScheme.equals("abfs") ? "abfss" : "abfs";
    NormalizedURL storageRoot =
        NormalizedURL.from(storageScheme + "://container@account.dfs.core.windows.net/warehouse");
    for (String path :
        List.of(
            storageRoot.toString(),
            aliasScheme + "://container@account.dfs.core.windows.net/warehouse",
            aliasScheme + "://container@account.dfs.core.windows.net",
            aliasScheme
                + "://container@account.dfs.core.windows.net/warehouse-sibling/metadata.json",
            aliasScheme + "://other@account.dfs.core.windows.net/warehouse/metadata.json",
            aliasScheme + "://container@other.dfs.core.windows.net/warehouse/metadata.json")) {
      assertThatThrownBy(
              () -> MetadataService.validateMetadataLocation(NormalizedURL.from(path), storageRoot))
          .isInstanceOfSatisfying(
              BaseException.class,
              failure -> {
                assertThat(failure.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
                assertThat(failure).hasMessageContaining("must be a subpath", path);
              });
    }
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

  @ParameterizedTest
  @ValueSource(strings = {"abfs://c@a.dfs.core.windows.net", "abfss://c@a.dfs.core.windows.net"})
  void rejectsEncodedAzureRegistrationPaths(String root) {
    for (String escaped : List.of("%20", "%2f", "%2e", "%25")) {
      assertThatThrownBy(
              () ->
                  MetadataService.registrationMetadataLocation(
                      root + "/table/" + escaped + "/v1.metadata.json"))
          .isInstanceOfSatisfying(
              BaseException.class,
              e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT))
          .hasMessageContaining("canonical cloud object path");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3://bucket", "gs://bucket"})
  void acceptsPercentKeysInObjectStorage(String storage) throws Exception {
    String root = storage + "/warehouse/a%20b";
    String metadataLocation = root + "/metadata/v1%25.metadata.json";
    FileIO fileIO =
        new LocalMappingFileIO(
            NormalizedURL.from(root),
            directory.resolve("cloud"),
            Map.of(
                S3FileIOProperties.ACCESS_KEY_ID, "test-key",
                S3FileIOProperties.SECRET_ACCESS_KEY, "test-secret",
                S3FileIOProperties.SESSION_TOKEN, "test-token",
                GCPProperties.GCS_OAUTH2_TOKEN, "test-token"),
            Map.of(),
            CredentialContext.READ_WRITE);
    var cloudTable = IcebergTestUtils.createTable(root, table.schema(), Map.of(), fileIO);
    try (var ignored = cloudTable.io()) {
      cloudTable
          .newFastAppend()
          .appendFile(
              DataFiles.builder(cloudTable.spec())
                  .withPath(root + "/data/ts=2024-01-01T10%3A00%3A00/file.avro")
                  .withFormat("avro")
                  .withFileSizeInBytes(1)
                  .withRecordCount(1)
                  .build())
          .commit();
      cloudTable
          .newRowDelta()
          .addDeletes(
              FileMetadata.deleteFileBuilder(cloudTable.spec())
                  .ofPositionDeletes()
                  .withPath(root + "/data/value=%2F%2e%25/delete.avro")
                  .withFormat("avro")
                  .withFileSizeInBytes(1)
                  .withRecordCount(1)
                  .build())
          .commit();
      cloudTable
          .updateProperties()
          .set(TableProperties.WRITE_METADATA_LOCATION, root + "/metadata")
          .set(TableProperties.WRITE_DATA_LOCATION, root + "/data/value=%25")
          .commit();
      TableMetadata original = cloudTable.operations().current();
      TableMetadataParser.write(original, fileIO.newOutputFile(metadataLocation));
      FileOperations operations = mock();
      when(operations.getFileIO(any())).thenReturn(fileIO);
      TableMetadata registered =
          new MetadataService(operations)
              .readRegistrationMetadata(
                  MetadataService.registrationMetadataLocation(metadataLocation),
                  NormalizedURL.from(storage + "/warehouse"));
      assertThat(registered.location()).isEqualTo(root);
      assertThat(registered.metadataFileLocation()).isEqualTo(metadataLocation);
      assertThat(registered.snapshots()).hasSize(2);
      assertThat(registered.properties())
          .containsEntry(TableProperties.WRITE_DATA_LOCATION, root + "/data/value=%25");
    }
  }

  @Test
  void rejectsInvalidSortOrderAsClientInput() throws Exception {
    ObjectNode json = metadataJson();
    json.put("default-sort-order-id", 1);
    json.putArray("sort-orders")
        .addObject()
        .put("order-id", 1)
        .putArray("fields")
        .addObject()
        .put("source-id", 999)
        .put("transform", "identity")
        .put("direction", "asc")
        .put("null-order", "nulls-first");
    assertRejected(json, "Cannot read Iceberg metadata");
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "manifest-list", "manifest", "unknown-spec"})
  void rejectsMalformedRegistrationFiles(String kind) throws Exception {
    if (kind.equals("json")) {
      Path metadata = directory.resolve("table/metadata/broken.metadata.json");
      Files.writeString(metadata, "{invalid json");
      assertThatThrownBy(
              () ->
                  service.readRegistrationMetadata(
                      NormalizedURL.from(metadata.toUri()), NormalizedURL.from(directory.toUri())))
          .isInstanceOfSatisfying(
              BaseException.class,
              e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT));
      return;
    }
    appendRecord();
    ObjectNode json = metadataJson();
    String message = "Cannot read Iceberg manifests";
    if (kind.equals("unknown-spec")) {
      PartitionSpec spec = PartitionSpec.builderFor(table.schema()).withSpecId(99).build();
      String path = table.location() + "/metadata/unknown-spec.avro";
      ManifestFile manifest;
      try (var writer = ManifestFiles.write(2, spec, table.io().newOutputFile(path), 1L)) {
        writer.add(
            DataFiles.builder(spec)
                .withPath(table.location() + "/data/file.avro")
                .withFormat("avro")
                .withFileSizeInBytes(1)
                .withRecordCount(1)
                .build());
        writer.close();
        manifest = writer.toManifestFile();
      }
      String listPath = table.location() + "/metadata/unknown-spec-list.avro";
      try (FileAppender<ManifestFile> writer =
          Avro.write(table.io().newOutputFile(listPath))
              .schema(ManifestFile.schema())
              .named("manifest_file")
              .createWriterFunc(GenericAvroWriter::create)
              .build()) {
        writer.add(manifest);
      }
      ObjectNode snapshot = (ObjectNode) json.withArray("snapshots").get(0);
      snapshot.put("manifest-list", listPath);
      message = "references unknown partition spec 99";
    } else {
      String file =
          kind.equals("manifest-list")
              ? table.currentSnapshot().manifestListLocation()
              : table.currentSnapshot().allManifests(table.io()).get(0).path();
      Files.writeString(Path.of(NormalizedURL.from(file).toUri()), "invalid avro content");
    }
    assertRejected(json, message);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void preservesWrappedTransportFailures(boolean manifestRead) throws Exception {
    appendRecord();
    FileOperations operations = mock();
    FileIO unavailable = mock(FileIO.class, CALLS_REAL_METHODS);
    when(unavailable.newInputFile(any(String.class)))
        .thenThrow(new RuntimeIOException(new SocketTimeoutException("private endpoint")));
    if (manifestRead) {
      when(operations.getFileIO(any())).thenReturn(new SimpleLocalFileIO(directory), unavailable);
    } else {
      when(operations.getFileIO(any())).thenReturn(unavailable);
    }
    assertThatThrownBy(
            () ->
                new MetadataService(operations)
                    .readRegistrationMetadata(
                        NormalizedURL.from(table.operations().current().metadataFileLocation()),
                        NormalizedURL.from(directory.toUri())))
        .isInstanceOfSatisfying(
            BaseException.class,
            e -> {
              assertThat(e.getErrorCode()).isEqualTo(ErrorCode.UNAVAILABLE);
              assertThat(e).hasMessage("Failed to access Iceberg storage.");
            });
  }

  @Test
  void checksDiscoveredRootBeforeOpeningManifests() throws Exception {
    appendRecord();
    FileOperations operations = mock();
    when(operations.getFileIO(any())).thenReturn(new SimpleLocalFileIO(directory));
    BaseException overlap = new BaseException(ErrorCode.INVALID_ARGUMENT, "Directory is occupied");
    assertThatThrownBy(
            () ->
                new MetadataService(operations)
                    .readRegistrationMetadata(
                        NormalizedURL.from(table.operations().current().metadataFileLocation()),
                        NormalizedURL.from(directory.toUri()),
                        root -> {
                          throw overlap;
                        }))
        .isSameAs(overlap);
    verify(operations, times(1)).getFileIO(any());
  }

  @Test
  void legacyV1ResponseDoesNotRetainValidationFileIO() throws Exception {
    appendRecord();
    ObjectNode json = metadataJson();
    json.put("format-version", 1);
    ObjectNode snapshot = (ObjectNode) json.withArray("snapshots").get(0);
    snapshot.remove("manifest-list");
    snapshot.remove("sequence-number");
    var manifests = snapshot.putArray("manifests");
    table.currentSnapshot().allManifests(table.io()).forEach(m -> manifests.add(m.path()));
    Path metadataFile = directory.resolve("table/metadata/legacy.metadata.json");
    Files.writeString(metadataFile, json.toString());
    FileOperations operations = mock();
    when(operations.getFileIO(any()))
        .thenAnswer(
            ignored ->
                new SimpleLocalFileIO(directory) {
                  private boolean closed;

                  @Override
                  public InputFile newInputFile(String path) {
                    assertThat(closed).as("FileIO must be open during a read").isFalse();
                    return super.newInputFile(path);
                  }

                  @Override
                  public void close() {
                    closed = true;
                    super.close();
                  }
                });
    TableMetadata metadata =
        new MetadataService(operations)
            .readRegistrationMetadata(
                NormalizedURL.from(metadataFile.toUri()), NormalizedURL.from(directory.toUri()));
    String response =
        IcebergObjectMapper.mapper()
            .writeValueAsString(LoadTableResponse.builder().withTableMetadata(metadata).build());
    assertThat(response).contains("manifests", manifests.get(0).asText());
    assertThat(Files.readString(metadataFile)).isEqualTo(json.toString());
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
