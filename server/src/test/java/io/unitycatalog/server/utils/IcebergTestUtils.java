package io.unitycatalog.server.utils;

import io.unitycatalog.server.persist.utils.SimpleLocalFileIO;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;

/** Builds real Iceberg files without a catalog server or Hadoop runtime. */
public final class IcebergTestUtils {
  private IcebergTestUtils() {}

  /** Creates an empty table; the caller closes its FileIO after use. */
  public static BaseTable createTable(Path directory, Schema schema) {
    return createTable(directory, schema, Map.of());
  }

  /** Creates an empty table with the given properties; the caller closes its FileIO after use. */
  public static BaseTable createTable(
      Path directory, Schema schema, Map<String, String> properties) {
    return createTable(
        NormalizedURL.from(directory.toUri()).toString(),
        schema,
        properties,
        new SimpleLocalFileIO(directory));
  }

  /** Creates an empty table at the given location; the caller closes its FileIO after use. */
  public static BaseTable createTable(
      String location, Schema schema, Map<String, String> properties, FileIO fileIO) {
    var operations =
        new BaseMetastoreTableOperations() {
          private String metadataLocation;

          @Override
          protected String tableName() {
            return location;
          }

          @Override
          public FileIO io() {
            return fileIO;
          }

          @Override
          protected void doRefresh() {
            refreshFromMetadataLocation(metadataLocation);
          }

          @Override
          protected void doCommit(TableMetadata base, TableMetadata metadata) {
            metadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
          }
        };
    operations.commit(
        null,
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), location, properties));
    return new BaseTable(operations, location);
  }

  /** Writes one record to an Avro file without committing it to the table. */
  public static DataFile writeDataFile(Table table, String name, GenericRecord record)
      throws IOException {
    var output = table.io().newOutputFile(table.location() + "/data/" + name + ".avro");
    try (FileAppender<GenericRecord> writer =
        Avro.write(output).schema(table.schema()).createWriterFunc(DataWriter::create).build()) {
      writer.add(record);
    }
    return DataFiles.builder(table.spec())
        .withPath(output.location())
        .withFormat("avro")
        .withFileSizeInBytes(table.io().newInputFile(output.location()).getLength())
        .withRecordCount(1)
        .build();
  }
}
