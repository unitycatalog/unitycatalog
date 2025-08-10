package io.unitycatalog.cli.delta;

import static io.unitycatalog.cli.utils.CliUtils.EMPTY;

import de.vandermeer.asciitable.AsciiTable;
import io.delta.kernel.*;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterable;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.ColumnInfo;
import java.net.URI;
import java.util.*;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * Utility class to create and read Delta tables. The create method creates a Delta table with the
 * given schema at the given path. The create method just initializes the delta log and does not
 * write any data to the table. The read method reads the data from the Delta table The code has
 * evolved from examples provided in <a
 * href="https://github.com/delta-io/delta/tree/master/kernel/examples/kernel-examples/src/main/java/io/delta/kernel/examples">Delta
 * examples</a>
 */
public class DeltaKernelUtils {

  public static String createDeltaTable(
      String tablePath, List<ColumnInfo> columns, AwsCredentials awsTempCredentials) {
    try {
      URI tablePathUri = URI.create(tablePath);
      Engine engine = getEngine(tablePathUri, awsTempCredentials);
      Table table = Table.forPath(engine, substituteSchemeForS3(tablePath));
      // construct the schema
      StructType tableSchema = getSchema(columns);
      TransactionBuilder txnBuilder =
          table.createTransactionBuilder(engine, "UnityCatalogCli", Operation.CREATE_TABLE);
      // Set the schema of the new table on the transaction builder
      txnBuilder = txnBuilder.withSchema(engine, tableSchema);
      // Build the transaction
      Transaction txn = txnBuilder.build(engine);
      // create an empty table
      TransactionCommitResult commitResult = txn.commit(engine, CloseableIterable.emptyIterable());
      if (commitResult.getVersion() >= 0) {
        System.out.println("Table created successfully at: " + tablePath);
      } else {
        throw new RuntimeException("Table creation failed");
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to create delta table", e);
    }
    return EMPTY;
  }

  public static String substituteSchemeForS3(String tablePath) {
    return tablePath.replace("s3://", "s3a://");
  }

  public static Engine getEngine(URI tablePathUri, AwsCredentials awsTempCredentials) {
    return DefaultEngine.create(getHDFSConfiguration(tablePathUri, awsTempCredentials));
  }

  public static FileSystem getFileSystem(URI tablePathURI, Configuration conf) {
    try {
      return FileSystem.get(tablePathURI, conf);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to get file system", e);
    }
  }

  public static Configuration getHDFSConfiguration(
      URI tablePathUri, AwsCredentials awsTempCredentials) {
    Configuration conf = new Configuration();
    if (tablePathUri.getScheme() != null
        && tablePathUri.getScheme().equals("s3")
        && awsTempCredentials == null) {
      throw new IllegalArgumentException("AWS temporary credentials are missing");
    }
    if (tablePathUri.getScheme().equals("s3")) {
      conf.set("fs.s3a.access.key", awsTempCredentials.getAccessKeyId());
      conf.set("fs.s3a.secret.key", awsTempCredentials.getSecretAccessKey());
      conf.set("fs.s3a.session.token", awsTempCredentials.getSessionToken());
      conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      conf.set("fs.s3a.path.style.access", "true");

      // Support custom S3 endpoints for S3-compatible services (MinIO, Ceph, etc.)
      // Read from environment variable or server configuration
      String s3Endpoint = System.getenv("UC_S3_ENDPOINT");

      // Alternative: read from server properties (for future use)
      // String s3Endpoint = System.getenv("UC_S3_ENDPOINT");
      // if (s3Endpoint == null) {
      //   s3Endpoint = System.getProperty("uc.s3.endpoint");
      // }

      if (s3Endpoint != null && !s3Endpoint.isEmpty()) {
        conf.set("fs.s3a.endpoint", s3Endpoint);
        // Also set connection SSL based on endpoint protocol
        if (s3Endpoint.startsWith("https://")) {
          conf.set("fs.s3a.connection.ssl.enabled", "true");
        } else if (s3Endpoint.startsWith("http://")) {
          conf.set("fs.s3a.connection.ssl.enabled", "false");
        }

        // Additional settings that might be needed for non-AWS S3 services
        conf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
        conf.set("fs.s3a.endpoint.region", "us-east-1");
      }
    } else if (tablePathUri.getScheme().equals("file")) {
      conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    } else {
      throw new IllegalArgumentException("Unsupported URI scheme: " + tablePathUri.getScheme());
    }
    return conf;
  }

  public static String readDeltaTable(
      String tablePath, AwsCredentials awsCredentials, int maxResults) {
    Engine engine = getEngine(URI.create(tablePath), awsCredentials);
    try {
      Table table = Table.forPath(engine, substituteSchemeForS3(tablePath));
      Snapshot snapshot = table.getLatestSnapshot(engine);
      StructType readSchema = snapshot.getSchema();

      // Check if we have too many columns for ASCII table display
      if (readSchema.fields().size() > 10) {
        // For tables with many columns, use a simpler display format
        return readDeltaTableSimple(engine, readSchema, snapshot, maxResults);
      }

      Object[] schema =
          readSchema.fields().stream()
              .map(x -> x.getName() + "(" + x.getDataType().toString() + ")")
              .toArray(String[]::new);
      AsciiTable at = new AsciiTable();
      at.addRule();
      at.addRow(schema);
      at.addRule();
      // might need to prune it later
      ScanBuilder scanBuilder = snapshot.getScanBuilder().withReadSchema(readSchema);
      List<Row> rowData =
          DeltaKernelReadUtils.readData(engine, readSchema, scanBuilder.build(), maxResults);
      for (Row row : rowData) {
        Object[] rowValues =
            IntStream.range(0, schema.length)
                .mapToObj(colOrdinal -> DeltaKernelReadUtils.getValue(row, colOrdinal))
                .toArray();
        at.addRow(rowValues);
        at.addRule();
      }
      return at.render();
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to read delta table", e);
    }
  }

  private static String readDeltaTableSimple(
      Engine engine, StructType readSchema, Snapshot snapshot, int maxResults) {
    try {
      StringBuilder sb = new StringBuilder();
      sb.append("Table Schema:\n");
      readSchema
          .fields()
          .forEach(
              field ->
                  sb.append("  - ")
                      .append(field.getName())
                      .append(" (")
                      .append(field.getDataType())
                      .append(")\n"));

      sb.append("\nData Preview (").append(maxResults).append(" rows):\n");
      sb.append("=".repeat(80)).append("\n");

      ScanBuilder scanBuilder = snapshot.getScanBuilder().withReadSchema(readSchema);
      List<Row> rowData =
          DeltaKernelReadUtils.readData(engine, readSchema, scanBuilder.build(), maxResults);

      int rowNum = 1;
      for (Row row : rowData) {
        sb.append("Row ").append(rowNum++).append(":\n");
        for (int i = 0; i < readSchema.fields().size(); i++) {
          String fieldName = readSchema.at(i).getName();
          String value = DeltaKernelReadUtils.getValue(row, i);
          sb.append("  ").append(fieldName).append(": ").append(value).append("\n");
        }
        sb.append("-".repeat(40)).append("\n");
      }

      return sb.toString();
    } catch (Exception e) {
      throw new RuntimeException("Failed to read table data", e);
    }
  }

  // TODO : INTERVAL, CHAR and NULL, ARRAY, MAP, STRUCT
  public static StructType getSchema(List<ColumnInfo> columns) {
    StructType structType = new StructType();
    for (ColumnInfo column : columns) {
      DataType dataType = getDataType(column);
      structType = structType.add(column.getName(), dataType);
    }
    return structType;
  }

  public static DataType getDataType(ColumnInfo column) {
    if (column.getTypeName() == null) {
      throw new IllegalArgumentException("Column type is missing: " + column.getName());
    }
    return findBasicTypeFromString(column.getTypeName().toString());
  }

  public static DataType findBasicTypeFromString(String typeText) {
    DataType dataType = null;
    switch (typeText) {
      case "INT":
        dataType = IntegerType.INTEGER;
        break;
      case "STRING":
      case "DOUBLE":
      case "BOOLEAN":
      case "LONG":
      case "FLOAT":
      case "SHORT":
      case "BYTE":
      case "DATE":
      case "TIMESTAMP":
      case "TIMESTAMP_NTZ":
      case "BINARY":
      case "DECIMAL":
        dataType = BasePrimitiveType.createPrimitive(typeText.toLowerCase(Locale.ROOT));
        break;
      default:
        throw new IllegalArgumentException("Unsupported basic data type: " + typeText);
    }
    return dataType;
  }
}
