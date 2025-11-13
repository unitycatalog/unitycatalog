package io.unitycatalog.cli.delta;

import de.vandermeer.asciitable.AsciiTable;
import io.delta.kernel.*;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterable;
import io.unitycatalog.cli.utils.Constants;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.net.URI;
import java.util.*;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to create and read Delta tables. The create method creates a Delta table with the
 * given schema at the given path. The create method just initializes the Delta log and does not
 * write any data to the table. The read method reads the data from the Delta table The code has
 * evolved from examples provided in <a
 * href="https://github.com/delta-io/delta/tree/master/kernel/examples/kernel-examples/src/main/java/io/delta/kernel/examples">Delta
 * examples</a>
 */
public class DeltaKernelUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeltaKernelUtils.class);

  public static void createDeltaTable(
      String tablePath, List<ColumnInfo> columns, TemporaryCredentials temporaryCredentials) {
    try {
      URI tablePathUri = URI.create(tablePath);
      Engine engine = getEngine(tablePathUri, temporaryCredentials);
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
        LOGGER.info("Table created successfully at: {}", tablePathUri);
      } else {
        throw new RuntimeException("Table creation failed");
      }
    } catch (Exception e) {
      String errorMsg = String.format("Failed to create Delta table at '%s'", tablePath);
      throw new IllegalArgumentException(errorMsg, e);
    }
  }

  public static String substituteSchemeForS3(String tablePath) {
    return tablePath.replace("s3://", "s3a://");
  }

  public static Engine getEngine(URI tablePathUri, TemporaryCredentials temporaryCredentials) {
    return DefaultEngine.create(getHDFSConfiguration(tablePathUri, temporaryCredentials));
  }

  public static FileSystem getFileSystem(URI tablePathURI, Configuration conf) {
    try {
      return FileSystem.get(tablePathURI, conf);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to get file system", e);
    }
  }

  public static Configuration getHDFSConfiguration(
      URI tablePathUri, TemporaryCredentials temporaryCredentials) {
    Configuration conf = new Configuration();
    String scheme = tablePathUri.getScheme();
    if (scheme == null) {
      throw new IllegalArgumentException("URI scheme is missing");
    }
    if (scheme.equals(Constants.URI_SCHEME_S3)) {
      AwsCredentials awsTempCredentials = temporaryCredentials.getAwsTempCredentials();
      if (awsTempCredentials == null) {
        throw new IllegalArgumentException("AWS temporary credentials are missing");
      }
      conf.set("fs.s3a.access.key", awsTempCredentials.getAccessKeyId());
      conf.set("fs.s3a.secret.key", awsTempCredentials.getSecretAccessKey());
      conf.set("fs.s3a.session.token", awsTempCredentials.getSessionToken());
      conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      conf.set("fs.s3a.path.style.access", "true");
    } else if (scheme.equals(Constants.URI_SCHEME_FILE)) {
      conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    } else {
      throw new IllegalArgumentException("Unsupported URI scheme: " + scheme);
    }
    return conf;
  }

  public static String readDeltaTable(
      String tablePath, TemporaryCredentials temporaryCredentials, int maxResults) {
    Engine engine = getEngine(URI.create(tablePath), temporaryCredentials);
    try {
      Table table = Table.forPath(engine, substituteSchemeForS3(tablePath));
      Snapshot snapshot = table.getLatestSnapshot(engine);
      StructType readSchema = snapshot.getSchema();
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
      throw new IllegalArgumentException("Failed to read Delta table", e);
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
