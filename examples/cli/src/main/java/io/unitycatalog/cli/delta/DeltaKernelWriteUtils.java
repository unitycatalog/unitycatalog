package io.unitycatalog.cli.delta;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.*;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.unitycatalog.cli.UnityCatalogCli;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.ColumnInfo;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Helper class to write a sample Delta table. Generates random data conforming to the given schema
 * and writes it to the given table path. For adding more data types, add the corresponding methods
 * in the ColumnVectorUtil class. Most of the code in this class has been copied from <a
 * href="https://github.com/delta-io/delta/blob/master/kernel/examples/kernel-examples/src/main/java/io/delta/kernel/examples/BaseTableWriter.java">BaseTableWriter.java</a>
 */
public class DeltaKernelWriteUtils {

  static String accessKey = "";
  static String secretKey = "";
  static String sessionToken = "";
  private static final Random random = new Random();

  public static String writeSampleDataToDeltaTable(
      String tablePath, List<ColumnInfo> columns, AwsCredentials tempCredentialResponse) {
    try {
      StructType schema = DeltaKernelUtils.getSchema(columns);
      URI tablePathUri = URI.create(tablePath);
      Engine engine = DeltaKernelUtils.getEngine(tablePathUri, tempCredentialResponse);
      boolean createVsUpdate = true;
      if (tablePathUri.getScheme().equals("file")) {
        createVsUpdate = !(new File(tablePathUri).isDirectory());
      }
      writeSampleDataToExistingDeltaTable(
          engine, DeltaKernelUtils.substituteSchemeForS3(tablePath), schema, createVsUpdate);
      return "Table written to successfully at: " + tablePath;
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to create delta table", e);
    }
  }

  public static void writeSampleDataToExistingDeltaTable(
      Engine engine, String tablePath, StructType tableSchema, boolean createVsUpdate)
      throws IOException {
    // Create a `Table` object with the given destination table path
    Table table = Table.forPath(engine, tablePath);
    // Create a transaction builder to build the transaction
    TransactionBuilder txnBuilder =
        table.createTransactionBuilder(
            engine,
            UnityCatalogCli.class.toString(), /* engineInfo */
            createVsUpdate ? Operation.CREATE_TABLE : Operation.WRITE);
    // Set the schema of the new table on the transaction builder
    // txnBuilder = txnBuilder.withSchema(engine, tableSchema);

    // Set the schema of the new table on the transaction builder
    if (createVsUpdate) {
      txnBuilder = txnBuilder.withSchema(engine, tableSchema);
    }
    // Build the transaction
    Transaction txn = txnBuilder.build(engine);
    // Get the transaction state
    Row txnState = txn.getTransactionState(engine);
    // Generate the sample data for the table that confirms to the table schema
    FilteredColumnarBatch batch1 = generateUnpartitionedDataBatch(tableSchema, 5, 0);
    FilteredColumnarBatch batch2 = generateUnpartitionedDataBatch(tableSchema, 5, 5);
    FilteredColumnarBatch batch3 = generateUnpartitionedDataBatch(tableSchema, 5, 10);
    CloseableIterator<FilteredColumnarBatch> data =
        toCloseableIterator(Arrays.asList(batch1, batch2, batch3).iterator());
    // First transform the logical data to physical data that needs to be written to the Parquet
    // files
    CloseableIterator<FilteredColumnarBatch> physicalData =
        Transaction.transformLogicalData(
            engine,
            txnState,
            data,
            // partition values - as this table is unpartitioned, it should be empty
            Collections.emptyMap());
    // Get the write context
    DataWriteContext writeContext =
        Transaction.getWriteContext(
            engine,
            txnState,
            // partition values - as this table is unpartitioned, it should be empty
            Collections.emptyMap());
    // Now write the physical data to Parquet files
    CloseableIterator<DataFileStatus> dataFiles =
        engine
            .getParquetHandler()
            .writeParquetFiles(
                writeContext.getTargetDirectory(),
                physicalData,
                writeContext.getStatisticsColumns());
    // Now convert the data file status to data actions that needs to be written to the Delta
    // table log
    CloseableIterator<Row> dataActions =
        Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);
    // Create a iterable out of the data actions. If the contents are too big to fit in memory,
    // the connector may choose to write the data actions to a temporary file and return an
    // iterator that reads from the file.
    CloseableIterable<Row> dataActionsIterable = CloseableIterable.inMemoryIterable(dataActions);
    // Commit the transaction.
    TransactionCommitResult commitResult = txn.commit(engine, dataActionsIterable);
    // Check the transaction commit result
    if (commitResult.getVersion() >= 0) {
      System.out.println("Table written to successfully at: " + tablePath);
    } else {
      throw new RuntimeException("Table writing failed");
    }
  }

  /** @return */
  static FilteredColumnarBatch generateUnpartitionedDataBatch(
      StructType tableSchema, int count, int offset) {
    ColumnVector[] vectors = createVectorsFromSchema(tableSchema, count, offset);
    ColumnarBatch batch = new DefaultColumnarBatch(count, tableSchema, vectors);
    return new FilteredColumnarBatch(
        batch, // data
        // Optional selection vector. If want to write only a subset of rows from the batch.
        Optional.empty());
  }

  // Method to create ColumnVectors based on the schema
  @SuppressWarnings("unchecked")
  public static ColumnVector[] createVectorsFromSchema(StructType schema, int count, int offset) {
    ColumnVector[] vectors = new ColumnVector[schema.length()];
    for (int i = 0; i < schema.length(); i++) {
      StructField field = schema.at(i);
      DataType dataType = field.getDataType();
      List<?> values;
      if (dataType == IntegerType.INTEGER) {
        values =
            generateRandomValues(dataType, count, field.getName().equalsIgnoreCase("id"), offset);
        vectors[i] = intVector((List<Integer>) values);
      } else if (dataType == StringType.STRING) {
        values = generateRandomValues(dataType, count, false, offset);
        vectors[i] = stringVector((List<String>) values);
      } else if (dataType == DoubleType.DOUBLE) {
        values = generateRandomValues(dataType, count, false, offset);
        vectors[i] = doubleVector((List<Double>) values);
      } else if (dataType == ByteType.BYTE) {
        values = generateRandomValues(dataType, count, false, offset);
        vectors[i] = byteVector((List<Byte>) values);
      } else if (dataType == LongType.LONG) {
        values = generateRandomValues(dataType, count, false, offset);
        vectors[i] = longVector((List<Long>) values);
      } else if (dataType == FloatType.FLOAT) {
        values = generateRandomValues(dataType, count, false, offset);
        vectors[i] = floatVector((List<Float>) values);
      } else if (dataType == ShortType.SHORT) {
        values = generateRandomValues(dataType, count, false, offset);
        vectors[i] = shortVector((List<Short>) values);
      }
      // Add more types as needed
    }
    return vectors;
  }

  // Utility function to generate random values based on the data type and whether it's an ID
  @SuppressWarnings("unchecked")
  public static <T> List<T> generateRandomValues(
      DataType dataType, int count, boolean isId, int offset) {
    List<T> values = new ArrayList<>(count);
    if (isId && dataType == IntegerType.INTEGER) {
      // Generate monotonically increasing IDs
      for (int i = 0; i < count; i++) {
        values.add((T) Integer.valueOf(i + 1 + offset)); // IDs starting from 1
      }
    } else {
      for (int i = 0; i < count; i++) {
        if (dataType == DoubleType.DOUBLE) {
          values.add((T) (Double) (random.nextDouble() * 1000)); // Random double values
        } else if (dataType == StringType.STRING) {
          values.add((T) generateRandomString(10)); // Random string of length 10
        } else if (dataType == IntegerType.INTEGER) {
          values.add((T) (Integer) random.nextInt(1000)); // Random integer values
        } else if (dataType == ByteType.BYTE) {
          values.add((T) (Byte) (byte) random.nextInt(1000)); // Random byte values
        } else if (dataType == LongType.LONG) {
          values.add((T) (Long) (long) random.nextInt(1000)); // Random long values
        } else if (dataType == FloatType.FLOAT) {
          values.add((T) (Float) (float) random.nextInt(1000)); // Random float values
        } else if (dataType == ShortType.SHORT) {
          values.add((T) (Short) (short) random.nextInt(1000)); // Random short values
        }
        // Add more types as needed
      }
    }
    return values;
  }

  // Helper method to generate a random string of a given length
  private static String generateRandomString(int length) {
    String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    return random
        .ints(length, 0, characters.length())
        .mapToObj(i -> String.valueOf(characters.charAt(i)))
        .collect(Collectors.joining());
  }

  static ColumnVector doubleVector(List<Double> data) {
    return ColumnVectorUtils.createVector(data, DoubleType.DOUBLE, data::get);
  }

  static ColumnVector stringVector(List<String> data) {
    return ColumnVectorUtils.createVector(data, StringType.STRING, data::get);
  }

  static ColumnVector intVector(List<Integer> data) {
    return ColumnVectorUtils.createVector(data, IntegerType.INTEGER, data::get);
  }

  static ColumnVector byteVector(List<Byte> data) {
    return ColumnVectorUtils.createVector(data, ByteType.BYTE, data::get);
  }

  static ColumnVector longVector(List<Long> data) {
    return ColumnVectorUtils.createVector(data, LongType.LONG, data::get);
  }

  static ColumnVector floatVector(List<Float> data) {
    return ColumnVectorUtils.createVector(data, FloatType.FLOAT, data::get);
  }

  static ColumnVector shortVector(List<Short> data) {
    return ColumnVectorUtils.createVector(data, ShortType.SHORT, data::get);
  }

  static ColumnVector stringSingleValueVector(String value, int size) {
    return ColumnVectorUtils.createSingleValueVector(value, size, StringType.STRING);
  }
}
