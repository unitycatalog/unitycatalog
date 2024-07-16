package io.unitycatalog.cli.delta;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

import de.vandermeer.asciitable.AsciiTable;
import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Utility class to read data from a Delta table. It helps read data from delta file existing at the
 * given table path and prints the contents to the console with the help of the {@link AsciiTable}
 * class. The code has evolved from examples provided in <a
 * href="https://github.com/delta-io/delta/blob/master/kernel/examples/kernel-examples/src/main/java/io/delta/kernel/examples/BaseTableReader.java">BaseTableReader.java</a>
 */
public class DeltaKernelReadUtils {

  protected static List<Row> getData(FilteredColumnarBatch data, int maxRowsToPrint) {
    int printedRowCount = 0;
    List<Row> toReturn = new ArrayList<>();
    try (CloseableIterator<Row> rows = data.getRows()) {
      while (rows.hasNext()) {
        toReturn.add(rows.next());
        printedRowCount++;
        if (printedRowCount == maxRowsToPrint) {
          break;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return toReturn;
  }

  public static String getValue(Row row, int columnOrdinal) {
    DataType dataType = row.getSchema().at(columnOrdinal).getDataType();
    if (row.isNullAt(columnOrdinal)) {
      return null;
    } else if (dataType instanceof BooleanType) {
      return Boolean.toString(row.getBoolean(columnOrdinal));
    } else if (dataType instanceof ByteType) {
      return Byte.toString(row.getByte(columnOrdinal));
    } else if (dataType instanceof ShortType) {
      return Short.toString(row.getShort(columnOrdinal));
    } else if (dataType instanceof IntegerType) {
      return Integer.toString(row.getInt(columnOrdinal));
    } else if (dataType instanceof DateType) {
      // DateType data is stored internally as the number of days since 1970-01-01
      int daysSinceEpochUTC = row.getInt(columnOrdinal);
      return LocalDate.ofEpochDay(daysSinceEpochUTC).toString();
    } else if (dataType instanceof LongType) {
      return Long.toString(row.getLong(columnOrdinal));
    } else if (dataType instanceof TimestampType) {
      // TimestampType data is stored internally as the number of microseconds since epoch
      long microSecsSinceEpochUTC = row.getLong(columnOrdinal);
      LocalDateTime dateTime =
          LocalDateTime.ofEpochSecond(
              microSecsSinceEpochUTC / 1_000_000 /* epochSecond */,
              (int) (1000 * microSecsSinceEpochUTC % 1_000_000) /* nanoOfSecond */,
              ZoneOffset.UTC);
      return dateTime.toString();
    } else if (dataType instanceof FloatType) {
      return Float.toString(row.getFloat(columnOrdinal));
    } else if (dataType instanceof DoubleType) {
      return Double.toString(row.getDouble(columnOrdinal));
    } else if (dataType instanceof StringType) {
      return row.getString(columnOrdinal);
    } else if (dataType instanceof BinaryType) {
      return new String(row.getBinary(columnOrdinal));
    } else if (dataType instanceof DecimalType) {
      return row.getDecimal(columnOrdinal).toString();
    } else {
      throw new UnsupportedOperationException("unsupported data type: " + dataType);
    }
  }

  public static List<Row> readData(Engine engine, StructType readSchema, Scan scan, int maxRowCount)
      throws IOException {
    // printSchema(readSchema);
    List<Row> toReturn = new ArrayList<>();
    Row scanState = scan.getScanState(engine);
    CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(engine);
    int readRecordCount = 0;
    try {
      StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanState);
      outer:
      while (scanFileIter.hasNext()) {
        FilteredColumnarBatch scanFilesBatch = scanFileIter.next();
        try (CloseableIterator<Row> scanFileRows = scanFilesBatch.getRows()) {
          while (scanFileRows.hasNext()) {
            Row scanFileRow = scanFileRows.next();
            FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
            CloseableIterator<ColumnarBatch> physicalDataIter =
                engine
                    .getParquetHandler()
                    .readParquetFiles(
                        singletonCloseableIterator(fileStatus),
                        physicalReadSchema,
                        Optional.empty());
            try (CloseableIterator<FilteredColumnarBatch> transformedData =
                Scan.transformPhysicalData(engine, scanState, scanFileRow, physicalDataIter)) {
              while (transformedData.hasNext()) {
                FilteredColumnarBatch filteredData = transformedData.next();
                List<Row> rows = getData(filteredData, maxRowCount - readRecordCount);
                readRecordCount += rows.size();
                toReturn.addAll(rows);
                if (readRecordCount >= maxRowCount) {
                  break outer;
                }
              }
            }
          }
        }
      }
    } finally {
      scanFileIter.close();
    }
    return toReturn;
  }
}
