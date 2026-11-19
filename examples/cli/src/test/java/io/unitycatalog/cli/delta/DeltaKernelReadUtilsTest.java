package io.unitycatalog.cli.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultLongVector;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.Optional;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class DeltaKernelReadUtilsTest {

  private static String readTimestamp(DataType type, long micros) throws IOException {
    ColumnVector vector = new DefaultLongVector(type, 1, Optional.empty(), new long[] {micros});
    StructType schema = new StructType().add("ts", type);
    try (CloseableIterator<Row> rows =
        new DefaultColumnarBatch(1, schema, new ColumnVector[] {vector}).getRows()) {
      return DeltaKernelReadUtils.getValue(rows.next(), 0);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "1704067200123456, 2024-01-01T00:00:00.123456",
    "1704067200000000, 2024-01-01T00:00",
    "1500000, 1970-01-01T00:00:01.500",
    "-1, 1969-12-31T23:59:59.999999",
    "-1500000, 1969-12-31T23:59:58.500",
    "-86399876544, 1969-12-31T00:00:00.123456"
  })
  public void testTimestampValues(long micros, String expected) throws IOException {
    assertThat(readTimestamp(TimestampType.TIMESTAMP, micros)).isEqualTo(expected);
    assertThat(readTimestamp(TimestampNTZType.TIMESTAMP_NTZ, micros)).isEqualTo(expected);
  }
}
