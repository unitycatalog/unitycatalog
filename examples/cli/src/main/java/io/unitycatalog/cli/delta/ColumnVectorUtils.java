package io.unitycatalog.cli.delta;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import java.util.List;
import java.util.function.Function;

/**
 * Utility class to create ColumnVector objects for testing purposes. The input data is a list of
 * values, the corresponding delta data type and a getter function to fetch the value at a given
 * index. We can also create a single repeated value ColumnVector of a given size. The code has
 * evolved from examples provided in <a
 * href="https://github.com/delta-io/delta/blob/master/kernel/examples/kernel-examples/src/main/java/io/delta/kernel/examples/BaseTableWriter.java">BaseTableWriter.java</a>
 */
public class ColumnVectorUtils {
  public static <T> ColumnVector createVector(
      List<T> data, DataType dataType, Function<Integer, T> getter) {
    return new ColumnVector() {
      @Override
      public DataType getDataType() {
        return dataType;
      }

      @Override
      public int getSize() {
        return data.size();
      }

      @Override
      public void close() {}

      @Override
      public boolean isNullAt(int rowId) {
        return data.get(rowId) == null;
      }

      @Override
      public double getDouble(int rowId) {
        if (dataType == DoubleType.DOUBLE) {
          return (Double) getter.apply(rowId);
        }
        return ColumnVector.super.getDouble(rowId);
      }

      @Override
      public String getString(int rowId) {
        if (dataType == StringType.STRING) {
          return (String) getter.apply(rowId);
        }
        return ColumnVector.super.getString(rowId);
      }

      @Override
      public int getInt(int rowId) {
        if (dataType == IntegerType.INTEGER) {
          return (Integer) getter.apply(rowId);
        }
        return ColumnVector.super.getInt(rowId);
      }
    };
  }

  public static <T> ColumnVector createSingleValueVector(T value, int size, DataType dataType) {
    return new ColumnVector() {
      @Override
      public DataType getDataType() {
        return dataType;
      }

      @Override
      public int getSize() {
        return size;
      }

      @Override
      public void close() {
        // Implement if needed
      }

      @Override
      public boolean isNullAt(int rowId) {
        return value == null;
      }

      @Override
      public double getDouble(int rowId) {
        if (dataType == DoubleType.DOUBLE) {
          return (Double) value;
        }
        return ColumnVector.super.getDouble(rowId);
      }

      @Override
      public String getString(int rowId) {
        if (dataType == StringType.STRING) {
          return (String) value;
        }
        return ColumnVector.super.getString(rowId);
      }

      @Override
      public int getInt(int rowId) {
        if (dataType == IntegerType.INTEGER) {
          return (Integer) value;
        }
        return ColumnVector.super.getInt(rowId);
      }
      // Implement other methods similarly based on dataType if needed
    };
  }
}
