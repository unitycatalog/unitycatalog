package io.unitycatalog.cli.delta;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import java.util.List;
import java.util.function.Function;

/**
 * Utility class to create the ColumnVector objects that table write fills with sample data. The
 * input data is a list of values, the corresponding Delta data type and a getter function to fetch
 * the value at a given index. The code has evolved from examples provided in <a
 * href="https://github.com/delta-io/delta/blob/master/kernel/examples/kernel-examples/src/main/java/io/delta/kernel/examples/BaseTableWriter.java">BaseTableWriter.java</a>
 */
public class ColumnVectorUtils {
  /**
   * Creates a ColumnVector over the given values.
   *
   * @param data the values, which give the vector size and which rows are null
   * @param dataType the Delta type of the vector. Only the getter for this type returns a value,
   *     and every other getter throws {@link UnsupportedOperationException}.
   * @param getter returns the value at a row index
   */
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

      @Override
      public byte getByte(int rowId) {
        if (dataType == ByteType.BYTE) {
          return (Byte) getter.apply(rowId);
        }
        return ColumnVector.super.getByte(rowId);
      }

      @Override
      public short getShort(int rowId) {
        if (dataType == ShortType.SHORT) {
          return (Short) getter.apply(rowId);
        }
        return ColumnVector.super.getShort(rowId);
      }

      @Override
      public long getLong(int rowId) {
        if (dataType == LongType.LONG) {
          return (Long) getter.apply(rowId);
        }
        return ColumnVector.super.getLong(rowId);
      }

      @Override
      public float getFloat(int rowId) {
        if (dataType == FloatType.FLOAT) {
          return (Float) getter.apply(rowId);
        }
        return ColumnVector.super.getFloat(rowId);
      }
    };
  }
}
