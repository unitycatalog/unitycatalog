package io.unitycatalog.cli.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ColumnVectorUtilsTest {

  private static final Map<DataType, BiFunction<ColumnVector, Integer, Object>> GETTERS =
      Map.of(
          ByteType.BYTE, ColumnVector::getByte,
          ShortType.SHORT, ColumnVector::getShort,
          IntegerType.INTEGER, ColumnVector::getInt,
          LongType.LONG, ColumnVector::getLong,
          FloatType.FLOAT, ColumnVector::getFloat,
          DoubleType.DOUBLE, ColumnVector::getDouble,
          StringType.STRING, ColumnVector::getString);

  static Stream<Arguments> typedValues() {
    return Stream.of(
        Arguments.of(ByteType.BYTE, (byte) -128, (byte) 127),
        Arguments.of(ShortType.SHORT, (short) -32768, (short) 32767),
        Arguments.of(IntegerType.INTEGER, -2147483648, 2147483647),
        Arguments.of(LongType.LONG, -9223372036854775808L, 9223372036854775807L),
        Arguments.of(FloatType.FLOAT, -1.5f, 3.25f),
        Arguments.of(DoubleType.DOUBLE, -1.5d, 3.25d),
        Arguments.of(StringType.STRING, "a", "b"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("typedValues")
  public void testCreateVectorReadsValuesOfItsTypeOnly(DataType type, Object first, Object last) {
    List<Object> data = Arrays.asList(first, null, last);
    ColumnVector vector = ColumnVectorUtils.createVector(data, type, data::get);

    assertThat(vector.getDataType()).isEqualTo(type);
    assertThat(vector.getSize()).isEqualTo(3);
    assertThat(vector.isNullAt(0)).isFalse();
    assertThat(vector.isNullAt(1)).isTrue();
    assertThat(vector.isNullAt(2)).isFalse();
    assertThat(GETTERS.get(type).apply(vector, 0)).isEqualTo(first);
    assertThat(GETTERS.get(type).apply(vector, 2)).isEqualTo(last);
    GETTERS.entrySet().stream()
        .filter(entry -> !entry.getKey().equals(type))
        .forEach(
            entry ->
                assertThatThrownBy(() -> entry.getValue().apply(vector, 0))
                    .as("%s getter on a %s vector", entry.getKey(), type)
                    .isInstanceOf(UnsupportedOperationException.class));
  }
}
