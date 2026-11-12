package io.unitycatalog.cli.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.StructType;
import io.unitycatalog.cli.delta.DeltaKernelUtils;
import io.unitycatalog.cli.delta.DeltaKernelWriteUtils;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class CliUtilsTest {
  @Test
  public void testParseDecimalColumns() {
    List<ColumnInfo> columns =
        CliUtils.parseColumns(
            "price DECIMAL(12, 3), amount DECIMAL, tax decimal (10,2), qty DECIMAL(7), id INT,"
                + " code CHAR(10)");
    assertThat(columns)
        .extracting(
            ColumnInfo::getName,
            ColumnInfo::getTypeText,
            ColumnInfo::getTypePrecision,
            ColumnInfo::getTypeScale,
            ColumnInfo::getPosition)
        .containsExactly(
            tuple("price", "decimal(12,3)", 12, 3, 0),
            tuple("amount", "decimal(10,0)", 10, 0, 1),
            tuple("tax", "decimal(10,2)", 10, 2, 2),
            tuple("qty", "decimal(7,0)", 7, 0, 3),
            tuple("id", "int", 0, 0, 4),
            tuple("code", "char", null, null, 5));
    assertThat(columns.get(0).getTypeJson()).contains("\"type\":\"decimal(12,3)\"");
    assertThat(columns.get(1).getTypeJson()).contains("\"type\":\"decimal(10,0)\"");
    assertThat(DeltaKernelUtils.getSchema(columns.subList(0, 4)))
        .isEqualTo(
            new StructType()
                .add("price", new DecimalType(12, 3))
                .add("amount", new DecimalType(10, 0))
                .add("tax", new DecimalType(10, 2))
                .add("qty", new DecimalType(7, 0)));
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "price DECIMAL(12,3,4) | DECIMAL takes at most 2 parameters: (12,3,4)",
        "price DECIMAL(12,)    | Invalid DECIMAL parameters: (12,)",
        "price DECIMAL(,)      | Invalid DECIMAL parameters: (,)",
        "price DECIMAL()       | Invalid DECIMAL parameters: ()",
        "price DECIMAL(a,2)    | Invalid DECIMAL parameters: (a,2)",
        "price DECIMAL(5,-1)   | Invalid DECIMAL parameters: (5,-1)",
        "price DECIMAL(0,0)    | DECIMAL precision must be between 1 and 38: 0",
        "price DECIMAL(39,0)   | DECIMAL precision must be between 1 and 38: 39",
        "price DECIMAL(5,6)    | DECIMAL scale must be between 0 and precision 5: 6",
        "price DECIMAL(12,3    | Malformed DECIMAL parameters, expected DECIMAL(p) or DECIMAL(p,s)",
        "price DECIMAL[10,2]   | Malformed DECIMAL parameters, expected DECIMAL(p) or DECIMAL(p,s)",
        "price DECIMAL 10      | Malformed DECIMAL parameters, expected DECIMAL(p) or DECIMAL(p,s)"
      })
  public void testParseInvalidDecimalColumns(String columns, String reason) {
    assertThatThrownBy(() -> CliUtils.parseColumns(columns))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Invalid column definition")
        .cause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(reason);
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {"DECIMAL(38,38) | 38 | 38", "DECIMAL(1,1) | 1 | 1", "DECIMAL(38,0) | 38 | 0"})
  public void testParseDecimalBoundaries(String type, int precision, int scale) {
    ColumnInfo column = CliUtils.parseColumns("price " + type).get(0);
    assertThat(column.getTypePrecision()).isEqualTo(precision);
    assertThat(column.getTypeScale()).isEqualTo(scale);
    assertThat(column.getTypeText()).isEqualTo("decimal(" + precision + "," + scale + ")");
    assertThat(DeltaKernelUtils.getDataType(column)).isEqualTo(new DecimalType(precision, scale));
  }

  @Test
  public void testParseNonDecimalColumnsStaysLenient() {
    assertThat(CliUtils.parseColumns("id INT name STRING, "))
        .extracting(ColumnInfo::getName, ColumnInfo::getTypeText)
        .containsExactly(tuple("id", "int"), tuple("name", "string"));
    assertThat(CliUtils.parseColumns("a INT (5), b INT(), c DECIMAL extra"))
        .extracting(
            ColumnInfo::getName,
            ColumnInfo::getTypeText,
            ColumnInfo::getTypePrecision,
            ColumnInfo::getTypeScale)
        .containsExactly(
            tuple("a", "int", 0, 0), tuple("b", "int", 0, 0), tuple("c", "decimal(10,0)", 10, 0));
  }

  @ParameterizedTest
  @CsvSource({
    "price, 'DECIMAL(10,2)'",
    "flag, BOOLEAN",
    "day, DATE",
    "ts, TIMESTAMP",
    "ts_ntz, TIMESTAMP_NTZ",
    "payload, BINARY"
  })
  public void testWriteRejectsUnsupportedColumnType(String column, String type) {
    StructType schema =
        DeltaKernelUtils.getSchema(CliUtils.parseColumns("id INT, " + column + " " + type));
    assertThatThrownBy(() -> DeltaKernelWriteUtils.createVectorsFromSchema(schema, 1, 0))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "table write does not support column "
                + column
                + " of type "
                + schema.get(column).getDataType());
  }

  @Test
  public void testWriteGeneratesValuesForSupportedColumnTypes() {
    StructType schema =
        DeltaKernelUtils.getSchema(
            CliUtils.parseColumns(
                "id INT, b BYTE, s SHORT, i INT, l LONG, f FLOAT, d DOUBLE, str STRING"));
    int count = 100;
    ColumnVector[] vectors = DeltaKernelWriteUtils.createVectorsFromSchema(schema, count, 10);
    assertThat(vectors).hasSize(schema.length());
    for (int col = 0; col < vectors.length; col++) {
      assertThat(vectors[col].getDataType()).isEqualTo(schema.at(col).getDataType());
      assertThat(vectors[col].getSize()).isEqualTo(count);
    }
    for (int row = 0; row < count; row++) {
      assertThat(vectors[0].getInt(row)).isEqualTo(row + 11);
      assertThat(vectors[1].getByte(row)).isNotNegative();
      assertThat(vectors[2].getShort(row)).isBetween((short) 0, (short) 999);
      assertThat(vectors[3].getInt(row)).isBetween(0, 999);
      assertThat(vectors[4].getLong(row)).isBetween(0L, 999L);
      assertThat(vectors[5].getFloat(row)).isBetween(0f, 999f);
      assertThat(vectors[6].getDouble(row)).isGreaterThanOrEqualTo(0).isLessThan(1000);
      assertThat(vectors[7].getString(row)).matches("[A-Za-z]{10}");
    }
  }

  @ParameterizedTest
  @CsvSource({",", "10,", ",2"})
  public void testDecimalColumnWithoutPrecisionOrScaleFails(Integer precision, Integer scale) {
    ColumnInfo column =
        new ColumnInfo()
            .name("price")
            .typeName(ColumnTypeName.DECIMAL)
            .typePrecision(precision)
            .typeScale(scale);
    assertThatThrownBy(() -> DeltaKernelUtils.getDataType(column))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("DECIMAL column is missing precision or scale: price");
  }
}
