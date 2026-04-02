package io.unitycatalog.server.delta.type;

import com.fasterxml.jackson.annotation.JsonValue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Decimal data type with precision and scale, parsed from "decimal(p,s)".
 *
 * <p>Serializes to a bare JSON string like "decimal(10,2)" via {@link JsonValue}, so it can be
 * safely set on DeltaColumn.type.
 */
@Getter
@EqualsAndHashCode
public final class DecimalDataType implements DataType {

  private static final Pattern PATTERN =
      Pattern.compile("decimal\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)");

  private final int precision;
  private final int scale;

  public DecimalDataType(int precision, int scale) {
    this.precision = precision;
    this.scale = scale;
  }

  /** Parse from string like "decimal(10,2)". Returns null if not a decimal type. */
  public static DecimalDataType parse(String s) {
    Matcher m = PATTERN.matcher(s);
    if (m.matches()) {
      return new DecimalDataType(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
    }
    return null;
  }

  @JsonValue
  @Override
  public String toString() {
    return "decimal(" + precision + "," + scale + ")";
  }
}
