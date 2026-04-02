package io.unitycatalog.server.delta.type;

import com.fasterxml.jackson.annotation.JsonValue;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * A primitive Delta data type represented as a bare string (e.g., "long", "string", "boolean",
 * "date", "timestamp", "binary"). Excludes decimal which has its own type.
 *
 * <p>Serializes to a bare JSON string via {@link JsonValue}, so it can be safely set on
 * DeltaColumn.type.
 */
@Getter
@EqualsAndHashCode
public final class PrimitiveDataType implements DataType {

  private final String name;

  public PrimitiveDataType(String name) {
    this.name = name;
  }

  @JsonValue
  @Override
  public String toString() {
    return name;
  }
}
