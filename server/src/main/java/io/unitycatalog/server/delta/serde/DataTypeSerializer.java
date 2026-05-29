package io.unitycatalog.server.delta.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.unitycatalog.server.delta.model.DataType;
import io.unitycatalog.server.delta.model.DecimalType;
import io.unitycatalog.server.delta.model.PrimitiveType;
import java.io.IOException;

/**
 * Custom serializer for DataType that writes primitives and decimals as bare strings, and
 * delegates complex types to the default Jackson serializer.
 */
public class DataTypeSerializer extends StdSerializer<DataType> {

  private final JsonSerializer<Object> defaultSerializer;

  public DataTypeSerializer(JsonSerializer<Object> defaultSerializer) {
    super(DataType.class);
    this.defaultSerializer = defaultSerializer;
  }

  private static String toTypeString(DataType value) {
    if (value instanceof DecimalType dt) {
      return "decimal(" + dt.getPrecision() + "," + dt.getScale() + ")";
    }
    if (value instanceof PrimitiveType) {
      return value.getType();
    }
    return null;
  }

  @Override
  public void serialize(DataType value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    String s = toTypeString(value);
    if (s != null) {
      gen.writeString(s);
    } else {
      defaultSerializer.serialize(value, gen, provider);
    }
  }

  @Override
  public void serializeWithType(
      DataType value, JsonGenerator gen, SerializerProvider provider, TypeSerializer typeSer)
      throws IOException {
    String s = toTypeString(value);
    if (s != null) {
      gen.writeString(s);
    } else {
      defaultSerializer.serializeWithType(value, gen, provider, typeSer);
    }
  }
}
